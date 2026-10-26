"""
Worker task: fetch one source.

Week 6 functionality preserved:
    - Clean Scrapy subprocess through engine.runner
    - ETag / Last-Modified incremental crawling
    - Whole-job Celery retries
    - Exponential retry delays
    - Permanent failure handling
    - Dead-letter JSONL logging
    - Redis source state
    - Scheduler lock release
    - 304 / unchanged reporting

Week 7 Stage 3:
    - Interrupted crawl detection
    - Recovery-aware crawl starts
    - Same source identity is preserved
    - Celery retries are NOT incorrectly counted as crash recovery
    - Crash/redelivery can resume using the existing source state

Important recovery distinction:

    Celery retry:
        self.request.retries > 0
        The same logical task is still active.
        Do NOT create another recovery attempt.

    Worker/process interruption:
        Celery redelivers the task with retries == 0.
        Redis still contains in_progress=1.
        This is treated as a recovery.
"""

import json
import logging
import os
import time

from engine.celery_app import app
from engine.runner import (
    JOB_RETRY_BASE_DELAY,
    MAX_JOB_RETRIES,
    PROJECT_ROOT,
    PermanentCrawlError,
    run_crawl,
)
from engine.state import CrawlState


logger = logging.getLogger(__name__)

state = CrawlState()

DEAD_LETTER_LOG = os.path.join(
    PROJECT_ROOT,
    "crawl_output",
    "dead_letter.jsonl",
)


@app.task(
    bind=True,
    max_retries=MAX_JOB_RETRIES,
    ignore_result=False,
)
def crawl_source(self, source: dict):

    name = source["name"]

    # ==============================================================
    # WEEK 7 RECOVERY-AWARE START
    # ==============================================================

    celery_retry_attempt = self.request.retries

    # A fresh Celery task with an existing in_progress state means
    # the previous worker/process may have been interrupted.
    #
    # A Celery retry (retries > 0) is NOT considered a new recovery.
    # It is the same logical job continuing after a temporary failure.
    recovering = (
        celery_retry_attempt == 0
        and state.needs_recovery(name)
    )

    run_id = state.mark_started(name)

    if recovering:

        logger.warning(
            "RECOVERY: resuming interrupted crawl for source "
            "'%s' (crawl_run_id=%s)",
            name,
            run_id,
        )

        state.mark_recovery_pending(
            name,
            "worker/process interruption; crawl redelivered",
        )

    else:

        logger.info(
            "Crawl started for source '%s' "
            "(crawl_run_id=%s, celery_retry=%d)",
            name,
            run_id,
            celery_retry_attempt,
        )

    try:

        # ==========================================================
        # INCREMENTAL VALIDATORS
        # ==========================================================

        # Validators from the last successful crawl:
        #
        #   ETag
        #   Last-Modified
        #   previous content hash
        #
        # These are preserved across recovery.
        #
        # Therefore a recovered crawl does NOT behave like a new
        # source and does not lose Week 6 incremental crawling.
        validators = state.get_validators(name)

        record = run_crawl(
            source,
            validators,
        )

        # ==========================================================
        # SUCCESS
        # ==========================================================

        state.mark_success(
            name,
            record,
        )

        state.mark_finished(
            name,
        )

        state.release_lock(
            name,
        )

        unchanged = bool(
            record.get("unchanged")
        )

        logger.info(
            "Crawl succeeded for source '%s'%s",
            name,
            " (unchanged, 304)" if unchanged else "",
        )

        return {
            "source": name,
            "status": "success",
            "unchanged": unchanged,
            "http_status": record.get(
                "http_status"
            ),
            "crawl_run_id": run_id,
            "recovered": recovering,
        }

    # ==============================================================
    # PERMANENT FAILURE
    # ==============================================================

    except PermanentCrawlError as exc:

        logger.error(
            "Permanent failure for '%s' "
            "(not retrying): %s",
            name,
            exc,
        )

        return _dead_letter(
            source,
            exc,
            crawl_run_id=run_id,
            recovered=recovering,
        )

    # ==============================================================
    # TEMPORARY / UNKNOWN FAILURE
    # ==============================================================

    except Exception as exc:

        attempt = self.request.retries

        # ----------------------------------------------------------
        # CELERY RETRY
        # ----------------------------------------------------------

        if attempt < MAX_JOB_RETRIES:

            delay = (
                JOB_RETRY_BASE_DELAY
                * (2 ** attempt)
            )

            state.mark_retry(
                name,
                str(exc),
            )

            logger.warning(
                "Crawl job failed for '%s' "
                "(attempt %d/%d), "
                "retrying in %ds: %s",
                name,
                attempt + 1,
                MAX_JOB_RETRIES,
                delay,
                exc,
            )

            # IMPORTANT:
            #
            # Do NOT mark the crawl finished here.
            # Do NOT release the scheduler lock here.
            #
            # Celery is going to retry the same logical job.
            #
            # This also prevents the retry from being incorrectly
            # interpreted as a worker-crash recovery.
            raise self.retry(
                exc=exc,
                countdown=delay,
            )

        # ----------------------------------------------------------
        # RETRIES EXHAUSTED
        # ----------------------------------------------------------

        return _dead_letter(
            source,
            exc,
            crawl_run_id=run_id,
            recovered=recovering,
        )


def _dead_letter(
    source: dict,
    exc: Exception,
    crawl_run_id: str | None = None,
    recovered: bool = False,
) -> dict:

    name = source["name"]

    # ==============================================================
    # REDIS DEAD-LETTER STATE
    # ==============================================================

    state.mark_dead_letter(
        name,
        str(exc),
    )

    # ==============================================================
    # JSONL DEAD-LETTER RECORD
    # ==============================================================

    _append_dead_letter(
        source,
        str(exc),
        crawl_run_id=crawl_run_id,
        recovered=recovered,
    )

    # ==============================================================
    # FINISH WORKER EXECUTION
    # ==============================================================

    state.mark_finished(
        name,
    )

    state.release_lock(
        name,
    )

    logger.error(
        "DEAD-LETTER: source '%s' failed: %s",
        name,
        exc,
    )

    return {
        "source": name,
        "status": "dead_letter",
        "error": str(exc),
        "crawl_run_id": crawl_run_id,
        "recovered": recovered,
    }


def _append_dead_letter(
    source: dict,
    reason: str,
    crawl_run_id: str | None = None,
    recovered: bool = False,
) -> None:

    os.makedirs(
        os.path.dirname(
            DEAD_LETTER_LOG
        ),
        exist_ok=True,
    )

    record = {
        "source_name": source["name"],
        "url": source["url"],
        "source_type": source["source_type"],
        "reason": reason[:1500],
        "timestamp": time.time(),
    }

    # Week 7 recovery metadata.
    #
    # These fields are additive; the existing Week 6 fields remain
    # unchanged.
    if crawl_run_id is not None:
        record["crawl_run_id"] = crawl_run_id

    record["recovered"] = recovered

    with open(
        DEAD_LETTER_LOG,
        "a",
        encoding="utf-8",
    ) as f:

        f.write(
            json.dumps(
                record,
                ensure_ascii=False,
            )
            + "\n"
        )