"""
Runs ONE crawl job as a clean Scrapy subprocess and reports the outcome.

Week 7:
- Supports incremental crawling using ETag / Last-Modified / content hash.
- Clearly identifies whether content is unchanged or needs processing.
- Keeps Week 6 retry, timeout, and permanent-failure behavior.
"""

import json
import os
import subprocess
import sys
import uuid
from urllib.parse import urlparse


PROJECT_ROOT = os.path.dirname(
    os.path.dirname(os.path.abspath(__file__))
)

RESULT_LOG = os.path.join(
    PROJECT_ROOT,
    "crawl_output",
    "crawl_results.jsonl"
)


# ------------------------------------------------------------------ #
# Job retry configuration
# ------------------------------------------------------------------ #

MAX_JOB_RETRIES = 3

JOB_RETRY_BASE_DELAY = int(
    os.environ.get(
        "JOB_RETRY_BASE_DELAY",
        10
    )
)


# ------------------------------------------------------------------ #
# Scrapy retry configuration
# ------------------------------------------------------------------ #

SCRAPY_RETRY_TIMES = 3

SCRAPY_MAX_DOWNLOAD_TIMEOUT = 60


# These transient 4xx errors are worth retrying.
TRANSIENT_4XX = {
    408,
    425,
    429,
}


# ------------------------------------------------------------------ #
# Exceptions
# ------------------------------------------------------------------ #

class CrawlError(RuntimeError):
    """Retryable crawl failure."""


class PermanentCrawlError(CrawlError):
    """Failure where retrying will not help."""


# ------------------------------------------------------------------ #
# Network helpers
# ------------------------------------------------------------------ #

def is_darknet(url: str) -> bool:
    host = (
        urlparse(url).hostname
        or ""
    ).lower()

    return (
        host.endswith(".onion")
        or host.endswith(".i2p")
    )


def request_timeout_for(source: dict) -> int:
    """
    Calculate the Scrapy request timeout while leaving enough time
    for internal retries and startup.
    """

    budget = (
        source.get("timeout_seconds", 120)
        - 20
    )

    per_attempt = (
        budget
        // (SCRAPY_RETRY_TIMES + 1)
    )

    floor = (
        30
        if is_darknet(source["url"])
        else 10
    )

    return int(
        max(
            floor,
            min(
                SCRAPY_MAX_DOWNLOAD_TIMEOUT,
                per_attempt
            )
        )
    )


def lock_ttl_for(source: dict) -> int:
    """
    Calculate the Redis scheduler lock lifetime so the lock survives
    the complete crawl job and its retries.
    """

    timeout = source.get(
        "timeout_seconds",
        120
    )

    backoff = sum(
        JOB_RETRY_BASE_DELAY * (2 ** i)
        for i in range(MAX_JOB_RETRIES)
    )

    return int(
        (MAX_JOB_RETRIES + 1)
        * (timeout + 10)
        + backoff
        + 300
    )


# ------------------------------------------------------------------ #
# Crawl-result log helpers
# ------------------------------------------------------------------ #

def _log_size() -> int:
    try:
        return os.path.getsize(
            RESULT_LOG
        )
    except OSError:
        return 0


def _find_record(
    job_id: str,
    offset: int
) -> dict | None:
    """
    Read only the part of crawl_results.jsonl written by this job.
    """

    try:
        with open(
            RESULT_LOG,
            "rb"
        ) as f:
            f.seek(offset)
            data = f.read()

    except OSError:
        return None

    needle = job_id.encode()

    found = None

    for line in data.splitlines():

        if needle not in line:
            continue

        try:
            record = json.loads(
                line
            )

        except json.JSONDecodeError:
            continue

        if record.get("job_id") == job_id:
            found = record

    return found


# ------------------------------------------------------------------ #
# Week 7 incremental classification
# ------------------------------------------------------------------ #

def _classify_incremental_result(
    record: dict,
    validators: dict | None
) -> dict:
    """
    Classify the crawl result for Week 7 incremental crawling.

    initial:
        No previous validator/hash existed.

    unchanged:
        Server returned 304 / unchanged content.

    changed:
        Existing content was fetched and its content changed.

    This classification does not bypass access controls. It only avoids
    reprocessing content that is already known to be unchanged.
    """

    previous = validators or {}

    had_previous_state = bool(
        previous.get("etag")
        or previous.get("last_modified")
        or previous.get("prev_hash")
    )

    if record.get("unchanged"):
        action = "skip_unchanged"
        reason = (
            "server reported 304 Not Modified"
        )

    elif not had_previous_state:
        action = "process_new"
        reason = (
            "no previous incremental state exists"
        )

    else:
        previous_hash = previous.get(
            "prev_hash"
        )

        current_hash = record.get(
            "content_hash"
        )

        if (
            previous_hash
            and current_hash
            and previous_hash == current_hash
        ):
            action = "skip_unchanged"
            reason = (
                "content hash is unchanged"
            )

        else:
            action = "process_changed"
            reason = (
                "new content or changed content detected"
            )

    result = dict(record)

    result["incremental"] = True
    result["incremental_action"] = action
    result["incremental_reason"] = reason

    return result


# ------------------------------------------------------------------ #
# Main crawl
# ------------------------------------------------------------------ #

def run_crawl(
    source: dict,
    validators: dict | None = None
) -> dict:
    """
    Run one crawl and return its result.

    Week 7 incremental behavior:
      previous state
            |
            v
      conditional request
            |
       +----+----+
       |         |
      304      changed/new
       |         |
       v         v
      skip     process

    Raises:
        CrawlError
        PermanentCrawlError
    """

    name = source["name"]

    timeout = source.get(
        "timeout_seconds",
        120
    )

    job_id = uuid.uuid4().hex

    # -------------------------------------------------------------- #
    # Scrapy command
    # -------------------------------------------------------------- #

    cmd = [
        sys.executable,
        "-m",
        "scrapy",
        "crawl",
        "single_source",

        "-a",
        f"url={source['url']}",

        "-a",
        f"source_name={name}",

        "-a",
        f"source_type={source['source_type']}",

        "-a",
        f"job_id={job_id}",

        "-s",
        f"DOWNLOAD_TIMEOUT={request_timeout_for(source)}",
    ]

    # Pass previous incremental validators.
    for arg, field in (
        ("etag", "etag"),
        ("last_modified", "last_modified"),
        ("prev_hash", "prev_hash"),
    ):

        value = (
            validators or {}
        ).get(field)

        if value:
            cmd += [
                "-a",
                f"{arg}={value}"
            ]

    env = dict(
        os.environ,
        PYTHONUTF8="1",
        PYTHONIOENCODING="utf-8"
    )

    offset = _log_size()

    # -------------------------------------------------------------- #
    # Run Scrapy
    # -------------------------------------------------------------- #

    try:

        result = subprocess.run(
            cmd,
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=timeout,
            env=env,
        )

    except subprocess.TimeoutExpired as exc:

        raise CrawlError(
            f"crawl for '{name}' exceeded "
            f"its {timeout}s timeout"
        ) from exc

    # -------------------------------------------------------------- #
    # Process exit status
    # -------------------------------------------------------------- #

    if result.returncode != 0:

        raise CrawlError(
            f"scrapy crawl exited "
            f"{result.returncode} for source "
            f"'{name}': "
            f"{result.stderr[-1500:]}"
        )

    # -------------------------------------------------------------- #
    # Read crawler result
    # -------------------------------------------------------------- #

    record = _find_record(
        job_id,
        offset
    )

    if record is None:

        raise CrawlError(
            f"scrapy crawl for '{name}' "
            f"exited cleanly but produced no "
            f"crawl-result record — treating "
            f"as a failed fetch"
        )

    # -------------------------------------------------------------- #
    # Failed crawl
    # -------------------------------------------------------------- #

    if record.get("status") != "success":

        message = (
            f"fetch failed for '{name}': "
            f"{record.get('error') or 'unknown error'}"
        )

        http_status = record.get(
            "http_status"
        )

        if (
            http_status
            and 400 <= http_status < 500
            and http_status not in TRANSIENT_4XX
        ):

            raise PermanentCrawlError(
                message
            )

        raise CrawlError(
            message
        )

    # -------------------------------------------------------------- #
    # Week 7 incremental classification
    # -------------------------------------------------------------- #

    record = _classify_incremental_result(
        record,
        validators
    )

    return record