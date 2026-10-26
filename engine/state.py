"""
Redis-backed crawl state for the DarkTrace Web Crawler.

Week 6 compatibility:
    - Bulk scheduler snapshot
    - Due checking with failure backoff
    - In-progress/stale-job detection
    - Redis scheduler locking
    - ETag / Last-Modified incremental crawling
    - Content hash tracking
    - Success / retry / dead-letter counters
    - Source health state

Week 7 Stage 3:
    - Crawl run ID
    - Interrupted crawl detection
    - Recovery state
    - Recovery counters
    - Recovery checkpoints
    - Persistent crawl-start information

One Redis hash per source:

    crawlstate:<source_name>

The source name remains the logical identity of the crawl.
Recovery does NOT create a new source.
"""

import os
import time
import uuid

import redis


REDIS_URL = os.environ.get(
    "CRAWL_STATE_REDIS_URL",
    "redis://localhost:6379/2",
)

MAX_BACKOFF_FACTOR = 8


class CrawlState:

    def __init__(self, redis_url: str = REDIS_URL):
        self.r = redis.Redis.from_url(
            redis_url,
            decode_responses=True,
        )

    # ==================================================================
    # Redis keys
    # ==================================================================

    def _key(self, source_name: str) -> str:
        return f"crawlstate:{source_name}"

    def _lock_key(self, source_name: str) -> str:
        return f"crawl-lock:{source_name}"

    # ==================================================================
    # Week 6 scheduler bulk state
    # ==================================================================

    def snapshot(self, source_names: list[str]) -> dict:
        """
        Fetch all source states in one Redis pipeline.
        Required by Week 6 scheduler.
        """
        if not source_names:
            return {}

        pipe = self.r.pipeline(transaction=False)

        for name in source_names:
            pipe.hgetall(self._key(name))

        return dict(
            zip(
                source_names,
                pipe.execute(),
            )
        )

    @staticmethod
    def in_progress_from(
        state: dict,
        stale_after: float,
    ) -> bool:
        """
        Return True only when the source is genuinely running.

        A stale in_progress flag must not block scheduling forever.
        """
        if state.get("in_progress") != "1":
            return False

        started = state.get("started_ts")

        if started is None:
            return True

        try:
            age = time.time() - float(started)
        except (TypeError, ValueError):
            return True

        return age < stale_after

    @staticmethod
    def due_from(
        state: dict,
        interval_minutes: int,
    ) -> bool:
        """
        Week 6 due calculation with exponential failure backoff.
        """
        last = state.get("last_crawled_ts")

        if last is None:
            return True

        try:
            failures = int(
                state.get(
                    "consecutive_failures",
                    0,
                ) or 0
            )
        except (TypeError, ValueError):
            failures = 0

        factor = min(
            2 ** failures,
            MAX_BACKOFF_FACTOR,
        )

        return (
            time.time() - float(last)
        ) >= interval_minutes * 60 * factor

    # ==================================================================
    # Single-source compatibility helpers
    # ==================================================================

    def is_in_progress(
        self,
        source_name: str,
        stale_after: float = 7200,
    ) -> bool:
        return self.in_progress_from(
            self.get_health(source_name),
            stale_after,
        )

    def is_due(
        self,
        source_name: str,
        interval_minutes: int,
    ) -> bool:
        return self.due_from(
            self.get_health(source_name),
            interval_minutes,
        )

    # ==================================================================
    # Week 6 scheduler locking
    # ==================================================================

    def acquire_lock(
        self,
        source_name: str,
        ttl: int = 3600,
    ) -> bool:
        """
        Atomically claim a source.
        """
        return bool(
            self.r.set(
                self._lock_key(source_name),
                "1",
                nx=True,
                ex=ttl,
            )
        )

    def release_lock(
        self,
        source_name: str,
    ) -> None:
        self.r.delete(
            self._lock_key(source_name)
        )

    # ==================================================================
    # Crawl lifecycle
    # ==================================================================

    def mark_started(
        self,
        source_name: str,
    ) -> str:
        """
        Start a crawl.

        If an existing in_progress state is still present, this start
        is treated as a recovery attempt.

        Returns the new crawl_run_id.
        """
        key = self._key(source_name)

        previous_in_progress = (
            self.r.hget(
                key,
                "in_progress",
            ) == "1"
        )

        previous_recovery_count = self.r.hget(
            key,
            "recovery_count",
        )

        try:
            recovery_count = int(
                previous_recovery_count or 0
            )
        except (TypeError, ValueError):
            recovery_count = 0

        now = time.time()
        run_id = str(uuid.uuid4())

        mapping = {
            "in_progress": "1",
            "started_ts": now,
            "last_started_ts": now,
            "last_status": "in_progress",
            "crawl_run_id": run_id,
        }

        if previous_in_progress:
            recovery_count += 1

            mapping.update(
                {
                    "recovery_pending": "1",
                    "recovery_count": recovery_count,
                    "last_recovery_ts": now,
                }
            )
        else:
            mapping["recovery_pending"] = "0"

            if previous_recovery_count is None:
                mapping["recovery_count"] = recovery_count

        self.r.hset(
            key,
            mapping=mapping,
        )

        return run_id

    def mark_success(
        self,
        source_name: str,
        record: dict | None = None,
    ) -> None:
        """
        Record successful crawl.

        Week 6:
            Stores ETag, Last-Modified and content hash.

        Week 7:
            Clears recovery-pending state and resets failures.
        """
        now = time.time()
        key = self._key(source_name)

        pipe = self.r.pipeline()

        pipe.hset(
            key,
            mapping={
                "last_crawled_ts": now,
                "last_success_ts": now,
                "last_status": "success",
                "consecutive_failures": 0,
                "recovery_pending": "0",
            },
        )

        pipe.hdel(
            key,
            "last_error",
        )

        pipe.hincrby(
            key,
            "total_success",
            1,
        )

        if record:
            validators = {
                "etag": record.get("etag"),
                "last_modified": record.get(
                    "last_modified"
                ),
                "last_content_hash": record.get(
                    "content_hash"
                ),
            }

            for field, value in validators.items():

                if value:
                    pipe.hset(
                        key,
                        field,
                        value,
                    )
                else:
                    pipe.hdel(
                        key,
                        field,
                    )

        pipe.execute()

        self.clear_recovery(source_name)

    def mark_retry(
        self,
        source_name: str,
        reason: str,
    ) -> None:
        """
        Mark temporary whole-job failure.

        The source remains in progress because Celery will retry it.
        """
        self.r.hset(
            self._key(source_name),
            mapping={
                "last_status": "retrying",
                "last_error": reason[:500],
            },
        )

    def mark_dead_letter(
        self,
        source_name: str,
        reason: str,
    ) -> None:
        """
        Permanently failed crawl after retries are exhausted.
        """
        now = time.time()
        key = self._key(source_name)

        pipe = self.r.pipeline()

        pipe.hset(
            key,
            mapping={
                "last_crawled_ts": now,
                "last_status": "dead_letter",
                "last_error": reason[:500],
                "recovery_pending": "0",
            },
        )

        pipe.hincrby(
            key,
            "total_failed",
            1,
        )

        pipe.hincrby(
            key,
            "consecutive_failures",
            1,
        )

        pipe.execute()

    def mark_finished(
        self,
        source_name: str,
    ) -> None:
        """
        End the current worker execution.

        Lock release remains the caller's responsibility.
        """
        self.r.hset(
            self._key(source_name),
            "in_progress",
            "0",
        )

    # ==================================================================
    # Incremental crawling
    # ==================================================================

    def get_validators(
        self,
        source_name: str,
    ) -> dict:
        """
        Return validators from the last successful crawl.

        Required by Week 6 conditional-request behavior.
        """
        values = self.r.hmget(
            self._key(source_name),
            "etag",
            "last_modified",
            "last_content_hash",
        )

        etag, last_modified, content_hash = values

        if not content_hash:
            return {}

        if not (
            etag
            or last_modified
        ):
            return {}

        return {
            "etag": etag,
            "last_modified": last_modified,
            "prev_hash": content_hash,
        }

    # ==================================================================
    # Week 7 recovery
    # ==================================================================

    def needs_recovery(
        self,
        source_name: str,
    ) -> bool:
        """
        True when the source has a crawl that was left in progress.
        """
        return (
            self.r.hget(
                self._key(source_name),
                "in_progress",
            )
            == "1"
        )

    def mark_recovery_pending(
        self,
        source_name: str,
        reason: str = "crawl interrupted",
    ) -> None:
        now = time.time()

        self.r.hset(
            self._key(source_name),
            mapping={
                "in_progress": "1",
                "last_status": "recovery_pending",
                "recovery_pending": "1",
                "last_recovery_ts": now,
                "last_error": reason[:500],
            },
        )

    def mark_checkpoint(
        self,
        source_name: str,
        checkpoint: str | None = None,
    ) -> None:
        mapping = {
            "last_checkpoint_ts": time.time(),
        }

        if checkpoint is not None:
            mapping["last_checkpoint"] = str(
                checkpoint
            )[:1000]

        self.r.hset(
            self._key(source_name),
            mapping=mapping,
        )

    def clear_recovery(
        self,
        source_name: str,
    ) -> None:
        key = self._key(source_name)

        self.r.hset(
            key,
            "recovery_pending",
            "0",
        )

        self.r.hdel(
            key,
            "last_checkpoint",
            "last_recovery_ts",
        )

    def get_recovery_state(
        self,
        source_name: str,
    ) -> dict:
        data = self.get_health(
            source_name
        )

        return {
            "source_name": source_name,
            "in_progress": data.get(
                "in_progress",
                "0",
            ),
            "last_status": data.get(
                "last_status"
            ),
            "crawl_run_id": data.get(
                "crawl_run_id"
            ),
            "last_started_ts": data.get(
                "last_started_ts"
            ),
            "last_checkpoint_ts": data.get(
                "last_checkpoint_ts"
            ),
            "last_checkpoint": data.get(
                "last_checkpoint"
            ),
            "recovery_pending": data.get(
                "recovery_pending",
                "0",
            ),
            "recovery_count": data.get(
                "recovery_count",
                "0",
            ),
            "last_recovery_ts": data.get(
                "last_recovery_ts"
            ),
            "last_error": data.get(
                "last_error"
            ),
        }

    def get_crawl_run_id(
        self,
        source_name: str,
    ) -> str | None:
        return self.r.hget(
            self._key(source_name),
            "crawl_run_id",
        )

    # ==================================================================
    # Health / inspection
    # ==================================================================

    def get_health(
        self,
        source_name: str,
    ) -> dict:
        return self.r.hgetall(
            self._key(source_name)
        )

    def get_health_status(
        self,
        source_name: str,
    ) -> str:
        """
        Simple compatibility helper for health tests.
        """
        data = self.get_health(
            source_name
        )

        status = data.get(
            "last_status"
        )

        if status == "success":
            return "recovered"

        if status == "dead_letter":
            return "unavailable"

        if status in {
            "retrying",
            "failed",
        }:
            return "degraded"

        return status or "unknown"

    def all_states(
        self,
        source_names: list[str],
    ) -> dict:
        return self.snapshot(
            source_names
        )