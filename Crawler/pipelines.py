"""
Pipelines, in run order:

1. ContentHashPipeline       - computes sha256 of raw content.
2. RawEvidenceStoragePipeline - stores original HTML and extracted text.
3. CrawlResultLogPipeline    - appends metadata to the JSONL audit log.
"""

import hashlib
import json
import os
import sys
import time
from datetime import datetime, timezone


class ContentHashPipeline:
    def process_item(self, item, spider=None):
        if item.get("unchanged"):
            return item

        raw_html = item.get("raw_html") or ""

        if item.get("status") == "success" and raw_html:
            item["content_hash"] = hashlib.sha256(
                raw_html.encode("utf-8", "ignore")
            ).hexdigest()
        else:
            item["content_hash"] = None

        return item


class RawEvidenceStoragePipeline:
    def __init__(self, raw_dir):
        self.raw_dir = raw_dir

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            raw_dir=crawler.settings.get(
                "RAW_STORAGE_DIR",
                "crawl_output/raw",
            )
        )

    def open_spider(self, spider=None):
        os.makedirs(self.raw_dir, exist_ok=True)

    def _path_for(self, content_hash):
        return os.path.join(
            self.raw_dir,
            f"{content_hash}.html",
        )

    def _text_path_for(self, content_hash):
        return os.path.join(
            self.raw_dir,
            f"{content_hash}.txt",
        )

    @staticmethod
    def _write_atomic(path, content):
        tmp_path = f"{path}.{os.getpid()}.tmp"

        with open(
            tmp_path,
            "w",
            encoding="utf-8",
        ) as f:
            f.write(content)

        os.replace(tmp_path, path)

    def process_item(self, item, spider=None):
        content_hash = item.get("content_hash")

        item["duplicate_of_existing"] = False
        item["evidence_path"] = None
        item["text_path"] = None

        if not content_hash:
            return item

        file_path = self._path_for(content_hash)
        text_path = self._text_path_for(content_hash)

        if item.get("unchanged"):
            if os.path.exists(file_path):
                item["evidence_path"] = file_path

            if os.path.exists(text_path):
                item["text_path"] = text_path

            return item

        if os.path.exists(file_path):
            item["duplicate_of_existing"] = True
        else:
            self._write_atomic(
                file_path,
                item.get("raw_html") or "",
            )

        if not os.path.exists(text_path):
            self._write_atomic(
                text_path,
                item.get("text_content") or "",
            )

        item["evidence_path"] = file_path
        item["text_path"] = text_path

        return item


class CrawlResultLogPipeline:
    def __init__(self, log_path):
        self.log_path = log_path
        self._file = None
        self._lock_file = None
        self._lock_path = None

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            log_path=crawler.settings.get(
                "RESULT_LOG_PATH",
                "crawl_output/crawl_results.jsonl",
            )
        )

    def open_spider(self, spider=None):
        os.makedirs(
            os.path.dirname(self.log_path) or ".",
            exist_ok=True,
        )

        self._lock_path = f"{self.log_path}.lock"

        self._file = open(
            self.log_path,
            "a",
            encoding="utf-8",
            newline="\n",
        )

        self._lock_file = open(
            self._lock_path,
            "a+b",
        )

        # Windows msvcrt.locking() requires at least one byte
        # in the file before locking a byte range.
        self._lock_file.seek(0, os.SEEK_END)

        if self._lock_file.tell() == 0:
            self._lock_file.write(b"\0")
            self._lock_file.flush()

    def close_spider(self, spider=None):
        if self._file:
            self._file.close()
            self._file = None

        if self._lock_file:
            self._lock_file.close()
            self._lock_file = None

    def _acquire_lock(self):
        if sys.platform == "win32":
            import msvcrt

            self._lock_file.seek(0)

            while True:
                try:
                    msvcrt.locking(
                        self._lock_file.fileno(),
                        msvcrt.LK_NBLCK,
                        1,
                    )
                    return
                except OSError:
                    time.sleep(0.05)

        else:
            import fcntl

            fcntl.flock(
                self._lock_file.fileno(),
                fcntl.LOCK_EX,
            )

    def _release_lock(self):
        if sys.platform == "win32":
            import msvcrt

            self._lock_file.seek(0)

            try:
                msvcrt.locking(
                    self._lock_file.fileno(),
                    msvcrt.LK_UNLCK,
                    1,
                )
            except OSError:
                pass

        else:
            import fcntl

            fcntl.flock(
                self._lock_file.fileno(),
                fcntl.LOCK_UN,
            )

    def process_item(self, item, spider=None):
        record = {
            "job_id": item.get("job_id"),
            "source_name": item.get("source_name"),
            "source_type": item.get("source_type"),
            "url": item.get("url"),

            "is_seed": bool(
                item.get("is_seed")
            ),

            "network": item.get("network"),

            "fetch_timestamp": (
                item.get("fetch_timestamp")
                or datetime.now(
                    timezone.utc
                ).isoformat()
            ),

            "crawler_build": item.get(
                "crawler_build"
            ),

            "status": item.get(
                "status",
                "success",
            ),

            "http_status": item.get(
                "http_status"
            ),

            "attempt": item.get(
                "attempt"
            ),

            "title": item.get(
                "title"
            ),

            "content_hash": item.get(
                "content_hash"
            ),

            "duplicate_of_existing": bool(
                item.get(
                    "duplicate_of_existing"
                )
            ),

            "unchanged": bool(
                item.get("unchanged")
            ),

            "etag": item.get(
                "etag"
            ),

            "last_modified": item.get(
                "last_modified"
            ),

            "evidence_path": item.get(
                "evidence_path"
            ),

            "text_path": item.get(
                "text_path"
            ),

            "error": item.get(
                "error"
            ),
        }

        line = (
            json.dumps(
                record,
                ensure_ascii=False,
            )
            + "\n"
        )

        self._acquire_lock()

        try:
            self._file.write(line)
            self._file.flush()
            os.fsync(
                self._file.fileno()
            )
        finally:
            self._release_lock()

        return item