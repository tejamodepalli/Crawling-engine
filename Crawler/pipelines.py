"""
Pipelines, in run order:

1. ContentHashPipeline       - computes sha256 of raw content (exact-duplicate
                                detection, evidence integrity).
2. RawEvidenceStoragePipeline - writes the untouched original content (and the
                                extracted text, <hash>.txt) to disk first ("Original content is preserved before
                                later processing"), and records whether the
                                same content was already stored.
3. CrawlResultLogPipeline    - appends the metadata record (source, URI,
                                fetch time, crawler build, hash, status) as a
                                JSON Lines audit log, for handoff to the next
                                DarkTrace stage.
"""
import hashlib
import json
import os
from datetime import datetime, timezone


class ContentHashPipeline:
    def process_item(self, item, spider=None):
        if item.get("unchanged"):
            # 304: keep the hash of the previous crawl, there is no new body.
            return item
        raw_html = item.get("raw_html") or ""
        if item.get("status") == "success" and raw_html:
            item["content_hash"] = hashlib.sha256(
                raw_html.encode("utf-8", "ignore")
            ).hexdigest()
        else:
            # Failed fetches have no content: don't hash (or store) an empty string.
            item["content_hash"] = None
        return item


class RawEvidenceStoragePipeline:
    def __init__(self, raw_dir):
        self.raw_dir = raw_dir

    @classmethod
    def from_crawler(cls, crawler):
        return cls(raw_dir=crawler.settings.get("RAW_STORAGE_DIR", "crawl_output/raw"))

    def open_spider(self, spider=None):
        os.makedirs(self.raw_dir, exist_ok=True)

    def _path_for(self, content_hash):
        return os.path.join(self.raw_dir, f"{content_hash}.html")

    def _text_path_for(self, content_hash):
        return os.path.join(self.raw_dir, f"{content_hash}.txt")

    @staticmethod
    def _write_atomic(path, content):
        tmp_path = f"{path}.{os.getpid()}.tmp"
        with open(tmp_path, "w", encoding="utf-8") as f:
            f.write(content)
        os.replace(tmp_path, path)  # atomic: no half-written evidence files

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
            # 304: point at the evidence stored by the earlier crawl, if still there.
            if os.path.exists(file_path):
                item["evidence_path"] = file_path
            if os.path.exists(text_path):
                item["text_path"] = text_path
            return item

        # Duplicate content -> file already exists, skip rewrite (cheap dedup).
        # This works across jobs/processes because it checks the disk, not memory.
        if os.path.exists(file_path):
            item["duplicate_of_existing"] = True
        else:
            self._write_atomic(file_path, item.get("raw_html") or "")

        # Extracted visible text, stored beside the raw HTML so the next
        # stage does not have to re-parse it.
        if not os.path.exists(text_path):
            self._write_atomic(text_path, item.get("text_content") or "")

        item["evidence_path"] = file_path
        item["text_path"] = text_path
        return item


class CrawlResultLogPipeline:
    def __init__(self, log_path):
        self.log_path = log_path
        self._file = None

    @classmethod
    def from_crawler(cls, crawler):
        return cls(log_path=crawler.settings.get("RESULT_LOG_PATH", "crawl_output/crawl_results.jsonl"))

    def open_spider(self, spider=None):
        os.makedirs(os.path.dirname(self.log_path) or ".", exist_ok=True)
        # newline="\n": identical line endings on every OS (no \r\n on Windows).
        self._file = open(self.log_path, "a", encoding="utf-8", newline="\n")

    def close_spider(self, spider=None):
        if self._file:
            self._file.close()

    def process_item(self, item, spider=None):
        record = {
            "job_id": item.get("job_id"),
            "source_name": item.get("source_name"),
            "source_type": item.get("source_type"),
            "url": item.get("url"),
            "network": item.get("network"),
            "fetch_timestamp": item.get("fetch_timestamp") or datetime.now(timezone.utc).isoformat(),
            "crawler_build": item.get("crawler_build"),
            "status": item.get("status", "success"),
            "http_status": item.get("http_status"),
            "attempt": item.get("attempt"),
            "title": item.get("title"),
            "content_hash": item.get("content_hash"),
            "duplicate_of_existing": bool(item.get("duplicate_of_existing")),
            "unchanged": bool(item.get("unchanged")),
            "etag": item.get("etag"),
            "last_modified": item.get("last_modified"),
            "evidence_path": item.get("evidence_path"),
            "text_path": item.get("text_path"),
            "error": item.get("error"),
        }
        self._file.write(json.dumps(record, ensure_ascii=False) + "\n")
        self._file.flush()
        return item
