from datetime import datetime, timezone
from urllib.parse import urljoin, urlparse
import os

import scrapy
from scrapy.http import TextResponse

from Crawler.items import CrawlResultItem


def _header(response, name):
    value = response.headers.get(name)

    if value is None:
        return None

    if isinstance(value, bytes):
        return value.decode("latin-1")

    return str(value)


def _is_text_content(response):
    content_type = (
        (_header(response, "Content-Type") or "")
        .split(";", 1)[0]
        .strip()
        .lower()
    )

    return isinstance(response, TextResponse) and (
        not content_type
        or content_type.startswith("text/")
        or content_type in {
            "application/xhtml+xml",
            "application/xml",
        }
    )


class SingleSourceSpider(scrapy.Spider):
    name = "single_source"

    custom_settings = {
        "CONCURRENT_REQUESTS_PER_DOMAIN": 1,
    }

    def __init__(
        self,
        url=None,
        source_name="source_1",
        source_type="normal",
        job_id=None,
        etag=None,
        last_modified=None,
        prev_hash=None,
        max_pages=5,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)

        if not url:
            raise ValueError(
                "Provide the authorized seed URL: -a url=<...>"
            )

        self.start_urls = [url]

        self.source_name = source_name
        self.source_type = source_type
        self.job_id = job_id or None

        self.etag = etag or None
        self.last_modified = last_modified or None
        self.prev_hash = prev_hash or None

        try:
            self.max_pages = max(1, int(max_pages))
        except (TypeError, ValueError):
            self.max_pages = 5

        self.pages_seen = 0
        self.visited_urls = set()

        parsed = urlparse(url)

        self.allowed_domain = parsed.netloc.lower()
        self.allowed_scheme = parsed.scheme.lower()

    async def start(self):
        for url in self.start_urls:
            yield self._build_request(
                url,
                conditional=True,
                is_seed=True,
            )

    def _build_request(
        self,
        url,
        conditional=False,
        is_seed=False,
        dont_filter=False,
    ):
        headers = {}

        # Conditional headers are used only for the original
        # seed request. This preserves incremental crawling behavior.
        if conditional:
            if self.etag:
                headers["If-None-Match"] = self.etag

            if self.last_modified:
                headers["If-Modified-Since"] = self.last_modified

        return scrapy.Request(
            url,
            callback=self.parse,
            errback=self.handle_failure,
            headers=headers,
            dont_filter=dont_filter,
            meta={
                "handle_httpstatus_list": [304],
                "is_seed": is_seed,
                "conditional_request": conditional,
            },
        )

    def _is_allowed_link(self, url):
        parsed = urlparse(url)

        if parsed.scheme.lower() not in {
            "http",
            "https",
        }:
            return False

        if parsed.netloc.lower() != self.allowed_domain:
            return False

        return True

    def _normalize_url(self, url):
        parsed = urlparse(url)

        return parsed._replace(
            fragment=""
        ).geturl()

    def _base_item(
        self,
        url,
        meta,
        status,
    ):
        item = CrawlResultItem()

        item["source_name"] = self.source_name
        item["source_type"] = self.source_type
        item["job_id"] = self.job_id
        item["url"] = url

        item["network"] = meta.get(
            "network",
            "clearnet",
        )

        item["fetch_timestamp"] = (
            datetime.now(timezone.utc).isoformat()
        )

        item["crawler_build"] = self.settings.get(
            "CRAWLER_BUILD"
        )

        item["status"] = status
        item["attempt"] = (
            meta.get("retry_times", 0) + 1
        )

        item["unchanged"] = False
        item["error"] = None

        item["title"] = ""
        item["raw_html"] = ""
        item["text_content"] = ""

        return item

    def _previous_evidence_exists(self):
        """
        A 304 response is accepted as unchanged only when
        both previous evidence files are available.
        """
        if not self.prev_hash:
            return False

        raw_dir = self.settings.get(
            "RAW_STORAGE_DIR",
            "crawl_output/raw",
        )

        html_path = os.path.join(
            raw_dir,
            f"{self.prev_hash}.html",
        )

        text_path = os.path.join(
            raw_dir,
            f"{self.prev_hash}.txt",
        )

        return (
            os.path.isfile(html_path)
            and os.path.isfile(text_path)
        )

    def parse(self, response):
        url = self._normalize_url(
            response.url
        )

        self.pages_seen += 1
        self.visited_urls.add(url)

        # -------------------------------------------------------------- #
        # 304 incremental response
        # -------------------------------------------------------------- #
        if (
            response.status == 304
            and response.meta.get("is_seed")
        ):
            # Accept 304 as unchanged only when both previous
            # evidence files are still available.
            if self._previous_evidence_exists():
                item = self._base_item(
                    url,
                    response.meta,
                    "success",
                )

                item["http_status"] = response.status
                item["unchanged"] = True
                item["content_hash"] = self.prev_hash
                item["etag"] = self.etag
                item["last_modified"] = self.last_modified

                self.logger.info(
                    "Source '%s' unchanged and previous evidence exists: %s",
                    self.source_name,
                    url,
                )

                yield item
                return

            # Previous evidence is missing.
            # Make one unconditional request to restore the evidence.
            self.logger.warning(
                "Source '%s' returned 304 but previous evidence is "
                "missing. Retrying seed request without conditional "
                "headers: %s",
                self.source_name,
                url,
            )

            yield self._build_request(
                url,
                conditional=False,
                is_seed=True,
                dont_filter=True,
            )
            return

        item = self._base_item(
            url,
            response.meta,
            "success",
        )

        item["http_status"] = response.status

        # -------------------------------------------------------------- #
        # Response validators
        # -------------------------------------------------------------- #
        item["etag"] = _header(
            response,
            "ETag",
        )

        item["last_modified"] = _header(
            response,
            "Last-Modified",
        )

        # -------------------------------------------------------------- #
        # Content validation
        # -------------------------------------------------------------- #
        if not _is_text_content(response):
            item["status"] = "failed"

            item["error"] = (
                "unsupported content type: "
                f"{_header(response, 'Content-Type')}"
            )

            yield item
            return

        item["title"] = response.xpath(
            "//title/text()"
        ).get(
            default=""
        ).strip()

        item["raw_html"] = response.text

        item["text_content"] = " ".join(
            text.strip()
            for text in response.xpath(
                "//body//text()"
            ).getall()
            if text.strip()
        )

        if not item["text_content"].strip():
            item["status"] = "failed"

            item["error"] = (
                "empty page (no visible text)"
            )

            item["raw_html"] = ""
            item["text_content"] = ""

            yield item
            return

        yield item

        # -------------------------------------------------------------- #
        # Controlled incremental discovery
        # -------------------------------------------------------------- #
        #
        # A successful 200 seed can discover additional same-domain
        # pages.
        #
        # A valid 304 returns above and does not reprocess links.
        #
        # A 304 with missing evidence performs one unconditional
        # seed request. If that request succeeds, it continues
        # through this normal 200 response path.
        #
        # Only authorized same-domain HTTP(S) links are followed.
        #
        if self.pages_seen >= self.max_pages:
            return

        for href in response.css(
            "a::attr(href)"
        ).getall():

            if self.pages_seen >= self.max_pages:
                break

            next_url = urljoin(
                response.url,
                href,
            )

            next_url = self._normalize_url(
                next_url
            )

            if not self._is_allowed_link(
                next_url
            ):
                continue

            if next_url in self.visited_urls:
                continue

            self.visited_urls.add(next_url)

            yield self._build_request(
                next_url,
                conditional=False,
                is_seed=False,
            )

    def handle_failure(self, failure):
        request = failure.request

        response = getattr(
            failure.value,
            "response",
            None,
        )

        http_status = (
            response.status
            if response is not None
            else None
        )

        error = (
            f"HTTP {http_status}"
            if http_status
            else (
                str(failure.value)
                or failure.type.__name__
            )
        )

        self.logger.error(
            "Fetch failed permanently for %s: %s",
            request.url,
            error,
        )

        item = self._base_item(
            request.url,
            request.meta,
            "failed",
        )

        item["http_status"] = http_status
        item["error"] = error[:500]

        yield item