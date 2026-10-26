"""
Request handling middlewares.

ProxyRoutingMiddleware  - routes each request through the right connectivity
                           (Tor for .onion, I2P HTTP proxy for .i2p, direct
                           for everything else) based on the target host, so
                           the same spider can hit onion sites and normal
                           sites without per-request config.

                           NOTE: Scrapy can only talk to HTTP(S) proxies. It
                           cannot speak SOCKS, so `socks5h://127.0.0.1:9050`
                           (Tor's raw port) fails with "SchemeNotSupported".
                           Put an HTTP proxy in front of Tor (Privoxy, default
                           http://127.0.0.1:8118) and point TOR_HTTP_PROXY at it.

BackoffRetryMiddleware  - retryable failures are retried with exponential
                           backoff + jitter, up to RETRY_TIMES attempts. It
                           honours a Retry-After header (capped), and it is
                           written as coroutines because returning Deferreds
                           from downloader middlewares is deprecated in recent
                           Scrapy. When retries are exhausted the failure
                           flows to the spider's errback, which writes the
                           single "failed" crawl-result record.
"""
import asyncio
import logging
import random
from urllib.parse import urlparse

from scrapy.downloadermiddlewares.retry import RetryMiddleware
from scrapy.utils.response import response_status_message

logger = logging.getLogger(__name__)


class ProxyRoutingMiddleware:
    """Sets request.meta['proxy'] based on the target domain's TLD."""

    def __init__(self, tor_proxy, i2p_proxy):
        self.tor_proxy = tor_proxy
        self.i2p_proxy = i2p_proxy

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            tor_proxy=crawler.settings.get("TOR_HTTP_PROXY"),
            i2p_proxy=crawler.settings.get("I2P_HTTP_PROXY"),
        )

    def process_request(self, request, spider=None):
        host = (urlparse(request.url).hostname or "").lower()

        if host.endswith(".onion"):
            request.meta["network"] = "tor"  # set first so failure records show the right network
            request.meta["proxy"] = _require_http_proxy(self.tor_proxy, "Tor", "TOR_HTTP_PROXY")
        elif host.endswith(".i2p"):
            request.meta["network"] = "i2p"
            request.meta["proxy"] = _require_http_proxy(self.i2p_proxy, "I2P", "I2P_HTTP_PROXY")
        else:
            # Normal site: no proxy, direct connection.
            request.meta["network"] = "clearnet"
        return None


def _require_http_proxy(proxy, label, setting_name):
    """Fail fast with an actionable message instead of a cryptic Twisted error."""
    scheme = urlparse(proxy or "").scheme.lower()
    if scheme not in ("http", "https"):
        raise ValueError(
            f"{label} proxy {proxy!r} is not an HTTP proxy. Scrapy cannot use SOCKS "
            f"proxies directly. Put an HTTP proxy in front (for Tor: Privoxy, "
            f"http://127.0.0.1:8118) and set {setting_name}."
        )
    return proxy


class BackoffRetryMiddleware(RetryMiddleware):
    """RetryMiddleware with exponential backoff, capped at RETRY_TIMES."""

    def __init__(self, settings):
        super().__init__(settings)
        self.base_delay = settings.getfloat("RETRY_BASE_DELAY", 2.0)
        self.max_delay = settings.getfloat("RETRY_MAX_DELAY", 30.0)

    async def process_response(self, request, response, spider=None):
        if request.meta.get("dont_retry", False):
            return response
        if response.status in self.retry_http_codes:
            reason = response_status_message(response.status)
            retry = await self._retry_with_backoff(request, reason, response)
            # None => retries exhausted: hand the response on; the spider's
            # errback turns it into the "failed" record (with its HTTP status).
            return retry or response
        return response

    async def process_exception(self, request, exception, spider=None):
        if isinstance(exception, self.exceptions_to_retry) and not request.meta.get(
            "dont_retry", False
        ):
            return await self._retry_with_backoff(request, exception)
        return None

    async def _retry_with_backoff(self, request, reason, response=None):
        # The parent builds the retry request, counts attempts, updates stats
        # and logs the give-up. It returns None once retries are exhausted.
        new_request = super()._retry(request, reason)

        if new_request is None:
            logger.warning(
                "DEAD-LETTER: %s exhausted retries — reason: %s", request.url, reason
            )
            stats = getattr(self.crawler, "stats", None)
            if stats:
                stats.inc_value("darktrace/dead_letter_count")
            return None

        retries = new_request.meta.get("retry_times", 1)
        delay = self._delay_for(retries, response)
        logger.info(
            "Retrying %s (attempt %d) after %.1fs — reason: %s",
            request.url, retries, delay, reason,
        )
        await asyncio.sleep(delay)
        return new_request

    def _delay_for(self, retries, response):
        delay = min(self.base_delay * (2 ** (retries - 1)), self.max_delay)
        delay += random.uniform(0, 1)  # jitter
        if response is not None:
            retry_after = response.headers.get("Retry-After")
            if retry_after:
                try:
                    delay = max(delay, min(float(retry_after), self.max_delay))
                except ValueError:
                    pass  # HTTP-date form: just use the computed backoff
        return delay
