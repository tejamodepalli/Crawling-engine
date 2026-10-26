"""
Crawl Result item.

Field set mirrors the "Crawl Result and Handoff" table from the design doc:
source, URI, fetch timestamp, crawler information, content hash, status/logs.
"""
import scrapy


class CrawlResultItem(scrapy.Item):
    # Identity of the crawl target
    source_name = scrapy.Field()      # logical name of the authorized source (e.g. "source_1")
    source_type = scrapy.Field()      # forum | marketplace | paste | blog_news | onion_site | normal
    url = scrapy.Field()              # the URI that was fetched
    network = scrapy.Field()          # "clearnet" | "tor" | "i2p"
    job_id = scrapy.Field()           # token the engine passes in, so it can find THIS job's record

    # Provenance / metadata
    fetch_timestamp = scrapy.Field()  # ISO8601 UTC
    crawler_build = scrapy.Field()    # crawler name + version, for audit trail
    status = scrapy.Field()           # "success" | "failed"
    http_status = scrapy.Field()      # HTTP status code, if any
    attempt = scrapy.Field()          # which attempt number produced this result
    error = scrapy.Field()            # human-readable failure reason (status == "failed")

    # Incremental crawling (conditional GET)
    unchanged = scrapy.Field()        # True when the server answered 304 Not Modified
    etag = scrapy.Field()             # ETag validator from the response
    last_modified = scrapy.Field()    # Last-Modified validator from the response

    # Content
    title = scrapy.Field()
    raw_html = scrapy.Field()         # preserved original content (evidence, before processing)
    text_content = scrapy.Field()     # extracted visible text
    content_hash = scrapy.Field()     # sha256 of raw_html, for duplicate detection
    duplicate_of_existing = scrapy.Field()  # True if identical evidence was already on disk
    evidence_path = scrapy.Field()    # where the raw content was stored on disk
    text_path = scrapy.Field()        # where the extracted visible text was stored (.txt)
