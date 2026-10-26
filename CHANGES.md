# Crawling Engine — finalization patch

Unzip over the project root (same folder layout). 11 files replaced, 2 added.
Nothing outside `Crawler/`, `engine/` and `requirements.txt` is touched
(`sources.yaml`, `scrapy.cfg`, `README.md` are yours).

    pip install -r requirements.txt      # scrapy>=2.13 is now required

## File map

| File | Action | What changed |
|---|---|---|
| `requirements.txt` | replace | `scrapy>=2.13` (needed for `async def start()`) |
| `Crawler/spiders/single_source.py` | replace | **Bug fix:** uses `async def start()`. Under the old `start_requests()` Scrapy 2.19 never called the errback for HTTP errors, so 404/403 etc. produced no record. Adds conditional GET (ETag / Last-Modified → 304), job_id, binary-content guard. |
| `Crawler/middlewares.py` | replace | **Bug fix:** Tor via `socks5h://` fails with `SchemeNotSupported` (Scrapy only speaks HTTP proxies) → now an HTTP proxy + clear error if misconfigured. Retry backoff rewritten as coroutines (returning a Deferred is deprecated in Scrapy 2.19), honours `Retry-After`. |
| `Crawler/pipelines.py` | replace | `error` field now reaches the log; no empty evidence files for failed fetches; `duplicate_of_existing` now checks disk (it was always `false`: each job is a fresh process); atomic evidence writes; new fields. |
| `Crawler/items.py` | replace | New fields: `job_id, error, unchanged, etag, last_modified, duplicate_of_existing`. |
| `Crawler/settings.py` | replace | `TOR_HTTP_PROXY` (env-overridable) replaces `TOR_SOCKS_PROXY`; cookies/telnet off, `DOWNLOAD_MAXSIZE`; `DOWNLOAD_DELAY_JITTER` replaces deprecated `RANDOMIZE_DOWNLOAD_DELAY`; build `0.2.0`. |
| `engine/runner.py` | **new** | One shared "run a crawl subprocess" function (used by tasks + checker). Uses `sys.executable -m scrapy`, matches the result by `job_id`, reads only new log bytes, sizes per-request timeout from the job budget, classifies permanent errors. |
| `engine/tasks.py` | replace | Thin wrapper over `runner`. HTTP 4xx (except 408/425/429) → dead-letter immediately instead of 3 pointless job retries. Saves ETag/hash after success. |
| `engine/scheduler.py` | replace | One pipelined Redis call for all sources; per-source validation (one bad entry no longer kills the tick); lock released if enqueue fails; lock TTL sized per source; `ignore_result`. |
| `engine/state.py` | replace | `snapshot()`, stale-safe `in_progress` (`started_ts`), failure backoff (interval ×2/×4/×8), validator storage. Old methods kept. |
| `engine/sources.py` | replace | Per-entry validation, `strict=False` mode, mtime cache, http(s)-URL check. |
| `engine/celery_app.py` | replace | Thread pool by default on Windows (prefork is unsupported there), `task_reject_on_worker_lost`, `result_expires`, visibility timeout. |
| `engine/verify_sources.py` | **new** | `python -m engine.verify_sources` — real crawl of every registered source, pass/fail table, exit code 1 on failure. |

## Running it (Windows)

```powershell
.\venv\Scripts\Activate.ps1

# 1. check every registered source works (no Redis/Celery needed)
python -m engine.verify_sources            # add --all to include disabled ones

# 2. workers - the pool is "threads" automatically on Windows, no -P needed
celery -A engine.celery_app worker -Q fast -n fast@%h --concurrency=8 --loglevel=INFO
celery -A engine.celery_app worker -Q standard -n standard@%h --concurrency=4 --loglevel=INFO
celery -A engine.celery_app worker -Q bulk -n bulk@%h --concurrency=2 --loglevel=INFO

# 3. scheduler
celery -A engine.celery_app beat --loglevel=INFO
```

Keep the `standard` worker running: the scheduler tick itself is dispatched to the default (`standard`) queue.
Because jobs are subprocess-wait only, concurrency can be much higher than CPU count.

## Tor / I2P

Scrapy cannot use SOCKS. Put an HTTP proxy in front of Tor:

1. Install Privoxy and add to its `config`: `forward-socks5t / 127.0.0.1:9050 .`
   (Privoxy then listens on `127.0.0.1:8118`).
2. Default `TOR_HTTP_PROXY` is already `http://127.0.0.1:8118`; override with the env var if different.
3. I2P is unchanged (`I2P_HTTP_PROXY`, default `http://127.0.0.1:4444`).

Then enable `onion_source_1` and run `python -m engine.verify_sources --name onion_source_1`.

## Behaviour changes to be aware of

* `crawl_results.jsonl` gains fields (`job_id, error, unchanged, etag, last_modified`); existing fields are unchanged. `duplicate_of_existing` is now meaningful.
* Records written by the middleware are gone: the spider writes the one failed record (with the HTTP status).
* A source with N consecutive failures is retried after interval × min(2^N, 8).
* `timeout_seconds` is also used to derive the per-request timeout: `(timeout − 20) / 4`, clamped to 10–60 s (min 30 s for .onion/.i2p). Give slow sources a bigger `timeout_seconds`.

## README updates to make

* §6 Proxy / Network Setup: replace `TOR_SOCKS_PROXY` with the Privoxy setup above.
* §10 Celery Queues: add the Windows worker commands above (`-n` names, thread pool).
* §14 Retry: note 4xx failures skip job retries.
* §19: "Incremental crawling using ETag / Last-Modified" is now implemented; the scheduler lock is implemented (the last table row still says "not yet").


---

## Patch 2 — finalization

| File | Change |
|---|---|
| `Crawler/spiders/single_source.py` | Empty pages (HTTP 200, no visible text) are now `failed` with error `empty page (no visible text)` instead of a hash-less "success". |
| `Crawler/pipelines.py`, `Crawler/items.py` | Extracted text is now persisted as `raw/<hash>.txt`; records gain `text_path`. |
| `README.md` | Stale SOCKS / ETag / scheduler-lock statements corrected; new sections 21-23 (data access, failure handling, validation). |
| `validate_engine.py` | **new** — end-to-end validation harness (local test server; `--quick`, `--e2e`, `--live`). |
