"""
Crawling Engine validation harness.

Put this file in the PROJECT ROOT (next to scrapy.cfg) and run from the venv:

    python validate_engine.py              # full run (~2-3 min, includes slow retry/timeout cases)
    python validate_engine.py --quick      # skips the slow cases (~30 s)
    python validate_engine.py --e2e        # + Celery/Redis checks (needs Redis, NO workers running)
    python validate_engine.py --live       # + one real crawl of each enabled source in sources.yaml

It starts a local HTTP server that behaves like good and bad sources, runs the
real engine.runner.run_crawl() against it (same code path as the Celery
workers), and prints one report. Paste everything between the two PASTE
markers back into the chat.

No project code is modified. Results append to crawl_output/ like a normal run.
"""
import argparse
import json
import os
import socket
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, ROOT)
os.chdir(ROOT)
# Make sure a system proxy can't intercept the localhost test traffic.
os.environ["NO_PROXY"] = "127.0.0.1,localhost"
os.environ["no_proxy"] = "127.0.0.1,localhost"

from engine.runner import CrawlError, PermanentCrawlError, run_crawl  # noqa: E402

CRASH = "scrapy crawl exited"   # runner message when the subprocess itself crashed
HITS: dict = {}
HITS_LOCK = threading.Lock()
PAGE = (
    "<html><head><title>Validation Page</title></head>"
    "<body><h1>Hello</h1><p>Some visible text for the crawler.</p></body></html>"
)


def _hit(path):
    with HITS_LOCK:
        HITS[path] = HITS.get(path, 0) + 1
        return HITS[path]


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def _send(self, code, body=b"", ctype="text/html; charset=utf-8", headers=None):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        for k, v in (headers or {}).items():
            self.send_header(k, v)
        try:
            self.end_headers()
            if self.command != "HEAD":
                self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            # The crawler may close a timed-out request before the mock server replies.
            return

    def do_GET(self):
        path = self.path.split("?")[0]
        n = _hit(self.path)
        if path.startswith("/ok"):
            self._send(200, PAGE.encode())
        elif path.startswith("/etag"):
            if self.headers.get("If-None-Match") == '"v1"':
                self._send(304, b"", headers={"ETag": '"v1"'})
            else:
                self._send(200, PAGE.encode(), headers={"ETag": '"v1"'})
        elif path.startswith("/404"):
            self._send(404, b"not found")
        elif path.startswith("/403"):
            self._send(403, b"forbidden")
        elif path.startswith("/always503"):
            self._send(503, b"unavailable")
        elif path.startswith("/flaky"):      # 503 twice, then OK
            self._send(503, b"x") if n <= 2 else self._send(200, PAGE.encode())
        elif path.startswith("/ratelimit"):  # 429 once with Retry-After, then OK
            if n == 1:
                self._send(429, b"slow down", headers={"Retry-After": "1"})
            else:
                self._send(200, PAGE.encode())
        elif path.startswith("/empty"):
            self._send(200, b"")
        elif path.startswith("/blank"):
            self._send(200, b"<html><head><title></title></head><body>   </body></html>")
        elif path.startswith("/pdf"):
            self._send(200, b"%PDF-1.4 fake", ctype="application/pdf")
        elif path.startswith("/slow"):
            time.sleep(25)
            self._send(200, PAGE.encode())
        else:
            self._send(404, b"?")


def start_server():
    srv = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv, srv.server_address[1]


def free_closed_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def src(name, url, timeout=120, priority="standard"):
    return {"name": name, "url": url, "source_type": "validation", "priority": priority,
            "interval_minutes": 5, "timeout_seconds": timeout, "enabled": True}


RESULTS = []


def record(name, ok, detail, seconds):
    RESULTS.append({"test": name, "result": "PASS" if ok else "FAIL",
                    "seconds": round(seconds, 1), "detail": str(detail)[:300]})
    print(f"  [{'PASS' if ok else 'FAIL'}] {name:<34} {seconds:5.1f}s  {str(detail)[:110]}", flush=True)


def case(name, fn):
    t = time.time()
    try:
        ok, detail = fn()
    except Exception as exc:  # harness-level crash
        ok, detail = False, f"HARNESS ERROR {type(exc).__name__}: {exc}"
    record(name, ok, detail, time.time() - t)


def expect_raises(exc_type, not_type, source):
    try:
        rec = run_crawl(source)
        return False, f"no exception; got status={rec.get('status')} http={rec.get('http_status')}"
    except Exception as exc:
        if CRASH in str(exc):
            return False, f"scrapy process crashed (not a handled failure): {str(exc)[:140]}"
        good = isinstance(exc, exc_type) and not (not_type and isinstance(exc, not_type))
        return good, f"{type(exc).__name__}: {str(exc)[:140]}"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--quick", action="store_true")
    ap.add_argument("--e2e", action="store_true")
    ap.add_argument("--live", action="store_true")
    args = ap.parse_args()

    srv, port = start_server()
    base = f"http://127.0.0.1:{port}"
    print(f"Local test server on {base}\n\nFUNCTIONAL TESTS")

    # ---- success path -----------------------------------------------------
    holder = {}

    def t_ok():
        r = run_crawl(src("v_ok", f"{base}/ok"))
        holder["ok"] = r
        good = (r["status"] == "success" and r["http_status"] == 200 and r["content_hash"]
                and r["title"] == "Validation Page" and os.path.exists(r["evidence_path"]))
        return bool(good), f"hash={str(r.get('content_hash'))[:12]} title={r.get('title')!r} evidence={bool(r.get('evidence_path'))}"
    case("success: 200 page stored", t_ok)

    def t_dup():
        r = run_crawl(src("v_ok2", f"{base}/ok?again=1"))
        return r.get("duplicate_of_existing") is True, f"duplicate_of_existing={r.get('duplicate_of_existing')}"
    case("dedup: same content detected", t_dup)

    def t_304():
        first = run_crawl(src("v_etag", f"{base}/etag"))
        v = {"etag": first.get("etag"), "last_modified": first.get("last_modified"),
             "prev_hash": first.get("content_hash")}
        second = run_crawl(src("v_etag", f"{base}/etag"), v)
        good = second.get("unchanged") is True and second.get("http_status") == 304 \
            and second.get("content_hash") == first.get("content_hash")
        return good, f"etag={first.get('etag')} second.unchanged={second.get('unchanged')} http={second.get('http_status')}"
    case("incremental: ETag -> 304 unchanged", t_304)

    # ---- failure handling -------------------------------------------------
    case("failure: 404 -> permanent (no retry)",
         lambda: expect_raises(PermanentCrawlError, None, src("v_404", f"{base}/404")))
    case("failure: 403 -> permanent (no retry)",
         lambda: expect_raises(PermanentCrawlError, None, src("v_403", f"{base}/403")))
    case("failure: binary PDF -> failed record",
         lambda: expect_raises(CrawlError, PermanentCrawlError, src("v_pdf", f"{base}/pdf")))

    def t_flaky():
        r = run_crawl(src("v_flaky", f"{base}/flaky"))
        return r["status"] == "success" and r.get("attempt") == 3, f"attempt={r.get('attempt')} (expect 3)"
    case("retry: 503,503,200 recovers", t_flaky)

    def t_429():
        r = run_crawl(src("v_429", f"{base}/ratelimit"))
        return r["status"] == "success", f"attempt={r.get('attempt')} (429 + Retry-After honoured)"
    case("retry: 429 Retry-After recovers", t_429)

    # ---- empty pages ------------------------------------------------------
    def t_empty():
        try:
            r = run_crawl(src("v_empty", f"{base}/empty"))
            return False, (f"empty 200 body accepted as status={r.get('status')} hash={r.get('content_hash')} "
                           f"evidence={r.get('evidence_path')} -> not flagged")
        except CrawlError as exc:
            if CRASH in str(exc):
                return False, f"scrapy process crashed: {str(exc)[:120]}"
            return True, f"flagged: {str(exc)[:120]}"
    case("empty: 0-byte 200 page is flagged", t_empty)

    def t_blank():
        try:
            r = run_crawl(src("v_blank", f"{base}/blank"))
            return False, f"blank-body page accepted as status={r.get('status')} -> not flagged"
        except CrawlError as exc:
            if CRASH in str(exc):
                return False, f"scrapy process crashed: {str(exc)[:120]}"
            return True, f"flagged: {str(exc)[:120]}"
    case("empty: blank-body 200 page is flagged", t_blank)

    # ---- slow cases -------------------------------------------------------
    if not args.quick:
        def t_503_cap():
            before = HITS.get("/always503", 0)
            ok, detail = expect_raises(CrawlError, PermanentCrawlError, src("v_503", f"{base}/always503"))
            hits = HITS.get("/always503", 0) - before
            return ok and hits == 4, f"{detail} | server hits={hits} (expect 4 = 1 + 3 retries)"
        case("retry cap: always-503 stops at 3 retries", t_503_cap)

        case("failure: connection refused",
             lambda: expect_raises(CrawlError, PermanentCrawlError,
                                   src("v_refused", f"http://127.0.0.1:{free_closed_port()}/")))

        def t_timeout():
            t0 = time.time()
            ok, detail = expect_raises(CrawlError, PermanentCrawlError,
                                       src("v_slow", f"{base}/slow", timeout=30))
            took = time.time() - t0
            return ok and took < 45, f"{detail} | took {took:.0f}s (job limit 30s)"
        case("timeout: slow source killed at job limit", t_timeout)

    # ---- throughput -------------------------------------------------------
    print("\nTHROUGHPUT")
    thr = {}
    try:
        t0 = time.time()
        run_crawl(src("v_t1", f"{base}/ok?t=single"))
        thr["single_job_seconds"] = round(time.time() - t0, 2)
        n, workers = 8, 4
        t0 = time.time()
        with ThreadPoolExecutor(max_workers=workers) as pool:
            list(pool.map(lambda i: run_crawl(src(f"v_t{i}", f"{base}/ok?t={i}")), range(n)))
        wall = time.time() - t0
        thr.update(jobs=n, parallel_workers=workers, wall_seconds=round(wall, 2),
                   jobs_per_minute=round(n / wall * 60, 1))
        print(f"  single job: {thr['single_job_seconds']}s | {n} jobs on {workers} workers: "
              f"{thr['wall_seconds']}s ({thr['jobs_per_minute']} jobs/min)")
    except Exception as exc:
        thr["error"] = f"{type(exc).__name__}: {exc}"
        print("  throughput run failed:", thr["error"])

    # ---- Celery / Redis ---------------------------------------------------
    if args.e2e:
        print("\nCELERY / REDIS (requires Redis, no workers running)")
        try:
            import redis as _redis
            from engine.celery_app import BROKER_URL
            from engine.scheduler import check_and_schedule
            from engine.sources import load_sources
            from engine.state import CrawlState
            from engine.tasks import crawl_source

            st = CrawlState()
            st.r.ping()

            def t_task_success():
                out = crawl_source.apply(args=[src("v_e2e_ok", f"{base}/ok?e2e=1")]).get()
                h = st.get_health("v_e2e_ok")
                good = out["status"] == "success" and h.get("last_status") == "success" \
                    and h.get("in_progress") == "0" and h.get("consecutive_failures") == "0"
                return good, f"task={out} state.last_status={h.get('last_status')}"
            case("task: success updates state", t_task_success)

            def t_task_dead():
                # E2E runs may be repeated against the same Redis instance;
                # start this assertion from a fresh source state.
                source = src("v_e2e_404", f"{base}/404")
                st.r.delete("crawlstate:v_e2e_404")
                st.release_lock("v_e2e_404")
                out = crawl_source.apply(args=[source]).get()
                h = st.get_health(source["name"])
                good = out["status"] == "dead_letter" and h.get("last_status") == "dead_letter" \
                    and h.get("consecutive_failures") == "1"
                return good, f"task={out['status']} state.last_status={h.get('last_status')} failures={h.get('consecutive_failures')}"
            case("task: 404 -> dead-letter + backoff counter", t_task_dead)

            def t_routing():
                broker = _redis.Redis.from_url(BROKER_URL)
                names = [s["name"] for s in load_sources(strict=False) if s.get("enabled", True)]
                for q in ("fast", "standard", "bulk"):
                    broker.delete(q)
                for n in names:
                    st.release_lock(n)
                    st.r.delete(f"crawlstate:{n}")
                out = check_and_schedule()
                lens = {q: broker.llen(q) for q in ("fast", "standard", "bulk")}
                out2 = check_and_schedule()  # second tick must NOT duplicate (lock)
                lens2 = {q: broker.llen(q) for q in ("fast", "standard", "bulk")}
                for q in ("fast", "standard", "bulk"):
                    broker.delete(q)
                for n in names:
                    st.release_lock(n)
                good = sorted(out["scheduled"]) == sorted(names) and out2["scheduled"] == [] and lens == lens2
                return good, f"scheduled={out['scheduled']} queues={lens} 2nd tick={out2['scheduled']}"
            case("scheduler: routes to queues, no duplicates", t_routing)
        except Exception as exc:
            record("celery/redis setup", False, f"{type(exc).__name__}: {exc}", 0)

    # ---- live sources -----------------------------------------------------
    if args.live:
        print("\nLIVE REGISTERED SOURCES (needs internet)")
        from engine.sources import load_sources
        for s in load_sources(strict=False):
            if s.get("enabled", True):
                def t_live(s=s):
                    r = run_crawl(s)
                    return r["status"] == "success", f"http={r.get('http_status')} title={str(r.get('title'))[:40]!r}"
                case(f"live: {s['name']}", t_live)

    srv.shutdown()
    failed = [r for r in RESULTS if r["result"] == "FAIL"]

    # ---- sample record (for Q2) ------------------------------------------
    sample = None
    try:
        with open(os.path.join(ROOT, "crawl_output", "crawl_results.jsonl"), encoding="utf-8") as f:
            for line in f:
                if '"v_ok"' in line:
                    sample = json.loads(line)
    except OSError:
        pass

    report = {"tests": RESULTS, "throughput": thr, "sample_success_record": sample,
              "passed": len(RESULTS) - len(failed), "failed": len(failed)}
    os.makedirs(os.path.join(ROOT, "crawl_output"), exist_ok=True)
    with open(os.path.join(ROOT, "crawl_output", "validation_report.json"), "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    print("\n=== PASTE FROM HERE ===")
    print(json.dumps(report, indent=2))
    print("=== PASTE TO HERE ===")
    print(f"\n{report['passed']} passed, {report['failed']} failed")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
