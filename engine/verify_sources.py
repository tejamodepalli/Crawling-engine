"""
Source checker: run ONE real crawl against every registered source and print
a pass/fail table. It uses exactly the same code path as the Celery workers
(engine.runner.run_crawl) but needs no Redis, Celery or Beat, and it does not
touch crawl state, so it's safe to run any time.

    python -m engine.verify_sources                    # enabled sources only
    python -m engine.verify_sources --all              # include disabled ones
    python -m engine.verify_sources --name news_source_1

Exit code is 0 if every checked source succeeded, 1 otherwise (CI-friendly).
"""
import argparse
import sys
import time
from concurrent.futures import ThreadPoolExecutor

from engine.runner import PermanentCrawlError, is_darknet, run_crawl
from engine.sources import load_sources, validate_source


def _check(source: dict) -> dict:
    started = time.time()
    row = {"name": source["name"], "priority": source["priority"],
           "network": "darknet" if is_darknet(source["url"]) else "clearnet"}
    try:
        record = run_crawl(source)
        row.update(ok=True, http=record.get("http_status"), note=(record.get("title") or "")[:50])
    except PermanentCrawlError as exc:
        row.update(ok=False, http=None, note=f"PERMANENT: {exc}")
    except Exception as exc:
        row.update(ok=False, http=None, note=str(exc))
    row["seconds"] = time.time() - started
    return row


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--all", action="store_true", help="also check disabled sources")
    parser.add_argument("--name", help="check only this source")
    args = parser.parse_args(argv)

    sources = load_sources(strict=False)
    if args.name:
        sources = [s for s in sources if s["name"] == args.name]
    elif not args.all:
        sources = [s for s in sources if s.get("enabled", True)]

    if not sources:
        print("No matching sources.")
        return 1

    with ThreadPoolExecutor(max_workers=min(4, len(sources))) as pool:
        rows = list(pool.map(_check, sources))

    print(f"\n{'SOURCE':<24}{'QUEUE':<10}{'NETWORK':<10}{'RESULT':<8}{'HTTP':<6}{'TIME':<7}NOTE")
    for r in rows:
        print(
            f"{r['name']:<24}{r['priority']:<10}{r['network']:<10}"
            f"{'OK' if r['ok'] else 'FAIL':<8}{str(r['http'] or '-'):<6}{r['seconds']:<7.1f}{r['note']}"
        )

    failed = [r for r in rows if not r["ok"]]
    print(f"\n{len(rows) - len(failed)}/{len(rows)} sources OK")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
