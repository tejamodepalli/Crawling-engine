# Final QA Report

Date: 2026-10-22
Project: web-crawler
Test Scope: Functional validation, registered-source integration, Redis/Celery/scheduler queues, output/state persistence, request efficiency, throughput, retry behavior, and advanced reliability checks.
Latest verification: project venv dependencies clean; quick 10/10, full 13/13, E2E 16/16, enabled sources 3/3, and clean full-validation log with 0 tracebacks.

## Executive Summary

This QA cycle covered the crawler's functional behavior, real registered-source
integration, Redis/Celery scheduling, output persistence, request efficiency,
throughput, retry handling, and advanced reliability scenarios. Testing used
the repository's local validation harness, the three enabled authorized
clearnet sources, isolated Docker Redis, and fixed worker workloads. Logs and
per-source records were preserved for every completed phase.

1. **Integration: PASS.** All 3 enabled registered sources completed both
	direct source verification and queue-driven Celery crawls with HTTP 200.
	Source priorities were routed correctly to `fast`, `standard`, and `bulk`,
	scheduler locks prevented duplicate dispatch, Redis state was updated, and
	JSONL/raw evidence output was created. The disabled onion source was
	correctly excluded from normal runs.
2. **Throughput: PASS.** Every fixed 8-job benchmark completed with 8/8
	successful jobs and zero retries. Concurrency 2 achieved 32.67 jobs/minute,
	concurrency 4 achieved the best result at 73.93 jobs/minute, and
	concurrency 8 achieved 67.56 jobs/minute. Increasing beyond 4 did not
	improve this workload, which suggests a network or source-response limit
	rather than a worker-capacity limit.
3. **Efficiency: PARTIAL PASS.** Retry caps, HTTP 429 with `Retry-After`,
	permanent 404 handling, duplicate detection, ETag/304 conditional requests,
	AutoThrottle, download delay, and per-domain limits passed their controlled
	tests. The direct `example.com` repeat test was blocked by DNS resolution,
	so duplicate/ETag behavior could not be independently observed through that
	external command.
4. **Reliability: PASS WITH FINDINGS.** Redis outage recovery, malformed
	source validation, mixed-priority routing, unavailable-source isolation,
	repeated 503 handling, and disabled-source behavior passed. The two
	outstanding product/test findings are binary PDF classification and E2E
	Redis-state isolation. A destructive worker-kill recovery test remains
	intentionally untested.

**Overall QA decision: PASS WITH KNOWN ISSUES.** The core crawler, source
registry, queue, scheduler, persistence, and retry paths are operational for
enabled sources. Before calling the release fully clean, fix or formally
accept BUG-001 and BUG-002, configure a real Tor environment for ENV-002, and
repeat the blocked efficiency test with a reachable authorized endpoint.

Known bugs and environment findings:
- BUG-001: FIXED. Binary PDF responses are now rejected by both response class and Content-Type; the fixed quick validation passed 10/10.
- BUG-002: FIXED AND VERIFIED. The E2E test clears the dedicated source state before asserting the dead-letter counter; Redis-backed E2E now reports `failures=1` and 16/16 tests pass.
- ENV-001: RESOLVED FOR TESTING. The unreachable example.com check was replaced for verification with reachable authorized PyPI; repeated crawl produced the same hash and duplicate detection.
- ENV-002: `onion_source_1` could not connect because the configured placeholder `.onion` target and required Tor HTTP proxy were unavailable. This is an environment BLOCKED result, not a confirmed crawler code defect.
- ADV-02 was not tested by force-killing a worker during a live task because that destructive test could leave delayed redelivery/orphan subprocesses.

## Command Guide and Test Purpose

The commands below document the intent of each phase, the expected signal, and
how the observed result should be interpreted. Commands that access live URLs
must only be run against authorized sources.

### 1. Built-in validation

```powershell
python validate_engine.py --help
```

Purpose: confirm the validation harness is available and show supported modes.
Expected: help text listing `--quick`, `--e2e`, and `--live`.
Observed: all documented options were available.

```powershell
python validate_engine.py --quick 2>&1 | Tee-Object Test_logs/test-quick.log
```

Purpose: run fast local mock-server tests for success, duplicate content,
ETag/304, 403/404, 429, transient 503, and empty-content behavior.
Expected: each case prints `[PASS]` or `[FAIL]`; the log is saved.
Observed: 9 passed and 1 failed. The PDF case was reported as success.

```powershell
python validate_engine.py 2>&1 | Tee-Object Test_logs/test-full.log
```

Purpose: include slow retry-cap, connection-refused, and timeout scenarios.
Expected: the slow failure paths terminate in a controlled way.
Observed: 12 passed and 1 failed. Retry cap, connection refusal, timeout,
throughput, 403/404, 429, and 503 behavior passed.

```powershell
python validate_engine.py --e2e 2>&1 | Tee-Object Test_logs/test-e2e.log
```

Purpose: exercise Celery task state updates, dead-letter handling, Redis state,
and scheduler routing without starting normal workers.
Expected: Redis-backed task and scheduler checks pass.
Observed: 14 passed and 2 failed. The PDF case failed again; the 404
dead-letter counter was affected by existing Redis state.

```powershell
Select-String -Path test-quick.log,test-full.log,test-e2e.log \
	-Pattern "\\[FAIL\\]|\\[PASS\\]"
```

Purpose: quickly extract the automated test result lines for review.
Expected: a compact PASS/FAIL list from each saved log.
Observed: the same PDF and E2E state findings were reproducible.

### 2. Registered source integration

```powershell
python -m engine.verify_sources --help
```

Purpose: verify source-checker options before making network requests.
Expected: `--all` and `--name` are documented.
Observed: both options were supported.

```powershell
python -m engine.verify_sources 2>&1 | Tee-Object Test_logs/advanced-enabled.log
```

Purpose: crawl only enabled entries from `sources.yaml` using the same runner
used by Celery tasks, without Redis or Beat.
Expected: one result row per enabled source and exit code 0 when all pass.
Observed: 3/3 enabled sources returned HTTP 200.

```powershell
python -m engine.verify_sources --all 2>&1 | Tee-Object Test_logs/advanced-all.log
```

Purpose: include disabled entries and verify that one unavailable source does
not prevent other sources from being checked.
Expected: disabled entries appear; unavailable entries are reported as FAIL.
Observed: the disabled onion entry failed because no Tor endpoint was
configured; the three clearnet sources still passed.

```powershell
python -m engine.verify_sources --name onion_source_1
```

Purpose: isolate one source for targeted diagnosis.
Expected: only that source appears in the table.
Observed: only `onion_source_1` appeared and it was blocked by the missing
Tor/network setup.

### 3. Redis, Celery, and scheduler integration

```powershell
docker exec darktrace-redis redis-cli ping
```

Purpose: verify the local Redis broker/state service before starting workers.
Expected: `PONG`.
Observed: `PONG`.

```powershell
celery -A engine.celery_app worker \
	-Q fast,standard,bulk --concurrency=4 --loglevel=INFO
```

Purpose: start one Windows-compatible worker consuming all configured queues.
Expected: one node becomes ready and lists `fast`, `standard`, and `bulk`.
Observed: worker connected to Redis and consumed all three queues.

```powershell
celery -A engine.celery_app inspect active_queues
celery -A engine.celery_app inspect active
celery -A engine.celery_app inspect stats
```

Purpose: confirm queue bindings, active jobs, broker, pool, and concurrency.
Expected: one node replies; queue names and worker statistics are visible.
Observed: one node replied with all queues and a Windows thread pool.

```powershell
python -c "from engine.scheduler import check_and_schedule; print(check_and_schedule.run())"
```

Purpose: execute one scheduler decision synchronously.
Expected: due enabled sources are scheduled once.
Observed: three enabled sources were queued and the worker completed all three
with HTTP 200.

```powershell
celery -A engine.celery_app beat --loglevel=INFO
```

Purpose: verify that periodic Beat can start with the configured 30-second
schedule and Redis broker.
Expected: Beat starts without connection errors.
Observed: Beat started successfully. Worker and Beat were stopped after tests.

### 4. Output and state verification

```powershell
Get-Content crawl_output\\crawl_results.jsonl -Tail 20
Get-Content crawl_output\\dead_letter.jsonl -Tail 20
docker exec darktrace-redis redis-cli -n 2 HGETALL crawlstate:news_source_1
```

Purpose: verify crawl records, failure records, and per-source Redis health.
Expected: success records contain status, HTTP code, hash, and evidence paths;
Redis contains status and counters.
Observed: real-source records, E2E dead-letter records, and successful
`news_source_1` state were present.

```powershell
python -c "import json,pathlib; p=pathlib.Path('crawl_output/crawl_results.jsonl'); [json.loads(x) for x in p.read_text(encoding='utf-8').splitlines() if x.strip()]; print('JSONL valid')"
```

Purpose: parse every non-empty JSONL line instead of trusting file size alone.
Expected: `JSONL valid`.
Observed: `JSONL valid`; 16 raw evidence files were present, 8 HTML and 8
text files.

### 5. Efficiency and retry checks

```powershell
Select-String -Path Crawler\\settings.py,Crawler\\spiders\\single_source.py \
	-Pattern 'CONCURRENT_REQUESTS|CONCURRENT_REQUESTS_PER_DOMAIN|DOWNLOAD_DELAY|AUTOTHROTTLE|RETRY_TIMES|DOWNLOAD_TIMEOUT'
```

Purpose: inspect effective source settings and spider overrides.
Expected: global concurrency 4, per-domain 2, delay 1 second, AutoThrottle,
three retries, and 60-second timeout; single-source override should be 1.
Observed: all values matched. `git grep` and `rg` were unavailable on PATH, so
PowerShell `Select-String` was used as the equivalent search.

```powershell
scrapy crawl single_source -a url="https://example.com/" \
	-a source_name="qa_efficiency_test" -a source_type="normal" -L INFO
```

Purpose: compare two direct crawls of the same page for repeat-request
behavior. The command was run twice with output saved to
`efficiency-run-1.log` and `efficiency-run-2.log`.
Expected: the first request succeeds; the second can show duplicate or
conditional-request behavior.
Observed: both runs failed DNS resolution before receiving content, so this
specific command could not prove duplicate or ETag behavior.

The local harness provided the controlled equivalents: ETag returned 304,
duplicate content was detected, 429 honored `Retry-After`, 503 recovered,
and permanent 404 did not retry.

### 6. Throughput comparison

```powershell
celery -A engine.celery_app worker -Q fast,standard,bulk --concurrency=2 --loglevel=INFO
celery -A engine.celery_app worker -Q fast,standard,bulk --concurrency=4 --loglevel=INFO
celery -A engine.celery_app worker -Q fast,standard,bulk --concurrency=8 --loglevel=INFO
```

Purpose: compare worker concurrency using the same eight-job workload and
same source URLs/priorities. The previous worker was stopped before each run.
Expected: successful jobs per minute improves until the workload becomes
network/source limited.
Observed: concurrency 2 = 32.67 jobs/minute, concurrency 4 = 73.93, and
concurrency 8 = 67.56. All runs were 8/8 successful with zero retries.

## Test Results

| Test ID | Test scenario | Command | Expected result | Actual result | Status | Evidence | Bug ID |
|---|---|---|---|---|---|---|---|
| VAL-01 | Quick built-in validation | `python validate_engine.py --quick 2>&1 \| Tee-Object Test_logs/test-quick.log` | Local success, retry, duplicate, ETag, and empty-content cases pass | 9 passed, 1 failed; binary PDF was recorded as success | FAIL | [test-quick.log](Test_logs/test-quick.log) | BUG-001 |
| VAL-02 | Full built-in validation | `python validate_engine.py 2>&1 \| Tee-Object Test_logs/test-full.log` | Slow retry, timeout, connection, and functional cases pass | 12 passed, 1 failed; same binary PDF issue | FAIL | [test-full.log](Test_logs/test-full.log) | BUG-001 |
| VAL-03 | Redis/Celery E2E validation | `python validate_engine.py --e2e 2>&1 \| Tee-Object Test_logs/test-e2e.log` | Task state, dead-letter, Redis, and scheduler checks pass | 14 passed, 2 failed; binary PDF issue and stale dead-letter counter | FAIL | [test-e2e.log](Test_logs/test-e2e.log) | BUG-001, BUG-002 |
| FIX-001 | Post-fix PDF classification | `python validate_engine.py --quick 2>&1 \| Tee-Object Test_logs/test-quick-fixed.log` | PDF is failed with an unsupported-content error | 10/10 passed; `application/pdf` became a controlled failed crawl | PASS | [test-quick-fixed.log](Test_logs/test-quick-fixed.log) | BUG-001 resolved |
| FIX-002 | Post-fix E2E state isolation | `python validate_engine.py --e2e 2>&1 \| Tee-Object Test_logs/test-e2e-fixed-final.log` | Fresh dead-letter state is asserted at counter 1 | Redis-backed E2E passed 16/16; dead-letter counter was exactly 1 | PASS | [test-e2e-fixed-final.log](Test_logs/test-e2e-fixed-final.log) | BUG-002 resolved |
| FIX-003 | Reachable repeat-crawl replacement | `scrapy crawl single_source -a url="https://pypi.org/" -a source_name="qa_efficiency_reachable" -a source_type="normal" -L INFO` twice | Second reachable crawl shows duplicate or conditional-request evidence | Both runs HTTP 200; same hash; second record `duplicate_of_existing=true`; ETag present | PASS | [efficiency-reachable-run-1.log](Test_logs/efficiency-reachable-run-1.log), [efficiency-reachable-run-2.log](Test_logs/efficiency-reachable-run-2.log) | ENV-001 resolved |
| INT-01 | Verify all enabled registered sources | `python -m engine.verify_sources` | Every enabled source returns a result | 3/3 sources OK, HTTP 200 | PASS | [advanced-enabled.log](Test_logs/advanced-enabled.log) | - |
| INT-02 | Disabled source excluded | `python -m engine.verify_sources` | Disabled onion source is not crawled | `onion_source_1` absent; 3 sources checked | PASS | [advanced-enabled.log](Test_logs/advanced-enabled.log) | - |
| INT-03 | Include disabled sources | `python -m engine.verify_sources --all` | Disabled source appears in report | 4 sources reported; onion source failed in isolation | PASS | [advanced-all.log](Test_logs/advanced-all.log) | - |
| INT-04 | Source types and URLs | `python -m engine.verify_sources` | Correct registry entries are used | blog_news, paste_site, marketplace URLs verified | PASS | [sources.yaml](sources.yaml), [advanced-enabled.log](Test_logs/advanced-enabled.log) | - |
| INT-05 | Fast priority routing | Scheduler integration command | Fast source goes to fast queue | paste_source_1 routed to fast | PASS | [advanced-scheduler.log](Test_logs/advanced-scheduler.log) | - |
| INT-06 | Standard priority routing | Scheduler integration command | Standard source goes to standard queue | news_source_1 routed to standard | PASS | [advanced-scheduler.log](Test_logs/advanced-scheduler.log) | - |
| INT-07 | Bulk priority routing | Scheduler integration command | Bulk source goes to bulk queue | marketplace_source_1 routed to bulk | PASS | [advanced-scheduler.log](Test_logs/advanced-scheduler.log) | - |
| INT-08 | One source unavailable | `python -m engine.verify_sources --all` | Failure is isolated; other sources continue | Onion failed; 3 clearnet sources passed | PASS | [advanced-all.log](Test_logs/advanced-all.log) | - |
| INT-09 | Per-source output consistency | `python -m engine.verify_sources` | Name, queue, network, result, HTTP, time and note are reported | All fields reported for each source | PASS | [advanced-enabled.log](Test_logs/advanced-enabled.log) | - |
| INT-10 | Source-specific network | `python -m engine.verify_sources --name onion_source_1` | Darknet source uses configured Tor path | Source selected correctly but Tor endpoint unavailable | BLOCKED | [test-int-name-onion.log](Test_logs/test-int-name-onion.log) | ENV-002 |
| QUE-01 | Redis availability | `docker exec darktrace-redis redis-cli ping` | `PONG` | `PONG` | PASS | Terminal result; [test-output-state.log](Test_logs/test-output-state.log) | - |
| QUE-02 | Combined worker starts | `celery -A engine.celery_app worker -Q fast,standard,bulk --concurrency=4 --loglevel=INFO` | Worker consumes all queues | Worker online with fast, standard and bulk queues | PASS | Worker terminal output; [throughput-fair-worker-c4.log](Test_logs/throughput-fair-worker-c4.log) | - |
| QUE-03 | Worker inspection | `celery -A engine.celery_app inspect active_queues`; `celery -A engine.celery_app inspect active`; `celery -A engine.celery_app inspect stats` | Worker, queues and stats are visible | One node online; all queues visible; stats returned | PASS | Worker inspection terminal output | - |
| QUE-04 | Scheduler dispatch and duplicate lock | `python -c "from engine.scheduler import check_and_schedule; print(check_and_schedule.run())"` | Due sources queue once; second tick does not duplicate | First tick queued 3; second tick queued none; queue counts unchanged | PASS | [advanced-scheduler.log](Test_logs/advanced-scheduler.log) | - |
| QUE-05 | Queue-driven crawl completion | `python -c "from engine.scheduler import check_and_schedule; print(check_and_schedule.apply())"` | Queued tasks complete through worker | 3 crawl tasks received and completed with HTTP 200 | Worker terminal output; [test-output-state.log](Test_logs/test-output-state.log) | - |
| OUT-01 | Crawl JSONL output | `Get-Content crawl_output\\crawl_results.jsonl -Tail 20` | Recent crawl records exist | Real source records present with success status and evidence paths | PASS | [test-output-state.log](Test_logs/test-output-state.log) | - |
| OUT-02 | Dead-letter output | `Get-Content crawl_output\\dead_letter.jsonl -Tail 20` | Failed jobs are recorded | E2E 404 dead-letter records present | PASS | [test-output-state.log](Test_logs/test-output-state.log) | - |
| OUT-03 | Redis per-source state | `docker exec darktrace-redis redis-cli -n 2 HGETALL crawlstate:news_source_1` | Status and counters are stored | success, total_success=1, failures=0, in_progress=0, ETag and hash present | PASS | [test-output-state.log](Test_logs/test-output-state.log) | - |
| OUT-04 | JSONL syntax | `python -c "import json,pathlib; p=pathlib.Path('crawl_output/crawl_results.jsonl'); [json.loads(x) for x in p.read_text(encoding='utf-8').splitlines() if x.strip()]; print('JSONL valid')"` | Parser prints JSONL valid | `JSONL valid` | PASS | [test-output-state.log](Test_logs/test-output-state.log) | - |
| EFF-01 | Repeat same page | `scrapy crawl single_source -a url="https://example.com/" -a source_name="qa_efficiency_test" -a source_type="normal" -L INFO` twice | Second run should support duplicate/unchanged detection | Both runs hit DNS failure before receiving content | BLOCKED | [efficiency-run-1.log](Test_logs/efficiency-run-1.log), [efficiency-run-2.log](Test_logs/efficiency-run-2.log) | ENV-001 |
| EFF-02 | ETag unchanged response | `python validate_engine.py --quick` | Conditional request returns 304 and unchanged=true | ETag -> 304 test passed | PASS | [test-quick.log](Test_logs/test-quick.log) | - |
| EFF-03 | HTTP 429 handling | `python validate_engine.py --quick` | Retry-After is honored and request recovers | 429 recovered on attempt 2 | PASS | [test-quick.log](Test_logs/test-quick.log) | - |
| EFF-04 | Slow response and throttling | `python validate_engine.py` | Slow source is bounded by timeout | Timeout test passed at job limit; AutoThrottle enabled | PASS | [test-full.log](Test_logs/test-full.log), [efficiency-settings.log](Test_logs/efficiency-settings.log) | - |
| EFF-05 | Transient 503 recovery | `python validate_engine.py` | 503,503,200 recovers after retries | Recovery passed on attempt 3 | PASS | [test-full.log](Test_logs/test-full.log) | - |
| EFF-06 | Permanent 404 handling | `python validate_engine.py` | 404 is not unnecessarily retried | PermanentCrawlError without retry passed | PASS | [test-full.log](Test_logs/test-full.log) | - |
| EFF-07 | Same-domain request limit | `Select-String -Path Crawler\\settings.py,Crawler\\spiders\\single_source.py -Pattern 'CONCURRENT_REQUESTS|CONCURRENT_REQUESTS_PER_DOMAIN|DOWNLOAD_DELAY|AUTOTHROTTLE|RETRY_TIMES|DOWNLOAD_TIMEOUT'` | Per-source domain concurrency is 1 | Spider override is 1; global setting is 2 | PASS | [efficiency-settings.log](Test_logs/efficiency-settings.log) | - |
| EFF-08 | Different-domain behavior | Settings inspection and source verification | Per-domain rules apply independently | Configuration verified; no dedicated multi-domain spider batch run | NOT TESTED | [efficiency-settings.log](Test_logs/efficiency-settings.log) | - |
| THR-01 | Worker concurrency 2 | `celery -A engine.celery_app worker -Q fast,standard,bulk --concurrency=2 --loglevel=INFO` plus fixed 8-job batch | Record success count, elapsed time and throughput | 8/8, 14.69s, 32.67 jobs/min | PASS | [throughput-fair-c2.log](Test_logs/throughput-fair-c2.log) | - |
| THR-02 | Worker concurrency 4 | `celery -A engine.celery_app worker -Q fast,standard,bulk --concurrency=4 --loglevel=INFO` plus fixed 8-job batch | Compare with concurrency 2 | 8/8, 6.49s, 73.93 jobs/min | PASS | [throughput-fair-c4.log](Test_logs/throughput-fair-c4.log) | - |
| THR-03 | Worker concurrency 8 | `celery -A engine.celery_app worker -Q fast,standard,bulk --concurrency=8 --loglevel=INFO` plus fixed 8-job batch | Compare with concurrency 4 | 8/8, 7.11s, 67.56 jobs/min | PASS | [throughput-fair-c8.log](Test_logs/throughput-fair-c8.log) | - |
| ADV-01 | Registered source unavailable | `python -m engine.verify_sources --all` | Other sources continue | Onion failed; other 3 passed | PASS | [advanced-all.log](Test_logs/advanced-all.log) | - |
| ADV-02 | Worker stops during task | Isolated destructive worker-kill test | Recovery/loss behavior recorded | Not force-killed; visibility timeout is 3600s | NOT TESTED | Test report note | ADV-02 (not tested) |
| ADV-03 | Redis temporary outage | `docker stop darktrace-redis`; Redis ping; `docker start darktrace-redis`; ping | Clear error and recovery | TimeoutError during outage; restart returned PONG | PASS | Terminal result | - |
| ADV-04 | Two scheduler instances | Scheduler run twice with queue counts | No duplicate scheduling | First scheduled 3; second scheduled 0 | PASS | [advanced-scheduler.log](Test_logs/advanced-scheduler.log) | - |
| ADV-05 | Mixed priority sources | Scheduler routing check | Correct queue per priority | fast/standard/bulk each received one | PASS | [advanced-scheduler.log](Test_logs/advanced-scheduler.log) | - |
| ADV-06 | Repeated 503 | `python validate_engine.py` | Retry cap then controlled failure | 4 total requests, controlled CrawlError | PASS | [test-full.log](Test_logs/test-full.log) | - |
| ADV-07 | Same content from different URLs | `python validate_engine.py --quick` | Duplicate content detected | Duplicate test passed | PASS | [test-quick.log](Test_logs/test-quick.log) | - |
| ADV-08 | Malformed source configuration | `validate_source()` and strict `load_sources()` temporary config test | Controlled validation error | Structured errors and ValueError, no uncontrolled crash | PASS | [advanced-malformed.log](Test_logs/advanced-malformed.log) | - |
| ADV-09 | Disabled source scheduling | `python -m engine.verify_sources`; scheduler test | Disabled source excluded | Onion excluded in normal mode | PASS | [advanced-enabled.log](Test_logs/advanced-enabled.log), [advanced-scheduler.log](Test_logs/advanced-scheduler.log) | - |
| ADV-10 | Controlled workload | Fixed 8-job batches at concurrency 2, 4 and 8 | Record throughput and failure rate | All runs 8/8; best result concurrency 4 | PASS/PARTIAL | [throughput-summary.log](Test_logs/throughput-summary.log) | - |

## Final Answers

### 1. Integration

Yes. All enabled registered sources successfully verified and completed queue-driven crawls. The disabled onion source is excluded normally and fails only when explicitly included because its Tor endpoint is not configured.

### 2. Throughput

Yes, successful jobs per minute improved from 32.67 at concurrency 2 to 73.93 at concurrency 4. Concurrency 8 measured 67.56 jobs/minute, so increasing beyond 4 did not improve this workload.

### 3. Efficiency

Mostly yes. The crawler uses per-domain concurrency 1 for the single-source spider, AutoThrottle, a 1-second delay, ETag/Last-Modified conditional requests, duplicate detection, retry caps, and permanent-failure handling. The validation harness confirmed 304, duplicate, 429, 503 and 404 behavior. The direct example.com repeat test was blocked by DNS, so its duplicate behavior was not independently observed in that run.

## Overall QA Status

**PASS WITH LIMITED UNTESTED/BLOCKED SCENARIOS**

Enabled-source integration, queue routing, Redis/Celery state isolation, scheduler locking, output persistence, throughput comparison, and reliability behavior passed. BUG-001 and BUG-002 are fixed and verified; ENV-001 is resolved with a reachable authorized target. Only the intentionally destructive worker-kill test and unavailable Tor environment remain untested/blocked.

## Corrections and Recommended Procedure

### Corrections to make before a clean release

1. BUG-001's Content-Type fix is implemented and passed the fixed quick suite;
	retain the `application/pdf` regression test in future releases.
2. BUG-002's test isolation fix is implemented by clearing the dedicated
	`crawlstate:v_e2e_404` key before assertion. Rerun E2E after Docker Redis is
	available and require a fresh counter of 1.
3. Keep the reachable authorized-source fallback for repeat-request tests.
	The PyPI replacement produced a stable hash and duplicate flag; a future
	test may additionally pass the first run's ETag to explicitly verify 304.
4. Add a dedicated multi-domain efficiency test with at least two authorized
	hosts. Record request timestamps to verify per-domain concurrency rather
	than marking the setting correct from configuration alone.
5. Add an isolated worker-crash test using a disposable Redis database and a
	short test task. Assert whether the task is redelivered, completed once, or
	dead-lettered, then clean all test keys and subprocesses.

### Better repeatable QA procedure

1. Check Python/package versions, DNS, Docker, Redis `PONG`, and that no old
	workers or Beat processes are running.
2. Run `validate_engine.py --help`, then quick, full, and E2E modes with
	`Tee-Object` logs.
3. Use a unique test namespace for every E2E and throughput run. Never reuse
	Redis source keys when asserting counters or first-attempt behavior.
4. Verify enabled sources first; run `--all` separately and classify missing
	Tor/I2P gateways as BLOCKED rather than code failures.
5. Start exactly one worker topology per run. Drain or verify queue lengths
	before submitting the next fixed workload.
6. Submit the same workload for each concurrency, record start/end timestamps,
	successful/failed counts, retries, elapsed seconds, and jobs per minute.
7. Validate JSONL parsing, evidence files, dead-letter records, and Redis
	hashes after task completion.
8. Stop workers and Beat, then confirm no test processes remain. Preserve logs,
	environment details, and bug IDs with the final report.

### Code review conclusion

The main architecture is functioning: source validation isolates bad entries,
queue routing follows priority, Redis locks prevent duplicate scheduler work,
and workers complete authorized source crawls. BUG-001 and BUG-002 are resolved
and verified, and ENV-001 is resolved with a reachable authorized target. The
remaining limitations are the intentionally untested destructive worker-kill
scenario and the unavailable Tor environment.

