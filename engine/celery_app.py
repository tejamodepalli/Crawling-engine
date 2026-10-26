"""
Celery application: Redis-backed task queue with fast/standard/bulk lanes,
per the design doc. The scheduler tick runs as a periodic (beat) task; actual
source fetches run as worker tasks routed to the queue matching the source's
configured priority.
"""
import os

from celery import Celery
from kombu import Exchange, Queue

BROKER_URL = os.environ.get("CELERY_BROKER_URL", "redis://localhost:6379/0")
RESULT_BACKEND = os.environ.get("CELERY_RESULT_BACKEND", "redis://localhost:6379/1")

app = Celery(
    "darktrace_engine",
    broker=BROKER_URL,
    backend=RESULT_BACKEND,
    include=["engine.tasks", "engine.scheduler"],
)

app.conf.task_queues = (
    Queue("fast", Exchange("fast"), routing_key="fast"),
    Queue("standard", Exchange("standard"), routing_key="standard"),
    Queue("bulk", Exchange("bulk"), routing_key="bulk"),
)
# Jobs are routed explicitly per-call (queue=source["priority"]) since routing
# depends on which source the job is for, not the task name. This default
# only matters for tasks dispatched without an explicit queue.
app.conf.task_default_queue = "standard"
app.conf.task_default_exchange = "standard"
app.conf.task_default_routing_key = "standard"

app.conf.task_acks_late = True          # don't ack until the job actually finishes
app.conf.worker_prefetch_multiplier = 1  # one job at a time per worker slot -> fairer across sources
app.conf.task_track_started = True

# --- Throughput ---------------------------------------------------------------
# Every job is "start a subprocess and wait for it" - almost pure waiting, no
# CPU. A thread pool is the right fit: one worker can run many jobs at once
# with far less memory than prefork, and it is the ONLY pool that works well on
# Windows (Celery's default prefork pool does not support Windows).
# Override with CELERY_WORKER_POOL=prefork|solo|threads|gevent if you need to.
app.conf.worker_pool = os.environ.get(
    "CELERY_WORKER_POOL",
    "threads" if os.name == "nt" else "prefork",
)

# If a worker dies mid-job, put the job back on the queue instead of losing it
# (works together with task_acks_late).
app.conf.task_reject_on_worker_lost = True

# Crawl results are only needed briefly; don't let them pile up in Redis db 1.
app.conf.result_expires = 3600

# Redis redelivers un-acked messages after visibility_timeout. It must be longer
# than the longest job; the longest job here is a few minutes.
app.conf.broker_transport_options = {"visibility_timeout": 3600}
app.conf.broker_connection_retry_on_startup = True

# Scheduler tick: check which sources are due and enqueue jobs for them.
app.conf.beat_schedule = {
    "check-and-schedule-sources": {
        "task": "engine.scheduler.check_and_schedule",
        "schedule": float(os.environ.get("SCHEDULER_TICK_SECONDS", 30)),
    },
}
app.conf.timezone = "UTC"
