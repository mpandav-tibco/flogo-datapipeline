#!/usr/bin/env python3
"""
BW6/BWCE console-log emitter (arm64-native stand-in for mpandav/demo-rest-app).

Why this exists: TIBCO BusinessWorks Container Edition 6.12 ships an x86-64-only
JRE and native launcher, so the real EAR image cannot run on the arm64 minikube
nodes used for this demo (see REPORT.md). This emitter reproduces the *exact*
console-log shape a BWCE AppNode writes to stdout — including periodic multiline
Java stack traces — so the flogo-datapipeline collector is exercised end to end
(multiline stitching, regex parse, enrichment, S3 fan-out) at realistic volume.

Kubernetes/containerd writes this process's stdout to
/var/log/pods/<ns>_<pod>_<uid>/<container>/*.log in CRI format, which the
collector DaemonSet tails.

Log line format (matches the collector's parse regex):
  YYYY-MM-DD HH:MM:SS.mmm LEVEL [thread] logger - message

Environment:
  APP_NAME       logical BW app name          (default: derived from HOSTNAME)
  NAMESPACE      k8s namespace (for context)  (default: $POD_NAMESPACE or "unknown")
  RATE_PER_SEC   request log lines per second (default: 5)
  ERROR_EVERY    ~1 error+stacktrace per N req (default: 40)
  EMIT_TOTAL     stop after this many log events and idle (default: 0 = forever)
  EXIT_WHEN_DONE exit instead of idling once EMIT_TOTAL is reached (default: false)

Verifiability (for the no-loss proof): every log event ends with
"[seq=<n> inst=<id>]" — a per-process sequence and a random instance id — and
a stack-trace event's first line also carries "lines=<k>", the number of
continuation lines that must be stitched into it. When EMIT_TOTAL is set the
last event is "EMIT_DONE total=<n>", so a verifier reading the destination
knows exactly which events must be there.
"""
import os
import random
import sys
import time
from datetime import datetime, timezone

APP_NAME = os.environ.get("APP_NAME") or os.environ.get("HOSTNAME", "Rest_Sample")
NAMESPACE = os.environ.get("NAMESPACE") or os.environ.get("POD_NAMESPACE", "unknown")
RATE = max(1, int(os.environ.get("RATE_PER_SEC", "5")))
ERROR_EVERY = max(1, int(os.environ.get("ERROR_EVERY", "40")))
TOTAL = int(os.environ.get("EMIT_TOTAL", "0"))
EXIT_WHEN_DONE = os.environ.get("EXIT_WHEN_DONE", "").lower() in ("1", "true", "yes")
INST = "%08x" % random.getrandbits(32)
SEQ = 0

APP = APP_NAME.replace("-", "_")

ENDPOINTS = [
    ("GET", "/v1/customers"),
    ("GET", "/v1/customers/{id}"),
    ("POST", "/v1/orders"),
    ("GET", "/v1/orders/{id}"),
    ("PUT", "/v1/inventory/{sku}"),
    ("GET", "/v1/products"),
    ("GET", "/v1/products/{sku}"),
    ("POST", "/v1/shipments"),
    ("GET", "/v1/health"),
]
PROCESSES = ["HTTPReceiver", "OrderProcess", "InventoryLookup", "CustomerQuery", "ShipmentDispatch"]
CLIENT_IPS = ["10.244.0.%d" % n for n in range(11, 60)]

STACKTRACES = [
    [
        "com.tibco.bw.core.BWEngineException: Activity [JDBCQuery] failed: connection timed out",
        "\tat com.tibco.bw.core.runtime.ActivityFacade.execute(ActivityFacade.java:212)",
        "\tat com.tibco.bw.palette.sqldb.query.QueryActivity.eval(QueryActivity.java:98)",
        "\tat com.tibco.bw.core.runtime.JobImpl.executeStep(JobImpl.java:531)",
        "\tat java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1136)",
        "\tat java.base/java.lang.Thread.run(Thread.java:840)",
        "Caused by: java.sql.SQLException: Connection timed out after 30000ms",
        "\tat com.tibco.dv.jdbc.pool.PooledConnection.acquire(PooledConnection.java:144)",
        "\t... 7 more",
    ],
    [
        "com.tibco.bw.core.rest.RestFaultException: 502 Bad Gateway from downstream https://inventory:8443/v1/inventory",
        "\tat com.tibco.bw.palette.rest.RestInvokeActivity.invoke(RestInvokeActivity.java:301)",
        "\tat com.tibco.bw.core.runtime.ActivityFacade.execute(ActivityFacade.java:212)",
        "\tat com.tibco.bw.core.runtime.JobImpl.executeStep(JobImpl.java:531)",
        "\tat java.base/java.lang.Thread.run(Thread.java:840)",
    ],
    [
        "java.lang.NullPointerException: Cannot read field \"total\" because \"order\" is null",
        "\tat com.tibco.bw.Rest_Sample.OrderProcess.computeTotal(OrderProcess.java:77)",
        "\tat com.tibco.bw.core.runtime.ActivityFacade.execute(ActivityFacade.java:212)",
        "\tat java.base/java.lang.Thread.run(Thread.java:840)",
    ],
]


def ts():
    now = datetime.now(timezone.utc)
    # Real BWCE console timestamp: ISO date, 'T' separator, comma milliseconds.
    return now.strftime("%Y-%m-%dT%H:%M:%S,") + "%03d" % (now.microsecond // 1000)


class Done(Exception):
    pass


def next_seq():
    global SEQ
    SEQ += 1
    return SEQ


def emit(level, thread, process, msg):
    seq = next_seq()
    if TOTAL and seq == TOTAL:
        level, msg = "INFO", "EMIT_DONE total=%d" % TOTAL
    logger = "com.tibco.bw.%s.%s" % (APP, process)
    sys.stdout.write("%s %-5s [%s] %s - %s [seq=%d inst=%s]\n" % (ts(), level, thread, logger, msg, seq, INST))
    if TOTAL and seq >= TOTAL:
        raise Done()


def emit_stack(thread, process):
    if TOTAL and SEQ + 1 >= TOTAL:
        return emit("INFO", thread, process, "")
    seq = next_seq()
    logger = "com.tibco.bw.%s.%s" % (APP, process)
    trace = random.choice(STACKTRACES)
    # First line carries the timestamp/level so the collector treats the whole
    # trace (continuation lines have no leading timestamp) as one record.
    sys.stdout.write("%s %-5s [%s] %s - Unhandled fault in process instance %d [seq=%d inst=%s lines=%d]\n"
                     % (ts(), "ERROR", thread, logger, random.randint(1000, 9999), seq, INST, len(trace)))
    for line in trace:
        sys.stdout.write(line + "\n")


def main():
    # Startup banner (mirrors a BWCE AppNode boot).
    boot = "bwEngThread:startup"
    emit("INFO", boot, "HTTPReceiver", "TIBCO BusinessWorks Container Edition 6.12.0 starting AppNode for application %s" % APP_NAME)
    emit("INFO", boot, "HTTPReceiver", "Loaded module %s v1.0.0; deploying application archive" % APP)
    emit("INFO", boot, "HTTPReceiver", "HTTP Connector bound on 0.0.0.0:8080 (basePath /v1)")
    emit("INFO", boot, "HTTPReceiver", "Application [%s] started successfully. Ready to accept requests." % APP_NAME)

    delay = 1.0 / RATE
    n = 0
    while True:
        n += 1
        thread = "bwEngThread:In-Memory Process Worker-%d" % random.randint(1, 8)
        method, path = random.choice(ENDPOINTS)
        proc = random.choice(PROCESSES)
        ip = random.choice(CLIENT_IPS)
        rid = "%08x" % random.getrandbits(32)

        emit("INFO", thread, "HTTPReceiver", "Received %s %s from %s [reqId=%s]" % (method, path, ip, rid))
        if random.random() < 0.6:
            emit("DEBUG", thread, proc, "Invoking process %s for reqId=%s" % (proc, rid))

        if n % ERROR_EVERY == 0:
            emit("WARN", thread, proc, "Downstream latency high (1200ms), applying retry policy [reqId=%s]" % rid)
            emit_stack(thread, proc)
            emit("INFO", thread, "HTTPReceiver", "Responded 500 in 1240ms [reqId=%s]" % rid)
        else:
            status = 200 if random.random() < 0.92 else random.choice([201, 400, 404])
            emit("INFO", thread, "HTTPReceiver", "Responded %d in %dms [reqId=%s]" % (status, random.randint(3, 180), rid))

        sys.stdout.flush()
        time.sleep(delay)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
    except Done:
        sys.stdout.flush()
        if not EXIT_WHEN_DONE:
            while True:  # keep the pod (and its log file) alive
                time.sleep(3600)
