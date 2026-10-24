#!/usr/bin/env python3
"""Synthetic node log writer for collector scale tests.

Simulates what kubelet + containerd do on a node for many containers at once,
without running that many pods: CRI-format log files under
<root>/<namespace>_<pod>_<uid>/<container>/0.log, kubelet-style rotation
(rename to 0.log.<YYYYMMDD-hhmmss>, gzip the older rotations, keep at most
MAX_FILES files), interleaved stdout/stderr with multiline Java stack traces,
and pod churn (finished pods are deleted and replaced by new ones).

Every log event carries [seq=<n> inst=<id>] (stack traces also lines=<k>) and
the last one is EMIT_DONE total=<n> — the format poc/verify.py proves.

Environment:
  ROOT            log root (default /var/log/synthpods)
  PREFIX          scenario prefix for namespace names (default s8)
  NODE            node name (default $NODE_NAME) — keeps pod names unique per node
  NAMESPACES      namespaces (default 24)
  CONTAINERS      concurrent containers on this node (default 400)
  RATE            events/s per container (default 8)
  TOTAL           events per container (default 3000)
  ROTATE_BYTES    rotate a container's log at this size (default 131072)
  MAX_FILES       kubelet containerLogMaxFiles (default 5)
  CHURN           fraction of finished containers deleted and replaced (default 0.25)
  DELETE_AFTER    seconds after EMIT_DONE before a churned pod's logs are deleted (default 5)
  STDERR_EVERY    1 in N events goes to stderr (default 10)
  ERROR_EVERY     1 in N events is an ERROR with a stack trace (default 25)
"""
import gzip
import os
import random
import shutil
import sys
import time
from datetime import datetime, timezone

E = os.environ
ROOT = E.get("ROOT", "/var/log/synthpods")
PREFIX = E.get("PREFIX", "s8")
NODE = E.get("NODE") or E.get("NODE_NAME", "node")
NAMESPACES = int(E.get("NAMESPACES", "24"))
CONTAINERS = int(E.get("CONTAINERS", "400"))
RATE = float(E.get("RATE", "8"))
TOTAL = int(E.get("TOTAL", "3000"))
ROTATE_BYTES = int(E.get("ROTATE_BYTES", "131072"))
MAX_FILES = int(E.get("MAX_FILES", "5"))
CHURN = float(E.get("CHURN", "0.25"))
DELETE_AFTER = float(E.get("DELETE_AFTER", "5"))
STDERR_EVERY = int(E.get("STDERR_EVERY", "10"))
ERROR_EVERY = int(E.get("ERROR_EVERY", "25"))

ALPHA = "bcdfghjklmnpqrstvwxz2456789"
APPS = ["order-service", "inventory-service", "customer-service", "payment-service", "shipment-service",
        "notification-service", "pricing-service", "catalog-service", "returns-service", "loyalty-service"]
TRACE = ["com.tibco.bw.core.BWEngineException: Activity [JDBCQuery] failed: connection timed out",
         "\tat com.tibco.bw.core.runtime.ActivityFacade.execute(ActivityFacade.java:212)",
         "\tat com.tibco.bw.palette.sqldb.query.QueryActivity.eval(QueryActivity.java:98)",
         "\tat java.base/java.lang.Thread.run(Thread.java:840)",
         "Caused by: java.sql.SQLException: Connection timed out after 30000ms",
         "\t... 7 more"]


def rid(n):
    return "".join(random.choice(ALPHA) for _ in range(n))


class Container:
    def __init__(self, idx):
        ns = "%s-team-%02d" % (PREFIX, idx % NAMESPACES + 1)
        app = APPS[idx % len(APPS)]
        # Unique per node: the node name is folded into the ReplicaSet hash.
        self.pod = "%s-%s-%s" % (app, rid(9), rid(5))
        self.app = app.replace("-", "_")
        self.dir = os.path.join(ROOT, "%s_%s_%s" % (ns, self.pod, "%08x-%s" % (random.getrandbits(32), NODE)), "bwapp")
        os.makedirs(self.dir, exist_ok=True)
        self.path = os.path.join(self.dir, "0.log")
        self.f = open(self.path, "ab", buffering=0)
        self.size = 0
        self.inst = "%08x" % random.getrandbits(32)
        self.seq = 0
        self.done_at = None
        self.last_rotate = 0.0

    def cri(self, stream, text):
        now = datetime.now(timezone.utc)
        return "%s.%09dZ %s F %s\n" % (now.strftime("%Y-%m-%dT%H:%M:%S"), now.microsecond * 1000, stream, text)

    def emit(self):
        self.seq += 1
        now = datetime.now(timezone.utc)
        ts = now.strftime("%Y-%m-%dT%H:%M:%S,") + "%03d" % (now.microsecond // 1000)
        stream = "stderr" if self.seq % STDERR_EVERY == 0 else "stdout"
        thread = "bwEngThread:In-Memory Process Worker-%d" % random.randint(1, 8)
        logger = "com.tibco.bw.%s.HTTPReceiver" % self.app
        if self.seq == TOTAL:
            lines = [self.cri(stream, "%s INFO  [%s] %s - EMIT_DONE total=%d [seq=%d inst=%s]" % (ts, thread, logger, TOTAL, self.seq, self.inst))]
        elif self.seq % ERROR_EVERY == 0:
            lines = [self.cri(stream, "%s ERROR [%s] %s - Unhandled fault [seq=%d inst=%s lines=%d]" % (ts, thread, logger, self.seq, self.inst, len(TRACE)))]
            lines += [self.cri(stream, t) for t in TRACE]
        else:
            lines = [self.cri(stream, "%s INFO  [%s] %s - Responded 200 in %dms [seq=%d inst=%s]" % (ts, thread, logger, random.randint(3, 180), self.seq, self.inst))]
        data = "".join(lines).encode()
        self.f.write(data)
        self.size += len(data)
        if self.seq >= TOTAL:
            self.done_at = time.time()
        if self.size >= ROTATE_BYTES and time.time() - self.last_rotate >= 1.1:
            self.rotate()

    def rotate(self):
        """kubelet: rename the live file, reopen, gzip older rotations, trim."""
        self.last_rotate = time.time()
        ts = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
        self.f.close()
        os.rename(self.path, "%s.%s" % (self.path, ts))
        self.f = open(self.path, "ab", buffering=0)
        self.size = 0
        rotated = sorted(n for n in os.listdir(self.dir) if n.startswith("0.log.") and not n.endswith(".tmp"))
        for n in rotated[:-1]:  # all but the newest rotation are compressed
            if n.endswith(".gz"):
                continue
            src = os.path.join(self.dir, n)
            with open(src, "rb") as fi, gzip.open(src + ".gz.tmp", "wb") as fo:
                shutil.copyfileobj(fi, fo)
            os.rename(src + ".gz.tmp", src + ".gz")
            os.remove(src)
        rotated = sorted(n for n in os.listdir(self.dir) if n.startswith("0.log.") and not n.endswith(".tmp"))
        for n in rotated[:max(0, len(rotated) - (MAX_FILES - 1))]:
            os.remove(os.path.join(self.dir, n))

    def delete(self):
        self.f.close()
        shutil.rmtree(os.path.dirname(self.dir), ignore_errors=True)


def main():
    os.makedirs(ROOT, exist_ok=True)
    live = [Container(i) for i in range(CONTAINERS)]
    finished, churned = [], 0
    idx = CONTAINERS
    started = time.time()
    tick = 0.1
    budget = 0.0
    while live or finished:
        t0 = time.time()
        budget += RATE * tick
        n = int(budget)
        budget -= n
        for c in list(live):
            for _ in range(n):
                if c.seq < TOTAL:
                    c.emit()
            if c.seq >= TOTAL:
                live.remove(c)
                finished.append(c)
        now = time.time()
        for c in list(finished):
            if c.done_at and now - c.done_at >= DELETE_AFTER:
                finished.remove(c)
                if random.random() < CHURN:
                    c.delete()
                    churned += 1
                    if time.time() - started < 0.6 * TOTAL / RATE:  # replace early finishers only
                        live.append(Container(idx))
                        idx += 1
                else:
                    c.f.close()
        dt = time.time() - t0
        if dt < tick:
            time.sleep(tick - dt)
    print("synthlog: node=%s containers=%d churned=%d elapsed=%.0fs" % (NODE, idx, churned, time.time() - started), flush=True)
    while True:
        time.sleep(3600)


if __name__ == "__main__":
    main()
