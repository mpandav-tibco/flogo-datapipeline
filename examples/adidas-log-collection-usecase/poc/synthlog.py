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
  SIDECAR_EVERY   1 in N pods also runs an istio-proxy-style sidecar whose
                  lines never match the BW timestamp (default 0 = none)
  PAYLOAD_EVERY   1 in N events logs a payload of PAYLOAD_BYTES (default 0 = none);
                  lines over 16 KiB are written as CRI partial (P) chunks,
                  exactly like containerd
  PAYLOAD_BYTES   payload size (default 40000)
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
SIDECAR_EVERY = int(E.get("SIDECAR_EVERY", "0"))
PAYLOAD_EVERY = int(E.get("PAYLOAD_EVERY", "0"))
PAYLOAD_BYTES = int(E.get("PAYLOAD_BYTES", "40000"))
CRI_MAX = 16384  # containerd splits longer lines into partial (P) chunks

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


class LogFile:
    """One container's CRI log (0.log) with kubelet-style rotation."""

    def __init__(self, d):
        self.dir = d
        os.makedirs(d, exist_ok=True)
        self.path = os.path.join(d, "0.log")
        self.f = open(self.path, "ab", buffering=0)
        self.size = 0
        self.last_rotate = 0.0

    def write(self, data):
        self.f.write(data)
        self.size += len(data)
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


def cri(stream, text):
    """CRI line(s) for one log entry; long ones split into P chunks."""
    now = datetime.now(timezone.utc)
    ts = "%s.%09dZ" % (now.strftime("%Y-%m-%dT%H:%M:%S"), now.microsecond * 1000)
    b = text.encode()
    out = []
    while len(b) > CRI_MAX:
        out.append(b"%s %s P %s\n" % (ts.encode(), stream.encode(), b[:CRI_MAX]))
        b = b[CRI_MAX:]
    out.append(b"%s %s F %s\n" % (ts.encode(), stream.encode(), b))
    return b"".join(out)


class Container:
    def __init__(self, idx):
        ns = "%s-team-%02d" % (PREFIX, idx % NAMESPACES + 1)
        app = APPS[idx % len(APPS)]
        # Unique per node: the node name is folded into the ReplicaSet hash.
        self.pod = "%s-%s-%s" % (app, rid(9), rid(5))
        self.app = app.replace("-", "_")
        self.poddir = os.path.join(ROOT, "%s_%s_%s" % (ns, self.pod, "%08x-%s" % (random.getrandbits(32), NODE)))
        self.log = LogFile(os.path.join(self.poddir, "bwapp"))
        self.inst = "%08x" % random.getrandbits(32)
        # istio-proxy-style sidecar in the same pod: its own verifiable instance
        self.side = LogFile(os.path.join(self.poddir, "istio-proxy")) if SIDECAR_EVERY and idx % SIDECAR_EVERY == 0 else None
        self.side_inst = "%08x" % random.getrandbits(32)
        self.seq = 0
        self.done_at = None

    def emit(self):
        self.seq += 1
        now = datetime.now(timezone.utc)
        ts = now.strftime("%Y-%m-%dT%H:%M:%S,") + "%03d" % (now.microsecond // 1000)
        stream = "stderr" if self.seq % STDERR_EVERY == 0 else "stdout"
        thread = "bwEngThread:In-Memory Process Worker-%d" % random.randint(1, 8)
        logger = "com.tibco.bw.%s.HTTPReceiver" % self.app
        if self.seq == TOTAL:
            lines = [cri(stream, "%s INFO  [%s] %s - EMIT_DONE total=%d [seq=%d inst=%s]" % (ts, thread, logger, TOTAL, self.seq, self.inst))]
        elif self.seq % ERROR_EVERY == 0:
            lines = [cri(stream, "%s ERROR [%s] %s - Unhandled fault [seq=%d inst=%s lines=%d]" % (ts, thread, logger, self.seq, self.inst, len(TRACE)))]
            lines += [cri(stream, t) for t in TRACE]
        elif PAYLOAD_EVERY and self.seq % PAYLOAD_EVERY == 0:
            # payload logging; the verifier checks the Q-run arrived whole
            lines = [cri("stdout", "%s INFO  [%s] %s - Payload %s [seq=%d inst=%s bytes=%d]" % (ts, thread, logger, "Q" * PAYLOAD_BYTES, self.seq, self.inst, PAYLOAD_BYTES))]
        else:
            lines = [cri(stream, "%s INFO  [%s] %s - Responded 200 in %dms [seq=%d inst=%s]" % (ts, thread, logger, random.randint(3, 180), self.seq, self.inst))]
        self.log.write(b"".join(lines))
        if self.side is not None:
            done = " EMIT_DONE total=%d" % TOTAL if self.seq == TOTAL else ""
            self.side.write(cri("stdout", '[%s] "GET /api/orders/%d HTTP/1.1" 200 - via_upstream - "-" 0 512 %d 3 "-"%s [seq=%d inst=%s]' % (
                now.strftime("%Y-%m-%dT%H:%M:%S.") + "%03dZ" % (now.microsecond // 1000), self.seq, random.randint(1, 90), done, self.seq, self.side_inst)))
        if self.seq >= TOTAL:
            self.done_at = time.time()

    def instances(self):
        return 2 if self.side is not None else 1

    def close(self):
        self.log.f.close()
        if self.side is not None:
            self.side.f.close()

    def delete(self):
        self.close()
        shutil.rmtree(self.poddir, ignore_errors=True)


def main():
    os.makedirs(ROOT, exist_ok=True)
    live = [Container(i) for i in range(CONTAINERS)]
    finished, churned = [], 0
    idx = CONTAINERS
    instances = sum(c.instances() for c in live)
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
                        nc = Container(idx)
                        live.append(nc)
                        instances += nc.instances()
                        idx += 1
                else:
                    c.close()
        dt = time.time() - t0
        if dt < tick:
            time.sleep(tick - dt)
    # containers= is the number of verifiable instances (BW apps + sidecars)
    print("synthlog: node=%s containers=%d pods=%d churned=%d elapsed=%.0fs" % (NODE, instances, idx, churned, time.time() - started), flush=True)
    while True:
        time.sleep(3600)


if __name__ == "__main__":
    main()
