#!/usr/bin/env python3
"""Generate the POC BW-app workload (Kubernetes YAML on stdout).

Every pod runs the verifiable BWCE-format emitter (workload/emitter.py): each
log event carries [seq=<n> inst=<id>] and the last one is EMIT_DONE, so the
verifier knows exactly which events must reach S3.

  poc/workload.py --prefix s1 --namespaces 12 --apps 5 --rate 10 --total 10000 | kubectl apply -f -
  poc/workload.py --prefix s5 --namespaces 3 --apps 10 --kind job --total 2000 ...

--rate is BW requests/s per pod; each request logs ~2.6 events on average
(receive, optional debug, response; plus warn + stack trace every
--error-every requests).
"""
import argparse

APPS = ["order-service", "inventory-service", "customer-service", "payment-service", "shipment-service",
        "notification-service", "pricing-service", "catalog-service", "returns-service", "loyalty-service"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--prefix", required=True, help="scenario prefix for namespace names, e.g. s1")
    ap.add_argument("--namespaces", type=int, default=6)
    ap.add_argument("--apps", type=int, default=5, help="apps (deployments) per namespace")
    ap.add_argument("--replicas", type=int, default=1)
    ap.add_argument("--rate", type=int, default=10, help="requests/s per pod (~2.6 log events each)")
    ap.add_argument("--total", type=int, default=10000, help="log events per pod (EMIT_TOTAL)")
    ap.add_argument("--error-every", type=int, default=40)
    ap.add_argument("--kind", choices=["deployment", "job"], default="deployment")
    ap.add_argument("--image", default="bwce-emitter:poc")
    a = ap.parse_args()

    docs = []
    for n in range(1, a.namespaces + 1):
        ns = "%s-team-%02d" % (a.prefix, n)
        docs.append("apiVersion: v1\nkind: Namespace\nmetadata:\n  name: %s\n  labels: { tier: apps, poc-scenario: %s }" % (ns, a.prefix))
        for i in range(a.apps):
            app = APPS[i % len(APPS)] + ("" if i < len(APPS) else "-%d" % (i // len(APPS)))
            env = ("        env:\n"
                   "          - { name: APP_NAME, value: %s }\n"
                   "          - { name: RATE_PER_SEC, value: \"%d\" }\n"
                   "          - { name: ERROR_EVERY, value: \"%d\" }\n"
                   "          - { name: EMIT_TOTAL, value: \"%d\" }\n"
                   "          - { name: EXIT_WHEN_DONE, value: \"%s\" }\n"
                   "          - name: POD_NAMESPACE\n"
                   "            valueFrom: { fieldRef: { fieldPath: metadata.namespace } }\n"
                   % (app, a.rate, a.error_every, a.total, "true" if a.kind == "job" else "false"))
            container = ("      containers:\n"
                         "      - name: bwapp\n"
                         "        image: %s\n"
                         "        imagePullPolicy: Never\n"
                         "        resources: { requests: { cpu: 10m, memory: 24Mi }, limits: { memory: 64Mi } }\n"
                         % a.image) + env
            labels = "app.kubernetes.io/name: %s, app.kubernetes.io/part-of: bw-apps, poc-scenario: %s" % (app, a.prefix)
            if a.kind == "deployment":
                docs.append(
                    "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: %s\n  namespace: %s\n"
                    "spec:\n  replicas: %d\n  selector: { matchLabels: { app.kubernetes.io/name: %s } }\n"
                    "  template:\n    metadata: { labels: { %s } }\n    spec:\n"
                    "      terminationGracePeriodSeconds: 1\n%s" % (app, ns, a.replicas, app, labels, container))
            else:
                # Short-lived pods: the Job pod exits when done and is deleted
                # shortly after, taking its log files with it.
                docs.append(
                    "apiVersion: batch/v1\nkind: Job\nmetadata:\n  name: %s\n  namespace: %s\n"
                    "spec:\n  ttlSecondsAfterFinished: 5\n  backoffLimit: 0\n"
                    "  template:\n    metadata: { labels: { %s } }\n    spec:\n"
                    "      restartPolicy: Never\n      terminationGracePeriodSeconds: 1\n%s" % (app, ns, labels, container))
    print("\n---\n".join(docs))


if __name__ == "__main__":
    main()
