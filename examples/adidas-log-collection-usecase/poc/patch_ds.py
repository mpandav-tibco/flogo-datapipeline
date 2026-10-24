#!/usr/bin/env python3
"""Adapt k8s/40-collector-daemonset.yaml (stdin → stdout) for the POC cluster:
image tag, local image pull policy and, with SYNTH_ROOT, an extra read-only
hostPath mount for the synthetic scale-test log root."""
import os
import sys

y = sys.stdin.read()
y = y.replace("image: flogo-datapipeline:arm64-local", "image: " + os.environ["COLLECTOR_IMG"])
y = y.replace("imagePullPolicy: IfNotPresent", "imagePullPolicy: Never")
root = os.environ.get("SYNTH_ROOT", "")
if root:
    y = y.replace(
        "            - name: collector-state\n              mountPath: /var/lib/flogo-datapipeline\n",
        "            - name: collector-state\n              mountPath: /var/lib/flogo-datapipeline\n"
        "            - name: synth-logs\n              mountPath: %s\n              readOnly: true\n" % root)
    y = y.replace(
        "        - name: pipeline\n          configMap:\n",
        "        - name: synth-logs\n          hostPath:\n            path: %s\n            type: DirectoryOrCreate\n"
        "        - name: pipeline\n          configMap:\n" % root)
sys.stdout.write(y)
