#!/usr/bin/env python3
"""Tiny enrichment endpoint for the lookup-transform benchmark case.

GET /enrich/<region> -> {"region_name": ..., "region_tier": ...}

The lookup transform keys on the low-cardinality `region` column, so its LRU
cache is warm after a handful of requests and the 53M-row case stays fast.
Run it in the background for the duration of the etl11 case:

  python3 harness/lookup_mock.py 9099 &
"""
import http.server
import json
import sys

META = {
    "NORTH":   {"region_name": "Northern Region",  "region_tier": "tier-1"},
    "SOUTH":   {"region_name": "Southern Region",  "region_tier": "tier-2"},
    "EAST":    {"region_name": "Eastern Region",   "region_tier": "tier-1"},
    "WEST":    {"region_name": "Western Region",   "region_tier": "tier-3"},
    "CENTRAL": {"region_name": "Central Region",   "region_tier": "tier-2"},
}


class Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        region = self.path.rstrip("/").split("/")[-1]
        body = json.dumps(META.get(region, {"region_name": "Unknown", "region_tier": "tier-0"})).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass  # quiet


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 9099
    httpd = http.server.ThreadingHTTPServer(("127.0.0.1", port), Handler)
    print("enrichment mock on http://127.0.0.1:%d/enrich/<region>" % port)
    httpd.serve_forever()
