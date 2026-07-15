"""Generic standard-library metrics source/target used only by P11 smoke."""
from __future__ import annotations

import os
import resource
import threading
import time
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

role = os.environ["ROLE"]
cluster = os.environ["CLUSTER"]
namespace = os.environ["POD_NAMESPACE"]
service = os.environ["SERVICE_NAME"]
target = os.environ.get("TARGET_URL")
destination = os.environ.get("DESTINATION_SERVICE", "")
protocol = "http"
lock = threading.Lock()
request_count = 0
last_latency = 0.0

def collect_requests():
    global request_count, last_latency
    while True:
        started = time.monotonic()
        try:
            with urllib.request.urlopen(target, timeout=1) as response:
                response.read()
            with lock:
                request_count += 1
                last_latency = time.monotonic() - started
        except OSError:
            pass
        time.sleep(0.2)

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path != "/metrics":
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"ok")
            return
        rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024
        labels = f'cluster="{cluster}",namespace="{namespace}",pod="{os.environ["HOSTNAME"]}",container="app",service="{service}"'
        lines = [f'proberca_smoke_process_rss_bytes{{{labels}}} {rss}']
        if role == "source":
            edge = f'cluster="{cluster}",namespace="{namespace}",source_service="{service}",destination_service="{destination}",protocol="{protocol}"'
            with lock:
                lines.extend((f'proberca_smoke_edge_rtt_seconds{{{edge}}} {last_latency}', f'proberca_smoke_requests_total{{{edge}}} {request_count}'))
        payload = ("\n".join(lines) + "\n").encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, *_args):
        return

if target:
    threading.Thread(target=collect_requests, daemon=True).start()
ThreadingHTTPServer(("0.0.0.0", 8000), Handler).serve_forever()
