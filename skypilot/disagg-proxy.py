#!/usr/bin/env python3
"""Disaggregated prefill/decode proxy for the FUSE-backed vLLM pair.

Adapted from vLLM's own benchmarks/disagg_benchmarks/disagg_prefill_proxy_server.py,
with three changes that matter on this cluster:

  1. stdlib only (urllib + http.server). The upstream version needs quart and
     aiohttp; this runs from a bare `python3` on a laptop or in any pod.
  2. Endpoints are configurable, because the two halves are separate SkyServe
     services on separate load balancers rather than two ports on localhost.
  3. It reports per-phase timing in the `X-Disagg-*` response headers, so a
     caller can see the split without reading engine logs.

The protocol is the whole trick, and it is smaller than people expect:

    prefill engine  <- the request with max_tokens forced to 1
                       (computes the KV for the prompt, writes it to the
                        shared storage path, emits one throwaway token)
    decode  engine  <- the ORIGINAL request
                       (same prompt => same sha256 of prompt tokens => the
                        connector finds the KV already on disk, skips prefill,
                        and goes straight to decoding)

Both engines run kv_role: kv_both. That looks wrong and is not -- see
README-disagg.md. The roles here are enforced by ROUTING, not by a flag.
"""
import json
import os
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

PREFILL = os.environ.get("PREFILL_URL", "http://localhost:30002")
DECODE = os.environ.get("DECODE_URL", "http://localhost:30003")
PORT = int(os.environ.get("PROXY_PORT", "9000"))
TIMEOUT = float(os.environ.get("PROXY_TIMEOUT", "600"))


def post(url, payload):
    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=TIMEOUT) as r:
        return r.status, r.read()


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):
        print("[proxy] " + fmt % args, flush=True)

    def do_POST(self):
        if not self.path.startswith("/v1/"):
            self.send_error(404, "only /v1/* is proxied")
            return
        body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
        try:
            original = json.loads(body)
        except json.JSONDecodeError as e:
            self.send_error(400, f"bad JSON: {e}")
            return

        # --- phase 1: prefill ------------------------------------------------
        # max_tokens=1 is what makes this a prefill: the engine must compute
        # the full prompt KV to emit even one token, and the connector saves
        # that KV on the way out. The token itself is discarded.
        prefill_req = dict(original)
        prefill_req["max_tokens"] = 1
        prefill_req.pop("stream", None)
        t0 = time.time()
        try:
            pstatus, pbody = post(f"{PREFILL}{self.path}", prefill_req)
        except (urllib.error.URLError, OSError) as e:
            # Fail loudly. Silently falling through to decode would still
            # return a correct answer -- decode would just prefill it itself --
            # and the disaggregation would be silently not happening.
            self.send_error(502, f"prefill unreachable: {e}")
            return
        t_prefill = time.time() - t0
        if pstatus != 200:
            self.send_error(502, f"prefill returned {pstatus}: {pbody[:200]!r}")
            return

        # --- phase 2: decode -------------------------------------------------
        # The original request, unmodified. Same prompt tokens => same hash =>
        # the KV written in phase 1 is found on the shared mount.
        t1 = time.time()
        try:
            dstatus, dbody = post(f"{DECODE}{self.path}", original)
        except (urllib.error.URLError, OSError) as e:
            self.send_error(502, f"decode unreachable: {e}")
            return
        t_decode = time.time() - t1

        self.send_response(dstatus)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(dbody)))
        self.send_header("X-Disagg-Prefill-Ms", f"{t_prefill * 1000:.1f}")
        self.send_header("X-Disagg-Decode-Ms", f"{t_decode * 1000:.1f}")
        self.end_headers()
        self.wfile.write(dbody)

    def do_GET(self):
        if self.path == "/healthz":
            out = json.dumps({"prefill": PREFILL, "decode": DECODE}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(out)))
            self.end_headers()
            self.wfile.write(out)
        else:
            self.send_error(404)


if __name__ == "__main__":
    print(f"[proxy] prefill={PREFILL}  decode={DECODE}  listening on :{PORT}", flush=True)
    ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()
