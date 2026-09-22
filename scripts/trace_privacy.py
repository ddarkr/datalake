#!/usr/bin/env python3
"""Remove OTLP span events/links before Alloy's durable queue.

Alloy 1.19.2 cannot clear typed span arrays with OTTL: set(..., nil) is a
no-op, and [] is not a typed span slice. This internal-only JSON hop keeps
span identity, timestamps, attributes and counts intact; Alloy applies the
metadata allowlist afterwards. No request or response bodies are logged.
"""

import gzip
import io
import json
import socket
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, HTTPServer

MAX_BODY = 16 * 1024 * 1024
UPSTREAM = "http://alloy:4319/v1/traces"


def redact(payload):
    envelope = json.loads(payload)
    for resource in envelope.get("resourceSpans", []):
        for scope in resource.get("scopeSpans", []):
            for span in scope.get("spans", []):
                span.pop("events", None)
                span.pop("links", None)
    return json.dumps(envelope, separators=(",", ":"), allow_nan=False).encode()


class Handler(BaseHTTPRequestHandler):
    def setup(self):
        super().setup()
        self.connection.settimeout(10)

    def log_message(self, *_args):
        pass

    def respond(self, status, body=b"", content_type="application/json"):
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        self.respond(200 if self.path == "/health" else 404)

    def do_POST(self):
        if self.path != "/v1/traces":
            self.respond(404)
            return
        authorization = self.headers.get("Authorization", "")
        if not authorization.startswith("Basic "):
            self.respond(401)
            return
        if self.headers.get("Transfer-Encoding"):
            self.respond(400)
            return
        if self.headers.get_content_type() != "application/json":
            self.respond(415)
            return
        try:
            length = int(self.headers.get("Content-Length", "-1"))
            if length < 0 or length > MAX_BODY:
                self.respond(413)
                return
            body = self.rfile.read(length)
            if len(body) != length:
                self.respond(400)
                return
            encoding = self.headers.get("Content-Encoding", "identity")
            if encoding == "gzip":
                with gzip.GzipFile(fileobj=io.BytesIO(body)) as compressed:
                    body = compressed.read(MAX_BODY + 1)
            elif encoding != "identity":
                self.respond(415)
                return
            if len(body) > MAX_BODY:
                self.respond(413)
                return
            body = redact(body)
        except (ValueError, TypeError, AttributeError, RecursionError, OSError, EOFError):
            self.respond(400)
            return
        request = urllib.request.Request(
            UPSTREAM, data=body,
            headers={"Content-Type": "application/json", "Authorization": authorization},
        )
        try:
            with urllib.request.urlopen(request, timeout=10) as response:
                status = response.status
                result = response.read(65537)
                content_type = response.headers.get_content_type()
            if len(result) > 65536:
                self.respond(502)
                return
            self.respond(status, result, content_type)
        except urllib.error.HTTPError as error:
            # Preserve retryable status, never reflect upstream diagnostics.
            status = error.code
            error.close()
            self.respond(status)
        except (urllib.error.URLError, TimeoutError, socket.timeout, OSError):
            self.respond(503)


if __name__ == "__main__":
    # ponytail: one in-flight trace request. Remove this hop when pinned Alloy
    # supports native typed-array clearing; keep the wire-level privacy check.
    with HTTPServer(("0.0.0.0", 4320), Handler) as server:
        print("Trace privacy filter listening on internal port 4320", flush=True)
        server.serve_forever()
