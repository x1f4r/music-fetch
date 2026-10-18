#!/usr/bin/env python3
"""Serve a directory on 127.0.0.1 with CORS open (for tests/hum_parity.rs in browsers).

    cors-server.py <dir> <port>
"""
import functools
import http.server
import sys


class Handler(http.server.SimpleHTTPRequestHandler):
    def end_headers(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        super().end_headers()

    def do_OPTIONS(self):
        # Firefox sends the engine's User-Agent header (Chrome drops it), which needs a
        # preflight from a web page. The extension itself is exempt from CORS.
        self.send_response(204)
        self.send_header("Access-Control-Allow-Methods", "GET, HEAD")
        self.send_header("Access-Control-Allow-Headers", self.headers.get("Access-Control-Request-Headers", "*"))
        self.send_header("Access-Control-Max-Age", "600")
        self.end_headers()

    def log_message(self, *args):
        pass


if __name__ == "__main__":
    directory, port = sys.argv[1], int(sys.argv[2])
    server = http.server.ThreadingHTTPServer(("127.0.0.1", port), functools.partial(Handler, directory=directory))
    server.serve_forever()
