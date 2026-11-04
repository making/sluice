"""A test upstream: echoes what filters change; /stream sends a chunk, waits 2s, sends another."""
import http.server, sys, time

class H(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    def log_message(self, *a): pass
    def handle_any(self):
        n = int(self.headers.get("content-length") or 0)
        body = self.rfile.read(n) if n else b""
        if self.path == "/stream":
            self.send_response(200); self.send_header("Transfer-Encoding", "chunked")
            self.send_header("X-Upstream-Private", "1"); self.end_headers()
            self.wfile.write(b"6\r\nfirst\n\r\n"); self.wfile.flush()
            time.sleep(2)
            self.wfile.write(b"7\r\nsecond\n\r\n0\r\n\r\n"); self.wfile.flush()
            return
        out = f"filtered={self.headers.get('x-filtered')} secret={self.headers.get('x-secret')} read={len(body)}\n".encode()
        self.send_response(200); self.send_header("Content-Length", str(len(out)))
        self.send_header("X-Upstream-Private", "1"); self.end_headers(); self.wfile.write(out)
    do_GET = do_POST = handle_any

http.server.ThreadingHTTPServer(("127.0.0.1", int(sys.argv[1])), H).serve_forever()
