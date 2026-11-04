#!/usr/bin/env python3
"""An OpenAI and Anthropic look-alike for the manual check: usage llm_upstream.py PORT [DELTAS] [DELAY]."""
import gzip
import json
import sys
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

DELTAS = int(sys.argv[2]) if len(sys.argv) > 2 else 5
DELAY = float(sys.argv[3]) if len(sys.argv) > 3 else 0.05


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *args):
        pass

    def chunk(self, text):
        data = text.encode()
        self.wfile.write(b"%x\r\n%s\r\n" % (len(data), data))
        self.wfile.flush()

    def sse(self, events):
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Transfer-Encoding", "chunked")
        self.end_headers()
        for event in events:
            self.chunk(event)
            time.sleep(DELAY)
        self.wfile.write(b"0\r\n\r\n")
        self.wfile.flush()

    def json(self, value):
        body = json.dumps(value).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        # an upstream that compresses whenever it is offered
        if "gzip" in self.headers.get("Accept-Encoding", ""):
            body = gzip.compress(body)
            self.send_header("Content-Encoding", "gzip")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        request = json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0))) or "{}")
        stream = request.get("stream", False)
        if self.path == "/v1/chat/completions":
            usage = {"prompt_tokens": 100, "completion_tokens": 20, "prompt_tokens_details": {"cached_tokens": 30}}
            if not stream:
                return self.json({"model": "gpt-x", "choices": [{"message": {"content": "hi"}}], "usage": usage})

            def events():
                for _ in range(DELTAS):
                    delta = {"model": "gpt-x", "choices": [{"delta": {"content": "word "}}], "usage": None}
                    yield "data: " + json.dumps(delta) + "\n\n"
                if request.get("stream_options", {}).get("include_usage"):
                    yield "data: " + json.dumps({"model": "gpt-x", "choices": [], "usage": usage}) + "\n\n"
                yield "data: [DONE]\n\n"

            return self.sse(events())
        if self.path == "/v1/messages":
            usage = {"input_tokens": 12, "cache_read_input_tokens": 56, "cache_creation_input_tokens": 78}
            if not stream:
                return self.json({"model": "claude-x", "content": [], "usage": {**usage, "output_tokens": 34}})

            def events():
                start = {"type": "message_start",
                         "message": {"model": "claude-x", "content": [], "usage": {**usage, "output_tokens": 1}}}
                yield "event: message_start\ndata: " + json.dumps(start) + "\n\n"
                for _ in range(DELTAS):
                    delta = {"type": "content_block_delta", "index": 0, "delta": {"type": "text_delta", "text": "word "}}
                    yield "event: content_block_delta\ndata: " + json.dumps(delta) + "\n\n"
                finish = {"type": "message_delta", "delta": {"stop_reason": "end_turn"},
                          "usage": {"input_tokens": 500, "output_tokens": 34}}
                yield "event: message_delta\ndata: " + json.dumps(finish) + "\n\n"
                yield 'event: message_stop\ndata: {"type": "message_stop"}\n\n'

            return self.sse(events())
        self.send_response(404)
        self.send_header("Content-Length", "0")
        self.end_headers()


ThreadingHTTPServer.request_queue_size = 512
ThreadingHTTPServer(("127.0.0.1", int(sys.argv[1])), Handler).serve_forever()
