# HTTP/1.1 relay framing

How the plaintext and TLS-terminated HTTP/1.1 relay parses heads and what it treats as
malformed.

The plaintext and TLS-terminated HTTP/1.1 relay reads request and response heads as
ISO-8859-1, so a field value carrying non-ASCII bytes (a UTF-8 header value, RFC 9110
obs-text) is routed, rewritten and tracked like any other, its bytes relayed as is. A
request it cannot follow is malformed: a head over 64KiB, a line not ended by CRLF, a bare
CR, a NUL, a malformed request line, a field line folded, whose name is not a token
(whitespace before the colon included) or whose value holds a control character other
than HTAB, more than one `Host`, a `Content-Length` that
is not one decimal number, a `Transfer-Encoding` whose final coding is not `chunked` or that
comes with a `Content-Length`, and a chunk-size line, chunk terminator or trailer section
out of the chunked syntax. Since the upstream might frame such bytes differently (request
smuggling), nothing of the malformed request or after it is relayed: once the responses to
the requests before it are relayed, the client gets `400 Bad Request` -- unless the
upstream already began answering the malformed request on its head -- and the connection is
closed, after reading what the client still sends for up to 2s, so the 400 is not lost to
a reset. A first request head with a `Host` header but no valid request line is
answered 400 too. A response whose framing the relay cannot follow (a head over 64KiB or
without a status code, an invalid `Content-Length` or chunk-size line) is relayed as an
unframed one: it ends, and the connection with it, when the upstream closes.
