Difficulty: Low

# Trust the RFC 7239 `Forwarded` header for access control

Todo 017 added `X-Forwarded-For` trust (`sluice.access-control.trusted-proxy-cidrs`; the
rightmost entry appended by a trusted proxy is judged). The standard `Forwarded` header is
not honored yet:

- `ConnectionHeadParser`: capture the `forwarded` header alongside `forwardedFor`
  (HTTP/1.1 and h2 heads, same paths)
- `AccessControl`: when a trusted proxy appended `Forwarded`, judge the rightmost element's
  `for=` parameter; skip elements whose `for` is missing, obfuscated, or not a literal
  (per the RFC grammar, including bracketed IPv6); fall back to the peer as today
- Decide and document the precedence when a request carries both headers
  (suggestion: `Forwarded` wins -- it is the proxy's explicit statement)
- Unit tests in `AccessControlTest` / `ConnectionHeadParserTest` style; the E2E needs no
  change unless the LB story changes
