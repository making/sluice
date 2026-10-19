Difficulty: Low

# JSON error responses

The data plane's error answers (`ErrorResponse`: no-route 503, forbidden 403,
unauthorized 401, busy 503 with `Retry-After`, bad-gateway 502) always render HTML.
When the request targets a JSON API -- `Content-Type: application/json` or
`Accept: application/json` -- the error body should be JSON too, not HTML. The console
error page (ce10681) is out of scope: browsers only.

## Plan

- Use Spring's `ProblemDetail` (RFC 9457) for the shape: status, title, detail, and the
  extras the pages carry (`Retry-After` is a header, `WWW-Authenticate` stays a header
  per RFC 9457 15.5.1).
- `ErrorResponse`: the head parser already exposes the request headers, so pick JSON when
  `Content-Type` or `Accept` prefers `application/json` (or `application/problem+json`),
  and serialize the same fields by hand -- the response is written raw on the wire, no
  Spring MVC serialization there. Keep the HTML path otherwise. Same decision on the h2
  `StreamPage` path.
- Tests: data-plane error bytes for a JSON-preferring head assert status, `content-type`
  `application/problem+json`, and the problem fields (plus `Retry-After` /
  `WWW-Authenticate` where the page carries them), for HTTP/1.1 and h2 both; the E2E
  side follows in `sluice-it` where an error path already exists.
