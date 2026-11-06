Difficulty: Low

# Built-in filter: headers

Set, add and remove request and response headers per route, from the properties.

## Why

The commonest edit, today only through a plugin or a custom build:

- Response security headers the upstream does not send (`Strict-Transport-Security`, `X-Content-Type-Options`, `Content-Security-Policy`), or headers it should not leak (`Server`, `X-Powered-By`).
- A credential toward the upstream that the tunnel's users never hold: `x-api-key` for an LLM API next to `llm-usage`, the value from the client's environment (`${ANTHROPIC_API_KEY}`), whatever the client sent of it removed first.

## Scope

- Settings in `args`: decide a form that binds as `Map<String, String>` and survives `overriddenBy`'s key-by-key merge, e.g. `args[request.set.x-api-key]`, `args[response.remove.server]`.
- Operations: `set` (replace all values), `add`, `remove`. Names are case-insensitive.
- Refused at the start: framing headers (the pipe keeps the upstream's framing whatever a filter does), `host`, control characters.
- A value never reaches a log or the console (it may be a secret).
- No body: `null` from the response handler, so the filter costs one head edit.

## Done when

A route's requests and responses carry the edits, a refused name stops the start, `README.md` "Filters" lists it, and `.kb/client-filters.md` describes the settings.
