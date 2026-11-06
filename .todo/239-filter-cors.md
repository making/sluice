Difficulty: Low

# Built-in filter: cors

Answer CORS preflights on the client and add the CORS headers to the route's responses.

## Why

An upstream reached through a tunnel is often a local dev server or a service with no CORS of its own. The server's `allow-preflight` only lets a preflight pass authentication; something still has to answer it. Answering on the client spares the upstream and works for any upstream.

## Scope

- Settings in `args`: allowed origins (exact, or `*`), methods, headers, `allow-credentials`, `max-age`, exposed headers.
- A preflight (`OPTIONS` with `Origin` and `Access-Control-Request-Method`) from an allowed origin is answered locally (`Exchange.localResponse`, `204`). A preflight from another origin goes to the upstream unchanged.
- An actual request from an allowed origin gets `Access-Control-Allow-Origin` (the origin, not `*`, when credentials are allowed) and `Vary: Origin` on its response. What the upstream already set is replaced.
- `*` with `allow-credentials=true` stops the start: browsers reject it.

## Done when

A browser-shaped preflight and request pass from an allowed origin and fail from another, `README.md` "Filters" lists it, and `.kb/client-filters.md` describes the settings.
