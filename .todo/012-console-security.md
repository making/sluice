Difficulty: Medium

# Secure the management console

`/console` (and the actuator) is served on `server.port` (8081) without authentication.
The console is read-only today, but it exposes client ids, peer addresses, upstream
target URLs and the route table.

- Decide the mechanism (Spring Security form / basic login, OIDC, or a shared token
  like `sluice.token`), and whether actuator health / prometheus stay anonymous for
  probes and scraping
- CSRF protection once the console gains actions (e.g. disconnect a client, drain a node)
- Response headers (CSP: the page loads only same-origin script and fonts; htmx 4 needs
  `hx-csp.js` or `inlineScriptNonce` under a strict CSP)
- Extend `ConsoleE2ETest` with the unauthenticated / authenticated cases
