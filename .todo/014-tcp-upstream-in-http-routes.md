Difficulty: Low

# tcp upstreams are also registered as http routes

`Router.register` puts every upstream into `byDomain`, including those with a
`listen-port` (tcp routes): a `db.local` -> `tcp://127.0.0.1:5432` upstream with a listen
port is also an http route for `db.local`, and one with an empty host is the catch-all.

Consequences to confirm with tests:

- An HTTP request with `Host: db.local` on the data port is relayed raw to the tcp target
- With an http and a tcp upstream sharing one host (`SameHostUpstreamsE2ETest`), the http
  lookup picks by registration order on a client id tie; declaring the tcp upstream first
  sends HTTP traffic to the tcp target

Decide whether a tcp upstream's host should take part in Host / SNI routing at all
(probably not: it is reached by listen port), then add the failing test and fix.
