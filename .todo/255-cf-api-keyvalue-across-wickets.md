Difficulty: Medium

# sluice-wicket-cf-api / sluice-wicket-runtime-wasi: keyvalue services across wickets

After 252, which spreads an app over the wickets. A keyvalue instance is the namespace `cf/<guid>` of a store the
wicket lists (`keyvalue_stores`: `name`, `type`, `default`); the plans are one wicket's stores (`Offer::new`),
and a deleted instance's data is purged once, through that wicket, refused (409) while a route of it maps the
namespace (`Controller::purge_pending`).

Across wickets:

- `sqlite` (the default `local`) is a file of the wicket's host: an app on wickets of two hosts reads and
  writes two namespaces, and a purge reaches one
- a `redis` store is shared, but nothing says two wickets' stores of one name are one server; a purge through
  one wicket is not refused for a route of another (a draining one)
- the `keyvalue` plans and the `wasi` capabilities are one wicket's

## To do

- The listing says whether a store is shared beyond its host, and which one it is (an identity of the server
  and the prefix, no credential in it)
- The plans: the stores every wicket offers, by identity; the capabilities likewise
- An app bound to a host-local store: its indices on one wicket, or a spread refused naming the store;
  decide, and what `cf bind-service` and `cf scale -i` answer
- A purge: through every wicket holding a host-local store's namespace; of a shared store through one, once
  no wicket maps the namespace (the state's apps, and each wicket's draining routes)
- READMEs: the cf-api's Services, the wasi wicket's Key-value stores. Todo 168 (gemfire) is a shared store too

## Tests

The CLI test's two wickets with a Redis store (`redis-server` as the wasi wicket's tests run it): a counter
spread over both counts once; one bound to `local` is kept on one wicket (or refused); a deleted instance's
namespace purged on each wicket that held it.
