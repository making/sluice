Difficulty: Medium

# sluice-wicket-cf-api: an app's removal racing a wicket's listing

`Controller::remove` deletes the app from every wicket at once, each delete holding its share
(`hold_share`) until that wicket answered, and takes the app out of the state once all did. The
first wicket to answer moves its revision: its watch lists it, `fix` finds the app wanted (still in
the state) and gone, takes the share, now free, and puts the app back. The state then forgets the
app, and the wicket keeps it as one the state does not know (`CELL: left in the wicket`), serving
it until `DELETE /api/apps/<guid>?owner=cf` of its console.

Seen once (2026-11-09), on main before todo 256: the CLI test's two wickets still listed an app after
`cf delete` of hello, counter, relay, img and site (`tests/cli.rs`, "gone from the wickets"); the next
runs passed. The other edits commit the state before their puts, or hold the shares they put until
their commit (`Api::try_shares`): the removal alone puts first.

## To do

- Hold every share the removal took until the state no longer has the app

## Tests

First, a test failing without it: the controller against two fake wickets (their app API: a listing
held until its revision moves, `PUT`, `DELETE`), the second answering `DELETE` late; the first lists
no app once the removal returned and its watch listed it.
