Difficulty: Low

# h2 demux credit charging: two mutations survive the tests

Left by 135 (8cd142c, 12f5a3c): the connection-window credit peers hold is charged to the
h2 queue budget and counted in a connection's share.

## Gaps

- The share boundary: changing the share check from `>=` to `>` (a connection exactly at
  `CONNECTION_SHARE` held and credit) is not detected.
- `NettyHttp2DemuxRelay#admitLocked`: the credit term in the header-block admission (the
  block share includes the credit) can be removed without a test failing.

## Tests

- A connection whose held bytes plus credit sit exactly at the share is withheld (the
  boundary pinned with the counters the relay exposes), and one byte under it is not.
- A header block on a connection whose credit alone takes it past `BLOCK_SHARE` is refused
  (REFUSED_STREAM / ENHANCE_YOUR_CALM per policy) while the budget has room; removing the
  credit term (mutation) fails it.
