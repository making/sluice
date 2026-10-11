Difficulty: High

# Per-connection credit flow control: wasmlet (041 child 5/6)

Parent: `.todo/041-tunnel-head-of-line-blocking.md` (Finding F5, section "Protocol
contract"). Depends on 046: the proto and Java side define and test the contract. Code:
`sluice-wasmlet/wasmlet/src/main.rs` (`Session::stream`, `attempt`, `Ctrl`, `Registry`)
and `wasm_host.rs` (`TunnelIo`, `serve`).

## Problem

The receive loop in `main.rs` `stream()` calls `sender.send(Ctrl::Payload(..)).await` on
the per-connection channel (`SERVE_BUFFER = 32`). CLOSE (`Ctrl::WriteClose`), ERROR
(`Ctrl::Abort`), the no-route ERROR reply (on the outbound `tx`) and the teardown `Abort`
loop in `attempt()` all await as well. With HTTP/1.1, hyper reads the request body only
when the guest polls it, so a guest that is slow to read a large POST parks the loop.
Every connection on the stream then stalls (usually bounded by the budget, 10s by
default). Outbound is one `mpsc::channel::<Frame>(64)` of 16KiB DATA chunks (`CHUNK`)
shared by all connections.

## State after 046

The proto has the fields (`build.rs` compiles it from `sluice-proto`). `main.rs` sets
`connection_window: 0` in its `Advertise`, so every wasmlet connection is legacy today and
the Java server never sends it a WindowUpdate. The Java reference implementation is
`VirtualConnection` (`sluice-proto`): read it for the exact rules, including the one 046
added to the contract (041, "Local close while the peer may still send").

## Design

- **Advertise**: `connection_window = 1MiB` (same constant as Java,
  `VirtualConnection.DEFAULT_WINDOW`).
- **Dispatch**: extract the per-frame dispatch of `stream()` into a plain (non-`async`)
  function. The type system then guarantees that it cannot await; keep it that way and
  say so in its doc comment.
- **Registry value**: becomes a struct holding the ctrl sender plus an `Arc` flow state:
  send credit, a waker slot (`futures::task::AtomicWaker` or `tokio::sync::Notify`), and
  the windowed flag.
- **Windowed connections** (`Connect.connection_window > 0`):
  - ctrl channel is `mpsc::unbounded_channel`;
  - dispatch tracks `outstanding` per connection; past the window -> ERROR "flow control
    window exceeded" + `Ctrl::Abort`;
  - `WindowUpdate` -> add credit and wake the writer.
- **Legacy connections**: keep the bounded channel. Their sends must not run inside the
  dispatch: spawn them or keep a per-connection forwarding task. Residual HOL for legacy
  peers is accepted, as in Java.
- **`TunnelIo::poll_write`**: at 0 credit, register the waker and return `Pending`;
  otherwise send `min(credit, CHUNK, len)` and subtract it.
- **`TunnelIo::poll_read`**: add the bytes handed to hyper to `consumed`; at half the
  window, emit WindowUpdate (see outbound) and reset.
- **Outbound**: add a control channel (unbounded: WindowUpdate, the no-route ERROR,
  Advertise) next to the data channel (64). Merge them into the request stream with a
  biased poll, control first. `poll_read` and the dispatch use only the control channel,
  so they never wait on data.
- **Teardown**: the `Abort` loop in `attempt()` and the no-route ERROR become non-awaiting
  sends.
- **Local close while the peer may still send** (contract rule from 046): a windowed
  connection ending before the server's CLOSE / ERROR arrived sends ERROR ("connection
  closed") instead of CLOSE, else the server's writer waits for credit forever. A
  `poll_write` waiting for credit fails once the server's ERROR (`Ctrl::Abort`) arrives.
- **Violation message**: exactly `flow control window exceeded` (Java
  `VirtualConnection.WINDOW_EXCEEDED`).

## Tests (`cargo test`; watch each fail first)

- Dispatch, with no gRPC needed now that it is a plain function:
  - conn 1's reader never polls and conn 1 is fed up to its window; a DATA for conn 2 is
    still delivered;
  - one byte past the window -> ERROR on the control channel + `Abort` to conn 1.
- `TunnelIo`:
  - `poll_write` is `Pending` at 0 credit and is woken by a grant (use a test waker that
    counts wakes);
  - `poll_write` waiting for credit fails on the server's ERROR;
  - `poll_read` emits one WindowUpdate per half window consumed;
  - a windowed connection closed before the server's CLOSE sends ERROR, not CLOSE.
- Legacy: CONNECT without a window keeps today's behavior (an existing serve test passes
  with the legacy path).
- If the CI job for wasmlet (todo 032) exists by then, these run there. Otherwise record
  the manual `cargo test` run in the commit message.
- Interop: a `sluice-it` E2E against a real wasmlet is todo 025's scope. If 025 is done,
  add the HOL scenario there (one route's guest does not read a large POST; another
  connection is served). If not, note it in 025.
