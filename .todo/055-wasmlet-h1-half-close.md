Difficulty: Medium

# wasmlet: a public client's half-close drops the http/1.1 response

Found while working on 047. A public client that shuts down its write side after
sending a request (`shutdownOutput`, `nc -N`, some HTTP/1.0 clients) gets no response
from a wasm route: the server relays the half-close as CLOSE, `TunnelIo` reports EOF,
and hyper's http1 server (`half_close` defaults to false) treats an EOF before the
response as "connection closed before message completed" and drops the in-flight
request. The wasmlet then sends only CLOSE. Both connection modes, before and after 047.

## Why not just `half_close(true)`

With `allow_half_close` hyper stops polling the read side mid-message
(`Conn::mid_message_detect_eof` returns `Pending` without reading). The server's ERROR
(`Ctrl::Abort`) is then never seen while a request is in flight, so the client-departure
detection breaks: `a_client_leaving_mid_request_frees_its_unit`,
`a_client_leaving_mid_body_frees_its_unit_before_the_budget` and
`a_client_leaving_mid_body_frees_the_cold_store_before_the_budget` fail (tried in 047).

## Direction

Keep hyper's read polling for aborts but stop it from seeing the half-close as an
abort. For example, `TunnelIo` holds back the EOF of a server CLOSE while a request is
in flight and reports it once the response is written, or the abort path wakes the
serve task through a channel of its own instead of the read side. Read the hyper h1
dispatcher before choosing (`proto/h1/conn.rs`, `dispatch.rs`).

## Test (`wasm_host.rs`; watch it fail first)

```rust
/// A public client that half-closes after its request (the server's
/// CLOSE right behind the request) still gets the response.
#[tokio::test]
async fn a_request_followed_by_the_servers_half_close_is_answered() {
    let handler = Handler::new(hello().await, Sandbox::default());
    for window in [0, crate::tunnel::WINDOW] {
        let mut peer = open_with(&handler, &get("/"), window).await;
        peer.send(Body::Close(crate::pb::Close { conn_id: 1 })).await;
        let response = String::from_utf8(response_raw(&mut peer).await).unwrap();
        assert!(response.starts_with("HTTP/1.1 200"), "window {window}: {response}");
    }
}
```

The three client-departure tests above must stay green.
