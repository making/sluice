//! `--listen`: the routes served over plain TCP as well, relayed as the tunnel's
//! connections are (no sluice server needed: a local loop for trying a rootfs).
//!
//! A connection is routed once, by its first request (`head`): the route of its
//! host -- the `Host` header, or the h2c `:authority` -- or the sole route
//! whatever the host (`sluice_wicket_tunnel::routes::Table::sole`), which reads
//! nothing first: its guest may speak first. What was read is relayed
//! verbatim, as the rest: a later request of the connection, an h2 stream or a
//! keep-alive request, stays on that route whatever its host, and no
//! `X-Forwarded-*` header is added. An unknown host is answered 404, a request
//! finding no room for its VM 502 ([`bad_gateway`]); a connection that is not
//! HTTP, or whose head runs past `head::MAX_HEAD` or takes longer than
//! [`HEAD_TIMEOUT`], is closed.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use sluice_wicket_server::accept;
use sluice_wicket_tunnel::listener::{Replay, host_of};
use sluice_wicket_tunnel::routes::Routes;
use sluice_wicket_tunnel::workers::Workers;
use tokio::io::{AsyncRead, AsyncReadExt as _, AsyncWrite, AsyncWriteExt as _};
use tokio::net::{TcpListener, TcpStream};

use crate::head::{self, Head};
use crate::route::Lease;
use crate::routes::Table;
use crate::wire;

/// How long a connection may take to send its head.
pub const HEAD_TIMEOUT: Duration = Duration::from_secs(10);

/// How long a refused connection is read after its answer, for the answer not to
/// be lost to a reset.
const LINGER: Duration = Duration::from_secs(2);

/// Serves each connection, at most `max_connections` at once (see
/// `sluice_wicket_server::accept::run`), on a worker, by the table of the
/// moment.
pub async fn run(
    listener: TcpListener,
    routes: Arc<Routes<Table>>,
    workers: Arc<Workers>,
    max_connections: usize,
) {
    accept::run(
        listener,
        max_connections,
        "--listen",
        |client, peer, slot| {
            let Ok(client) = client.into_std() else {
                return;
            };
            let routes = routes.clone();
            workers.place().spawn(async move {
                let _slot = slot;
                // the worker's from here on
                if let Ok(client) = TcpStream::from_std(client) {
                    serve(Replay::new(client), peer, &routes).await;
                }
            });
        },
    )
    .await
}

async fn serve(mut client: Replay<TcpStream>, peer: SocketAddr, routes: &Routes<Table>) {
    let route = match routes.current().sole() {
        Some(route) => route,
        None => {
            let head = match tokio::time::timeout(HEAD_TIMEOUT, head::read(&mut client)).await {
                Ok(Ok(head)) => head,
                Ok(Err(e)) => return eprintln!("listener conn {peer}: {e}"),
                Err(_) => {
                    let timeout = HEAD_TIMEOUT.as_secs();
                    return eprintln!("listener conn {peer}: no request head within {timeout}s");
                }
            };
            match routes.current().by_host(host_of(head.host())) {
                Some(route) => route,
                None => {
                    let message = format!("no firecracker route for host '{}'", head.host());
                    return answer(&mut client, &head, "404 Not Found", &message).await;
                }
            }
        }
    };
    match route.open(&peer.ip().to_string()).await {
        Ok(Lease { stream, hold }) => {
            let relayed = wire::relay(&mut client, stream, route.idle()).await;
            // a reused clone kept before the client sees the end
            let ended = hold.end(relayed.is_ok());
            drop(client);
            if let Err(e) = relayed {
                eprintln!("listener ({}): {e}", route.label());
            }
            ended.await;
        }
        Err(e) => {
            eprintln!("listener ({}): {e}", route.label());
            if let Some(why) = route.no_room(&e) {
                bad_gateway(&mut client, &why).await;
            }
        }
    }
}

/// Answers the request of a connection its route found no room for 502, saying `why`: no guest
/// answers it, and only the wicket knows. False when it is no HTTP request, or none came
/// within [`HEAD_TIMEOUT`]; a tunnel's connection reads its head here, as a sole route's does.
pub async fn bad_gateway<S>(client: &mut Replay<S>, why: &str) -> bool
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    match tokio::time::timeout(HEAD_TIMEOUT, head::read(client)).await {
        Ok(Ok(head)) => {
            answer(client, &head, "502 Bad Gateway", why).await;
            true
        }
        _ => false,
    }
}

/// Answers a connection `status` with `message`, and closes it: http/1.1, or the h2 stream of
/// its first request, the connection ended after it (GOAWAY).
async fn answer<S>(client: &mut Replay<S>, head: &Head, status: &str, message: &str)
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let answer = match head {
        Head::Http1 { .. } => format!(
            "HTTP/1.1 {status}\r\ncache-control: no-store\r\ncontent-length: {}\r\n\
             connection: close\r\n\r\n{message}",
            message.len()
        )
        .into_bytes(),
        Head::H2 { stream, .. } => h2_answer(*stream, status, message),
    };
    if client.write_all(&answer).await.is_err() || client.shutdown().await.is_err() {
        return;
    }
    let _ = tokio::time::timeout(LINGER, async {
        let mut discarded = [0u8; 4096];
        while client.read(&mut discarded).await.is_ok_and(|read| read > 0) {}
    })
    .await;
}

/// The frames answering `stream` `status` (its code first) with `message`: the server's SETTINGS,
/// the client's acknowledged, the stream's response, a GOAWAY after it.
fn h2_answer(stream: u32, status: &str, message: &str) -> Vec<u8> {
    let frame = |kind: u8, flags: u8, stream: u32, payload: &[u8]| {
        let len = (payload.len() as u32).to_be_bytes();
        [&len[1..], &[kind, flags], &stream.to_be_bytes(), payload].concat()
    };
    // HPACK, unindexed: :status (static index 8), its code, cache-control (24): no-store
    let code = &status.as_bytes()[..3];
    let block = [&[0x08, 0x03][..], code, &[0x0f, 0x09, 0x08], b"no-store"].concat();
    let goaway = [stream.to_be_bytes(), 0u32.to_be_bytes()].concat();
    [
        frame(0x4, 0, 0, &[]),
        frame(0x4, 0x1, 0, &[]),
        // END_HEADERS
        frame(0x1, 0x4, stream, &block),
        // END_STREAM
        frame(0x0, 0x1, stream, message.as_bytes()),
        frame(0x7, 0, 0, &goaway),
    ]
    .concat()
}
