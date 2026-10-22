//! The HTTP/2 window the wicket announces for the tunnel's gRPC connection,
//! measured in round trips: a proxy turns the receiver's answers (its flow
//! control credit among them) into discrete rounds, so the bytes that passed
//! between two releases are what the sender could send on one round trip's
//! credit, however fast the machine is.

use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};

use prost::Message;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;

use crate::proto::frame::Body;
use crate::proto::{Data, Frame};
use crate::session::{transport_window, with_transport_window};
use crate::tunnel;

const FRAME_PAYLOAD: usize = 16 * 1024;

/// How long the sending direction must be quiet before the held answers are
/// released: a sender bound by credit has sent what it could by then.
const QUIET: Duration = Duration::from_millis(150);

/// Rounds start once the sending direction passed the handshake's bytes.
const ARM_AFTER: u64 = 64 * 1024;

#[derive(Default)]
struct Rounds {
    armed: bool,
    sent_total: u64,
    sent_in_round: u64,
    last_sent: Option<Instant>,
    held: Vec<u8>,
    rounds: Vec<u64>,
}

/// A loopback proxy in front of `target`: the server's bytes pass at once;
/// once armed, the client's are held until the server was quiet for
/// [`QUIET`], then released together.
async fn round_trip_proxy(
    target: std::net::SocketAddr,
) -> (std::net::SocketAddr, Arc<StdMutex<Rounds>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let state = Arc::new(StdMutex::new(Rounds::default()));
    let shared = state.clone();
    tokio::spawn(async move {
        let (client, _) = listener.accept().await.unwrap();
        let server = TcpStream::connect(target).await.unwrap();
        let (mut from_client, mut to_client) = client.into_split();
        let (mut from_server, to_server) = server.into_split();
        let to_server = Arc::new(Mutex::new(to_server));

        let sending = shared.clone();
        tokio::spawn(async move {
            let mut buf = vec![0u8; 64 * 1024];
            loop {
                let n = match from_server.read(&mut buf).await {
                    Ok(0) | Err(_) => return,
                    Ok(n) => n,
                };
                {
                    let mut s = sending.lock().unwrap();
                    s.sent_total += n as u64;
                    s.last_sent = Some(Instant::now());
                    if s.armed {
                        s.sent_in_round += n as u64;
                    } else if s.sent_total > ARM_AFTER {
                        s.armed = true;
                        s.sent_in_round = n as u64;
                    }
                }
                if to_client.write_all(&buf[..n]).await.is_err() {
                    return;
                }
            }
        });

        let answers = shared.clone();
        let out = to_server.clone();
        tokio::spawn(async move {
            let mut buf = vec![0u8; 64 * 1024];
            loop {
                let n = match from_client.read(&mut buf).await {
                    Ok(0) | Err(_) => return,
                    Ok(n) => n,
                };
                let pass = {
                    let mut s = answers.lock().unwrap();
                    if s.armed {
                        s.held.extend_from_slice(&buf[..n]);
                        false
                    } else {
                        true
                    }
                };
                if pass && out.lock().await.write_all(&buf[..n]).await.is_err() {
                    return;
                }
            }
        });

        loop {
            tokio::time::sleep(Duration::from_millis(10)).await;
            let release = {
                let mut s = shared.lock().unwrap();
                let quiet = s.last_sent.is_some_and(|t| t.elapsed() >= QUIET);
                if s.armed && quiet && !s.held.is_empty() {
                    let round = std::mem::take(&mut s.sent_in_round);
                    s.rounds.push(round);
                    Some(std::mem::take(&mut s.held))
                } else {
                    None
                }
            };
            if let Some(bytes) = release
                && to_server.lock().await.write_all(&bytes).await.is_err()
            {
                return;
            }
        }
    });
    (address, state)
}

/// A gRPC server (raw h2) answering a call with `total` bytes of DATA frames
/// spread over 16KiB frames, then OK trailers.
async fn sending_server(total: usize) -> std::net::SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut connection = h2::server::handshake(socket).await.unwrap();
        while let Some(accepted) = connection.accept().await {
            let (request, mut respond) = accepted.unwrap();
            tokio::spawn(async move {
                let _request_body = request.into_body();
                let response = http::Response::builder()
                    .header("content-type", "application/grpc")
                    .body(())
                    .unwrap();
                let mut send = respond.send_response(response, false).unwrap();
                let mut left = total;
                while left > 0 {
                    let payload = left.min(FRAME_PAYLOAD);
                    left -= payload;
                    let frame = Frame {
                        body: Some(Body::Data(Data {
                            conn_id: 1,
                            payload: vec![7; payload],
                        })),
                    };
                    let encoded = frame.encode_to_vec();
                    let mut message = vec![0u8];
                    message.extend_from_slice(&(encoded.len() as u32).to_be_bytes());
                    message.extend_from_slice(&encoded);
                    let mut bytes = bytes::Bytes::from(message);
                    while !bytes.is_empty() {
                        send.reserve_capacity(bytes.len());
                        let granted = std::future::poll_fn(|cx| send.poll_capacity(cx))
                            .await
                            .unwrap()
                            .unwrap();
                        let chunk = bytes.split_to(granted.min(bytes.len()));
                        send.send_data(chunk, false).unwrap();
                    }
                }
                let mut trailers = http::HeaderMap::new();
                trailers.insert("grpc-status", "0".parse().unwrap());
                send.send_trailers(trailers).unwrap();
                // keep the connection until the client has read it all
                tokio::time::sleep(Duration::from_secs(30)).await;
            });
        }
    });
    address
}

/// Downloads `total` payload bytes over a gRPC connection configured as the
/// wicket configures it, behind the proxy; returns the bytes each round trip
/// carried.
async fn download(total: usize, window: u32) -> Vec<u64> {
    let server = sending_server(total).await;
    let (proxy, state) = round_trip_proxy(server).await;
    let endpoint = with_transport_window(
        tonic::transport::Endpoint::from_shared(format!("http://{proxy}")).unwrap(),
        window,
    );
    let channel = endpoint.connect().await.unwrap();
    let outbound = futures_util::stream::pending::<Frame>();
    let mut inbound = crate::proto::tunnel_client::TunnelClient::new(channel)
        .connect(tonic::Request::new(outbound))
        .await
        .unwrap()
        .into_inner();
    let mut received = 0usize;
    while received < total {
        let frame = tokio::time::timeout(Duration::from_secs(60), inbound.message())
            .await
            .expect("download stalled")
            .unwrap()
            .expect("stream ended early");
        if let Some(Body::Data(data)) = frame.body {
            received += data.payload.len();
        }
    }
    assert_eq!(received, total);
    let s = state.lock().unwrap();
    // the last round has no answer held back to release it
    let mut rounds = s.rounds.clone();
    rounds.push(s.sent_in_round);
    rounds
}

#[test]
fn the_window_is_twice_what_the_connections_may_have_in_flight() {
    let window = u64::from(tunnel::WINDOW);
    assert_eq!(u64::from(transport_window(256)), 2 * 256 * window);
    assert_eq!(u64::from(transport_window(1)), 2 * window);
    // never below one connection window, never beyond the HTTP/2 maximum
    assert_eq!(transport_window(0), tunnel::WINDOW);
    assert_eq!(transport_window(usize::MAX), i32::MAX as u32);
}

/// 32MiB sent to the wicket's gRPC connection, the answers held back and
/// released once the sender was quiet: with the window fixed at twice the
/// default connection budget the transfer takes a few round trips at most;
/// at hyper's defaults (2MiB per stream) it takes at least sixteen.
#[tokio::test]
async fn a_large_download_takes_few_round_trips() {
    const TOTAL: usize = 32 * 1024 * 1024;
    let window = transport_window(crate::config::DEFAULT_MAX_CONNECTIONS);
    assert!(window as usize >= 2 * TOTAL, "the precondition: {window}");
    let rounds = download(TOTAL, window).await;
    let sent: u64 = rounds.iter().sum();
    assert!(sent >= TOTAL as u64 / 2, "{sent} of {TOTAL} in {rounds:?}");
    assert!(
        rounds.len() <= 3,
        "{} round trips for {TOTAL} bytes: {rounds:?}",
        rounds.len()
    );
}
