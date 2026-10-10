// Sluice guest handler that composes its response from a raw TCP upstream
// (wasi:sockets 0.3): `GET /?addr=<ip>:<port>` sends an HTTP/1.0 GET to the
// address and returns what came back. Exercises the route capabilities: the
// connect is denied unless the route grants `cli,inherit-network`.
//
// Build (from `examples/`): cargo build --release -p relay
wit_bindgen::generate!({
    path: "../../wit",
    inline: "
        package sluice:relay;

        world relay {
            import wasi:sockets/types@0.3.0;
            import wasi:http/types@0.3.0;
            export wasi:http/handler@0.3.0;
        }
    ",
    features: ["clocks-timezone"],
    generate_all,
});

use std::net::SocketAddr;

use exports::wasi::http::handler::Guest;
use wasi::http::types::{ErrorCode, Headers, Request, Response};
use wasi::sockets::types::{
    IpAddressFamily, IpSocketAddress, Ipv4SocketAddress, Ipv6SocketAddress, TcpSocket,
};

struct Component;

impl Guest for Component {
    async fn handle(request: Request) -> Result<Response, ErrorCode> {
        let path = request.get_path_with_query().unwrap_or_default();
        let addr = path
            .split_once("addr=")
            .map(|(_, rest)| rest.split('&').next().unwrap_or_default().to_string());
        let (status, payload) = match addr.as_deref().map(str::parse::<SocketAddr>) {
            Some(Ok(addr)) => match relay(addr).await {
                Ok(raw) => (200, [format!("relayed from {addr}\n").into_bytes(), raw].concat()),
                Err(e) => (502, format!("relay {addr} failed: {e}\n").into_bytes()),
            },
            _ => (400, b"expected ?addr=<ip>:<port>\n".to_vec()),
        };
        respond(status, payload)
    }
}

export!(Component);

/// One HTTP/1.0 exchange over a fresh TCP connection; returns the raw reply.
async fn relay(addr: SocketAddr) -> Result<Vec<u8>, String> {
    let (family, remote) = match addr {
        SocketAddr::V4(v4) => (
            IpAddressFamily::Ipv4,
            IpSocketAddress::Ipv4(Ipv4SocketAddress {
                port: v4.port(),
                address: v4.ip().octets().into(),
            }),
        ),
        SocketAddr::V6(v6) => {
            let s = v6.ip().segments();
            (
                IpAddressFamily::Ipv6,
                IpSocketAddress::Ipv6(Ipv6SocketAddress {
                    port: v6.port(),
                    flow_info: v6.flowinfo(),
                    address: (s[0], s[1], s[2], s[3], s[4], s[5], s[6], s[7]),
                    scope_id: v6.scope_id(),
                }),
            )
        }
    };
    let socket = TcpSocket::create(family).map_err(|e| format!("create: {e:?}"))?;
    socket
        .connect(remote)
        .await
        .map_err(|e| format!("connect: {e:?}"))?;

    let (mut tx, rx) = wit_stream::new();
    let sent = socket.send(rx);
    let request = format!("GET / HTTP/1.0\r\nhost: {addr}\r\n\r\n");
    let _ = tx.write_all(request.into_bytes()).await;
    drop(tx);
    sent.await.map_err(|e| format!("send: {e:?}"))?;

    let (mut body, done) = socket.receive();
    let mut raw = Vec::new();
    let mut chunk = Vec::with_capacity(8192);
    loop {
        let (status, buf) = body.read(chunk).await;
        chunk = buf;
        match status {
            wit_bindgen::StreamResult::Complete(_) => raw.append(&mut chunk),
            wit_bindgen::StreamResult::Dropped | wit_bindgen::StreamResult::Cancelled => break,
        }
    }
    done.await.map_err(|e| format!("receive: {e:?}"))?;
    Ok(raw)
}

fn respond(status: u16, payload: Vec<u8>) -> Result<Response, ErrorCode> {
    let headers = Headers::new();
    let _ = headers.append("content-type", b"text/plain");
    let (mut body_tx, body_rx) = wit_stream::new();
    let (trailers_tx, trailers_rx) = wit_future::new(|| todo!());
    let (response, _transmit) = Response::new(headers, Some(body_rx), trailers_rx);
    let _ = response.set_status_code(status);
    wit_bindgen::spawn_local(async move {
        let _ = body_tx.write_all(payload).await;
        drop(body_tx);
        let _ = trailers_tx.write(Ok(None)).await;
    });
    Ok(response)
}
