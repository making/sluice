// Sluice guest handler that composes its response from an http upstream
// (wasi:http/client 0.3): `GET /?url=<http(s) url>` sends a GET to the url and
// returns its status and body. Outgoing http needs no route capability, as
// under `wasmtime serve` (the `-S http` surface of `wasmtime run`).
//
// Build (from `examples/`): cargo build --release -p fetch
wit_bindgen::generate!({
    path: "../../wit",
    inline: "
        package sluice:fetch;

        world fetch {
            import wasi:http/client@0.3.0;
            import wasi:http/types@0.3.0;
            export wasi:http/handler@0.3.0;
        }
    ",
    features: ["clocks-timezone"],
    generate_all,
});

use exports::wasi::http::handler::Guest;
use wasi::http::client;
use wasi::http::types::{ErrorCode, Headers, Request, Response, Scheme};
use wit_bindgen::StreamResult;

/// Upper bound for the relayed upstream body.
const BODY_MAX: usize = 1024 * 1024;

struct Component;

impl Guest for Component {
    async fn handle(request: Request) -> Result<Response, ErrorCode> {
        let path = request.get_path_with_query().unwrap_or_default();
        // The url is taken verbatim up to the end, so it may carry its own query.
        let url = path.split_once("url=").map(|(_, url)| url.to_string());
        let (status, payload) = match url.as_deref().map(outgoing) {
            Some(Ok(outgoing)) => {
                let url = url.unwrap();
                match fetch(outgoing).await {
                    Ok((status, body)) => {
                        let head = format!("fetched {url} -> {status}\n");
                        (200, [head.into_bytes(), body].concat())
                    }
                    Err(code) => (502, format!("fetch {url} failed: {code:?}\n").into_bytes()),
                }
            }
            Some(Err(e)) => (400, format!("{e}\n").into_bytes()),
            None => (400, b"expected ?url=<http(s) url>\n".to_vec()),
        };
        respond(status, payload)
    }
}

export!(Component);

/// A body-less GET for `url` (`http(s)://authority[/path?query]`).
fn outgoing(url: &str) -> Result<Request, String> {
    let (scheme, rest) = match url.split_once("://") {
        Some(("http", rest)) => (Scheme::Http, rest),
        Some(("https", rest)) => (Scheme::Https, rest),
        _ => return Err(format!("unsupported url '{url}' (http / https)")),
    };
    let (authority, path) = match rest.find('/') {
        Some(slash) => rest.split_at(slash),
        None => (rest, "/"),
    };
    let (trailers_tx, trailers_rx) = wit_future::new(|| Ok(None));
    drop(trailers_tx);
    let (request, _transmit) = Request::new(Headers::new(), None, trailers_rx, None);
    let invalid = |part: &str| format!("invalid {part} in '{url}'");
    request.set_scheme(Some(&scheme)).map_err(|()| invalid("scheme"))?;
    request
        .set_authority(Some(authority))
        .map_err(|()| invalid("authority"))?;
    request
        .set_path_with_query(Some(path))
        .map_err(|()| invalid("path"))?;
    Ok(request)
}

/// Sends `request` and reads the response body (bounded).
async fn fetch(request: Request) -> Result<(u16, Vec<u8>), ErrorCode> {
    let response = client::send(request).await?;
    let status = response.get_status_code();
    let (result_tx, result_rx) = wit_future::new(|| Ok(()));
    let (mut body, _trailers) = Response::consume_body(response, result_rx);
    let mut collected = Vec::new();
    let mut chunk = Vec::with_capacity(8192);
    while collected.len() < BODY_MAX {
        let (status, buf) = body.read(chunk).await;
        chunk = buf;
        match status {
            StreamResult::Complete(_) => collected.append(&mut chunk),
            StreamResult::Dropped | StreamResult::Cancelled => break,
        }
    }
    drop(result_tx);
    Ok((status, collected))
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
