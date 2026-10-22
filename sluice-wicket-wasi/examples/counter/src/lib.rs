// Sluice guest handler keeping state in wasi:keyvalue (0.2.0-draft2): a
// counter, and the store's operations one path each. The route needs
// `wasi=keyvalue`; `?store=<identifier>` opens another identifier than the
// default `""`.
//
// Build (from `examples/`): cargo build --release -p counter
wit_bindgen::generate!({
    path: "../../wit",
    inline: "
        package sluice:counter;

        world counter {
            import wasi:http/types@0.3.0;
            import wasi:keyvalue/store@0.2.0-draft2;
            import wasi:keyvalue/atomics@0.2.0-draft2;
            import wasi:keyvalue/batch@0.2.0-draft2;
            export wasi:http/handler@0.3.0;
        }
    ",
    features: ["clocks-timezone"],
    generate_all,
});

use exports::wasi::http::handler::Guest;
use wasi::http::types::{ErrorCode, Headers, Method, Request, Response};
use wasi::keyvalue::{atomics, batch, store};
use wit_bindgen::StreamResult;

/// Upper bound for the request body (a value to set).
const BODY_MAX: usize = 4 * 1024 * 1024;

struct Component;

impl Guest for Component {
    async fn handle(request: Request) -> Result<Response, ErrorCode> {
        let method = request.get_method();
        let path_with_query = request.get_path_with_query().unwrap_or_default();
        let (path, query) = path_with_query
            .split_once('?')
            .unwrap_or((&path_with_query, ""));
        let (path, query) = (path.to_string(), query.to_string());
        let body = read_body(request).await;
        let identifier = query
            .split('&')
            .find_map(|pair| pair.strip_prefix("store="))
            .unwrap_or("");
        let bucket = match store::open(identifier) {
            Ok(bucket) => bucket,
            Err(e) => return Ok(text(500, format!("open {identifier:?}: {e:?}\n"))),
        };
        let (status, payload) = match (method, path.as_str()) {
            (Method::Get, "/") => match atomics::increment(&bucket, "hits", 1) {
                Ok(hits) => (200, format!("hits {hits}\n")),
                Err(e) => (500, format!("increment: {e:?}\n")),
            },
            (Method::Get, "/cas") => cas_increment(&bucket, "cas-hits", query.contains("race")),
            (Method::Get, "/keys") => list(&bucket),
            (method, path) if path.starts_with("/kv/") => {
                let key = &path["/kv/".len()..];
                match method {
                    Method::Get => match bucket.get(key) {
                        Ok(Some(value)) => (200, String::from_utf8_lossy(&value).into_owned()),
                        Ok(None) => (404, format!("no key {key:?}\n")),
                        Err(e) => (500, format!("get: {e:?}\n")),
                    },
                    Method::Put => answer("set", bucket.set(key, &body)),
                    Method::Delete => answer("delete", bucket.delete(key)),
                    _ => (405, "GET, PUT or DELETE\n".into()),
                }
            }
            (Method::Post, path) if path.starts_with("/fill/") => {
                let count: usize = path["/fill/".len()..].parse().unwrap_or(0);
                let pairs: Vec<_> = (0..count)
                    .map(|n| (format!("key-{n:05}"), n.to_string().into_bytes()))
                    .collect();
                answer("set-many", batch::set_many(&bucket, &pairs))
            }
            _ => (404, format!("no such operation: {path}\n")),
        };
        Ok(text(status, payload))
    }
}

export!(Component);

fn answer(operation: &str, outcome: Result<(), store::Error>) -> (u16, String) {
    match outcome {
        Ok(()) => (200, "ok\n".into()),
        Err(e) => (500, format!("{operation}: {e:?}\n")),
    }
}

/// Increments `key` by read-modify-write through a CAS handle, retrying a
/// lost race. `race` writes the key between the read and the first swap, as
/// another request would: that swap fails and the retry wins.
fn cas_increment(bucket: &store::Bucket, key: &str, race: bool) -> (u16, String) {
    let mut cas = match atomics::Cas::new(bucket, key) {
        Ok(cas) => cas,
        Err(e) => return (500, format!("cas: {e:?}\n")),
    };
    for attempt in 1.. {
        let n = match cas.current() {
            Ok(current) => current
                .map(|value| String::from_utf8_lossy(&value).parse().unwrap_or(0))
                .unwrap_or(0i64),
            Err(e) => return (500, format!("cas current: {e:?}\n")),
        };
        if race
            && attempt == 1
            && let Err(e) = bucket.set(key, (n + 1).to_string().as_bytes())
        {
            return (500, format!("set: {e:?}\n"));
        }
        match atomics::swap(cas, (n + 1).to_string().as_bytes()) {
            Ok(()) => return (200, format!("{key} {} after {attempt} attempt(s)\n", n + 1)),
            Err(atomics::CasError::CasFailed(retry)) => cas = retry,
            Err(atomics::CasError::StoreError(e)) => return (500, format!("swap: {e:?}\n")),
        }
    }
    unreachable!("returns from the loop")
}

/// Every key, page by page.
fn list(bucket: &store::Bucket) -> (u16, String) {
    let (mut keys, mut pages, mut cursor) = (0, 0, None);
    loop {
        match bucket.list_keys(cursor.as_deref()) {
            Ok(page) => {
                keys += page.keys.len();
                pages += 1;
                cursor = page.cursor;
                if cursor.is_none() {
                    return (200, format!("{keys} keys in {pages} page(s)\n"));
                }
            }
            Err(e) => return (500, format!("list-keys: {e:?}\n")),
        }
    }
}

async fn read_body(request: Request) -> Vec<u8> {
    let (result_tx, result_rx) = wit_future::new(|| Ok(()));
    let (mut body, _trailers) = Request::consume_body(request, result_rx);
    let mut all = Vec::new();
    let mut chunk: Vec<u8> = Vec::with_capacity(16 * 1024);
    loop {
        let (status, buf) = body.read(chunk).await;
        chunk = buf;
        all.extend_from_slice(&chunk);
        // a read fills only the spare capacity: empty it for the next
        chunk.clear();
        match status {
            StreamResult::Complete(_) if all.len() <= BODY_MAX => {}
            _ => break,
        }
    }
    drop(result_tx);
    all
}

fn text(status: u16, payload: String) -> Response {
    let headers = Headers::new();
    let _ = headers.append("content-type", b"text/plain");
    let (mut body_tx, body_rx) = wit_stream::new();
    let (trailers_tx, trailers_rx) = wit_future::new(|| todo!());
    let (response, _transmit) = Response::new(headers, Some(body_rx), trailers_rx);
    let _ = response.set_status_code(status);
    wit_bindgen::spawn_local(async move {
        let _ = body_tx.write_all(payload.into_bytes()).await;
        drop(body_tx);
        let _ = trailers_tx.write(Ok(None)).await;
    });
    response
}
