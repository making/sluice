// Spike guest: a wasi:http 0.3 handler using wasi:keyvalue@0.2.0-draft2.
//
// Build: cargo build --release   (-> target/wasm32-wasip3/release/kv_spike_guest.wasm)
wit_bindgen::generate!({
    path: ["../../../../sluice-wicket-wasi/wit", "../wit/keyvalue"],
    inline: "
        package sluice:kv-spike;

        world kv-spike {
            import wasi:clocks/monotonic-clock@0.3.0;
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
use wasi::clocks::monotonic_clock;
use wasi::http::types::{ErrorCode, Request, Response};
use wasi::keyvalue::{atomics, batch, store};
use wit_bindgen::StreamResult;

struct Component;

impl Guest for Component {
    async fn handle(request: Request) -> Result<Response, ErrorCode> {
        let path = request.get_path_with_query().unwrap_or_default();
        let body = read_body(request).await;
        let mut segments = path.trim_start_matches('/').splitn(3, '/');
        let (op, bucket_id, key) = (
            segments.next().unwrap_or(""),
            segments.next().unwrap_or(""),
            segments.next().unwrap_or("").to_string(),
        );
        // `_` names the default bucket "" in the path
        let bucket_id = if bucket_id == "_" { "" } else { bucket_id };

        if op == "concurrent" {
            return Ok(concurrent(bucket_id.to_string()));
        }

        let bucket = match store::open(bucket_id) {
            Ok(bucket) => bucket,
            Err(e) => return Ok(text(format!("open {bucket_id:?}: {e:?}\n"))),
        };
        let out = match op {
            "get" => format!("{:?}\n", bucket.get(&key).map(|v| v.map(lossy))),
            "set" => format!("{:?}\n", bucket.set(&key, &body)),
            "del" => format!("{:?}\n", bucket.delete(&key)),
            "exists" => format!("{:?}\n", bucket.exists(&key)),
            "incr" => format!("{:?}\n", atomics::increment(&bucket, &key, 1)),
            "list" => {
                let mut pages = Vec::new();
                let mut cursor = None;
                loop {
                    match bucket.list_keys(cursor.as_deref()) {
                        Ok(page) => {
                            pages.push(page.keys);
                            cursor = page.cursor;
                            if cursor.is_none() {
                                break;
                            }
                        }
                        Err(e) => {
                            pages.push(vec![format!("{e:?}")]);
                            break;
                        }
                    }
                }
                format!("{pages:?}\n")
            }
            "cas" => cas_increment(&bucket, &key),
            "batch" => {
                let set = batch::set_many(
                    &bucket,
                    &[
                        ("b1".to_string(), b"one".to_vec()),
                        ("b2".to_string(), b"two".to_vec()),
                    ],
                );
                let keys = ["b1".to_string(), "b2".to_string(), "missing".to_string()];
                let got = batch::get_many(&bucket, &keys).map(|pairs| {
                    pairs
                        .into_iter()
                        .map(|(k, v)| (k, v.map(lossy)))
                        .collect::<Vec<_>>()
                });
                let deleted = batch::delete_many(&bucket, &keys);
                format!("set={set:?} get={got:?} delete={deleted:?}\n")
            }
            _ => format!("unknown op {op:?}\n"),
        };
        Ok(text(out))
    }
}

export!(Component);

/// Read-modify-write through the CAS handle, retrying on a lost race.
fn cas_increment(bucket: &store::Bucket, key: &str) -> String {
    let mut cas = match atomics::Cas::new(bucket, key) {
        Ok(cas) => cas,
        Err(e) => return format!("cas new: {e:?}\n"),
    };
    for attempt in 1..=10 {
        let current = match cas.current() {
            Ok(v) => v,
            Err(e) => return format!("cas current: {e:?}\n"),
        };
        let n: i64 = current
            .map(|v| lossy(v).parse().unwrap_or(0))
            .unwrap_or(0);
        match atomics::swap(cas, (n + 1).to_string().as_bytes()) {
            Ok(()) => return format!("swapped to {} after {attempt} attempt(s)\n", n + 1),
            Err(atomics::CasError::CasFailed(next)) => cas = next,
            Err(atomics::CasError::StoreError(e)) => return format!("cas swap: {e:?}\n"),
        }
    }
    "gave up\n".into()
}

/// Streams 6 ticks 50ms apart while another guest task makes one keyvalue
/// call: shows whether a pending (slow) keyvalue call stalls the instance.
fn concurrent(bucket_id: String) -> Response {
    let headers = wasi::http::types::Headers::new();
    let (mut body_tx, body_rx) = wit_stream::new();
    let (trailers_tx, trailers_rx) = wit_future::new(|| todo!());
    let (response, _transmit) = Response::new(headers, Some(body_rx), trailers_rx);
    let start = monotonic_clock::now();
    wit_bindgen::spawn_local(async move {
        for tick in 0..6 {
            let ms = (monotonic_clock::now() - start) / 1_000_000;
            let _ = body_tx
                .write_all(format!("tick {tick} at {ms}ms\n").into_bytes())
                .await;
            monotonic_clock::wait_for(50_000_000).await;
        }
        drop(body_tx);
        let _ = trailers_tx.write(Ok(None)).await;
    });
    wit_bindgen::spawn_local(async move {
        // let the ticker start first
        monotonic_clock::wait_for(20_000_000).await;
        let before = (monotonic_clock::now() - start) / 1_000_000;
        let got = store::open(&bucket_id).and_then(|bucket| bucket.get("k"));
        let after = (monotonic_clock::now() - start) / 1_000_000;
        eprintln!("guest: keyvalue get {before}ms -> {after}ms: {got:?}");
    });
    response
}

async fn read_body(request: Request) -> Vec<u8> {
    let (result_tx, result_rx) = wit_future::new(|| Ok(()));
    let (mut body, _trailers) = Request::consume_body(request, result_rx);
    let mut all = Vec::new();
    let mut chunk: Vec<u8> = Vec::with_capacity(8192);
    loop {
        let (status, buf) = body.read(chunk).await;
        chunk = buf;
        all.extend_from_slice(&chunk);
        chunk.clear();
        match status {
            StreamResult::Complete(_) => {}
            StreamResult::Dropped | StreamResult::Cancelled => break,
        }
    }
    drop(result_tx);
    all
}

fn text(payload: String) -> Response {
    let headers = wasi::http::types::Headers::new();
    let _ = headers.append("content-type", b"text/plain");
    let (mut body_tx, body_rx) = wit_stream::new();
    let (trailers_tx, trailers_rx) = wit_future::new(|| todo!());
    let (response, _transmit) = Response::new(headers, Some(body_rx), trailers_rx);
    wit_bindgen::spawn_local(async move {
        let _ = body_tx.write_all(payload.into_bytes()).await;
        drop(body_tx);
        let _ = trailers_tx.write(Ok(None)).await;
    });
    response
}

fn lossy(bytes: Vec<u8>) -> String {
    String::from_utf8_lossy(&bytes).into_owned()
}
