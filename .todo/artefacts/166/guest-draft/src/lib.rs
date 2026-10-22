// Spike guest on wasi:keyvalue@0.2.0-draft, the version `wasmtime serve -Skeyvalue`
// links: the same component runs there and on the wicket.
//
// Build: cargo build --release   (-> target/wasm32-wasip3/release/kv_spike_guest_draft.wasm)
wit_bindgen::generate!({
    path: ["../../../../sluice-wicket-wasi/wit", "../wit/keyvalue-draft"],
    inline: "
        package sluice:kv-spike-draft;

        world kv-spike-draft {
            import wasi:http/types@0.3.0;
            import wasi:keyvalue/store@0.2.0-draft;
            import wasi:keyvalue/atomics@0.2.0-draft;
            import wasi:keyvalue/batch@0.2.0-draft;
            export wasi:http/handler@0.3.0;
        }
    ",
    generate_all,
});

use exports::wasi::http::handler::Guest;
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
        let bucket_id = if bucket_id == "_" { "" } else { bucket_id };

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
            // set then get in one request: one bucket handle
            "setget" => format!(
                "{:?} {:?}\n",
                bucket.set(&key, &body),
                bucket.get(&key).map(|v| v.map(lossy))
            ),
            "list" => format!("{:?}\n", bucket.list_keys(None).map(|page| (page.keys, page.cursor))),
            "batch" => {
                let keys = ["a".to_string(), "missing".to_string()];
                format!(
                    "{:?}\n",
                    batch::get_many(&bucket, &keys).map(|pairs| pairs
                        .into_iter()
                        .map(|pair| pair.map(|(k, v)| (k, lossy(v))))
                        .collect::<Vec<_>>())
                )
            }
            _ => format!("unknown op {op:?}\n"),
        };
        Ok(text(out))
    }
}

export!(Component);

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
