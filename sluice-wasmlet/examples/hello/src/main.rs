// Sluice guest handler: a wasi:http 0.3 incoming-handler component.
//
// Build: cargo build --release --target wasm32-wasip2 -p sluice-guest
wit_bindgen::generate!({
    path: "../../wit",
    world: "sluice:guest/handler",
    features: ["clocks-timezone"],
    generate_all,
});

use exports::wasi::http::handler::Guest;
use wasi::http::types::{ErrorCode, Request, Response};
use wit_bindgen::StreamResult;

struct Component;

impl Guest for Component {
    async fn handle(request: Request) -> Result<Response, ErrorCode> {
        let method = request.get_method();
        let path = request.get_path_with_query().unwrap_or_default();

        // Read the request body (bounded); the PoC only reports its size.
        let (result_tx, result_rx) = wit_future::new(|| Ok(()));
        let (mut body, _trailers) = Request::consume_body(request, result_rx);
        let mut chunk: Vec<u8> = Vec::with_capacity(8192);
        let mut body_len = 0usize;
        loop {
            let (status, buf) = body.read(chunk).await;
            chunk = buf;
            match status {
                StreamResult::Complete(n) => body_len += n,
                StreamResult::Dropped | StreamResult::Cancelled => break,
            }
            if body_len > 1024 * 1024 {
                break;
            }
        }
        drop(result_tx);

        if path == "/panic" {
            // Sandbox demo: a guest trap must not take the tunnel down.
            panic!("guest panic (PoC)");
        }

        if path == "/spin" {
            // Limits demo: a CPU-bound guest, interrupted by the epoch budget.
            spin();
        }

        if path == "/balloon" {
            // Limits demo: a memory-hungry guest, capped by the memory limiter.
            balloon();
        }

        if path == "/stream" {
            // 10 MiB in paced chunks: exercises the streaming response path
            // (bytes must reach the client while the body is still produced).
            const CHUNKS: usize = 20;
            const CHUNK_LEN: usize = 512 * 1024;
            const PACE_NANOS: u64 = 50 * 1000 * 1000;

            let response_headers = wasi::http::types::Headers::new();
            let _ = response_headers.append("content-type", b"application/octet-stream");
            let _ = response_headers.append("content-length", b"10485760");
            let (mut body_tx, body_rx) = wit_stream::new();
            let (trailers_tx, trailers_rx) = wit_future::new(|| todo!());
            let (response, _transmit) = Response::new(response_headers, Some(body_rx), trailers_rx);
            wit_bindgen::spawn(async move {
                for _ in 0..CHUNKS {
                    let _ = body_tx.write_all(vec![0xAB; CHUNK_LEN]).await;
                    wasi::clocks::monotonic_clock::wait_for(PACE_NANOS).await;
                }
                drop(body_tx);
                let _ = trailers_tx.write(Ok(None)).await;
            });
            return Ok(response);
        }

        let payload =
            format!("hello from wasm\nmethod={method:?}\npath={path}\nbody_bytes={body_len}\n");
        // Fresh headers: the request's would carry a mismatched content-length.
        let response_headers = wasi::http::types::Headers::new();
        let _ = response_headers.append("content-type", b"text/plain");
        let (mut body_tx, body_rx) = wit_stream::new();
        let (trailers_tx, trailers_rx) = wit_future::new(|| todo!());
        let (response, _transmit) = Response::new(response_headers, Some(body_rx), trailers_rx);
        wit_bindgen::spawn(async move {
            let _ = body_tx.write_all(payload.into_bytes()).await;
            drop(body_tx);
            let _ = trailers_tx.write(Ok(None)).await;
        });
        Ok(response)
    }
}

export!(Component);

/// Burns CPU forever; the host's epoch budget is what ends it (504).
fn spin() -> ! {
    let mut x = 1u64;
    loop {
        x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        std::hint::black_box(x);
    }
}

/// Keeps allocating 1 MiB chunks; the host's memory limiter is what ends it
/// (allocation failure -> trap).
fn balloon() -> ! {
    let mut keep: Vec<Vec<u8>> = Vec::new();
    loop {
        keep.push(vec![0x41; 1024 * 1024]);
    }
}

fn main() {}
