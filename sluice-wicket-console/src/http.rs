//! The console's answers, and the request bodies it reads: in memory (a
//! component, an app's JSON) or streamed to a file under its digest (a
//! rootfs of hundreds of MiB, [`receive`]).

use std::path::{Path, PathBuf};

use bytes::Bytes;
use http::request::Parts;
use http::{HeaderValue, StatusCode, header};
use http_body_util::{BodyExt as _, Full, LengthLimitError, Limited};
use serde::Serialize;
use sha2::Digest as _;
use tokio::io::AsyncWriteExt as _;

pub use sluice_wicket_server::http::Body;

pub type Response = hyper::Response<Full<Bytes>>;

/// The answer refusing a request, before it is one: its status and the JSON
/// error the page shows.
pub struct Refusal {
    pub status: StatusCode,
    pub view: ErrorView,
}

impl Refusal {
    pub fn new(status: StatusCode, message: impl Into<String>) -> Refusal {
        Refusal {
            status,
            view: ErrorView {
                error: message.into(),
                host: None,
                missing: Vec::new(),
                did_not_start: false,
                no_room: false,
            },
        }
    }
}

impl From<Refusal> for Response {
    fn from(refusal: Refusal) -> Response {
        json(refusal.status, &refusal.view)
    }
}

#[derive(Serialize)]
pub struct ErrorView {
    pub error: String,
    /// The host of the route that did not load (422).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub host: Option<String>,
    /// The digests of the files an app named and no upload kept (409).
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub missing: Vec<String>,
    /// The route's VMs did not start, of what they are (422): a controller need not try again
    /// until something it asks changes.
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub did_not_start: bool,
    /// The routes would need more than the wicket's budget (422): a controller places the app
    /// elsewhere.
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub no_room: bool,
}

/// An edit the routes took: the generation serving it, and what changed.
#[derive(Serialize)]
pub struct Edited {
    pub generation: u64,
    pub message: String,
}

pub fn json(status: StatusCode, body: &impl Serialize) -> Response {
    hyper::Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::CACHE_CONTROL, "no-store")
        .body(Full::new(Bytes::from(
            serde_json::to_vec(body).expect("serializable"),
        )))
        .expect("a valid response")
}

pub fn error(status: StatusCode, message: impl Into<String>) -> Response {
    Refusal::new(status, message).into()
}

/// One of the page's files.
pub fn asset(content_type: &'static str, body: &'static [u8]) -> Response {
    hyper::Response::builder()
        .header(header::CONTENT_TYPE, content_type)
        // they change with the binary: revalidated, so an upgrade shows
        .header(header::CACHE_CONTROL, "no-cache")
        .body(Full::new(Bytes::from_static(body)))
        .expect("a valid response")
}

/// A redirect to `location`, setting `cookie` when there is one.
pub fn redirect(location: &str, cookie: Option<String>) -> Response {
    let mut response = hyper::Response::builder()
        .status(StatusCode::FOUND)
        .header(header::LOCATION, location)
        .header(header::CACHE_CONTROL, "no-store");
    if let Some(cookie) = cookie {
        response = response.header(header::SET_COOKIE, cookie);
    }
    response
        .body(Full::new(Bytes::new()))
        .expect("a plain response")
}

/// A page of the sign-in: `title` and the `body` fragment (the page's own
/// style is behind the sign-in).
pub fn page(status: StatusCode, title: &str, body: String) -> Response {
    hyper::Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "text/html; charset=utf-8")
        .header(header::CACHE_CONTROL, "no-store")
        .body(Full::new(Bytes::from(format!(
            "<!DOCTYPE html><html lang=\"en\"><head><meta charset=\"UTF-8\"><meta name=\"viewport\" content=\"width=device-width,initial-scale=1\"><title>{title} - Sluice wicket</title></head><body><h1>{title}</h1>{body}</body></html>"
        ))))
        .expect("a plain response")
}

/// `text` safe inside an HTML element.
pub fn escape(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

/// The pairs of a query, url-decoded.
pub fn pairs(query: Option<&str>) -> Vec<(String, String)> {
    form_urlencoded::parse(query.unwrap_or_default().as_bytes())
        .into_owned()
        .collect()
}

/// The length a request declares, if any.
fn declared(parts: &Parts) -> Option<u64> {
    parts
        .headers
        .get(header::CONTENT_LENGTH)
        .and_then(|length| length.to_str().ok())
        .and_then(|length| length.parse::<u64>().ok())
}

/// The body of a request, `limit` bytes at most (else 413, `too_large`),
/// read before anything is answered: a refusal answered before would be lost
/// to the reset of the connection (see [`drain`]).
pub async fn read(
    parts: &Parts,
    body: Body,
    limit: usize,
    too_large: &str,
) -> Result<Bytes, Refusal> {
    if declared(parts).is_some_and(|length| length > limit as u64) {
        return Err(Refusal::new(StatusCode::PAYLOAD_TOO_LARGE, too_large));
    }
    match Limited::new(body, limit).collect().await {
        Ok(collected) => Ok(collected.to_bytes()),
        Err(e) if e.is::<LengthLimitError>() => {
            Err(Refusal::new(StatusCode::PAYLOAD_TOO_LARGE, too_large))
        }
        Err(e) => Err(Refusal::new(
            StatusCode::BAD_REQUEST,
            format!("the upload failed: {e}"),
        )),
    }
}

/// Reads what is left of a request's body, `limit` bytes at most, and drops
/// it. hyper reads no further than a request's answer, and a connection
/// closed with bytes unread is reset, which may lose the answer to the
/// client: a refusal of an upload would read as a network error. A request
/// that expects `100 Continue` and was not read is not: its client sent no
/// body, and takes the answer instead.
pub async fn drain(parts: &Parts, mut body: Body, limit: u64) {
    let continues = parts
        .headers
        .get(header::EXPECT)
        .is_some_and(|expect| expect.as_bytes().eq_ignore_ascii_case(b"100-continue"));
    if continues {
        return;
    }
    let mut read = 0;
    while let Some(Ok(frame)) = body.frame().await {
        read += frame.data_ref().map_or(0, |data| data.len() as u64);
        if read > limit {
            return;
        }
    }
}

/// A body received to a file of the console's directory, by [`receive`]:
/// hidden (a partial write) until it is kept, removed when dropped
/// otherwise.
pub struct Received {
    path: Option<PathBuf>,
    pub sha256: String,
    pub bytes: u64,
}

impl Received {
    /// The file at `path`, hidden on the file system it is kept on, as if received.
    pub fn adopt(path: PathBuf, sha256: String, bytes: u64) -> Received {
        Received {
            path: Some(path),
            sha256,
            bytes,
        }
    }

    /// Renames the file to `to` (made in its directory, which is made when
    /// missing), unless a file is there: one of the same digest, as the
    /// caller names it.
    pub fn keep(mut self, to: &Path) -> Result<(), String> {
        let path = self.path.take().expect("not kept yet");
        let failed = |e: std::io::Error| format!("write {}: {e}", to.display());
        if crate::store::regular(to) {
            let _ = std::fs::remove_file(&path);
            return Ok(());
        }
        let renamed = std::fs::create_dir_all(to.parent().expect("a file in a directory"))
            .and_then(|()| std::fs::rename(&path, to));
        if renamed.is_err() {
            let _ = std::fs::remove_file(&path);
        }
        renamed.map_err(failed)
    }
}

impl Drop for Received {
    fn drop(&mut self) {
        if let Some(path) = &self.path {
            let _ = std::fs::remove_file(path);
        }
    }
}

/// How [`receive`] takes a body.
pub struct Intake<'a, F> {
    /// Where the file is written.
    pub dir: &'a Path,
    /// Bytes at most, else 413 with `too_large`.
    pub limit: u64,
    pub too_large: &'a str,
    /// The refusal of an empty body.
    pub empty: &'a str,
    /// `check` sees the first `head` bytes (all of them when shorter) as they
    /// arrive, and refuses the body with 400 before the rest is written.
    pub head: usize,
    pub check: F,
}

/// Receives a request's body to a hidden file while hashing it, as `intake`
/// has it.
pub async fn receive<F: Fn(&[u8]) -> Result<(), String>>(
    parts: &Parts,
    mut body: Body,
    intake: Intake<'_, F>,
) -> Result<Received, Refusal> {
    let Intake {
        dir,
        limit,
        too_large,
        empty,
        head,
        check,
    } = intake;
    if declared(parts).is_some_and(|length| length > limit) {
        return Err(Refusal::new(StatusCode::PAYLOAD_TOO_LARGE, too_large));
    }
    let failed = |e: std::io::Error| {
        Refusal::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("write {}: {e}", dir.display()),
        )
    };
    tokio::fs::create_dir_all(dir).await.map_err(failed)?;
    let mut name = [0u8; 12];
    getrandom::fill(&mut name).map_err(|e| failed(std::io::Error::other(e.to_string())))?;
    let name: String = name.iter().map(|byte| format!("{byte:02x}")).collect();
    let path = dir.join(format!(".upload-{name}.partial"));
    let mut file = tokio::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)
        .await
        .map_err(failed)?;
    // removed whatever comes from here on, unless kept
    let mut received = Received {
        path: Some(path),
        sha256: String::new(),
        bytes: 0,
    };
    let mut hasher = sha2::Sha256::new();
    let mut first = Vec::with_capacity(head);
    let mut checked = false;
    while let Some(frame) = body.frame().await {
        let frame = frame.map_err(|e| {
            Refusal::new(StatusCode::BAD_REQUEST, format!("the upload failed: {e}"))
        })?;
        let Some(data) = frame.data_ref() else {
            continue;
        };
        received.bytes += data.len() as u64;
        if received.bytes > limit {
            return Err(Refusal::new(StatusCode::PAYLOAD_TOO_LARGE, too_large));
        }
        if !checked {
            let wanted = (head - first.len()).min(data.len());
            first.extend_from_slice(&data[..wanted]);
            if first.len() == head {
                check(&first).map_err(|e| Refusal::new(StatusCode::BAD_REQUEST, e))?;
                checked = true;
            }
        }
        hasher.update(data);
        file.write_all(data).await.map_err(failed)?;
    }
    if received.bytes == 0 {
        return Err(Refusal::new(StatusCode::BAD_REQUEST, empty));
    }
    if !checked {
        check(&first).map_err(|e| Refusal::new(StatusCode::BAD_REQUEST, e))?;
    }
    file.sync_all().await.map_err(failed)?;
    received.sha256 = hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    Ok(received)
}

/// The headers of every answer: the page loads its own files only and runs
/// no inline script or style, nothing sniffs a type, no referrer leaves.
pub fn secure(response: &mut Response) {
    const CSP: &str = "default-src 'none'; script-src 'self'; style-src 'self'; font-src 'self'; \
                       img-src 'self' data:; connect-src 'self'; base-uri 'none'; \
                       form-action 'none'; frame-ancestors 'none'";
    let headers = response.headers_mut();
    headers.insert(
        header::CONTENT_SECURITY_POLICY,
        HeaderValue::from_static(CSP),
    );
    headers.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    headers.insert(
        header::REFERRER_POLICY,
        HeaderValue::from_static("no-referrer"),
    );
}
