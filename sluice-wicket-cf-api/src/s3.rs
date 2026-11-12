//! The S3 calls of the blobs' bucket (`crate::blobs`): an object put whole from a file, read
//! as a stream, asked for, deleted, and the bucket's keys listed; signed with Signature
//! Version 4 (the payload's digest signed: the bucket checks what it got). An endpoint named is
//! addressed by path (`<endpoint>/<bucket>/<key>`: MinIO-likes, gateways), AWS by the
//! bucket's host.
//!
//! An object is there whole or not at all: a put is one request, so nothing unfinished is left
//! in the bucket (5 GiB at most, far above an upload's and a droplet's bounds).

use std::path::Path;
use std::time::Duration;

use sha2::{Digest, Sha256};

use crate::blobs::BlobSettings;
use crate::store::{epoch_secs, rfc3339};

/// A call of no body to speak of.
const CALL_TIMEOUT: Duration = Duration::from_secs(30);

/// A put or the head of a read: an object as large as they come.
const BODY_TIMEOUT: Duration = Duration::from_secs(660);

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// How often a call is sent when the bucket did not answer it, or answered 5xx.
const ATTEMPTS: u32 = 3;

pub struct S3 {
    client: reqwest::Client,
    /// `https://host[:port]`.
    origin: String,
    /// As the `host` header has it.
    host: String,
    /// The bucket's path: `/<bucket>` addressed by path, else none.
    bucket: String,
    /// Before every key: none, or ending in `/`.
    prefix: String,
    region: String,
    access_key_id: String,
    secret_access_key: String,
}

/// An object being read.
pub struct Object {
    pub bytes: u64,
    pub response: reqwest::Response,
}

impl S3 {
    pub fn new(settings: &BlobSettings) -> Result<S3, String> {
        let endpoint = match &settings.endpoint {
            Some(endpoint) => endpoint.trim_end_matches('/').to_string(),
            None => format!(
                "https://{}.s3.{}.amazonaws.com",
                settings.bucket, settings.region
            ),
        };
        let url = reqwest::Url::parse(&endpoint)
            .ok()
            .filter(|url| matches!(url.scheme(), "http" | "https") && url.path() == "/")
            .ok_or("--blobs-endpoint: expected http(s)://host[:port]")?;
        let host = url.host_str().ok_or("--blobs-endpoint: no host")?;
        let host = match url.port() {
            Some(port) => format!("{host}:{port}"),
            None => host.to_string(),
        };
        let tls = crate::tls::client_config(settings.ca_pem.as_deref(), "--blobs-ca-cert")?;
        let client = reqwest::Client::builder()
            .connect_timeout(CONNECT_TIMEOUT)
            .tls_backend_preconfigured(tls)
            .build()
            .map_err(|e| format!("--blobs: {e}"))?;
        Ok(S3 {
            client,
            origin: format!("{}://{host}", url.scheme()),
            host,
            bucket: match settings.endpoint {
                Some(_) => format!("/{}", settings.bucket),
                None => String::new(),
            },
            prefix: settings.prefix.clone(),
            region: settings.region.clone(),
            access_key_id: settings.access_key_id.clone(),
            secret_access_key: settings.secret_access_key.clone(),
        })
    }

    /// Puts the file at `from`, of the digest `sha256`, as the object `key`: whole, or not.
    pub async fn put(&self, key: &str, from: &Path, sha256: &str) -> Result<(), String> {
        let uri = self.uri(key);
        let sent = (self.send("PUT", &uri, "", sha256, BODY_TIMEOUT, Some(from))).await;
        sent.map(|_| ()).map_err(|e| format!("s3: put {key}: {e}"))
    }

    /// The object `key`; None: there is none.
    pub async fn get(&self, key: &str) -> Result<Option<Object>, String> {
        let failed = |e: String| format!("s3: get {key}: {e}");
        let sent = self.call("GET", &self.uri(key), "", BODY_TIMEOUT).await;
        let Some(response) = sent.map_err(failed)? else {
            return Ok(None);
        };
        let bytes = response
            .content_length()
            .ok_or_else(|| failed("no content-length".into()))?;
        Ok(Some(Object { bytes, response }))
    }

    pub async fn has(&self, key: &str) -> Result<bool, String> {
        let sent = self.call("HEAD", &self.uri(key), "", CALL_TIMEOUT).await;
        (sent.map(|found| found.is_some())).map_err(|e| format!("s3: head {key}: {e}"))
    }

    /// Deletes the object `key`; one gone already is deleted.
    pub async fn delete(&self, key: &str) -> Result<(), String> {
        let sent = self.call("DELETE", &self.uri(key), "", CALL_TIMEOUT).await;
        sent.map(|_| ())
            .map_err(|e| format!("s3: delete {key}: {e}"))
    }

    /// The keys of the bucket below the prefix, less it.
    pub async fn list(&self) -> Result<Vec<String>, String> {
        let failed = |e: String| format!("s3: list: {e}");
        let uri = match self.bucket.as_str() {
            "" => "/",
            bucket => bucket,
        };
        let mut keys = Vec::new();
        let mut token: Option<String> = None;
        loop {
            // in the order of their names, as they are signed
            let mut query = String::new();
            if let Some(token) = &token {
                query.push_str(&format!("continuation-token={}&", encode(token)));
            }
            query.push_str(&format!("list-type=2&prefix={}", encode(&self.prefix)));
            let sent = self.call("GET", uri, &query, CALL_TIMEOUT).await;
            let response = sent
                .map_err(failed)?
                .ok_or_else(|| failed("no bucket".into()))?;
            let listing = response.text().await.map_err(|e| failed(e.to_string()))?;
            for key in elements(&listing, "Key") {
                if let Some(key) = key.strip_prefix(&self.prefix) {
                    keys.push(key.to_string());
                }
            }
            token = elements(&listing, "NextContinuationToken").pop();
            if token.is_none() {
                return Ok(keys);
            }
        }
    }

    /// Makes the bucket, unless it is there: a test server's, the integration test's too.
    pub async fn make_bucket(&self) -> Result<(), String> {
        let uri = match self.bucket.as_str() {
            "" => "/",
            bucket => bucket,
        };
        match self.call("PUT", uri, "", CALL_TIMEOUT).await {
            Err(e) if !e.starts_with("409") => Err(format!("s3: make the bucket: {e}")),
            _ => Ok(()),
        }
    }

    fn uri(&self, key: &str) -> String {
        format!("{}/{}{key}", self.bucket, self.prefix)
    }

    /// A call of no body; None: answered 404.
    async fn call(
        &self,
        method: &str,
        uri: &str,
        query: &str,
        timeout: Duration,
    ) -> Result<Option<reqwest::Response>, String> {
        let nothing = hex(&Sha256::digest(b""));
        self.send(method, uri, query, &nothing, timeout, None).await
    }

    /// Sends the call, its body (of the digest `payload`) the file `body`, read on each
    /// attempt; None: answered 404.
    async fn send(
        &self,
        method: &str,
        uri: &str,
        query: &str,
        payload: &str,
        timeout: Duration,
        body: Option<&Path>,
    ) -> Result<Option<reqwest::Response>, String> {
        let url = match query {
            "" => format!("{}{uri}", self.origin),
            query => format!("{}{uri}?{query}", self.origin),
        };
        let mut attempt = 1;
        loop {
            let method = reqwest::Method::from_bytes(method.as_bytes()).expect("a method");
            let mut request = self.client.request(method.clone(), &url).timeout(timeout);
            for (name, value) in self.signed(method.as_str(), uri, query, payload, epoch_secs()) {
                request = request.header(name, value);
            }
            if let Some(body) = body {
                let unread = |e: std::io::Error| format!("{}: {e}", body.display());
                let file = tokio::fs::File::open(body).await.map_err(unread)?;
                let bytes = file.metadata().await.map_err(unread)?.len();
                request = request.header("content-length", bytes).body(file);
            }
            let failed = match request.send().await {
                Ok(response) if response.status().is_success() => return Ok(Some(response)),
                Ok(response) if response.status() == reqwest::StatusCode::NOT_FOUND => {
                    // of a bucket, not of a key: said
                    let body = response.text().await.unwrap_or_default();
                    if elements(&body, "Code")
                        .iter()
                        .any(|code| code == "NoSuchBucket")
                    {
                        return Err("404 NoSuchBucket".into());
                    }
                    return Ok(None);
                }
                Ok(response) => {
                    let status = response.status();
                    let body = response.text().await.unwrap_or_default();
                    let code = elements(&body, "Code").pop().unwrap_or_default();
                    let failed = format!("{} {code}", status.as_u16());
                    if !status.is_server_error() {
                        return Err(failed.trim_end().to_string());
                    }
                    failed
                }
                Err(e) => e.without_url().to_string(),
            };
            if attempt == ATTEMPTS {
                return Err(failed.trim_end().to_string());
            }
            tokio::time::sleep(Duration::from_millis(200 << attempt)).await;
            attempt += 1;
        }
    }

    /// The headers signing the call at `now` (seconds since the epoch).
    fn signed(
        &self,
        method: &str,
        uri: &str,
        query: &str,
        payload: &str,
        now: u64,
    ) -> [(&'static str, String); 3] {
        // 2026-10-30T01:17:47Z as 20261030T011747Z
        let at: String = (rfc3339(now).chars())
            .filter(|c| !matches!(c, '-' | ':'))
            .collect();
        let day = &at[..8];
        let request = format!(
            "{method}\n{uri}\n{query}\nhost:{}\nx-amz-content-sha256:{payload}\nx-amz-date:{at}\n\n\
             {SIGNED}\n{payload}",
            self.host
        );
        let scope = format!("{day}/{}/s3/aws4_request", self.region);
        let to_sign = format!(
            "AWS4-HMAC-SHA256\n{at}\n{scope}\n{}",
            hex(&Sha256::digest(request.as_bytes()))
        );
        let mut key = hmac(format!("AWS4{}", self.secret_access_key).as_bytes(), day);
        for part in [self.region.as_str(), "s3", "aws4_request", &to_sign] {
            key = hmac(&key, part);
        }
        let authorization = format!(
            "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={SIGNED}, Signature={}",
            self.access_key_id,
            hex(&key)
        );
        [
            ("x-amz-date", at),
            ("x-amz-content-sha256", payload.to_string()),
            ("authorization", authorization),
        ]
    }
}

/// The headers signed.
const SIGNED: &str = "host;x-amz-content-sha256;x-amz-date";

fn hmac(key: &[u8], data: &str) -> [u8; 32] {
    let mut block = [0u8; 64];
    if key.len() > block.len() {
        block[..32].copy_from_slice(&Sha256::digest(key));
    } else {
        block[..key.len()].copy_from_slice(key);
    }
    let inner = Sha256::new()
        .chain_update(block.map(|byte| byte ^ 0x36))
        .chain_update(data)
        .finalize();
    Sha256::new()
        .chain_update(block.map(|byte| byte ^ 0x5c))
        .chain_update(inner)
        .finalize()
        .into()
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// `value` as a query's value is signed: all but the unreserved characters escaped.
fn encode(value: &str) -> String {
    let mut encoded = String::new();
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                encoded.push(byte as char);
            }
            byte => encoded.push_str(&format!("%{byte:02X}")),
        }
    }
    encoded
}

/// The text of each element `name` of the XML `body`.
fn elements(body: &str, name: &str) -> Vec<String> {
    let (open, close) = (format!("<{name}>"), format!("</{name}>"));
    let mut found = Vec::new();
    let mut rest = body;
    while let Some((_, after)) = rest.split_once(&open) {
        let Some((text, after)) = after.split_once(&close) else {
            break;
        };
        let text = text
            .replace("&lt;", "<")
            .replace("&gt;", ">")
            .replace("&quot;", "\"")
            .replace("&apos;", "'")
            .replace("&amp;", "&");
        found.push(text);
        rest = after;
    }
    found
}
