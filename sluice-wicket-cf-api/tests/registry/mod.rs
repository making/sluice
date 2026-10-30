//! An OCI distribution registry for the test, as ghcr.io serves one: every
//! `/v2/` request needs a bearer token from `/token` -- an anonymous one reads
//! the public repositories, the user's (basic authentication at `/token`)
//! the private ones too, those under `private/` -- and a blob comes through
//! a redirect to its storage. Plain HTTP on the loopback, one request per
//! connection, on a thread of its own.

use std::collections::HashMap;
use std::io::{Read as _, Write as _};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use base64::Engine as _;
use serde_json::json;
use sha2::{Digest, Sha256};

const MANIFEST: &str = "application/vnd.oci.image.manifest.v1+json";

pub struct Registry {
    /// `127.0.0.1:<port>`: the registry of the references.
    pub address: String,
    content: Arc<Mutex<Content>>,
    stop: Arc<AtomicBool>,
}

struct Content {
    address: String,
    /// The user and the password of the private repositories.
    user: (String, String),
    /// `(repository, tag or digest)` -> the manifest.
    manifests: HashMap<(String, String), Vec<u8>>,
    /// Digest -> bytes.
    blobs: HashMap<String, Vec<u8>>,
    /// Token -> whether it is the user's.
    tokens: HashMap<String, bool>,
    /// The manifests served.
    pulls: usize,
}

impl Registry {
    /// A registry whose private repositories `user` reads with `password`.
    pub fn start(user: &str, password: &str) -> Registry {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let content = Arc::new(Mutex::new(Content {
            address: address.clone(),
            user: (user.to_string(), password.to_string()),
            manifests: HashMap::new(),
            blobs: HashMap::new(),
            tokens: HashMap::new(),
            pulls: 0,
        }));
        let stop = Arc::new(AtomicBool::new(false));
        let (serving, stopping) = (content.clone(), stop.clone());
        std::thread::spawn(move || {
            while !stopping.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((stream, _)) => {
                        stream.set_nonblocking(false).unwrap();
                        serving.lock().unwrap().serve(stream);
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(10));
                    }
                    Err(e) => panic!("registry accept: {e}"),
                }
            }
        });
        Registry {
            address,
            content,
            stop,
        }
    }

    /// Pushes `component` as the wasm OCI artifact (`wkg oci push`'s) to
    /// `repository:tag`, moving the tag: its manifest's digest.
    pub fn push(&self, repository: &str, tag: &str, component: &[u8]) -> String {
        let config = serde_json::to_vec(&json!({
            "architecture": "wasm",
            "os": "wasip2",
            "layerDigests": [digest(component)],
        }))
        .unwrap();
        let manifest = serde_json::to_vec(&json!({
            "schemaVersion": 2,
            "mediaType": MANIFEST,
            "config": {"mediaType": "application/vnd.wasm.config.v0+json", "digest": digest(&config), "size": config.len()},
            "layers": [{"mediaType": "application/wasm", "digest": digest(component), "size": component.len()}],
        }))
        .unwrap();
        let manifest_digest = digest(&manifest);
        let mut content = self.content.lock().unwrap();
        for reference in [tag, &manifest_digest] {
            content.manifests.insert(
                (repository.to_string(), reference.to_string()),
                manifest.clone(),
            );
        }
        content.blobs.insert(digest(&config), config);
        content.blobs.insert(digest(component), component.to_vec());
        manifest_digest
    }

    /// The manifests served so far.
    pub fn pulls(&self) -> usize {
        self.content.lock().unwrap().pulls
    }
}

impl Drop for Registry {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
    }
}

fn digest(bytes: &[u8]) -> String {
    let hex: String = Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    format!("sha256:{hex}")
}

impl Content {
    fn serve(&mut self, mut stream: TcpStream) {
        stream
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        let mut head = Vec::new();
        let mut byte = [0u8; 1];
        while !head.ends_with(b"\r\n\r\n") {
            if stream.read(&mut byte).unwrap_or(0) == 0 {
                return;
            }
            head.push(byte[0]);
        }
        let head = String::from_utf8_lossy(&head).to_string();
        let mut lines = head.lines();
        let request = lines.next().unwrap_or_default().to_string();
        let headers: HashMap<String, String> = lines
            .filter_map(|line| line.split_once(':'))
            .map(|(name, value)| (name.to_ascii_lowercase(), value.trim().to_string()))
            .collect();
        let mut parts = request.split(' ');
        let (method, target) = (parts.next().unwrap(), parts.next().unwrap());
        let (path, query) = target.split_once('?').unwrap_or((target, ""));
        let (status, extra, body) = self.respond(method, path, query, &headers);
        let mut response = format!(
            "HTTP/1.1 {status} {}\r\nContent-Length: {}\r\nConnection: close\r\n",
            if status < 400 { "OK" } else { "Error" },
            body.len()
        );
        for (name, value) in extra {
            response.push_str(&format!("{name}: {value}\r\n"));
        }
        response.push_str("\r\n");
        let _ = stream.write_all(response.as_bytes());
        let _ = stream.write_all(&body);
    }

    /// The status, the headers and the body of the answer.
    fn respond(
        &mut self,
        method: &str,
        path: &str,
        query: &str,
        headers: &HashMap<String, String>,
    ) -> (u16, Vec<(String, String)>, Vec<u8>) {
        let error = |status: u16, code: &str, message: &str| {
            let body = json!({"errors": [{"code": code, "message": message}]});
            (status, Vec::new(), body.to_string().into_bytes())
        };
        if let ("GET", "/token") = (method, path) {
            let user = match headers.get("authorization") {
                Some(basic) => {
                    let (user, password) = &self.user;
                    let wanted = base64::engine::general_purpose::STANDARD
                        .encode(format!("{user}:{password}"));
                    if *basic != format!("Basic {wanted}") {
                        return error(401, "UNAUTHORIZED", "bad credentials");
                    }
                    true
                }
                None => false,
            };
            let scope: HashMap<String, String> = form_urlencoded::parse(query.as_bytes())
                .into_owned()
                .collect();
            assert!(
                scope["scope"].ends_with(":pull"),
                "a pull's scope: {scope:?}"
            );
            let token = format!("t{}", self.tokens.len());
            self.tokens.insert(token.clone(), user);
            return (
                200,
                Vec::new(),
                json!({"token": token}).to_string().into_bytes(),
            );
        }
        if let Some(blob) = path.strip_prefix("/storage/") {
            return match self.blobs.get(blob) {
                Some(bytes) => (200, Vec::new(), bytes.clone()),
                None => error(404, "BLOB_UNKNOWN", "blob unknown"),
            };
        }
        let Some(rest) = path.strip_prefix("/v2/") else {
            return error(404, "NOT_FOUND", path);
        };
        let (repository, kind, reference) = match rest
            .rsplit_once("/manifests/")
            .map(|(repository, reference)| (repository, "manifests", reference))
            .or_else(|| {
                rest.rsplit_once("/blobs/")
                    .map(|(repository, reference)| (repository, "blobs", reference))
            }) {
            Some(found) => found,
            None => return error(404, "NOT_FOUND", path),
        };
        let token = headers
            .get("authorization")
            .and_then(|value| value.strip_prefix("Bearer "))
            .and_then(|token| self.tokens.get(token));
        if !token.is_some_and(|user| *user || !repository.starts_with("private/")) {
            let (status, mut extra, body) = error(401, "UNAUTHORIZED", "authentication required");
            extra.push((
                "WWW-Authenticate".into(),
                format!(
                    "Bearer realm=\"http://{}/token\",service=\"registry.test\",scope=\"repository:{repository}:pull\"",
                    self.address
                ),
            ));
            return (status, extra, body);
        }
        match kind {
            "manifests" => match self
                .manifests
                .get(&(repository.to_string(), reference.to_string()))
            {
                Some(manifest) => {
                    self.pulls += 1;
                    let extra = vec![
                        ("Content-Type".into(), MANIFEST.into()),
                        ("Docker-Content-Digest".into(), digest(manifest)),
                    ];
                    (200, extra, manifest.clone())
                }
                None => error(404, "MANIFEST_UNKNOWN", "manifest unknown"),
            },
            _ => {
                let location = format!("http://{}/storage/{reference}", self.address);
                (307, vec![("Location".into(), location)], Vec::new())
            }
        }
    }
}
