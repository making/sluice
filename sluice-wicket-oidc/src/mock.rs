//! A provider for the tests: discovery, an authorization endpoint consenting
//! at once as the user set by [`MockProvider::sign_in`], a token endpoint
//! answering an ID token (its signature is not checked: the client takes the
//! token straight from this endpoint). Plain HTTP on the loopback, one
//! request per connection, on a thread of its own.

use std::collections::HashMap;
use std::io::{Read as _, Write as _};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use base64::Engine as _;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

/// The provider: `issuer` is its URL.
pub struct MockProvider {
    pub issuer: String,
    user: Arc<Mutex<Value>>,
    stop: Arc<AtomicBool>,
}

impl MockProvider {
    pub fn start(client_id: &str, client_secret: &str) -> MockProvider {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let issuer = format!("http://{}", listener.local_addr().unwrap());
        let user = Arc::new(Mutex::new(Value::Null));
        let stop = Arc::new(AtomicBool::new(false));
        let state = Arc::new(Mutex::new(MockState {
            issuer: issuer.clone(),
            client_id: client_id.to_string(),
            client_secret: client_secret.to_string(),
            user: user.clone(),
            codes: HashMap::new(),
        }));
        let stopping = stop.clone();
        std::thread::spawn(move || {
            while !stopping.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((stream, _)) => {
                        stream.set_nonblocking(false).unwrap();
                        state.lock().unwrap().serve(stream);
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(10));
                    }
                    Err(e) => panic!("mock provider accept: {e}"),
                }
            }
        });
        MockProvider { issuer, user, stop }
    }

    /// Who the next sign-ins are: the ID token's claims (sub, email, ...).
    pub fn sign_in(&self, claims: Value) {
        *self.user.lock().unwrap() = claims;
    }
}

impl Drop for MockProvider {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
    }
}

struct MockState {
    issuer: String,
    client_id: String,
    client_secret: String,
    user: Arc<Mutex<Value>>,
    /// The codes issued: the nonce, the PKCE challenge and the redirect URI
    /// of their authorization request.
    codes: HashMap<String, (String, String, String)>,
}

impl MockState {
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
        let length: usize = headers
            .get("content-length")
            .and_then(|length| length.parse().ok())
            .unwrap_or(0);
        let mut body = vec![0u8; length];
        stream.read_exact(&mut body).unwrap();
        let mut parts = request.split(' ');
        let (method, target) = (parts.next().unwrap(), parts.next().unwrap());
        let (path, query) = target.split_once('?').unwrap_or((target, ""));
        let query: HashMap<String, String> = form_urlencoded::parse(query.as_bytes())
            .into_owned()
            .collect();
        let (status, location, body) = match (method, path) {
            ("GET", "/.well-known/openid-configuration") => (
                200,
                None,
                json!({
                    "issuer": self.issuer,
                    "authorization_endpoint": format!("{}/authorize", self.issuer),
                    "token_endpoint": format!("{}/token", self.issuer),
                    "jwks_uri": format!("{}/jwks", self.issuer),
                    "response_types_supported": ["code"],
                    "subject_types_supported": ["public"],
                    "id_token_signing_alg_values_supported": ["RS256"],
                    "token_endpoint_auth_methods_supported": ["client_secret_basic"],
                    "code_challenge_methods_supported": ["S256"],
                })
                .to_string(),
            ),
            ("GET", "/authorize") => {
                assert_eq!(query["client_id"], self.client_id);
                assert_eq!(query["response_type"], "code");
                assert_eq!(query["code_challenge_method"], "S256");
                let code = format!("code-{}", self.codes.len() + 1);
                self.codes.insert(
                    code.clone(),
                    (
                        query["nonce"].clone(),
                        query["code_challenge"].clone(),
                        query["redirect_uri"].clone(),
                    ),
                );
                (
                    302,
                    Some(format!(
                        "{}?code={code}&state={}",
                        query["redirect_uri"], query["state"]
                    )),
                    String::new(),
                )
            }
            ("POST", "/token") => {
                let form: HashMap<String, String> =
                    form_urlencoded::parse(&body).into_owned().collect();
                let basic = base64::engine::general_purpose::STANDARD
                    .encode(format!("{}:{}", self.client_id, self.client_secret));
                if headers.get("authorization") != Some(&format!("Basic {basic}")) {
                    (401, None, json!({"error": "invalid_client"}).to_string())
                } else {
                    let Some((nonce, challenge, redirect_uri)) = self
                        .codes
                        .remove(form.get("code").map_or("", String::as_str))
                    else {
                        panic!("the mock provider never issued this code: {form:?}");
                    };
                    assert_eq!(form["grant_type"], "authorization_code");
                    assert_eq!(form["redirect_uri"], redirect_uri);
                    let verified = base64::engine::general_purpose::URL_SAFE_NO_PAD
                        .encode(Sha256::digest(form["code_verifier"].as_bytes()));
                    assert_eq!(verified, challenge, "PKCE");
                    let mut claims = self.user.lock().unwrap().clone();
                    let now = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_secs();
                    claims["iss"] = json!(self.issuer);
                    claims["aud"] = json!(self.client_id);
                    claims["iat"] = json!(now);
                    claims["exp"] = json!(now + 300);
                    claims["nonce"] = json!(nonce);
                    let encode = |value: &Value| {
                        base64::engine::general_purpose::URL_SAFE_NO_PAD
                            .encode(serde_json::to_vec(value).unwrap())
                    };
                    let id_token = format!(
                        "{}.{}.not-checked",
                        encode(&json!({"alg": "RS256", "kid": "mock"})),
                        encode(&claims)
                    );
                    (
                        200,
                        None,
                        json!({
                            "access_token": "mock-access", "token_type": "bearer",
                            "expires_in": 300, "id_token": id_token,
                        })
                        .to_string(),
                    )
                }
            }
            _ => (404, None, format!("no {method} {path}")),
        };
        let location = location.map_or(String::new(), |at| format!("Location: {at}\r\n"));
        let _ = stream.write_all(
            format!(
                "HTTP/1.1 {status} X\r\n{location}Content-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            )
            .as_bytes(),
        );
    }
}
