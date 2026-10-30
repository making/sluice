//! The UAA endpoints the cf CLI logs in with: `GET /uaa/login` (the prompts)
//! and `POST /uaa/oauth/token`. The tokens are JWTs signed (HS256) with a key
//! kept in the data directory, so a token outlives a restart; every other
//! request needs a valid one. Who gets one is the [`Mode`]: any credentials
//! (the password, refresh_token and client_credentials grants) or, with an
//! OpenID Provider, the one-time code `/uaa/passcode` showed (`cf login
//! --sso`) and the clients configured. A refresh token is good once: the
//! sessions, keyed by it, are kept in `sessions.json` and rotate.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use crate::oidc::{ClaimRule, Identity};
use crate::store::{epoch_secs, guid, write_by_rename};

/// How long an access token is valid.
const ACCESS_SECS: u64 = 12 * 3600;
/// How long a refresh token is valid.
const REFRESH_SECS: u64 = 30 * 86_400;
/// How long a passcode may wait for `cf login --sso`.
pub const PASSCODE_TTL: Duration = Duration::from_secs(5 * 60);
/// Passcodes waiting at most.
const MAX_PASSCODES: usize = 1000;

const SCOPES: [&str; 4] = [
    "cloud_controller.read",
    "cloud_controller.write",
    "cloud_controller.admin",
    "openid",
];

/// Who gets a token.
#[derive(Debug, Clone)]
pub enum Mode {
    /// Anyone: any credentials pass (the loopback-only mode).
    Open,
    /// Users the provider signed in whose ID token satisfies the rules
    /// (`--oidc-claim`), and the clients configured (`--client-credentials`).
    Provider {
        clients: Vec<(String, String)>,
        rules: Vec<ClaimRule>,
    },
}

/// Signs and verifies the tokens.
pub struct Uaa {
    key: [u8; 32],
    mode: Mode,
    sessions: Mutex<Sessions>,
    passcodes: Mutex<HashMap<String, (Identity, Instant)>>,
}

/// What a token says.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Claims {
    pub jti: String,
    pub sub: String,
    pub scope: Vec<String>,
    pub client_id: String,
    pub cid: String,
    pub azp: String,
    pub grant_type: String,
    pub user_id: String,
    /// `uaa` (the open mode), `oidc` (a user the provider signed in) or
    /// `client` (client credentials).
    pub origin: String,
    pub user_name: String,
    pub email: String,
    pub iat: u64,
    pub exp: u64,
    pub iss: String,
    pub zid: String,
    pub aud: Vec<String>,
}

/// Whose tokens: a login, kept by the session for its refreshes.
#[derive(Serialize, Deserialize, Debug, Clone)]
struct Subject {
    user_name: String,
    email: String,
    client: String,
    origin: String,
    /// The ID token's claims of a user the provider signed in, for the rules
    /// at each refresh; Null otherwise.
    claims: Value,
}

/// A login: the refresh token it holds now, and when that one expires.
#[derive(Serialize, Deserialize, Debug, Clone)]
struct Session {
    subject: Subject,
    expires: u64,
}

/// The sessions by their refresh token's `jti`, in `sessions.json`.
struct Sessions {
    file: PathBuf,
    by_jti: HashMap<String, Session>,
}

impl Sessions {
    fn open(dir: &Path) -> Result<Sessions, String> {
        let file = dir.join("sessions.json");
        let by_jti = match std::fs::read(&file) {
            Ok(bytes) => {
                serde_json::from_slice(&bytes).map_err(|e| format!("{}: {e}", file.display()))?
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => HashMap::new(),
            Err(e) => return Err(format!("{}: {e}", file.display())),
        };
        Ok(Sessions { file, by_jti })
    }

    /// Writes the sessions still valid.
    fn save(&mut self) {
        let now = epoch_secs();
        self.by_jti.retain(|_, session| session.expires > now);
        let bytes = serde_json::to_vec(&self.by_jti).expect("plain data");
        if let Err(e) = write_by_rename(&self.file, &bytes) {
            eprintln!("UAA: the sessions were not saved: {e}");
        }
    }
}

impl Uaa {
    /// The key of `jwt.key` in `dir`, made when missing; the file its
    /// owner's only (0600). The sessions of `sessions.json` next to it.
    pub fn open(dir: &Path, mode: Mode) -> Result<Uaa, String> {
        let file = dir.join("jwt.key");
        let failed = |e: std::io::Error| format!("{}: {e}", file.display());
        let mut key = [0u8; 32];
        match std::fs::read_to_string(&file) {
            Ok(hex) => {
                let hex = hex.trim();
                let bytes = (0..hex.len())
                    .step_by(2)
                    .map(|i| u8::from_str_radix(hex.get(i..i + 2).unwrap_or("zz"), 16))
                    .collect::<Result<Vec<u8>, _>>()
                    .ok()
                    .filter(|bytes| bytes.len() == 32)
                    .ok_or_else(|| format!("{}: not a 64 hex digit key", file.display()))?;
                key.copy_from_slice(&bytes);
                // one an older cf-api wrote as any file
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt as _;
                    let private = std::fs::Permissions::from_mode(0o600);
                    std::fs::set_permissions(&file, private).map_err(failed)?;
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                getrandom::fill(&mut key).map_err(|e| format!("no random key: {e}"))?;
                let hex: String = key.iter().map(|byte| format!("{byte:02x}")).collect();
                write_by_rename(&file, hex.as_bytes())?;
            }
            Err(e) => return Err(failed(e)),
        }
        Ok(Uaa {
            key,
            mode,
            sessions: Mutex::new(Sessions::open(dir)?),
            passcodes: Mutex::new(HashMap::new()),
        })
    }

    /// Whether users log in through the provider.
    pub fn provider(&self) -> bool {
        matches!(self.mode, Mode::Provider { .. })
    }

    /// The token response of `grant` for `subject`, issued by `issuer`: a
    /// new session, keyed by the refresh token.
    fn issue(&self, subject: Subject, grant: &str, issuer: &str) -> Value {
        let issued = epoch_secs();
        let client = subject.client.clone();
        let claims = Claims {
            jti: guid(),
            sub: user_id(&subject.user_name),
            scope: SCOPES.iter().map(|scope| scope.to_string()).collect(),
            client_id: client.clone(),
            cid: client.clone(),
            azp: client.clone(),
            grant_type: grant.to_string(),
            user_id: user_id(&subject.user_name),
            origin: subject.origin.clone(),
            user_name: subject.user_name.clone(),
            email: subject.email.clone(),
            iat: issued,
            exp: issued + ACCESS_SECS,
            iss: issuer.to_string(),
            zid: "uaa".into(),
            aud: vec!["cloud_controller".into(), "openid".into(), client],
        };
        let refresh = Claims {
            jti: format!("{}-r", claims.jti),
            exp: issued + REFRESH_SECS,
            grant_type: "refresh_token".into(),
            ..claims.clone()
        };
        {
            let mut sessions = self.sessions.lock().expect("no panic held it");
            sessions.by_jti.insert(
                refresh.jti.clone(),
                Session {
                    subject,
                    expires: refresh.exp,
                },
            );
            sessions.save();
        }
        json!({
            "access_token": self.sign(&claims),
            "token_type": "bearer",
            "refresh_token": self.sign(&refresh),
            "expires_in": ACCESS_SECS - 1,
            "scope": SCOPES.join(" "),
            "jti": claims.jti,
        })
    }

    fn sign(&self, claims: &Claims) -> String {
        let head = URL_SAFE_NO_PAD.encode(br#"{"alg":"HS256","typ":"JWT","kid":"cf-api"}"#);
        let body = URL_SAFE_NO_PAD.encode(serde_json::to_vec(claims).expect("plain data"));
        let signed = format!("{head}.{body}");
        let signature = URL_SAFE_NO_PAD.encode(hmac_sha256(&self.key, signed.as_bytes()));
        format!("{signed}.{signature}")
    }

    /// The claims of `token` when it is signed with the key, not expired
    /// and of this mode: with the provider, a token of the open mode is no
    /// good.
    pub fn verify(&self, token: &str) -> Result<Claims, String> {
        let (signed, signature) = token.rsplit_once('.').ok_or("not a JWT")?;
        let (head, body) = signed.split_once('.').ok_or("not a JWT")?;
        let given = URL_SAFE_NO_PAD
            .decode(signature)
            .map_err(|_| "bad signature encoding")?;
        let expected = hmac_sha256(&self.key, signed.as_bytes());
        if !constant_time_eq(&given, &expected) {
            return Err("bad signature".into());
        }
        let header: Value = serde_json::from_slice(
            &URL_SAFE_NO_PAD
                .decode(head)
                .map_err(|_| "bad header encoding")?,
        )
        .map_err(|_| "bad header")?;
        if header["alg"] != "HS256" {
            return Err("not HS256".into());
        }
        let claims: Claims = serde_json::from_slice(
            &URL_SAFE_NO_PAD
                .decode(body)
                .map_err(|_| "bad claims encoding")?,
        )
        .map_err(|e| format!("bad claims: {e}"))?;
        if claims.exp <= epoch_secs() {
            return Err("expired".into());
        }
        if self.provider() && claims.origin == "uaa" {
            return Err("issued before the OpenID Provider was configured: log in again".into());
        }
        Ok(claims)
    }

    /// A one-time code for `identity`, good for [`PASSCODE_TTL`]: what
    /// `cf login --sso` asks for.
    pub fn passcode(&self, identity: Identity) -> Result<String, String> {
        let mut passcodes = self.passcodes.lock().expect("no panic held it");
        passcodes.retain(|_, (_, since)| since.elapsed() < PASSCODE_TTL);
        if passcodes.len() >= MAX_PASSCODES {
            return Err("too many codes waiting: try again later".into());
        }
        let code = loop {
            let code = random_code();
            if !passcodes.contains_key(&code) {
                break code;
            }
        };
        passcodes.insert(code.clone(), (identity, Instant::now()));
        Ok(code)
    }

    /// Takes the passcode: the identity it was shown to, once.
    fn redeem(&self, code: &str) -> Result<Identity, String> {
        self.passcodes
            .lock()
            .expect("no panic held it")
            .remove(code)
            .filter(|(_, since)| since.elapsed() < PASSCODE_TTL)
            .map(|(identity, _)| identity)
            .ok_or_else(|| "the code is unknown, expired or was used already: get one more".into())
    }

    /// Answers a token request: the form of `body`, with `basic` the client
    /// of the Authorization header when there is one.
    pub fn grant(
        &self,
        body: &[u8],
        basic: Option<(String, String)>,
        issuer: &str,
    ) -> Result<Value, String> {
        let form: HashMap<String, String> = form_urlencoded::parse(body).into_owned().collect();
        let (client, secret) = match (form.get("client_id").filter(|c| !c.is_empty()), basic) {
            (Some(client), _) => (client.clone(), form.get("client_secret").cloned()),
            (None, Some((client, secret))) if !client.is_empty() => (client, Some(secret)),
            (None, _) => ("cf".to_string(), None),
        };
        let user = |user_name: &str, email: &str, origin: &str, claims: Value| Subject {
            user_name: user_name.to_string(),
            email: email.to_string(),
            client: client.clone(),
            origin: origin.to_string(),
            claims,
        };
        match form.get("grant_type").map(String::as_str) {
            Some("password") => match &self.mode {
                Mode::Open => {
                    let name = form
                        .get("username")
                        .filter(|user| !user.is_empty())
                        .ok_or("username is required")?;
                    Ok(self.issue(user(name, name, "uaa", Value::Null), "password", issuer))
                }
                Mode::Provider { .. } => {
                    let Some(code) = form.get("passcode").filter(|code| !code.is_empty()) else {
                        let passcode = issuer
                            .strip_suffix("/oauth/token")
                            .unwrap_or(issuer)
                            .to_string()
                            + "/passcode";
                        return Err(format!(
                            "passwords are not taken here: cf login --sso, with a one-time code from {passcode}"
                        ));
                    };
                    let identity = self.redeem(code)?;
                    Ok(self.issue(
                        user(
                            &identity.user_name,
                            &identity.email,
                            "oidc",
                            identity.claims,
                        ),
                        "password",
                        issuer,
                    ))
                }
            },
            Some("client_credentials") => {
                let origin = match &self.mode {
                    Mode::Open => "uaa",
                    Mode::Provider { clients, .. } => {
                        let known = clients.iter().any(|(id, expected)| {
                            *id == client
                                && secret.as_ref().is_some_and(|given| {
                                    constant_time_eq(given.as_bytes(), expected.as_bytes())
                                })
                        });
                        if !known {
                            return Err(format!("unknown client '{client}' or wrong secret"));
                        }
                        "client"
                    }
                };
                Ok(self.issue(
                    user(&client, &client, origin, Value::Null),
                    "client_credentials",
                    issuer,
                ))
            }
            Some("refresh_token") => {
                let token = form
                    .get("refresh_token")
                    .ok_or("refresh_token is required")?;
                let claims = self
                    .verify(token)
                    .map_err(|e| format!("invalid refresh token: {e}"))?;
                if claims.grant_type != "refresh_token" {
                    return Err("not a refresh token".into());
                }
                // taken out before anything else: good once
                let session = self
                    .sessions
                    .lock()
                    .expect("no panic held it")
                    .by_jti
                    .remove(&claims.jti)
                    .ok_or("the refresh token is unknown or was used already: log in again")?;
                if let Mode::Provider { rules, .. } = &self.mode
                    && session.subject.origin == "oidc"
                    && let Err(why) = crate::oidc::allowed(rules, &session.subject.claims)
                {
                    self.sessions.lock().expect("no panic held it").save();
                    return Err(format!(
                        "{} is no longer allowed ({why}): log in again",
                        session.subject.user_name
                    ));
                }
                Ok(self.issue(session.subject, "refresh_token", issuer))
            }
            Some(other) => Err(format!("unsupported grant type '{other}'")),
            None => Err("grant_type is required".into()),
        }
    }
}

/// What `GET /uaa/login` answers: the prompts the CLI asks with.
pub fn login_view(uaa: &str) -> Value {
    json!({
        "app": {"version": "78.0.0"},
        "showLoginLinks": true,
        "links": {"uaa": uaa, "passwd": "/forgot_password", "login": uaa, "register": "/create_account"},
        "zone_name": "uaa",
        "entityID": "cloudfoundry-saml-login",
        "commit_id": "cf-api",
        "idpDefinitions": {},
        "prompts": {
            "username": ["text", "Email"],
            "password": ["password", "Password"],
            "passcode": ["password", format!("Temporary Authentication Code ( Get one at {uaa}/passcode )")]
        },
        "timestamp": crate::store::now(),
    })
}

/// A stable id of a user name (a UUID v5 of it).
fn user_id(user: &str) -> String {
    let digest = Sha256::digest(user.as_bytes());
    uuid::Builder::from_sha1_bytes(digest[..16].try_into().expect("16 bytes"))
        .into_uuid()
        .to_string()
}

/// 8 characters of `a-z0-9`, each drawn evenly.
fn random_code() -> String {
    const ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
    let mut code = String::new();
    while code.len() < 8 {
        let mut bytes = [0u8; 16];
        getrandom::fill(&mut bytes).expect("the OS has randomness");
        // below the largest multiple of 36: no bias
        for byte in bytes.into_iter().filter(|byte| *byte < 252) {
            if code.len() < 8 {
                code.push(ALPHABET[usize::from(byte) % ALPHABET.len()] as char);
            }
        }
    }
    code
}

/// RFC 2104, with SHA-256: the key is 32 bytes, below the block size.
fn hmac_sha256(key: &[u8; 32], data: &[u8]) -> [u8; 32] {
    let mut ipad = [0x36u8; 64];
    let mut opad = [0x5cu8; 64];
    for (i, byte) in key.iter().enumerate() {
        ipad[i] ^= byte;
        opad[i] ^= byte;
    }
    let inner = Sha256::new()
        .chain_update(ipad)
        .chain_update(data)
        .finalize();
    Sha256::new()
        .chain_update(opad)
        .chain_update(inner)
        .finalize()
        .into()
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |diff, (x, y)| diff | (x ^ y)) == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    /// RFC 4231 test case 2.
    #[test]
    fn hmac_sha256_matches_the_known_answer() {
        let mut key = [0u8; 32];
        key[..4].copy_from_slice(b"Jefe");
        // the RFC's key is the 4 bytes; a zero-padded key hashes the same (padding to the block)
        let mac = hmac_sha256(&key, b"what do ya want for nothing?");
        let hex: String = mac.iter().map(|byte| format!("{byte:02x}")).collect();
        assert_eq!(
            hex,
            "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
        );
    }

    fn fresh_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("sluice-uaa-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn refresh(uaa: &Uaa, tokens: &Value, issuer: &str) -> Result<Value, String> {
        uaa.grant(
            format!(
                "grant_type=refresh_token&refresh_token={}",
                tokens["refresh_token"].as_str().unwrap()
            )
            .as_bytes(),
            Some(("cf".into(), String::new())),
            issuer,
        )
    }

    fn user_of(uaa: &Uaa, tokens: &Value) -> String {
        uaa.verify(tokens["access_token"].as_str().unwrap())
            .unwrap()
            .user_name
    }

    #[test]
    fn tokens_verify_and_refresh_once_each_until_they_expire() {
        let dir = fresh_dir("open");
        let uaa = Uaa::open(&dir, Mode::Open).unwrap();
        let issuer = "http://127.0.0.1:9091/uaa/oauth/token";
        let tokens = uaa
            .grant(
                b"grant_type=password&username=alice&password=x",
                None,
                issuer,
            )
            .unwrap();
        let claims = uaa
            .verify(tokens["access_token"].as_str().unwrap())
            .unwrap();
        assert_eq!(
            (claims.user_name.as_str(), claims.origin.as_str()),
            ("alice", "uaa")
        );
        assert_eq!(claims.iss, issuer);
        // the same key again: the token and the session are still good after
        // a restart; the files are their owner's only, one an older cf-api
        // wrote included
        #[cfg(unix)]
        let mode = |name: &str| {
            use std::os::unix::fs::PermissionsExt as _;
            let file = dir.join(name);
            let mode = std::fs::metadata(&file).unwrap().permissions().mode() & 0o777;
            std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o644)).unwrap();
            mode
        };
        #[cfg(unix)]
        assert_eq!((mode("jwt.key"), mode("sessions.json")), (0o600, 0o600));
        let again = Uaa::open(&dir, Mode::Open).unwrap();
        #[cfg(unix)]
        assert_eq!(mode("jwt.key"), 0o600);
        let refreshed = refresh(&again, &tokens, issuer).unwrap();
        assert_eq!(user_of(&again, &refreshed), "alice");
        // rotated: the refresh token is good once, the new one good
        assert!(
            refresh(&again, &tokens, issuer)
                .unwrap_err()
                .contains("used already")
        );
        assert_eq!(
            user_of(&again, &refresh(&again, &refreshed, issuer).unwrap()),
            "alice"
        );
        // a client's own token names it
        let client = again
            .grant(
                b"grant_type=client_credentials",
                Some(("robot".into(), "s".into())),
                issuer,
            )
            .unwrap();
        assert_eq!(user_of(&again, &client), "robot");
        // an access token is no refresh token; another key's token fails
        assert!(
            again
                .grant(
                    format!(
                        "grant_type=refresh_token&refresh_token={}",
                        tokens["access_token"].as_str().unwrap()
                    )
                    .as_bytes(),
                    None,
                    issuer
                )
                .is_err()
        );
        let other = Uaa::open(&fresh_dir("other"), Mode::Open).unwrap();
        assert_eq!(
            other
                .verify(tokens["access_token"].as_str().unwrap())
                .unwrap_err(),
            "bad signature"
        );
        assert!(uaa.verify("not.a.jwt").is_err());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn the_provider_mode_takes_passcodes_and_its_clients_only() {
        let dir = fresh_dir("provider");
        let issuer = "https://api.example.com/uaa/oauth/token";
        let open = Uaa::open(&dir, Mode::Open).unwrap();
        let before = open
            .grant(b"grant_type=password&username=me", None, issuer)
            .unwrap();
        let rules = |group: &str| {
            vec![ClaimRule {
                claim: "groups".into(),
                patterns: vec![group.into()],
            }]
        };
        let mode = |group: &str| Mode::Provider {
            clients: vec![("robot".into(), "r0bot".into())],
            rules: rules(group),
        };
        let uaa = Uaa::open(&dir, mode("devs")).unwrap();
        // the open mode's token is no good now
        assert!(
            uaa.verify(before["access_token"].as_str().unwrap())
                .unwrap_err()
                .contains("log in again")
        );
        // a password: the hint; a passcode: once
        let refused = uaa
            .grant(b"grant_type=password&username=me&password=x", None, issuer)
            .unwrap_err();
        assert!(
            refused.contains("cf login --sso")
                && refused.contains("https://api.example.com/uaa/passcode"),
            "{refused}"
        );
        let alice = Identity {
            user_name: "alice".into(),
            email: "alice@example.com".into(),
            claims: json!({"email": "alice@example.com", "groups": ["devs"]}),
        };
        let code = uaa.passcode(alice).unwrap();
        assert_eq!(code.len(), 8);
        let form = format!("grant_type=password&passcode={code}");
        let tokens = uaa
            .grant(form.as_bytes(), Some(("cf".into(), String::new())), issuer)
            .unwrap();
        let claims = uaa
            .verify(tokens["access_token"].as_str().unwrap())
            .unwrap();
        assert_eq!(
            (
                claims.user_name.as_str(),
                claims.email.as_str(),
                claims.origin.as_str()
            ),
            ("alice", "alice@example.com", "oidc")
        );
        assert!(
            uaa.grant(form.as_bytes(), None, issuer)
                .unwrap_err()
                .contains("used already")
        );
        assert!(
            uaa.grant(b"grant_type=password&passcode=nope", None, issuer)
                .is_err()
        );
        // the clients configured, with their secret
        let robot = uaa
            .grant(
                b"grant_type=client_credentials",
                Some(("robot".into(), "r0bot".into())),
                issuer,
            )
            .unwrap();
        assert_eq!(user_of(&uaa, &robot), "robot");
        assert!(
            uaa.grant(
                b"grant_type=client_credentials&client_id=robot&client_secret=wrong",
                None,
                issuer
            )
            .unwrap_err()
            .contains("unknown client 'robot' or wrong secret")
        );
        assert!(
            uaa.grant(
                b"grant_type=client_credentials",
                Some(("cf".into(), String::new())),
                issuer
            )
            .is_err()
        );
        // a refresh re-reads the rules: alice is out of the group now
        let refreshed = refresh(&uaa, &tokens, issuer).unwrap();
        let later = Uaa::open(&dir, mode("ops")).unwrap();
        let refused = refresh(&later, &refreshed, issuer).unwrap_err();
        assert!(
            refused.starts_with("alice is no longer allowed (the claim groups is [\"devs\"]"),
            "{refused}"
        );
        // and the session went with it
        assert!(
            refresh(&later, &refreshed, issuer)
                .unwrap_err()
                .contains("used already")
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
}
