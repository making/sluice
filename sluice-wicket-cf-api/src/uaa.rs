//! The UAA endpoints the cf CLI logs in with: `GET /uaa/login` (the prompts)
//! and `POST /uaa/oauth/token`. The tokens are JWTs signed (HS256) with a key
//! that is kept, so a token outlives a restart; every other request needs a
//! valid one. Who gets one is the [`Mode`]: any credentials (the password,
//! refresh_token and client_credentials grants) or, with an OpenID Provider,
//! the one-time code `/uaa/passcode` showed (`cf login --sso`) and the clients
//! configured. A refresh token is good once: the sessions, keyed by it, rotate.
//!
//! The logins (the key, the sessions, the passcodes waiting) are one process's,
//! in the data directory (`jwt.key`, `sessions.json`; the passcodes in memory),
//! or with `--state` the store's (`crate::postgres`): every instance's, a refresh
//! token and a passcode good once among them all. A sign-in under way at the
//! provider is neither's: the browser keeps it, sealed ([`Uaa::seal`]).

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use crate::oidc::{ClaimRule, Identity, Signer, constant_time_eq, hmac_sha256};
use crate::postgres::Postgres;
use crate::store::{epoch_secs, guid, write_by_rename};

/// How long an access token is valid.
const ACCESS_SECS: u64 = 12 * 3600;
/// How long a refresh token is valid.
const REFRESH_SECS: u64 = 30 * 86_400;
/// How long a passcode may wait for `cf login --sso`.
pub const PASSCODE_TTL: Duration = Duration::from_secs(5 * 60);
/// Passcodes waiting at most.
const MAX_PASSCODES: usize = 1000;
/// How often the store's keys are read again: one made is known to every instance that
/// much later.
const KEYS_EVERY: Duration = Duration::from_secs(60);
/// The `kid` of the data directory's key.
const DIR_KID: &str = "cf-api";

const ANOTHER_KEY: &str = "signed with another key: log in again";

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

/// Why no token is granted.
#[derive(Debug, PartialEq)]
pub enum Refusal {
    /// Whoever asks gets none: why.
    Denied(String),
    /// The store of the logins does not answer.
    Unavailable(String),
}

impl From<&str> for Refusal {
    fn from(why: &str) -> Refusal {
        Refusal::Denied(why.to_string())
    }
}

/// A key of the tokens, named by their header's `kid`.
struct Key {
    kid: String,
    key: [u8; 32],
    /// Whether tokens are signed with it by now.
    signs: bool,
}

/// Signs and verifies the tokens.
pub struct Uaa {
    /// The keys, oldest first: the last that signs does, each verifies.
    keys: Arc<RwLock<Vec<Key>>>,
    mode: Mode,
    logins: Logins,
    /// Reads the store's keys again, every [`KEYS_EVERY`].
    following: Option<tokio::task::JoinHandle<()>>,
}

impl Drop for Uaa {
    fn drop(&mut self) {
        if let Some(following) = &self.following {
            following.abort();
        }
    }
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
    /// None of a client's, as UAA has it: the CLI then takes it for a client (`cf
    /// set-space-role --client`), its id its `user_id`.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub user_name: String,
    pub email: String,
    pub iat: u64,
    pub exp: u64,
    pub iss: String,
    pub zid: String,
    pub aud: Vec<String>,
}

impl Claims {
    /// Who holds it: the user's name, or the client's id.
    pub fn name(&self) -> &str {
        match self.user_name.is_empty() {
            true => &self.client_id,
            false => &self.user_name,
        }
    }
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

/// The most sessions kept: a login past it evicts the one expiring soonest,
/// so an open mode's unauthenticated grants cannot grow the store forever.
const MAX_SESSIONS: usize = 10_000;

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

    /// Adds a session, evicting the expired ones and then those expiring
    /// soonest to keep at most [`MAX_SESSIONS`].
    fn insert(&mut self, jti: String, session: Session) {
        let now = epoch_secs();
        self.by_jti.retain(|_, session| session.expires > now);
        while self.by_jti.len() >= MAX_SESSIONS {
            let Some(oldest) = self
                .by_jti
                .iter()
                .min_by_key(|(_, session)| session.expires)
                .map(|(jti, _)| jti.clone())
            else {
                break;
            };
            self.by_jti.remove(&oldest);
        }
        self.by_jti.insert(jti, session);
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

/// Where the sessions and the passcodes waiting are kept.
enum Logins {
    /// One process's: the sessions in `sessions.json`, the passcodes in memory.
    Dir {
        sessions: Mutex<Sessions>,
        passcodes: Mutex<HashMap<String, (Identity, Instant)>>,
    },
    /// Every instance's: rows of the store, their expiry by its clock.
    Store(Arc<Postgres>),
}

fn stored<T: serde::de::DeserializeOwned>(json: String) -> Result<T, String> {
    serde_json::from_str(&json).map_err(|e| format!("--state: a login it keeps: {e}"))
}

fn identity_json(identity: &Identity) -> String {
    json!({
        "user_name": identity.user_name,
        "email": identity.email,
        "claims": identity.claims,
    })
    .to_string()
}

fn identity_of(json: String) -> Result<Identity, String> {
    let mut kept: Value = stored(json)?;
    let text = |name: &str| kept[name].as_str().unwrap_or_default().to_string();
    Ok(Identity {
        user_name: text("user_name"),
        email: text("email"),
        claims: kept["claims"].take(),
    })
}

impl Logins {
    /// Keeps a new session, for [`REFRESH_SECS`].
    async fn put(&self, jti: String, subject: Subject) -> Result<(), String> {
        match self {
            Logins::Dir { sessions, .. } => {
                let mut sessions = sessions.lock().expect("no panic held it");
                let expires = epoch_secs() + REFRESH_SECS;
                sessions.insert(jti, Session { subject, expires });
                sessions.save();
                Ok(())
            }
            Logins::Store(store) => {
                let subject = serde_json::to_string(&subject).expect("plain data");
                let most = MAX_SESSIONS as i64 - 1;
                (store.put_session(&jti, &subject, REFRESH_SECS as i64, most)).await
            }
        }
    }

    /// Makes the session `old` the session `new`: whose it is, once.
    async fn rotate(&self, old: &str, new: String) -> Result<Option<Subject>, String> {
        match self {
            Logins::Dir { sessions, .. } => {
                let mut sessions = sessions.lock().expect("no panic held it");
                let Some(session) = sessions.by_jti.remove(old) else {
                    return Ok(None);
                };
                let expires = epoch_secs() + REFRESH_SECS;
                let subject = session.subject.clone();
                sessions.insert(new, Session { subject, expires });
                sessions.save();
                Ok(Some(session.subject))
            }
            Logins::Store(store) => (store.rotate_session(old, &new, REFRESH_SECS as i64).await?)
                .map(stored)
                .transpose(),
        }
    }

    /// Whose the session `jti` is.
    async fn subject(&self, jti: &str) -> Result<Option<Subject>, String> {
        match self {
            Logins::Dir { sessions, .. } => {
                let sessions = sessions.lock().expect("no panic held it");
                Ok((sessions.by_jti.get(jti)).map(|session| session.subject.clone()))
            }
            Logins::Store(store) => store.session(jti).await?.map(stored).transpose(),
        }
    }

    /// Ends the session `jti`.
    async fn end(&self, jti: &str) -> Result<(), String> {
        match self {
            Logins::Dir { sessions, .. } => {
                let mut sessions = sessions.lock().expect("no panic held it");
                sessions.by_jti.remove(jti);
                sessions.save();
                Ok(())
            }
            Logins::Store(store) => store.drop_session(jti).await,
        }
    }

    /// Keeps the passcode `code` of `identity` for [`PASSCODE_TTL`]; false: it is taken, or
    /// [`MAX_PASSCODES`] wait already.
    async fn offer(&self, code: &str, identity: &Identity) -> Result<bool, String> {
        match self {
            Logins::Dir { passcodes, .. } => {
                let mut passcodes = passcodes.lock().expect("no panic held it");
                passcodes.retain(|_, (_, since)| since.elapsed() < PASSCODE_TTL);
                if passcodes.len() >= MAX_PASSCODES || passcodes.contains_key(code) {
                    return Ok(false);
                }
                passcodes.insert(code.to_string(), (identity.clone(), Instant::now()));
                Ok(true)
            }
            Logins::Store(store) => {
                let (secs, most) = (PASSCODE_TTL.as_secs() as i64, MAX_PASSCODES as i64);
                (store.put_passcode(code, &identity_json(identity), secs, most)).await
            }
        }
    }

    /// Takes the passcode `code`: whose it is, once.
    async fn take(&self, code: &str) -> Result<Option<Identity>, String> {
        match self {
            Logins::Dir { passcodes, .. } => Ok(passcodes
                .lock()
                .expect("no panic held it")
                .remove(code)
                .filter(|(_, since)| since.elapsed() < PASSCODE_TTL)
                .map(|(identity, _)| identity)),
            Logins::Store(store) => (store.take_passcode(code).await?)
                .map(identity_of)
                .transpose(),
        }
    }
}

/// The key of `file`, 64 hex digits; None without the file.
fn key_of(file: &Path) -> Result<Option<[u8; 32]>, String> {
    let hex = match std::fs::read_to_string(file) {
        Ok(hex) => hex,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(format!("{}: {e}", file.display())),
    };
    let hex = hex.trim();
    (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(hex.get(i..i + 2).unwrap_or("zz"), 16))
        .collect::<Result<Vec<u8>, _>>()
        .ok()
        .and_then(|bytes| bytes.try_into().ok())
        .map(Some)
        .ok_or_else(|| format!("{}: not a 64 hex digit key", file.display()))
}

fn random_key() -> Result<[u8; 32], String> {
    let mut key = [0u8; 32];
    getrandom::fill(&mut key).map_err(|e| format!("no random key: {e}"))?;
    Ok(key)
}

/// The keys of `store`, as they are by now.
async fn keys_of(store: &Postgres) -> Result<Vec<Key>, String> {
    (store.keys(REFRESH_SECS as i64).await?.into_iter())
        .map(|kept| {
            Ok(Key {
                key: (kept.key.try_into()).map_err(|_| "--state: a key is not 32 bytes")?,
                kid: kept.kid,
                signs: kept.signs,
            })
        })
        .collect()
}

/// Reads the keys of `store` again into `known`.
async fn follow_keys(known: &RwLock<Vec<Key>>, store: &Postgres) -> Result<(), String> {
    let keys = keys_of(store).await?;
    if keys.iter().any(|key| key.signs) {
        *known.write().expect("no panic held it") = keys;
    }
    Ok(())
}

impl Uaa {
    /// The logins of `dir`: the key of its `jwt.key`, made when missing, the file its
    /// owner's only (0600), and the sessions of `sessions.json` next to it. Or, with
    /// `store`, the store's: one with no key takes the directory's, with its sessions, else
    /// makes one; the files stay as they were and are written no more.
    pub async fn open(dir: &Path, mode: Mode, store: Option<Arc<Postgres>>) -> Result<Uaa, String> {
        let file = dir.join("jwt.key");
        let Some(store) = store else {
            let key = match key_of(&file)? {
                Some(key) => {
                    // one an older cf-api wrote as any file
                    #[cfg(unix)]
                    {
                        use std::os::unix::fs::PermissionsExt as _;
                        let private = std::fs::Permissions::from_mode(0o600);
                        std::fs::set_permissions(&file, private)
                            .map_err(|e| format!("{}: {e}", file.display()))?;
                    }
                    key
                }
                None => {
                    let key = random_key()?;
                    let hex: String = key.iter().map(|byte| format!("{byte:02x}")).collect();
                    write_by_rename(&file, hex.as_bytes())?;
                    key
                }
            };
            let key = Key {
                kid: DIR_KID.to_string(),
                key,
                signs: true,
            };
            return Ok(Uaa {
                keys: Arc::new(RwLock::new(vec![key])),
                mode,
                logins: Logins::Dir {
                    sessions: Mutex::new(Sessions::open(dir)?),
                    passcodes: Mutex::new(HashMap::new()),
                },
                following: None,
            });
        };
        let mut keys = keys_of(&store).await?;
        if keys.is_empty() {
            let (kid, key, sessions) = match key_of(&file)? {
                Some(key) => (DIR_KID.to_string(), key, Sessions::open(dir)?.by_jti),
                None => (guid(), random_key()?, HashMap::new()),
            };
            let sessions: Vec<(String, String, i64)> = (sessions.into_iter())
                .map(|(jti, session)| {
                    let subject = serde_json::to_string(&session.subject).expect("plain data");
                    (jti, subject, session.expires as i64)
                })
                .collect();
            store.first_key(&kid, &key, &sessions).await?;
            keys = keys_of(&store).await?;
            if keys.iter().any(|kept| kept.kid == kid) && kid == DIR_KID {
                println!("UAA: {} taken into the store", file.display());
            }
        }
        if !keys.iter().any(|key| key.signs) {
            return Err("--state: no key of the tokens signs yet".into());
        }
        let keys = Arc::new(RwLock::new(keys));
        let (known, from) = (Arc::downgrade(&keys), store.clone());
        let following = tokio::spawn(async move {
            loop {
                tokio::time::sleep(KEYS_EVERY).await;
                let Some(known) = known.upgrade() else {
                    return;
                };
                if let Err(e) = follow_keys(&known, &from).await {
                    eprintln!("UAA: the keys were not read again: {e}");
                }
            }
        });
        Ok(Uaa {
            keys,
            mode,
            logins: Logins::Store(store),
            following: Some(following),
        })
    }

    /// Whether users log in through the provider.
    pub fn provider(&self) -> bool {
        matches!(self.mode, Mode::Provider { .. })
    }

    /// Whether `id` is a client that gets tokens: in the open mode, any.
    pub fn knows_client(&self, id: &str) -> bool {
        match &self.mode {
            Mode::Open => true,
            Mode::Provider { clients, .. } => clients.iter().any(|(known, _)| known == id),
        }
    }

    /// The origin of the users who log in: `uaa` in the open mode, `oidc` with the provider.
    pub fn origin(&self) -> &'static str {
        match self.mode {
            Mode::Open => "uaa",
            Mode::Provider { .. } => "oidc",
        }
    }

    /// The token response of `grant` for `subject`, issued by `issuer`: the tokens of the
    /// session `jti`, the refresh token's `jti`.
    fn tokens(&self, jti: &str, subject: &Subject, grant: &str, issuer: &str) -> Value {
        let issued = epoch_secs();
        let client = subject.client.clone();
        // a client of --client-credentials is no user: its id names it, as a role's user
        let (user, user_name) = match subject.origin.as_str() {
            "client" => (client.clone(), String::new()),
            _ => (user_id(&subject.user_name), subject.user_name.clone()),
        };
        let refresh = Claims {
            jti: jti.to_string(),
            sub: user.clone(),
            scope: SCOPES.iter().map(|scope| scope.to_string()).collect(),
            client_id: client.clone(),
            cid: client.clone(),
            azp: client.clone(),
            grant_type: "refresh_token".into(),
            user_id: user,
            origin: subject.origin.clone(),
            user_name,
            email: subject.email.clone(),
            iat: issued,
            exp: issued + REFRESH_SECS,
            iss: issuer.to_string(),
            zid: "uaa".into(),
            aud: vec!["cloud_controller".into(), "openid".into(), client],
        };
        let claims = Claims {
            jti: jti.strip_suffix("-r").unwrap_or(jti).to_string(),
            exp: issued + ACCESS_SECS,
            grant_type: grant.to_string(),
            ..refresh.clone()
        };
        json!({
            "access_token": self.sign(&claims),
            "token_type": "bearer",
            "refresh_token": self.sign(&refresh),
            "expires_in": ACCESS_SECS - 1,
            "scope": SCOPES.join(" "),
            "jti": claims.jti,
        })
    }

    /// A new login of `subject`: its session kept, the tokens.
    async fn issue(&self, subject: Subject, grant: &str, issuer: &str) -> Result<Value, Refusal> {
        let jti = format!("{}-r", guid());
        let tokens = self.tokens(&jti, &subject, grant, issuer);
        (self.logins.put(jti, subject).await).map_err(Refusal::Unavailable)?;
        Ok(tokens)
    }

    /// With the last key that signs.
    fn with_signing_key<R>(&self, f: impl FnOnce(&Key) -> R) -> R {
        let keys = self.keys.read().expect("no panic held it");
        f((keys.iter().rev().find(|key| key.signs)).expect("a key signs: an open's check"))
    }

    fn sign(&self, claims: &Claims) -> String {
        self.with_signing_key(|key| {
            let head = json!({"alg": "HS256", "typ": "JWT", "kid": key.kid});
            let head = URL_SAFE_NO_PAD.encode(head.to_string());
            let body = URL_SAFE_NO_PAD.encode(serde_json::to_vec(claims).expect("plain data"));
            let signed = format!("{head}.{body}");
            let signature = URL_SAFE_NO_PAD.encode(hmac_sha256(&key.key, signed.as_bytes()));
            format!("{signed}.{signature}")
        })
    }

    /// The claims of `token` when it is signed with the key its header names, not expired
    /// and of this mode: with the provider, a token of the open mode is no good.
    pub fn verify(&self, token: &str) -> Result<Claims, String> {
        let (signed, signature) = token.rsplit_once('.').ok_or("not a JWT")?;
        let (head, body) = signed.split_once('.').ok_or("not a JWT")?;
        let given = URL_SAFE_NO_PAD
            .decode(signature)
            .map_err(|_| "bad signature encoding")?;
        let header: Value = serde_json::from_slice(
            &URL_SAFE_NO_PAD
                .decode(head)
                .map_err(|_| "bad header encoding")?,
        )
        .map_err(|_| "bad header")?;
        if header["alg"] != "HS256" {
            return Err("not HS256".into());
        }
        let expected = {
            let keys = self.keys.read().expect("no panic held it");
            let key = (keys.iter()).find(|key| header["kid"] == key.kid.as_str());
            hmac_sha256(&key.ok_or(ANOTHER_KEY)?.key, signed.as_bytes())
        };
        if !constant_time_eq(&given, &expected) {
            return Err(ANOTHER_KEY.into());
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

    /// Seals what a sign-in under way at the provider leaves with the browser, with a key of
    /// the tokens' key's: opened by any instance, forged by no one.
    pub fn seal(&self, payload: &[u8]) -> String {
        self.with_signing_key(|key| sealer(key).sign(payload))
    }

    /// The payload of `sealed` when [`Uaa::seal`] sealed it.
    pub fn unseal(&self, sealed: &str) -> Option<Vec<u8>> {
        let keys = self.keys.read().expect("no panic held it");
        keys.iter().find_map(|key| sealer(key).verify(sealed))
    }

    /// What the rules of who may do more than the login (`--security-group-editor`) read of
    /// the holder of `token`: the claims of its login's ID token, kept by its session (the
    /// token has them not), None once a refresh replaced the token; in the open mode, the
    /// name it logged in with.
    pub async fn identity(&self, token: &Claims) -> Result<Option<Value>, String> {
        if token.origin == "uaa" {
            return Ok(Some(
                json!({"user_name": token.user_name, "email": token.email}),
            ));
        }
        let session = self.logins.subject(&format!("{}-r", token.jti)).await?;
        Ok(session.map(|subject| subject.claims))
    }

    /// A one-time code for `identity`, good for [`PASSCODE_TTL`]: what
    /// `cf login --sso` asks for.
    pub async fn passcode(&self, identity: Identity) -> Result<String, String> {
        // a code taken is drawn again; all refused: too many wait
        for _ in 0..3 {
            let code = random_code();
            if self.logins.offer(&code, &identity).await? {
                return Ok(code);
            }
        }
        Err("too many codes waiting: try again later".into())
    }

    /// Answers a token request: the form of `body`, with `basic` the client
    /// of the Authorization header when there is one.
    pub async fn grant(
        &self,
        body: &[u8],
        basic: Option<(String, String)>,
        issuer: &str,
    ) -> Result<Value, Refusal> {
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
        let denied = |why: String| Err(Refusal::Denied(why));
        match form.get("grant_type").map(String::as_str) {
            Some("password") => match &self.mode {
                Mode::Open => {
                    let name = form
                        .get("username")
                        .filter(|user| !user.is_empty())
                        .ok_or("username is required")?;
                    let subject = user(name, name, "uaa", Value::Null);
                    self.issue(subject, "password", issuer).await
                }
                Mode::Provider { .. } => {
                    let Some(code) = form.get("passcode").filter(|code| !code.is_empty()) else {
                        let passcode = issuer
                            .strip_suffix("/oauth/token")
                            .unwrap_or(issuer)
                            .to_string()
                            + "/passcode";
                        return denied(format!(
                            "passwords are not taken here: cf login --sso, with a one-time code from {passcode}"
                        ));
                    };
                    let taken = self.logins.take(code).await;
                    let identity = taken
                        .map_err(Refusal::Unavailable)?
                        .ok_or("the code is unknown, expired or was used already: get one more")?;
                    let subject = user(
                        &identity.user_name,
                        &identity.email,
                        "oidc",
                        identity.claims,
                    );
                    self.issue(subject, "password", issuer).await
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
                            return denied(format!("unknown client '{client}' or wrong secret"));
                        }
                        "client"
                    }
                };
                let subject = user(&client, &client, origin, Value::Null);
                self.issue(subject, "client_credentials", issuer).await
            }
            Some("refresh_token") => {
                let token = form
                    .get("refresh_token")
                    .ok_or("refresh_token is required")?;
                let claims = self
                    .verify(token)
                    .map_err(|e| Refusal::Denied(format!("invalid refresh token: {e}")))?;
                if claims.grant_type != "refresh_token" {
                    return denied("not a refresh token".into());
                }
                // before anything else: good once, for one of those asking at once
                let jti = format!("{}-r", guid());
                let rotated = self.logins.rotate(&claims.jti, jti.clone()).await;
                let subject = rotated
                    .map_err(Refusal::Unavailable)?
                    .ok_or("the refresh token is unknown or was used already: log in again")?;
                if let Mode::Provider { rules, .. } = &self.mode
                    && subject.origin == "oidc"
                    && let Err(why) = crate::oidc::allowed(rules, &subject.claims)
                {
                    (self.logins.end(&jti).await).map_err(Refusal::Unavailable)?;
                    return denied(format!(
                        "{} is no longer allowed ({why}): log in again",
                        subject.user_name
                    ));
                }
                Ok(self.tokens(&jti, &subject, "refresh_token", issuer))
            }
            Some(other) => denied(format!("unsupported grant type '{other}'")),
            None => denied("grant_type is required".into()),
        }
    }
}

/// What seals with `key`: a key of its own, so nothing sealed is a token's signature.
fn sealer(key: &Key) -> Signer {
    Signer::with_key(hmac_sha256(&key.key, b"sluice-wicket-cf-api: a sign-in"))
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

/// A stable id of a user name (a UUID v5 of it): its tokens' `user_id`.
pub fn user_id(user: &str) -> String {
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::oidc::mock::MockProvider;
    use crate::oidc::{OidcSettings, Provider, SignIn};
    use crate::postgres::PostgresSettings;
    use crate::store::{Store, postgres_server};

    fn fresh_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("sluice-uaa-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// The store of the database `url`, as an instance on `dir` has it.
    async fn store_of(dir: &Path, url: &str) -> Arc<Postgres> {
        let settings = PostgresSettings {
            url: format!("{url}?sslmode=disable"),
            ca_pem: None,
        };
        let (domains, wickets) = (["localhost".to_string()], ["a".to_string()]);
        let runs = BTreeMap::new();
        let store = Store::open(
            dir,
            "o",
            "s",
            &domains,
            &wickets,
            &runs,
            Some(&settings),
            None,
        );
        store.await.unwrap().shared().unwrap()
    }

    async fn refresh(uaa: &Uaa, tokens: &Value, issuer: &str) -> Result<Value, Refusal> {
        uaa.grant(
            format!(
                "grant_type=refresh_token&refresh_token={}",
                tokens["refresh_token"].as_str().unwrap()
            )
            .as_bytes(),
            Some(("cf".into(), String::new())),
            issuer,
        )
        .await
    }

    fn user_of(uaa: &Uaa, tokens: &Value) -> String {
        uaa.verify(tokens["access_token"].as_str().unwrap())
            .unwrap()
            .name()
            .to_string()
    }

    /// Why whoever asked got no token.
    fn denied(granted: Result<Value, Refusal>) -> String {
        match granted {
            Err(Refusal::Denied(why)) => why,
            other => panic!("not denied: {other:?}"),
        }
    }

    /// What one of two asking at once got, and why the other got none.
    fn one_of(granted: (Result<Value, Refusal>, Result<Value, Refusal>)) -> (Value, String) {
        match granted {
            (Ok(tokens), refused) | (refused, Ok(tokens)) => (tokens, denied(refused)),
            neither => panic!("neither got it: {neither:?}"),
        }
    }

    /// The key the access token of `tokens` names.
    fn kid(tokens: &Value) -> String {
        let head = tokens["access_token"].as_str().unwrap();
        let head = URL_SAFE_NO_PAD.decode(head.split_once('.').unwrap().0);
        let head: Value = serde_json::from_slice(&head.unwrap()).unwrap();
        head["kid"].as_str().unwrap().to_string()
    }

    /// The logins of a directory, after a restart of its process; and of a store, which
    /// takes the directory's, on two instances: what one issued is good on the other, a
    /// refresh token for one of the two asking at once, a key made in the store taken up by
    /// both.
    #[tokio::test]
    async fn tokens_verify_and_refresh_once_each_until_they_expire() {
        for shared in [false, true] {
            let dir = fresh_dir("open");
            let uaa = Uaa::open(&dir, Mode::Open, None).await.unwrap();
            let issuer = "http://127.0.0.1:9091/uaa/oauth/token";
            let login = async |uaa: &Uaa| {
                let form = b"grant_type=password&username=alice&password=x";
                uaa.grant(form, None, issuer).await.unwrap()
            };
            let tokens = login(&uaa).await;
            let claims = uaa
                .verify(tokens["access_token"].as_str().unwrap())
                .unwrap();
            assert_eq!(
                (claims.user_name.as_str(), claims.origin.as_str()),
                ("alice", "uaa")
            );
            assert_eq!(
                uaa.identity(&claims).await.unwrap(),
                Some(json!({"user_name": "alice", "email": "alice"}))
            );
            assert_eq!(claims.iss, issuer);
            // the files are their owner's only, one an older cf-api wrote included
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
            // the same key again: the token and the session are still good after a restart,
            // and in the store that took them, on the instance beside too
            let database = match shared {
                true => Some(postgres_server::database().await),
                false => None,
            };
            let stores = match &database {
                Some((url, _)) => {
                    let beside = fresh_dir("open-beside");
                    Some((store_of(&dir, url).await, store_of(&beside, url).await))
                }
                None => None,
            };
            drop(uaa);
            let first = stores.as_ref().map(|(first, _)| first.clone());
            let again = Uaa::open(&dir, Mode::Open, first).await.unwrap();
            let beside = match &stores {
                Some((_, second)) => {
                    let beside = fresh_dir("open-beside");
                    Some(Uaa::open(&beside, Mode::Open, Some(second.clone())).await)
                }
                None => {
                    #[cfg(unix)]
                    assert_eq!(mode("jwt.key"), 0o600);
                    None
                }
            };
            let other = beside
                .as_ref()
                .map_or(&again, |beside| beside.as_ref().unwrap());
            assert_eq!(user_of(other, &tokens), "alice");
            // rotated: the refresh token is good once, the new one good
            let (refreshed, refused) = one_of(tokio::join!(
                refresh(&again, &tokens, issuer),
                refresh(other, &tokens, issuer)
            ));
            assert!(refused.contains("used already"), "{refused}");
            assert_eq!(user_of(&again, &refreshed), "alice");
            assert!(denied(refresh(&again, &tokens, issuer).await).contains("used already"));
            assert_eq!(
                user_of(&again, &refresh(other, &refreshed, issuer).await.unwrap()),
                "alice"
            );
            // a client's own token names it
            let client = again
                .grant(
                    b"grant_type=client_credentials",
                    Some(("robot".into(), "s".into())),
                    issuer,
                )
                .await
                .unwrap();
            assert_eq!(user_of(other, &client), "robot");
            // an access token is no refresh token; another key's token fails
            let access = format!(
                "grant_type=refresh_token&refresh_token={}",
                tokens["access_token"].as_str().unwrap()
            );
            assert!(again.grant(access.as_bytes(), None, issuer).await.is_err());
            let stranger = Uaa::open(&fresh_dir("other"), Mode::Open, None).await;
            let access = tokens["access_token"].as_str().unwrap();
            assert_eq!(stranger.unwrap().verify(access).unwrap_err(), ANOTHER_KEY);
            assert!(again.verify("not.a.jwt").is_err());
            let _ = std::fs::remove_dir_all(&dir);
            let (Some((_, behind)), Some((first, second))) = (&database, &stores) else {
                continue;
            };
            // a key made ahead of its time is read by every instance before one signs with it
            let made = async |made: &str| {
                let key = "decode(repeat('ab', 32), 'hex')";
                let put = format!(
                    "INSERT INTO sluice_cf_keys (seq, kid, key, made) \
                     VALUES (2, 'next', {key}, {made}) \
                     ON CONFLICT (seq) DO UPDATE SET made = EXCLUDED.made"
                );
                behind.batch_execute(&put).await.unwrap();
            };
            made("now() + interval '1 hour'").await;
            follow_keys(&again.keys, first).await.unwrap();
            follow_keys(&other.keys, second).await.unwrap();
            assert_eq!(kid(&login(&again).await), DIR_KID);
            made("now()").await;
            follow_keys(&again.keys, first).await.unwrap();
            let next = login(&again).await;
            assert_eq!(kid(&next), "next");
            assert_eq!(user_of(other, &next), "alice");
            // the one before it verifies until its last token expired
            assert_eq!(user_of(other, &client), "robot");
            made("now() - interval '31 days'").await;
            follow_keys(&other.keys, second).await.unwrap();
            let access = client["access_token"].as_str().unwrap();
            assert_eq!(other.verify(access).unwrap_err(), ANOTHER_KEY);
            assert_eq!(user_of(other, &next), "alice");
            // a new session evicts the expired ones, then those expiring soonest
            first.put_session("gone", "{}", -1, 9).await.unwrap();
            first.put_session("soon", "{}", 10, 9).await.unwrap();
            first
                .put_session("last", "{}", 99_999_999, 1)
                .await
                .unwrap();
            let left = "SELECT jti FROM sluice_cf_sessions ORDER BY expires";
            let left = behind.query(left, &[]).await.unwrap();
            let left: Vec<String> = left.iter().map(|row| row.get(0)).collect();
            assert_eq!(left.len(), 2, "{left:?}");
            assert!(left[0].ends_with("-r") && left[1] == "last", "{left:?}");
        }
    }

    /// With a provider: a sign-in begun on one instance and finished on the other, its
    /// passcode spent on either, once.
    #[tokio::test]
    async fn the_provider_mode_takes_passcodes_and_its_clients_only() {
        // reqwest brings no crypto provider of its own
        let _ = rustls::crypto::ring::default_provider().install_default();
        let idp = MockProvider::start("cf", "s3cret");
        for shared in [false, true] {
            let dir = fresh_dir("provider");
            let issuer = "https://api.example.com/uaa/oauth/token";
            let stores = match shared {
                true => {
                    let (url, _) = postgres_server::database().await;
                    let beside = fresh_dir("provider-beside");
                    Some((store_of(&dir, &url).await, store_of(&beside, &url).await))
                }
                false => None,
            };
            let first = || stores.as_ref().map(|(first, _)| first.clone());
            let open = Uaa::open(&dir, Mode::Open, first()).await.unwrap();
            let before = open
                .grant(b"grant_type=password&username=me", None, issuer)
                .await
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
            let uaa = Uaa::open(&dir, mode("devs"), first()).await.unwrap();
            let beside = match &stores {
                Some((_, second)) => {
                    Some(Uaa::open(&dir, mode("devs"), Some(second.clone())).await)
                }
                None => None,
            };
            let other = beside
                .as_ref()
                .map_or(&uaa, |beside| beside.as_ref().unwrap());
            // the open mode's token is no good now
            assert!(
                uaa.verify(before["access_token"].as_str().unwrap())
                    .unwrap_err()
                    .contains("log in again")
            );
            // a password: the hint
            let refused = denied(
                uaa.grant(b"grant_type=password&username=me&password=x", None, issuer)
                    .await,
            );
            assert!(
                refused.contains("cf login --sso")
                    && refused.contains("https://api.example.com/uaa/passcode"),
                "{refused}"
            );
            // alice signs in at the provider: what the browser keeps of it meanwhile is
            // opened by the other instance, and by no one else
            let provider = || {
                Provider::new(OidcSettings {
                    issuer: idp.issuer.clone(),
                    client_id: "cf".into(),
                    client_secret: Some("s3cret".into()),
                    scopes: vec!["openid".into()],
                    claims: rules("devs"),
                    ca_pem: None,
                })
                .unwrap()
            };
            idp.sign_in(json!({
                "sub": "1", "preferred_username": "alice", "email": "alice@example.com",
                "groups": ["devs"],
            }));
            let callback = "https://api.example.com/uaa/callback";
            let (location, sign_in) = provider().begin_kept(callback).await.unwrap();
            let sealed = uaa.seal(&serde_json::to_vec(&sign_in).unwrap());
            let stranger = Uaa::open(&fresh_dir("stranger"), mode("devs"), None).await;
            assert!(stranger.unwrap().unseal(&sealed).is_none());
            let http = reqwest::Client::builder().redirect(reqwest::redirect::Policy::none());
            let back = http.build().unwrap().get(&location).send().await.unwrap();
            let back = back.headers()["location"].to_str().unwrap().to_string();
            let back: HashMap<String, String> =
                form_urlencoded::parse(back.split_once('?').unwrap().1.as_bytes())
                    .into_owned()
                    .collect();
            let kept: SignIn = serde_json::from_slice(&other.unseal(&sealed).unwrap()).unwrap();
            let finishes = provider();
            let finish = async |state: &str| {
                (finishes.finish_kept(&back["code"], state, &kept, callback)).await
            };
            assert!(finish("another").await.unwrap_err().contains("is unknown"));
            let alice = finish(&back["state"]).await.unwrap();
            // a passcode: once, on either
            let code = other.passcode(alice).await.unwrap();
            assert_eq!(code.len(), 8);
            let form = format!("grant_type=password&passcode={code}");
            let cf = || Some(("cf".to_string(), String::new()));
            let (tokens, refused) = one_of(tokio::join!(
                uaa.grant(form.as_bytes(), cf(), issuer),
                other.grant(form.as_bytes(), cf(), issuer)
            ));
            assert!(refused.contains("used already"), "{refused}");
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
            // the claims of the login, from its session: a refresh replaces the token
            let groups = async |uaa: &Uaa, claims: &Claims| {
                let identity = uaa.identity(claims).await.unwrap();
                identity.map(|claims| claims["groups"].clone())
            };
            assert_eq!(groups(other, &claims).await, Some(json!(["devs"])));
            assert!(
                denied(uaa.grant(form.as_bytes(), None, issuer).await).contains("used already")
            );
            assert!(
                uaa.grant(b"grant_type=password&passcode=nope", None, issuer)
                    .await
                    .is_err()
            );
            if let Some((first, _)) = &stores {
                // no more wait than may
                assert!(!first.put_passcode("zzzzzzzz", "{}", 60, 0).await.unwrap());
            }
            // the clients configured, with their secret
            let robot = uaa
                .grant(
                    b"grant_type=client_credentials",
                    Some(("robot".into(), "r0bot".into())),
                    issuer,
                )
                .await
                .unwrap();
            assert_eq!(user_of(other, &robot), "robot");
            assert!(
                denied(
                    uaa.grant(
                        b"grant_type=client_credentials&client_id=robot&client_secret=wrong",
                        None,
                        issuer
                    )
                    .await
                )
                .contains("unknown client 'robot' or wrong secret")
            );
            assert!(
                uaa.grant(b"grant_type=client_credentials", cf(), issuer)
                    .await
                    .is_err()
            );
            // a refresh re-reads the rules: alice is out of the group now
            let refreshed = refresh(other, &tokens, issuer).await.unwrap();
            assert_eq!(groups(&uaa, &claims).await, None);
            let current = uaa
                .verify(refreshed["access_token"].as_str().unwrap())
                .unwrap();
            assert_eq!(groups(&uaa, &current).await, Some(json!(["devs"])));
            let later = Uaa::open(&dir, mode("ops"), first()).await.unwrap();
            let refused = denied(refresh(&later, &refreshed, issuer).await);
            assert!(
                refused.starts_with("alice is no longer allowed (the claim groups is [\"devs\"]"),
                "{refused}"
            );
            // and the session went with it, on every instance
            let asked = beside
                .as_ref()
                .map_or(&later, |beside| beside.as_ref().unwrap());
            assert!(denied(refresh(asked, &refreshed, issuer).await).contains("used already"));
            let _ = std::fs::remove_dir_all(&dir);
        }
    }
}

#[cfg(test)]
mod session_tests {
    use super::*;

    fn subject() -> Subject {
        Subject {
            user_name: "me".into(),
            email: "me@example.com".into(),
            client: "cf".into(),
            origin: "open".into(),
            claims: Value::Null,
        }
    }

    fn session(expires: u64) -> Session {
        Session {
            subject: subject(),
            expires,
        }
    }

    #[test]
    fn the_session_store_keeps_at_most_the_cap_evicting_expired_then_soonest() {
        let now = epoch_secs();
        let mut sessions = Sessions {
            file: PathBuf::from("sessions.json"),
            by_jti: HashMap::new(),
        };
        // an expired one goes with the next insert of a live one
        sessions
            .by_jti
            .insert("expired".into(), session(now.saturating_sub(1)));
        sessions.insert("first".into(), session(now + 1000));
        assert!(!sessions.by_jti.contains_key("expired"));
        assert!(sessions.by_jti.contains_key("first"));
        for i in 0..MAX_SESSIONS {
            sessions.insert(format!("s{i}"), session(now + 1000 + i as u64));
        }
        assert_eq!(sessions.by_jti.len(), MAX_SESSIONS);
        sessions.insert("newest".into(), session(now + 100_000));
        assert_eq!(sessions.by_jti.len(), MAX_SESSIONS);
        assert!(!sessions.by_jti.contains_key("s0"), "the soonest expiring");
        assert!(sessions.by_jti.contains_key("newest"));
        assert!(
            sessions
                .by_jti
                .contains_key(&format!("s{}", MAX_SESSIONS - 1))
        );
    }
}
