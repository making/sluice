//! The UAA endpoints the cf CLI logs in with: `GET /uaa/login` (the prompts)
//! and `POST /uaa/oauth/token`. Any credentials are taken (the password,
//! refresh_token and client_credentials grants) and answered with a JWT
//! signed (HS256) with a key kept in the data directory, so a token outlives
//! a restart; every other request needs a valid one.

use std::collections::HashMap;
use std::path::Path;

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use crate::store::{epoch_secs, guid};

/// How long an access token is valid.
const ACCESS_SECS: u64 = 12 * 3600;
/// How long a refresh token is valid.
const REFRESH_SECS: u64 = 30 * 86_400;

const SCOPES: [&str; 4] = [
    "cloud_controller.read",
    "cloud_controller.write",
    "cloud_controller.admin",
    "openid",
];

/// Signs and verifies the tokens.
pub struct Uaa {
    key: [u8; 32],
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
    pub origin: String,
    pub user_name: String,
    pub email: String,
    pub iat: u64,
    pub exp: u64,
    pub iss: String,
    pub zid: String,
    pub aud: Vec<String>,
}

impl Uaa {
    /// The key of `jwt.key` in `dir`, made when missing.
    pub fn open(dir: &Path) -> Result<Uaa, String> {
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
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                getrandom::fill(&mut key).map_err(|e| format!("no random key: {e}"))?;
                let hex: String = key.iter().map(|byte| format!("{byte:02x}")).collect();
                let tmp = file.with_extension("tmp");
                std::fs::write(&tmp, hex)
                    .and_then(|()| std::fs::rename(&tmp, &file))
                    .map_err(failed)?;
            }
            Err(e) => return Err(failed(e)),
        }
        Ok(Uaa { key })
    }

    /// The token response of `grant` (`password`, `client_credentials` or
    /// `refresh_token`) for `user` through `client`, issued by `issuer`.
    pub fn tokens(&self, user: &str, client: &str, grant: &str, issuer: &str) -> Value {
        let issued = epoch_secs();
        let claims = Claims {
            jti: guid(),
            sub: user_id(user),
            scope: SCOPES.iter().map(|scope| scope.to_string()).collect(),
            client_id: client.to_string(),
            cid: client.to_string(),
            azp: client.to_string(),
            grant_type: grant.to_string(),
            user_id: user_id(user),
            origin: "uaa".into(),
            user_name: user.to_string(),
            email: user.to_string(),
            iat: issued,
            exp: issued + ACCESS_SECS,
            iss: issuer.to_string(),
            zid: "uaa".into(),
            aud: vec!["cloud_controller".into(), "openid".into(), client.into()],
        };
        let refresh = Claims {
            jti: format!("{}-r", claims.jti),
            exp: issued + REFRESH_SECS,
            grant_type: "refresh_token".into(),
            ..claims.clone()
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

    fn sign(&self, claims: &Claims) -> String {
        let head = URL_SAFE_NO_PAD.encode(br#"{"alg":"HS256","typ":"JWT","kid":"cf-api"}"#);
        let body = URL_SAFE_NO_PAD.encode(serde_json::to_vec(claims).expect("plain data"));
        let signed = format!("{head}.{body}");
        let signature = URL_SAFE_NO_PAD.encode(hmac_sha256(&self.key, signed.as_bytes()));
        format!("{signed}.{signature}")
    }

    /// The claims of `token` when it is signed with the key and not expired.
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
        Ok(claims)
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
        let client = form
            .get("client_id")
            .cloned()
            .or_else(|| basic.map(|(client, _)| client))
            .filter(|client| !client.is_empty())
            .unwrap_or_else(|| "cf".into());
        match form.get("grant_type").map(String::as_str) {
            Some("password") => {
                let user = form
                    .get("username")
                    .filter(|user| !user.is_empty())
                    .ok_or("username is required")?;
                Ok(self.tokens(user, &client, "password", issuer))
            }
            Some("client_credentials") => {
                Ok(self.tokens(&client, &client, "client_credentials", issuer))
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
                Ok(self.tokens(
                    &claims.user_name,
                    &claims.client_id,
                    "refresh_token",
                    issuer,
                ))
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

    #[test]
    fn tokens_verify_and_refresh_until_they_expire() {
        let dir = std::env::temp_dir().join(format!("sluice-uaa-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let uaa = Uaa::open(&dir).unwrap();
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
        assert_eq!(claims.user_name, "alice");
        assert_eq!(claims.iss, issuer);
        // the same key again: the token is still good after a restart
        let again = Uaa::open(&dir).unwrap();
        let refreshed = again
            .grant(
                format!(
                    "grant_type=refresh_token&refresh_token={}",
                    tokens["refresh_token"].as_str().unwrap()
                )
                .as_bytes(),
                Some(("cf".into(), String::new())),
                issuer,
            )
            .unwrap();
        assert_eq!(
            again
                .verify(refreshed["access_token"].as_str().unwrap())
                .unwrap()
                .user_name,
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
        assert_eq!(
            again
                .verify(client["access_token"].as_str().unwrap())
                .unwrap()
                .user_name,
            "robot"
        );
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
        let other = Uaa { key: [7u8; 32] };
        assert_eq!(
            other
                .verify(tokens["access_token"].as_str().unwrap())
                .unwrap_err(),
            "bad signature"
        );
        assert!(uaa.verify("not.a.jwt").is_err());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
