//! The OpenID Provider behind `cf login --sso`: `GET /uaa/passcode` sends the
//! browser through the authorization code flow (PKCE, a nonce) and the
//! callback exchanges the code for an ID token at the provider's token
//! endpoint, found by discovery. The ID token comes straight from that
//! endpoint over TLS, authenticated by the client's secret, so its claims are
//! taken as they come (OpenID Connect Core 3.1.3.7, item 6) once `iss`,
//! `aud`, `exp` and the nonce check out; `--oidc-claim` then says who may
//! log in.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use rustls::pki_types::CertificateDer;
use rustls::pki_types::pem::PemObject as _;
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::store::epoch_secs;

/// How long a sign-in may take between the redirect and the callback.
const PENDING_TTL: Duration = Duration::from_secs(10 * 60);
/// Sign-ins under way at most.
const MAX_PENDING: usize = 1000;
/// How long a request to the provider may take.
const TIMEOUT: Duration = Duration::from_secs(15);

/// The provider and the client registered with it.
#[derive(Clone, Debug)]
pub struct OidcSettings {
    /// The issuer: discovery is at `<issuer>/.well-known/openid-configuration`.
    pub issuer: String,
    pub client_id: String,
    /// None: a public client (PKCE only).
    pub client_secret: Option<String>,
    /// The scopes asked for; `openid` among them.
    pub scopes: Vec<String>,
    /// Who may log in: a user's ID token must satisfy every rule.
    pub claims: Vec<ClaimRule>,
    /// A PEM CA the provider's certificate is verified against; None: the
    /// webPKI roots.
    pub ca_pem: Option<Vec<u8>>,
}

/// A claim's accepted values, `*` matching any run of characters: the claim
/// must hold (or, a list, contain) one of them.
#[derive(Clone, Debug, PartialEq)]
pub struct ClaimRule {
    pub claim: String,
    pub patterns: Vec<String>,
}

/// Who signed in, as the ID token says.
#[derive(Clone, Debug)]
pub struct Identity {
    /// `preferred_username`, else `email`, else `sub`.
    pub user_name: String,
    pub email: String,
    /// The ID token's claims, for the rules at the next refresh.
    pub claims: Value,
}

/// What discovery told.
#[derive(Debug)]
struct Discovery {
    authorization_endpoint: String,
    token_endpoint: String,
    /// The client authenticates with a Basic header, else in the form.
    basic_auth: bool,
}

/// A sign-in under way: the browser was sent to the provider.
struct Pending {
    verifier: String,
    nonce: String,
    since: Instant,
}

pub struct Provider {
    pub settings: OidcSettings,
    http: reqwest::Client,
    discovered: tokio::sync::Mutex<Option<Arc<Discovery>>>,
    pending: Mutex<HashMap<String, Pending>>,
}

impl Provider {
    pub fn new(settings: OidcSettings) -> Result<Provider, String> {
        let mut roots = rustls::RootCertStore::empty();
        match &settings.ca_pem {
            Some(pem) => {
                for cert in CertificateDer::pem_slice_iter(pem) {
                    let cert = cert.map_err(|e| format!("--oidc-ca-cert: {e}"))?;
                    roots
                        .add(cert)
                        .map_err(|e| format!("--oidc-ca-cert: {e}"))?;
                }
                if roots.is_empty() {
                    return Err("--oidc-ca-cert: no certificate in it".into());
                }
            }
            None => roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned()),
        }
        let tls = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .map_err(|e| format!("tls: {e}"))?
        .with_root_certificates(roots)
        .with_no_client_auth();
        let http = reqwest::Client::builder()
            .use_preconfigured_tls(tls)
            .timeout(TIMEOUT)
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|e| format!("http client: {e}"))?;
        Ok(Provider {
            settings,
            http,
            discovered: tokio::sync::Mutex::new(None),
            pending: Mutex::new(HashMap::new()),
        })
    }

    /// The provider's endpoints, found once; a failure is tried again at the
    /// next call (the provider may start after the cf-api).
    async fn discover(&self) -> Result<Arc<Discovery>, String> {
        let mut discovered = self.discovered.lock().await;
        if let Some(discovery) = discovered.as_ref() {
            return Ok(discovery.clone());
        }
        let issuer = self.settings.issuer.trim_end_matches('/');
        let url = format!("{issuer}/.well-known/openid-configuration");
        let document: Value = self
            .http
            .get(&url)
            .send()
            .await
            .and_then(|response| response.error_for_status())
            .map_err(|e| format!("{url}: {e}"))?
            .json()
            .await
            .map_err(|e| format!("{url}: {e}"))?;
        if document["issuer"]
            .as_str()
            .map(|found| found.trim_end_matches('/'))
            != Some(issuer)
        {
            return Err(format!(
                "{url}: issuer {} is not {}",
                document["issuer"], self.settings.issuer
            ));
        }
        let endpoint = |name: &str| {
            document[name]
                .as_str()
                .map(str::to_string)
                .ok_or_else(|| format!("{url}: no {name}"))
        };
        let methods = document["token_endpoint_auth_methods_supported"]
            .as_array()
            .cloned()
            .unwrap_or_default();
        let discovery = Arc::new(Discovery {
            authorization_endpoint: endpoint("authorization_endpoint")?,
            token_endpoint: endpoint("token_endpoint")?,
            // the default of the spec when none is told
            basic_auth: methods.is_empty() || methods.iter().any(|m| m == "client_secret_basic"),
        });
        *discovered = Some(discovery.clone());
        Ok(discovery)
    }

    /// Where to send the browser: the authorization request, its state kept
    /// for the callback at `redirect_uri`.
    pub async fn begin(&self, redirect_uri: &str) -> Result<String, String> {
        let discovery = self.discover().await?;
        let state = random();
        let verifier = random();
        let nonce = random();
        let challenge = URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes()));
        {
            let mut pending = self.pending.lock().expect("no panic held it");
            pending.retain(|_, p| p.since.elapsed() < PENDING_TTL);
            if pending.len() >= MAX_PENDING {
                return Err("too many sign-ins under way: try again later".into());
            }
            pending.insert(
                state.clone(),
                Pending {
                    verifier,
                    nonce: nonce.clone(),
                    since: Instant::now(),
                },
            );
        }
        let query = form_urlencoded::Serializer::new(String::new())
            .append_pair("response_type", "code")
            .append_pair("client_id", &self.settings.client_id)
            .append_pair("redirect_uri", redirect_uri)
            .append_pair("scope", &self.settings.scopes.join(" "))
            .append_pair("state", &state)
            .append_pair("nonce", &nonce)
            .append_pair("code_challenge", &challenge)
            .append_pair("code_challenge_method", "S256")
            .finish();
        let separator = if discovery.authorization_endpoint.contains('?') {
            '&'
        } else {
            '?'
        };
        Ok(format!(
            "{}{separator}{query}",
            discovery.authorization_endpoint
        ))
    }

    /// The callback: `code` exchanged for the ID token, which must be the
    /// sign-in `state` began; who it names, when the rules allow them.
    pub async fn finish(
        &self,
        code: &str,
        state: &str,
        redirect_uri: &str,
    ) -> Result<Identity, String> {
        let pending = self
            .pending
            .lock()
            .expect("no panic held it")
            .remove(state)
            .filter(|p| p.since.elapsed() < PENDING_TTL)
            .ok_or("this sign-in is unknown or took too long: start again at /uaa/passcode")?;
        let discovery = self.discover().await?;
        let mut form = vec![
            ("grant_type", "authorization_code"),
            ("code", code),
            ("redirect_uri", redirect_uri),
            ("code_verifier", pending.verifier.as_str()),
        ];
        let secret = self.settings.client_secret.as_deref();
        let mut request = self.http.post(&discovery.token_endpoint);
        if discovery.basic_auth && secret.is_some() {
            request = request.basic_auth(&self.settings.client_id, secret);
        } else {
            form.push(("client_id", self.settings.client_id.as_str()));
            if let Some(secret) = secret {
                form.push(("client_secret", secret));
            }
        }
        let body = form_urlencoded::Serializer::new(String::new())
            .extend_pairs(form)
            .finish();
        let response = request
            .header(
                reqwest::header::CONTENT_TYPE,
                "application/x-www-form-urlencoded",
            )
            .body(body)
            .send()
            .await
            .map_err(|e| format!("{}: {e}", discovery.token_endpoint))?;
        let status = response.status();
        let body: Value = response.json().await.unwrap_or(Value::Null);
        if !status.is_success() {
            return Err(format!(
                "{}: {status}: {}",
                discovery.token_endpoint,
                body["error_description"]
                    .as_str()
                    .or(body["error"].as_str())
                    .unwrap_or("no reason given")
            ));
        }
        let id_token = body["id_token"]
            .as_str()
            .ok_or("the token response holds no id_token: is the scope openid granted?")?;
        let claims = claims_of(id_token)?;
        if claims["iss"].as_str().map(|iss| iss.trim_end_matches('/'))
            != Some(self.settings.issuer.trim_end_matches('/'))
        {
            return Err(format!("the ID token's issuer is {}", claims["iss"]));
        }
        let audience = match &claims["aud"] {
            Value::String(aud) => aud == &self.settings.client_id,
            Value::Array(auds) => auds.iter().any(|aud| aud == &self.settings.client_id),
            _ => false,
        };
        if !audience {
            return Err(format!("the ID token's audience is {}", claims["aud"]));
        }
        if claims["exp"].as_u64().is_none_or(|exp| exp <= epoch_secs()) {
            return Err("the ID token has expired".into());
        }
        if claims["nonce"].as_str() != Some(pending.nonce.as_str()) {
            return Err("the ID token's nonce is not this sign-in's".into());
        }
        allowed(&self.settings.claims, &claims)?;
        let text = |name: &str| claims[name].as_str().filter(|value| !value.is_empty());
        let user_name = text("preferred_username")
            .or_else(|| text("email"))
            .or_else(|| text("sub"))
            .ok_or("the ID token names no user (no preferred_username, email or sub)")?
            .to_string();
        Ok(Identity {
            user_name,
            email: text("email").unwrap_or_default().to_string(),
            claims,
        })
    }
}

/// Checks `claims` against every rule: why the user is not allowed.
pub fn allowed(rules: &[ClaimRule], claims: &Value) -> Result<(), String> {
    for rule in rules {
        let value = &claims[&rule.claim];
        let holds = |value: &Value| {
            let text = match value {
                Value::String(text) => text.clone(),
                Value::Null => return false,
                other => other.to_string(),
            };
            rule.patterns.iter().any(|pattern| glob(pattern, &text))
        };
        let matched = match value {
            Value::Array(values) => values.iter().any(holds),
            other => holds(other),
        };
        if !matched {
            return Err(format!(
                "the claim {} is {}, not {}",
                rule.claim,
                if value.is_null() {
                    "missing".to_string()
                } else {
                    value.to_string()
                },
                rule.patterns
                    .iter()
                    .map(|pattern| format!("'{pattern}'"))
                    .collect::<Vec<_>>()
                    .join(" or ")
            ));
        }
    }
    Ok(())
}

/// Whether `value` is `pattern`, `*` matching any run of characters.
fn glob(pattern: &str, value: &str) -> bool {
    let mut parts = pattern.split('*');
    let first = parts.next().unwrap_or_default();
    let Some(mut rest) = value.strip_prefix(first) else {
        return false;
    };
    let mut parts: Vec<&str> = parts.collect();
    let Some(last) = parts.pop() else {
        return rest.is_empty();
    };
    for part in parts {
        match rest.find(part) {
            Some(at) => rest = &rest[at + part.len()..],
            None => return false,
        }
    }
    rest.ends_with(last)
}

/// The claims of a JWT, unverified (see the module).
fn claims_of(token: &str) -> Result<Value, String> {
    let body = token.split('.').nth(1).ok_or("the id_token is not a JWT")?;
    serde_json::from_slice(
        &URL_SAFE_NO_PAD
            .decode(body)
            .map_err(|_| "the id_token's claims are not base64url")?,
    )
    .map_err(|e| format!("the id_token's claims: {e}"))
}

/// 32 random bytes, base64url (43 characters: a state, a nonce, a PKCE
/// verifier).
fn random() -> String {
    let mut bytes = [0u8; 32];
    getrandom::fill(&mut bytes).expect("the OS has randomness");
    URL_SAFE_NO_PAD.encode(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn rules_match_strings_lists_and_wildcards() {
        let rules = [
            ClaimRule {
                claim: "email".into(),
                patterns: vec!["*@example.com".into(), "root@example.org".into()],
            },
            ClaimRule {
                claim: "groups".into(),
                patterns: vec!["dev*".into()],
            },
        ];
        let alice = json!({"email": "alice@example.com", "groups": ["ops", "developers"]});
        assert_eq!(allowed(&rules, &alice), Ok(()));
        let root = json!({"email": "root@example.org", "groups": ["dev"]});
        assert_eq!(allowed(&rules, &root), Ok(()));
        let bob = json!({"email": "bob@example.com", "groups": ["ops"]});
        assert_eq!(
            allowed(&rules, &bob).unwrap_err(),
            "the claim groups is [\"ops\"], not 'dev*'"
        );
        let eve = json!({"email": "eve@example.net", "groups": ["dev"]});
        assert!(allowed(&rules, &eve).unwrap_err().contains("email"));
        let nobody = json!({"groups": ["dev"]});
        assert_eq!(
            allowed(&rules, &nobody).unwrap_err(),
            "the claim email is missing, not '*@example.com' or 'root@example.org'"
        );
        // a number compares as text; no rules: anyone
        let verified = [ClaimRule {
            claim: "email_verified".into(),
            patterns: vec!["true".into()],
        }];
        assert!(allowed(&verified, &json!({"email_verified": true})).is_ok());
        assert!(allowed(&verified, &json!({"email_verified": false})).is_err());
        assert!(allowed(&[], &json!({})).is_ok());
        assert!(glob("a*b*c", "abc") && glob("a*b*c", "a-b-c") && !glob("a*b*c", "ac"));
        assert!(glob("*", "") && glob("x", "x") && !glob("x", "xx") && !glob("", "x"));
    }

    #[test]
    fn claims_come_from_the_second_segment() {
        let claims = json!({"iss": "https://idp", "sub": "1"});
        let token = format!(
            "e30.{}.sig",
            URL_SAFE_NO_PAD.encode(serde_json::to_vec(&claims).unwrap())
        );
        assert_eq!(claims_of(&token).unwrap(), claims);
        assert!(claims_of("e30").is_err());
        assert!(claims_of("e30.!!.sig").is_err());
    }
}
