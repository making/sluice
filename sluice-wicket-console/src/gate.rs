//! Who the console answers. With `--console-token` every request needs the
//! token, as the password of HTTP Basic (any user name: the browser asks for
//! it) or as a Bearer token. With `--oidc-issuer` a browser signs in through
//! the OpenID Provider instead (`/login`, `/oauth2/callback`, `/logout`): the
//! session is a cookie sealed with a key drawn at startup, good for
//! [`SESSION_MAX_AGE`]. Without either the console listens on a loopback
//! address only and answers only requests for an IP address or `localhost`,
//! so a DNS rebinding page cannot reach it. Edits take no simple method --
//! neither a form nor `sendBeacon` can send one -- and one from another
//! origin is refused.

use std::net::SocketAddr;
use std::time::Duration;

use base64::Engine as _;
use http::request::Parts;
use http::{HeaderValue, Method, StatusCode, header};
use serde::{Deserialize, Serialize};
use sluice_wicket_oidc::{Provider, Signer};
use tokio::net::TcpListener;

use crate::http::{Response, error, escape, page, redirect};

/// How long a browser stays signed in.
pub const SESSION_MAX_AGE: Duration = Duration::from_secs(8 * 3600);

const SESSION_COOKIE: &str = "sluice-wicket-session";

/// The console's credentials.
pub struct Gate {
    /// None: no credentials asked, on a loopback address.
    pub token: Option<String>,
    /// The browsers' sign-in through the OpenID Provider; None: the token.
    pub sign_in: Option<SignIn>,
    /// The realm of the browser's prompt for the token: the wicket's name.
    pub realm: &'static str,
}

/// The sign-in through the OpenID Provider (`--oidc-issuer`).
pub struct SignIn {
    pub provider: Provider,
    /// Seals the sessions; drawn at startup: a restart signs every browser
    /// out.
    pub signer: Signer,
}

/// What a session cookie says.
#[derive(Serialize, Deserialize)]
struct Session {
    user: String,
    /// Seconds since the epoch.
    exp: u64,
}

/// Binds `address`; `authenticated` (a token or a provider) or a loopback
/// address only, checked before anything listens.
pub async fn bind(address: &str, authenticated: bool) -> Result<TcpListener, String> {
    let failed = |e: std::io::Error| format!("--console {address}: {e}");
    let resolved: Vec<SocketAddr> = tokio::net::lookup_host(address)
        .await
        .map_err(failed)?
        .collect();
    if !authenticated && resolved.iter().any(|at| !at.ip().is_loopback()) {
        return Err(format!(
            "--console {address}: a console on a non-loopback address needs --console-token or --oidc-issuer"
        ));
    }
    TcpListener::bind(&resolved[..]).await.map_err(failed)
}

impl Gate {
    /// Whether the request of `parts` is the sign-in's (`/login`,
    /// `/oauth2/callback`, `/logout`): the way in, before any refusal.
    pub fn signs_in(&self, parts: &Parts) -> bool {
        self.sign_in.is_some()
            && matches!(parts.uri.path(), "/login" | "/oauth2/callback" | "/logout")
    }

    /// The answer refusing a request without the token or a session; without
    /// either configured, one for a host name but localhost (DNS rebinding);
    /// and an edit from another origin. None: the request is admitted.
    pub fn refusal(&self, parts: &Parts) -> Option<Response> {
        let headers = &parts.headers;
        let host = headers
            .get(header::HOST)
            .and_then(|host| host.to_str().ok())
            .unwrap_or_default();
        let token = self
            .token
            .as_deref()
            .is_some_and(|token| authorized(headers.get(header::AUTHORIZATION), token));
        match (&self.token, &self.sign_in) {
            (None, None) if !local_authority(host) => {
                return Some(error(
                    StatusCode::FORBIDDEN,
                    "without --console-token or --oidc-issuer the console answers requests for \
                     an IP address or localhost only",
                ));
            }
            (None, None) => {}
            _ if token || self.session(headers).is_some() => {}
            (token, sign_in) => {
                // a browser's page: to the sign-in; an API request: 401
                let page = sign_in.is_some()
                    && parts.method == Method::GET
                    && !parts.uri.path().starts_with("/api/")
                    && headers
                        .get(header::ACCEPT)
                        .and_then(|accept| accept.to_str().ok())
                        .is_some_and(|accept| accept.contains("text/html"));
                if page {
                    return Some(redirect("/login", None));
                }
                let mut response = error(
                    StatusCode::UNAUTHORIZED,
                    if sign_in.is_some() {
                        "the console needs a sign-in (/login), or its token"
                    } else {
                        "the console needs its token"
                    },
                );
                // the browser's prompt, where the token is the way in
                if token.is_some() && sign_in.is_none() {
                    let prompt =
                        format!("Basic realm=\"{} console\", charset=\"UTF-8\"", self.realm);
                    response.headers_mut().insert(
                        header::WWW_AUTHENTICATE,
                        HeaderValue::from_str(&prompt).expect("a plain realm"),
                    );
                }
                return Some(response);
            }
        }
        let edit = !matches!(parts.method, Method::GET | Method::HEAD);
        let foreign = headers.get(header::ORIGIN).is_some_and(|origin| {
            let authority = origin
                .to_str()
                .ok()
                .and_then(|origin| origin.split_once("://"))
                .map(|(_, authority)| authority);
            !authority.is_some_and(|authority| authority.eq_ignore_ascii_case(host))
        });
        (edit && foreign).then(|| {
            error(
                StatusCode::FORBIDDEN,
                "an edit from another origin is refused",
            )
        })
    }

    /// Who the session cookie of `headers` names, when it is sealed with the
    /// key and not expired.
    pub fn session(&self, headers: &http::HeaderMap) -> Option<String> {
        let signer = &self.sign_in.as_ref()?.signer;
        let sealed = headers
            .get_all(header::COOKIE)
            .iter()
            .filter_map(|cookies| cookies.to_str().ok())
            .flat_map(|cookies| cookies.split(';'))
            .map(str::trim)
            .find_map(|cookie| {
                cookie
                    .strip_prefix(SESSION_COOKIE)
                    .and_then(|rest| rest.strip_prefix('='))
            })?;
        let session: Session = serde_json::from_slice(&signer.verify(sealed)?).ok()?;
        (session.exp > sluice_wicket_oidc::epoch_secs()).then_some(session.user)
    }

    /// `/login`, `/oauth2/callback` and `/logout`: the sign-in through the
    /// provider, the session set by the callback.
    pub async fn sign_in_route(&self, parts: &Parts) -> Response {
        let sign_in = self.sign_in.as_ref().expect("signs_in");
        let headers = &parts.headers;
        let host = headers
            .get(header::HOST)
            .and_then(|host| host.to_str().ok())
            .unwrap_or("localhost");
        // the console speaks plain HTTP: https is a proxy's ahead of it
        let https = headers
            .get("x-forwarded-proto")
            .and_then(|proto| proto.to_str().ok())
            .is_some_and(|proto| proto == "https");
        let callback = format!(
            "{}://{host}/oauth2/callback",
            if https { "https" } else { "http" }
        );
        let query = crate::http::pairs(parts.uri.query());
        let q = |key: &str| {
            query
                .iter()
                .find(|(k, _)| k == key)
                .map(|(_, value)| value.as_str())
        };
        if parts.method != Method::GET {
            return error(StatusCode::METHOD_NOT_ALLOWED, "the sign-in takes GET");
        }
        match parts.uri.path() {
            "/login" => match sign_in.provider.begin(&callback).await {
                Ok(location) => redirect(&location, None),
                Err(e) => page(
                    StatusCode::BAD_GATEWAY,
                    "The provider is unreachable",
                    format!("<p>{}</p>", escape(&e)),
                ),
            },
            "/logout" => redirect("/", Some(cookie("", 0, https))),
            _ => {
                if let Some(error) = q("error") {
                    return page(
                        StatusCode::BAD_REQUEST,
                        "The provider refused the sign-in",
                        format!(
                            "<p>{}: {}</p>",
                            escape(error),
                            escape(q("error_description").unwrap_or("no reason given"))
                        ),
                    );
                }
                let (Some(code), Some(state)) = (q("code"), q("state")) else {
                    return page(
                        StatusCode::BAD_REQUEST,
                        "Not a callback",
                        "<p>The provider sent no code and state: <a href=\"/login\">sign in again</a>.</p>".into(),
                    );
                };
                match sign_in.provider.finish(code, state, &callback).await {
                    Ok(identity) => {
                        let session = Session {
                            user: identity.user_name,
                            exp: sluice_wicket_oidc::epoch_secs() + SESSION_MAX_AGE.as_secs(),
                        };
                        let sealed = sign_in
                            .signer
                            .sign(&serde_json::to_vec(&session).expect("plain data"));
                        redirect("/", Some(cookie(&sealed, SESSION_MAX_AGE.as_secs(), https)))
                    }
                    Err(e) if e.starts_with("the claim ") => page(
                        StatusCode::FORBIDDEN,
                        "Not allowed",
                        format!(
                            "<p>{}</p><p><a href=\"/login\">Sign in again</a></p>",
                            escape(&e)
                        ),
                    ),
                    Err(e) => page(
                        StatusCode::BAD_REQUEST,
                        "The sign-in failed",
                        format!(
                            "<p>{}</p><p><a href=\"/login\">Sign in again</a></p>",
                            escape(&e)
                        ),
                    ),
                }
            }
        }
    }
}

/// Whether `authorization` carries `token`: as the password of HTTP Basic
/// (any user name) or as a Bearer token. Compared in a time that does not tell
/// where they differ.
fn authorized(authorization: Option<&HeaderValue>, token: &str) -> bool {
    let Some((scheme, credentials)) = authorization
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.trim().split_once(' '))
    else {
        return false;
    };
    let given = if scheme.eq_ignore_ascii_case("bearer") {
        credentials.trim().as_bytes().to_vec()
    } else if scheme.eq_ignore_ascii_case("basic") {
        let Ok(decoded) = base64::engine::general_purpose::STANDARD.decode(credentials.trim())
        else {
            return false;
        };
        match decoded.iter().position(|&byte| byte == b':') {
            Some(colon) => decoded[colon + 1..].to_vec(),
            None => return false,
        }
    } else {
        return false;
    };
    sluice_wicket_oidc::constant_time_eq(&given, token.as_bytes())
}

/// Whether `authority` (a `Host` header) names an IP address or localhost.
fn local_authority(authority: &str) -> bool {
    let Ok(authority) = authority.parse::<http::uri::Authority>() else {
        return false;
    };
    let host = authority.host();
    let host = host
        .strip_prefix('[')
        .and_then(|host| host.strip_suffix(']'))
        .unwrap_or(host);
    host.parse::<std::net::IpAddr>().is_ok() || host.eq_ignore_ascii_case("localhost")
}

/// The session cookie holding `sealed` for `max_age` seconds (0: cleared).
fn cookie(sealed: &str, max_age: u64, https: bool) -> String {
    format!(
        "{SESSION_COOKIE}={sealed}; Path=/; HttpOnly; SameSite=Lax; Max-Age={max_age}{}",
        if https { "; Secure" } else { "" }
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The token is the HTTP Basic password, under any user name, or a Bearer
    /// token; without a token the console listens on loopback only and
    /// answers only a request for an IP address or localhost.
    #[tokio::test]
    async fn the_token_and_the_local_authorities() {
        let header = |value: String| HeaderValue::from_str(&value).unwrap();
        let basic = |credentials: &str| {
            header(format!(
                "Basic {}",
                base64::engine::general_purpose::STANDARD.encode(credentials)
            ))
        };
        for given in [
            basic("anyone:s3cret"),
            basic(":s3cret"),
            header("bearer s3cret".into()),
        ] {
            assert!(authorized(Some(&given), "s3cret"), "{given:?}");
        }
        for given in [
            basic("s3cret:"),
            basic("s3cret"),
            header("Basic !!".into()),
            header("Bearer s3cre".into()),
            header("Bearer s3crets".into()),
            header("Digest s3cret".into()),
        ] {
            assert!(!authorized(Some(&given), "s3cret"), "{given:?}");
        }
        assert!(!authorized(None, "s3cret"));

        // refused before anything listens: no socket on these addresses
        for exposed in ["0.0.0.0:0", "[::]:0", "192.0.2.1:9090"] {
            let refused = bind(exposed, false).await.unwrap_err();
            assert!(
                refused.ends_with("needs --console-token or --oidc-issuer"),
                "{refused}"
            );
        }
        for loopback in ["127.0.0.1:0", "localhost:0"] {
            bind(loopback, false).await.unwrap();
        }

        for local in [
            "127.0.0.1:9090",
            "[::1]:9090",
            "localhost:9090",
            "LOCALHOST",
            "10.0.0.1",
        ] {
            assert!(local_authority(local), "{local}");
        }
        for foreign in ["rebound.example:9090", "localhost.example", "wicket", ""] {
            assert!(!local_authority(foreign), "{foreign}");
        }

        // an edit from another origin is refused, one from the console's own is not
        let gate = Gate {
            token: None,
            sign_in: None,
            realm: "test",
        };
        let parts = |method: &str, host: &str, origin: Option<&str>| {
            let mut request = http::Request::builder()
                .method(method)
                .uri("/api/routes/a.localhost")
                .header(header::HOST, host);
            if let Some(origin) = origin {
                request = request.header(header::ORIGIN, origin);
            }
            request.body(()).unwrap().into_parts().0
        };
        let at = "127.0.0.1:9090";
        assert!(
            gate.refusal(&parts("PUT", at, Some("http://127.0.0.1:9090")))
                .is_none()
        );
        assert!(gate.refusal(&parts("PUT", at, None)).is_none());
        assert!(
            gate.refusal(&parts("GET", at, Some("http://evil.example")))
                .is_none()
        );
        let refused = gate.refusal(&parts("DELETE", at, Some("http://evil.example")));
        assert_eq!(refused.unwrap().status(), StatusCode::FORBIDDEN);
        let rebound = gate.refusal(&parts("GET", "rebound.example:9090", None));
        assert_eq!(rebound.unwrap().status(), StatusCode::FORBIDDEN);
    }
}
