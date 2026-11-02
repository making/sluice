//! An OCI image's manifests and blobs, pulled from its registry through the
//! OCI distribution API. A registry on the loopback (`localhost`,
//! `127.0.0.0/8`, `[::1]`) is reached over plain HTTP, as the docker daemon
//! reaches one; any other over HTTPS, verified against the webPKI roots. Asked
//! to authenticate, a pull gets a token from the registry's realm -- anonymous,
//! or with the given credentials -- or answers a basic challenge with them.

use std::collections::HashMap;
use std::time::Duration;

use base64::Engine as _;
use reqwest::StatusCode;
use reqwest::header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE, WWW_AUTHENTICATE};
use serde_json::Value;
use sha2::{Digest as _, Sha256};

/// What a manifest is asked as: an image's, or an index of them.
const MANIFEST_TYPES: &str = "application/vnd.oci.image.manifest.v1+json, application/vnd.oci.image.index.v1+json, application/vnd.docker.distribution.manifest.v2+json, application/vnd.docker.distribution.manifest.list.v2+json";
/// The media types of an index.
pub const INDEX_TYPES: [&str; 2] = [
    "application/vnd.oci.image.index.v1+json",
    "application/vnd.docker.distribution.manifest.list.v2+json",
];
/// The distribution spec's largest manifest.
const MAX_MANIFEST_BYTES: u64 = 4 << 20;

/// An image reference, `[registry/]repository[:tag][@sha256:<digest>]`, read
/// as docker reads one: a first segment holding a `.` or a `:`, or
/// `localhost`, names the registry, else it is Docker Hub's (`library/`
/// before a lone name); no tag and no digest means `latest`.
#[derive(Debug, PartialEq)]
pub struct Reference {
    /// `host[:port]` the API is reached at (Docker Hub's is
    /// `registry-1.docker.io`).
    pub registry: String,
    pub repository: String,
    pub tag: Option<String>,
    /// `sha256:<hex>`.
    pub digest: Option<String>,
}

impl Reference {
    pub fn parse(image: &str) -> Result<Reference, String> {
        let invalid = |why: &str| {
            format!(
                "'{image}' is no image reference ({why}): [registry/]repository[:tag][@sha256:<digest>]"
            )
        };
        if image.is_empty() {
            return Err(invalid("empty"));
        }
        let (rest, digest) = match image.split_once('@') {
            Some((rest, digest)) => match digest.strip_prefix("sha256:") {
                Some(hex) if is_sha256(hex) => (rest, Some(digest.to_string())),
                _ => return Err(invalid("a digest is sha256:<64 hex digits>")),
            },
            None => (image, None),
        };
        // a tag follows the last ':' past the last '/' (a registry's port precedes one)
        let (name, tag) = match rest.rfind(':') {
            Some(colon) if rest.rfind('/').is_none_or(|slash| colon > slash) => {
                (&rest[..colon], Some(rest[colon + 1..].to_string()))
            }
            _ => (rest, None),
        };
        if let Some(tag) = &tag {
            let mut chars = tag.chars();
            let first = chars.next();
            if !first.is_some_and(|c| c.is_ascii_alphanumeric() || c == '_')
                || tag.len() > 128
                || !chars.all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | '-'))
            {
                return Err(invalid("a tag is 128 of A-Z, a-z, 0-9, '_', '.' and '-'"));
            }
        }
        let (registry, repository) = match name.split_once('/') {
            Some((first, path)) if first.contains(['.', ':']) || first == "localhost" => {
                (first, path.to_string())
            }
            _ => ("docker.io", name.to_string()),
        };
        let host_chars = |host: &str| {
            !host.is_empty()
                && host.bytes().all(|b| {
                    b.is_ascii_alphanumeric() || matches!(b, b'.' | b'-' | b':' | b'[' | b']')
                })
        };
        if !host_chars(registry) {
            return Err(invalid("a registry is host[:port]"));
        }
        let segment = |segment: &str| {
            segment
                .bytes()
                .next()
                .is_some_and(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
                && segment
                    .bytes()
                    .last()
                    .is_some_and(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
                && segment.bytes().all(|b| {
                    b.is_ascii_lowercase() || b.is_ascii_digit() || matches!(b, b'.' | b'_' | b'-')
                })
        };
        if repository.len() > 255 || !repository.split('/').all(segment) {
            return Err(invalid(
                "a repository is lower case: a-z, 0-9 and '.', '_' or '-' between them, segments separated by '/'",
            ));
        }
        let (registry, repository) = match registry {
            "docker.io" | "index.docker.io" if !repository.contains('/') => (
                "registry-1.docker.io".to_string(),
                format!("library/{repository}"),
            ),
            "docker.io" | "index.docker.io" => ("registry-1.docker.io".to_string(), repository),
            registry => (registry.to_string(), repository),
        };
        Ok(Reference {
            registry,
            repository,
            tag,
            digest,
        })
    }

    /// What the manifest is asked by: the digest, else the tag.
    pub fn by(&self) -> &str {
        self.digest
            .as_deref()
            .or(self.tag.as_deref())
            .unwrap_or("latest")
    }

    /// Whether the registry is on the loopback, reached over plain HTTP.
    fn plain_http(&self) -> bool {
        let host = match self.registry.strip_prefix('[') {
            Some(bracketed) => bracketed.split(']').next().unwrap_or_default(),
            None => self.registry.split(':').next().unwrap_or_default(),
        };
        host == "localhost"
            || host
                .parse::<std::net::IpAddr>()
                .is_ok_and(|ip| ip.is_loopback())
    }

    /// The repository's API, `<scheme>://<registry>/v2/<repository>`.
    fn base(&self) -> String {
        let scheme = if self.plain_http() { "http" } else { "https" };
        format!("{scheme}://{}/v2/{}", self.registry, self.repository)
    }
}

/// The client of the registries.
#[derive(Clone)]
pub struct Registry {
    http: reqwest::Client,
}

impl Registry {
    /// Names itself `agent` to the registries.
    pub fn new(agent: &str) -> Result<Registry, String> {
        let http = sluice_wicket_oidc::http_client(None, "the registries")?
            .connect_timeout(Duration::from_secs(10))
            .read_timeout(Duration::from_secs(60))
            .user_agent(agent)
            .build()
            .map_err(|e| format!("http client: {e}"))?;
        Ok(Registry { http })
    }

    /// A pull of `reference`, with the `(username, password)` of a private one.
    pub fn session<'a>(
        &'a self,
        reference: &'a Reference,
        credentials: Option<(&'a str, &'a str)>,
    ) -> Session<'a> {
        Session {
            http: &self.http,
            reference,
            credentials,
            authorization: None,
        }
    }
}

/// One pull: the authorization a challenge got is kept for the requests
/// after it.
pub struct Session<'a> {
    http: &'a reqwest::Client,
    reference: &'a Reference,
    credentials: Option<(&'a str, &'a str)>,
    authorization: Option<String>,
}

impl Session<'_> {
    /// The manifest `by` names: its digest and its JSON, the response's
    /// media type in `mediaType` when it holds none.
    pub async fn manifest(&mut self, by: &str) -> Result<(String, Value), String> {
        let url = format!("{}/manifests/{by}", self.reference.base());
        let response = self.get(&url, MANIFEST_TYPES).await?;
        let media_type = response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .map(|value| {
                value
                    .split(';')
                    .next()
                    .unwrap_or_default()
                    .trim()
                    .to_string()
            });
        let bytes = read(response, MAX_MANIFEST_BYTES, "the manifest").await?;
        let mut manifest: Value = serde_json::from_slice(&bytes)
            .map_err(|e| format!("the manifest {by} is no JSON: {e}"))?;
        if manifest["mediaType"].is_null()
            && let (Some(object), Some(media_type)) = (manifest.as_object_mut(), media_type)
        {
            object.insert("mediaType".into(), Value::String(media_type));
        }
        Ok((format!("sha256:{}", sha256(&bytes)), manifest))
    }

    /// The blob `digest` as the registry answers it, its bytes unread.
    pub async fn blob(&mut self, digest: &str, accept: &str) -> Result<reqwest::Response, String> {
        let url = format!("{}/blobs/{digest}", self.reference.base());
        self.get(&url, accept).await
    }

    /// The bytes of the blob `digest` of `size` bytes, checked against both.
    pub async fn read_blob(
        &mut self,
        digest: &str,
        size: u64,
        accept: &str,
        what: &str,
    ) -> Result<Vec<u8>, String> {
        let response = self.blob(digest, accept).await?;
        let bytes = read(response, size, what).await?;
        if bytes.len() as u64 != size || format!("sha256:{}", sha256(&bytes)) != digest {
            return Err(format!(
                "the layer {digest} is not what its size ({size}) and digest say: {} bytes came",
                bytes.len()
            ));
        }
        Ok(bytes)
    }

    /// GETs `url`, answering the registry's challenge once; a refusal is an
    /// error. A redirect elsewhere (a blob's storage) goes without the
    /// authorization.
    async fn get(&mut self, url: &str, accept: &str) -> Result<reqwest::Response, String> {
        let mut challenged = false;
        loop {
            let mut request = self.http.get(url).header(ACCEPT, accept);
            if let Some(authorization) = &self.authorization {
                request = request.header(AUTHORIZATION, authorization);
            }
            let response = request
                .send()
                .await
                .map_err(|e| format!("{}: {}", self.reference.registry, causes(&e)))?;
            let challenge = response
                .headers()
                .get(WWW_AUTHENTICATE)
                .and_then(|value| value.to_str().ok())
                .map(String::from);
            match (response.status(), challenge) {
                (StatusCode::UNAUTHORIZED, Some(challenge)) if !challenged => {
                    self.authorization = Some(self.answer(&challenge).await?);
                    challenged = true;
                }
                (status, _) if status.is_success() => return Ok(response),
                _ => return Err(self.refused(response).await),
            }
        }
    }

    /// The authorization a challenge asks for: a bearer token from its
    /// realm, or the credentials for a basic one.
    async fn answer(&self, challenge: &str) -> Result<String, String> {
        let (scheme, rest) = challenge.split_once(' ').unwrap_or((challenge, ""));
        let parameters = parameters(rest);
        match scheme.to_ascii_lowercase().as_str() {
            "bearer" => {
                let realm = parameters
                    .get("realm")
                    .ok_or("the registry's bearer challenge names no realm")?;
                let mut url = reqwest::Url::parse(realm)
                    .map_err(|e| format!("the registry's token realm {realm}: {e}"))?;
                if url.scheme() != "https" && !self.reference.plain_http() {
                    return Err(format!(
                        "the registry's token realm {realm} is no https URL"
                    ));
                }
                let scope = parameters
                    .get("scope")
                    .cloned()
                    .unwrap_or_else(|| format!("repository:{}:pull", self.reference.repository));
                {
                    let mut query = url.query_pairs_mut();
                    if let Some(service) = parameters.get("service") {
                        query.append_pair("service", service);
                    }
                    query.append_pair("scope", &scope);
                }
                let mut request = self.http.get(url);
                if let Some((username, password)) = self.credentials {
                    request = request.basic_auth(username, Some(password));
                }
                let response = request
                    .send()
                    .await
                    .map_err(|e| format!("the registry's token realm: {}", causes(&e)))?;
                if !response.status().is_success() {
                    return Err(self.refused(response).await);
                }
                let body: Value = response
                    .json()
                    .await
                    .map_err(|e| format!("the registry's token: {e}"))?;
                let token = body["token"]
                    .as_str()
                    .or_else(|| body["access_token"].as_str())
                    .filter(|token| !token.is_empty())
                    .ok_or("the registry's token realm answered no token")?;
                Ok(format!("Bearer {token}"))
            }
            "basic" => self
                .credentials
                .map(|(username, password)| {
                    let pair = format!("{username}:{password}");
                    format!(
                        "Basic {}",
                        base64::engine::general_purpose::STANDARD.encode(pair)
                    )
                })
                .ok_or_else(|| "the registry asks for credentials".to_string()),
            other => Err(format!(
                "the registry asks for authentication of the scheme '{other}', which a pull does not speak"
            )),
        }
    }

    /// The registry's refusal as the user reads it: the request, the status
    /// and the distribution error's code and message; for a refused pull, a
    /// hint at the credentials.
    async fn refused(&self, response: reqwest::Response) -> String {
        let status = response.status();
        let path = response.url().path().to_string();
        let body = response.bytes().await.unwrap_or_default();
        let detail = serde_json::from_slice::<Value>(&body)
            .ok()
            .and_then(|body| {
                let error = &body["errors"][0];
                Some(format!(
                    ": {}: {}",
                    error["code"].as_str()?,
                    error["message"].as_str().unwrap_or_default()
                ))
            })
            .unwrap_or_else(|| {
                let text = String::from_utf8_lossy(&body[..body.len().min(200)]);
                let text = text.trim();
                if text.is_empty() {
                    String::new()
                } else {
                    format!(": {text}")
                }
            });
        let hint = match (status.as_u16(), self.credentials.is_some()) {
            (401 | 403, false) => {
                ": the image does not exist, or it is private: it needs credentials"
            }
            (401 | 403, true) => ": the image does not exist, or the credentials are refused",
            _ => "",
        };
        format!("the registry refused GET {path} ({status}{detail}){hint}")
    }
}

/// A descriptor's digest: a sha256 one.
pub fn descriptor_digest(descriptor: &Value) -> Result<String, String> {
    let digest = descriptor["digest"].as_str().unwrap_or_default();
    match digest.strip_prefix("sha256:") {
        Some(hex) if is_sha256(hex) => Ok(digest.to_string()),
        _ => Err(format!("'{digest}' is no sha256 digest")),
    }
}

fn is_sha256(hex: &str) -> bool {
    hex.len() == 64
        && hex
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

/// The SHA-256 of `bytes`, in hex.
pub fn sha256(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// The parameters of a challenge: `key=value` or `key="value"` separated by
/// commas, a quoted value holding commas and `\`-escapes; the keys in lower
/// case.
fn parameters(text: &str) -> HashMap<String, String> {
    let mut parameters = HashMap::new();
    let mut chars = text.chars().peekable();
    loop {
        while chars.next_if(|c| *c == ',' || c.is_whitespace()).is_some() {}
        let key: String = chars.by_ref().take_while(|c| *c != '=').collect();
        if key.is_empty() {
            return parameters;
        }
        let mut value = String::new();
        if chars.next_if_eq(&'"').is_some() {
            while let Some(c) = chars.next() {
                match c {
                    '\\' => value.extend(chars.next()),
                    '"' => break,
                    c => value.push(c),
                }
            }
        } else {
            while let Some(c) = chars.next_if(|c| *c != ',') {
                value.push(c);
            }
        }
        parameters.insert(key.trim().to_ascii_lowercase(), value.trim().to_string());
    }
}

/// The body, `limit` bytes at most.
pub async fn read(
    mut response: reqwest::Response,
    limit: u64,
    what: &str,
) -> Result<Vec<u8>, String> {
    let too_large = || format!("{what} is more than {limit} bytes");
    if response
        .content_length()
        .is_some_and(|length| length > limit)
    {
        return Err(too_large());
    }
    let mut body = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|e| format!("{what}: {}", causes(&e)))?
    {
        if (body.len() + chunk.len()) as u64 > limit {
            return Err(too_large());
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

/// An error with its causes: reqwest's own names the request only.
pub fn causes(error: &dyn std::error::Error) -> String {
    let mut text = error.to_string();
    let mut source = error.source();
    while let Some(cause) = source {
        text.push_str(&format!(": {cause}"));
        source = cause.source();
    }
    text
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_references_as_docker_does_and_challenges_as_registries_send_them() {
        let read = |image: &str| Reference::parse(image).unwrap();
        let reference =
            |registry: &str, repository: &str, tag: Option<&str>, digest: Option<&str>| Reference {
                registry: registry.into(),
                repository: repository.into(),
                tag: tag.map(String::from),
                digest: digest.map(String::from),
            };
        let hub = "registry-1.docker.io";
        assert_eq!(read("hello"), reference(hub, "library/hello", None, None));
        assert_eq!(read("hello").by(), "latest");
        assert_eq!(read("docker.io/library/hello"), read("hello"));
        assert_eq!(
            read("me/hello:1.2.0"),
            reference(hub, "me/hello", Some("1.2.0"), None)
        );
        let ghcr = read("ghcr.io/me/wasm/hello:1.2.0");
        assert_eq!(
            ghcr,
            reference("ghcr.io", "me/wasm/hello", Some("1.2.0"), None)
        );
        assert_eq!(ghcr.base(), "https://ghcr.io/v2/me/wasm/hello");
        let pinned = format!("sha256:{}", "ab".repeat(32));
        let local = read(&format!("127.0.0.1:5000/a/b:1@{pinned}"));
        assert_eq!(
            local,
            reference("127.0.0.1:5000", "a/b", Some("1"), Some(&pinned))
        );
        assert_eq!(local.by(), pinned);
        assert_eq!(local.base(), "http://127.0.0.1:5000/v2/a/b");
        for (image, plain) in [
            ("localhost:5000/hello", true),
            ("localhost/hello", true),
            ("[::1]:5000/hello", true),
            ("localhost.example.com:5000/hello", false),
            ("10.0.0.1:5000/hello", false),
        ] {
            assert_eq!(read(image).plain_http(), plain, "{image}");
        }
        for (image, why) in [
            ("", "empty"),
            ("Hello", "lower case"),
            ("ghcr.io//hello", "lower case"),
            ("ghcr.io/me/-hello", "lower case"),
            ("ghcr.io/me/he llo", "lower case"),
            ("hello:", "a tag is"),
            ("hello:.1", "a tag is"),
            ("hello@sha256:abc", "a digest is"),
            ("hello@sha512:abc", "a digest is"),
            ("ghcr.io?x/hello", "a registry is"),
        ] {
            let error = Reference::parse(image).unwrap_err();
            assert!(error.contains(why), "{image}: {error}");
        }

        let challenge = parameters(
            r#"realm="https://ghcr.io/token",service="ghcr.io", scope="repository:me/hello:pull,push""#,
        );
        assert_eq!(challenge["realm"], "https://ghcr.io/token");
        assert_eq!(challenge["service"], "ghcr.io");
        assert_eq!(challenge["scope"], "repository:me/hello:pull,push");
        let escaped = parameters(r#"Realm="a \"b\", c",error=insufficient_scope"#);
        assert_eq!(escaped["realm"], r#"a "b", c"#);
        assert_eq!(escaped["error"], "insufficient_scope");
        assert!(parameters("").is_empty());
    }
}
