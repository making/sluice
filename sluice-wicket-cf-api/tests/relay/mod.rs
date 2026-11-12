//! What routes to the test's cf-apis, as a balancer with no session affinity: each request to
//! the next instance in turn, whatever connection it came on, a new connection for each. An
//! instance refusing the connection (stopped, killed) was sent nothing: the request goes to the
//! one after it, and so does a `GET` whose instance went away before it answered (killed with
//! it under way). Any other request that got no answer is a 502, as of any balancer.
//! With a certificate it serves TLS and dials the instances over TLS, verified against that
//! certificate (the instances' own).

use std::convert::Infallible;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt as _, Empty, Full};
use hyper::body::{Bytes, Incoming};
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::{TlsAcceptor, TlsConnector};

type Body = BoxBody<Bytes, hyper::Error>;

pub struct Relay {
    runtime: Option<tokio::runtime::Runtime>,
    taken: Arc<Vec<AtomicUsize>>,
    changing: Arc<Vec<AtomicUsize>>,
}

struct Upstreams {
    instances: Vec<String>,
    next: AtomicUsize,
    taken: Arc<Vec<AtomicUsize>>,
    /// The requests but the `GET`s each instance has not answered yet.
    changing: Arc<Vec<AtomicUsize>>,
    tls: Option<TlsConnector>,
}

impl Relay {
    /// Listens on `listen` for the instances at `instances` (`host:port` each), with `tls` the
    /// certificate and key both sides serve.
    pub fn start(
        listen: &str,
        instances: Vec<String>,
        tls: Option<(CertificateDer<'static>, PrivateKeyDer<'static>)>,
    ) -> Relay {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let counts = || -> Arc<Vec<AtomicUsize>> {
            Arc::new(instances.iter().map(|_| AtomicUsize::new(0)).collect())
        };
        let (taken, changing) = (counts(), counts());
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let (acceptor, connector) = match tls {
            Some((cert, key)) => {
                let mut roots = rustls::RootCertStore::empty();
                roots.add(cert.clone()).unwrap();
                let client = rustls::ClientConfig::builder_with_provider(provider.clone())
                    .with_safe_default_protocol_versions()
                    .unwrap()
                    .with_root_certificates(roots)
                    .with_no_client_auth();
                let server = rustls::ServerConfig::builder_with_provider(provider)
                    .with_safe_default_protocol_versions()
                    .unwrap()
                    .with_no_client_auth()
                    .with_single_cert(vec![cert], key)
                    .unwrap();
                (
                    Some(TlsAcceptor::from(Arc::new(server))),
                    Some(TlsConnector::from(Arc::new(client))),
                )
            }
            None => (None, None),
        };
        let upstreams = Arc::new(Upstreams {
            instances,
            next: AtomicUsize::new(0),
            taken: taken.clone(),
            changing: changing.clone(),
            tls: connector,
        });
        let listener = runtime
            .block_on(TcpListener::bind(listen))
            .unwrap_or_else(|e| panic!("the relay on {listen}: {e}"));
        runtime.spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let (upstreams, acceptor) = (upstreams.clone(), acceptor.clone());
                tokio::spawn(async move {
                    match acceptor {
                        Some(acceptor) => {
                            if let Ok(stream) = acceptor.accept(stream).await {
                                serve(stream, upstreams).await;
                            }
                        }
                        None => serve(stream, upstreams).await,
                    }
                });
            }
        });
        Relay {
            runtime: Some(runtime),
            taken,
            changing,
        }
    }

    /// Whether the instance `at` has a request under way that is no `GET`: one a kill of it
    /// would lose, sent to no other.
    pub fn changing(&self, at: usize) -> bool {
        self.changing[at].load(Ordering::Relaxed) > 0
    }

    /// How many requests each instance took.
    pub fn taken(&self) -> Vec<usize> {
        let taken = self.taken.iter();
        taken.map(|count| count.load(Ordering::Relaxed)).collect()
    }
}

impl Drop for Relay {
    fn drop(&mut self) {
        // its listener closed by the time this returns: the address is free again
        if let Some(runtime) = self.runtime.take() {
            runtime.shutdown_timeout(Duration::from_secs(5));
        }
    }
}

async fn serve<S>(stream: S, upstreams: Arc<Upstreams>)
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let service = hyper::service::service_fn(move |request| {
        let upstreams = upstreams.clone();
        async move { Ok::<_, Infallible>(upstreams.send(request).await) }
    });
    let _ = hyper::server::conn::http1::Builder::new()
        .serve_connection(TokioIo::new(stream), service)
        .await;
}

fn answer(status: StatusCode, why: String) -> Response<Body> {
    let body = Full::new(Bytes::from(why)).map_err(|never| match never {});
    let mut response = Response::new(body.boxed());
    *response.status_mut() = status;
    response
}

impl Upstreams {
    /// The request to the next instance taking a connection, its answer as it comes. A `GET`
    /// whose instance went away before it answered is sent to the one after it.
    async fn send(&self, request: Request<Incoming>) -> Response<Body> {
        let first = self.next.fetch_add(1, Ordering::Relaxed);
        let count = self.instances.len();
        let (head, body) = request.into_parts();
        let again = (head.method == hyper::Method::GET).then(|| head.clone());
        let mut request = Some(Request::from_parts(head, body.boxed()));
        let mut failed = None;
        for at in (0..count).map(|turn| (first + turn) % count) {
            let Ok(stream) = TcpStream::connect(&self.instances[at]).await else {
                continue;
            };
            let Some(sending) = request.take() else {
                break;
            };
            self.taken[at].fetch_add(1, Ordering::Relaxed);
            let changing = again.is_none().then(|| UnderWay::begin(&self.changing[at]));
            let sent = match &self.tls {
                Some(tls) => {
                    let name = ServerName::try_from("127.0.0.1").unwrap();
                    match tls.connect(name, stream).await {
                        Ok(stream) => exchange(stream, sending).await,
                        Err(e) => Err(e.to_string()),
                    }
                }
                None => exchange(stream, sending).await,
            };
            drop(changing);
            match sent {
                Ok(response) => return response,
                Err(e) => {
                    failed = Some(format!("the instance {}: {e}", self.instances[at]));
                    let empty = Empty::new().map_err(|never| match never {});
                    request = (again.clone()).map(|head| Request::from_parts(head, empty.boxed()));
                }
            }
        }
        match failed {
            Some(why) => answer(StatusCode::BAD_GATEWAY, format!("the relay: {why}\n")),
            None => answer(
                StatusCode::SERVICE_UNAVAILABLE,
                "the relay: no instance takes a connection\n".into(),
            ),
        }
    }
}

/// A request counted while it is under way: until its answer began, or its client left.
struct UnderWay<'a>(&'a AtomicUsize);

impl UnderWay<'_> {
    fn begin(count: &AtomicUsize) -> UnderWay<'_> {
        count.fetch_add(1, Ordering::Relaxed);
        UnderWay(count)
    }
}

impl Drop for UnderWay<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

async fn exchange<S>(stream: S, request: Request<Body>) -> Result<Response<Body>, String>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (mut sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
        .await
        .map_err(|e| e.to_string())?;
    tokio::spawn(connection);
    let response = sender.send_request(request).await;
    let response = response.map_err(|e| e.to_string())?;
    Ok(response.map(|body| body.boxed()))
}
