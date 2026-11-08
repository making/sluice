//! OTLP/HTTP export of the lines the flume receives, opt-in with
//! `--otlp-endpoint`: a standing watcher of the hub feeds the exporter, which
//! batches what it exports and posts it as `ExportLogsServiceRequest`
//! (`application/x-protobuf`, `.kb/live-access-log.md`).

use std::collections::BTreeMap;
use std::io::Write;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use prost::Message;
use reqwest::header::{CONTENT_ENCODING, CONTENT_TYPE, HeaderName, HeaderValue};
use rustls::pki_types::pem::PemObject as _;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio::sync::{mpsc, watch};

use crate::hub::{self, Hub};
use crate::proto::{AccessFilter, WatchEvent, watch_event, watch_gap};

/// The OTLP logs protos, vendored under `proto/` (their `README.md`).
pub mod otlp {
    pub mod opentelemetry {
        pub mod proto {
            pub mod common {
                pub mod v1 {
                    tonic::include_proto!("opentelemetry.proto.common.v1");
                }
            }
            pub mod resource {
                pub mod v1 {
                    tonic::include_proto!("opentelemetry.proto.resource.v1");
                }
            }
            pub mod logs {
                pub mod v1 {
                    tonic::include_proto!("opentelemetry.proto.logs.v1");
                }
            }
            pub mod collector {
                pub mod logs {
                    pub mod v1 {
                        tonic::include_proto!("opentelemetry.proto.collector.logs.v1");
                    }
                }
            }
        }
    }
}

use otlp::opentelemetry::proto::collector::logs::v1::{
    ExportLogsServiceRequest, ExportLogsServiceResponse,
};
use otlp::opentelemetry::proto::common::v1::{AnyValue, KeyValue, any_value};
use otlp::opentelemetry::proto::logs::v1::{LogRecord, ResourceLogs, ScopeLogs, SeverityNumber};
use otlp::opentelemetry::proto::resource::v1::Resource;

use otlp::opentelemetry::proto::common::v1::InstrumentationScope;

/// One request may take this long before it counts as a failure.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
/// How long a shutdown keeps trying to send the pending batch.
const SHUTDOWN_GRACE: Duration = Duration::from_secs(5);
/// A batch closes at this many encoded bytes as well as at its record count.
const MAX_BATCH_BYTES: usize = 1 << 20;
/// The records a batch may close early: the first retry wait, doubling.
const RETRY_BASE: Duration = Duration::from_secs(1);
const RETRY_MAX: Duration = Duration::from_secs(30);
/// How often the dropped / rejected counts are summed on stderr at most.
const COUNT_REPORT: Duration = Duration::from_secs(60);
/// Events the export's watcher may have in flight, as a console's watch.
const WATCH_BUFFER: usize = 64;
/// The `service.instance.id` of the flume itself, under which a `BEHIND` gap
/// (the flume lost lines, not a node) is exported.
const FLUME_INSTANCE: &str = "sluice-flume";

/// The OTLP options as given: `--otlp-endpoint` and the raw values of the
/// others, their files read.
#[derive(Debug, Default)]
pub struct RawOtlp {
    pub endpoint: String,
    pub service_name: Option<String>,
    pub resource_attributes: Option<String>,
    pub compression: Option<String>,
    pub headers: Option<String>,
    /// The contents of `--otlp-headers-file`.
    pub headers_file: Option<String>,
    pub ca_pem: Option<Vec<u8>>,
    pub client_cert_pem: Option<Vec<u8>>,
    pub client_key_pem: Option<Vec<u8>>,
    pub batch_size: Option<String>,
    /// Milliseconds.
    pub batch_delay: Option<String>,
    pub kinds: Option<String>,
    pub route_keys: Option<String>,
    pub node_ids: Option<String>,
}

/// What the exporter sends and how.
#[derive(Debug, Clone)]
pub struct OtlpSettings {
    url: reqwest::Url,
    service_name: String,
    resource_attributes: Vec<(String, String)>,
    compression: Compression,
    headers: Vec<(HeaderName, HeaderValue)>,
    http: reqwest::Client,
    pub(crate) batch_size: usize,
    batch_delay: Duration,
    /// The standing watcher's filter: the nodes send only what it covers.
    filter: AccessFilter,
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum Compression {
    Gzip,
    None,
}

impl OtlpSettings {
    /// Parses the options; every error names the one it is about.
    pub fn new(raw: RawOtlp) -> Result<Self, String> {
        let mut url = reqwest::Url::parse(raw.endpoint.trim())
            .map_err(|e| format!("--otlp-endpoint: {e}"))?;
        if !matches!(url.scheme(), "http" | "https") {
            return Err(format!(
                "--otlp-endpoint: not http or https: {}",
                url.scheme()
            ));
        }
        if url.host().is_none() {
            return Err("--otlp-endpoint: no host".to_string());
        }
        // the base URL of OTEL_EXPORTER_OTLP_ENDPOINT: the signal's path is added
        if url.path().is_empty() || url.path() == "/" {
            url.set_path("/v1/logs");
        }
        let compression = match raw.compression.as_deref().unwrap_or("gzip") {
            "gzip" => Compression::Gzip,
            "none" => Compression::None,
            other => {
                return Err(format!("--otlp-compression: {other} (gzip or none)"));
            }
        };
        let mut headers = Vec::new();
        for pairs in [raw.headers.as_deref(), raw.headers_file.as_deref()]
            .into_iter()
            .flatten()
        {
            for pair in pairs.split(',').map(str::trim).filter(|p| !p.is_empty()) {
                let Some((name, value)) = pair.split_once('=') else {
                    return Err(format!("--otlp-headers: no '=' in {pair}"));
                };
                let name = name.trim();
                let header: HeaderName = name
                    .parse()
                    .map_err(|e| format!("--otlp-headers: {name}: {e}"))?;
                let value = percent_decode(value.trim())
                    .map_err(|e| format!("--otlp-headers: {name}: {e}"))?;
                let value: HeaderValue = value
                    .parse()
                    .map_err(|e| format!("--otlp-headers: {name}: {e}"))?;
                headers.push((header, value));
            }
        }
        let identity = match (raw.client_cert_pem, raw.client_key_pem) {
            (Some(cert), Some(key)) => Some((cert, key)),
            (None, None) => None,
            _ => return Err("--otlp-client-cert and --otlp-client-key go together".to_string()),
        };
        let tls = client_config(raw.ca_pem.as_deref(), identity.as_ref())?;
        let http = reqwest::Client::builder()
            .use_preconfigured_tls(tls)
            .build()
            .map_err(|e| format!("--otlp-endpoint: {e}"))?;
        let filter = AccessFilter {
            route_keys: list(raw.route_keys),
            kinds: list(raw.kinds),
            node_ids: list(raw.node_ids),
        };
        hub::Hub::check(&filter).map_err(|e| format!("--otlp filter: {e}"))?;
        let number = |raw: Option<&str>, default: usize, option: &str| match raw {
            Some(raw) => raw
                .trim()
                .parse::<usize>()
                .map_err(|_| format!("{option}: not a number: {raw}")),
            None => Ok(default),
        };
        Ok(OtlpSettings {
            url,
            service_name: raw
                .service_name
                .filter(|name| !name.trim().is_empty())
                .unwrap_or_else(|| "sluice".to_string()),
            resource_attributes: pairs(
                raw.resource_attributes.as_deref(),
                "--otlp-resource-attributes",
            )?,
            compression,
            headers,
            http,
            batch_size: number(raw.batch_size.as_deref(), 512, "--otlp-batch-size")?.max(1),
            batch_delay: Duration::from_millis(number(
                raw.batch_delay.as_deref(),
                1000,
                "--otlp-batch-delay",
            )? as u64),
            filter,
        })
    }

    /// Where it posts, as configured.
    pub fn endpoint(&self) -> &str {
        self.url.as_str()
    }
}

/// `k=v,k=v`, the values as given.
fn pairs(raw: Option<&str>, option: &str) -> Result<Vec<(String, String)>, String> {
    let mut pairs = Vec::new();
    for pair in raw.unwrap_or_default().split(',').map(str::trim) {
        if pair.is_empty() {
            continue;
        }
        let Some((key, value)) = pair.split_once('=') else {
            return Err(format!("{option}: no '=' in {pair}"));
        };
        pairs.push((key.trim().to_string(), value.to_string()));
    }
    Ok(pairs)
}

fn list(raw: Option<String>) -> Vec<String> {
    raw.unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
        .map(String::from)
        .collect()
}

/// `%XX` sequences decoded, as `OTEL_EXPORTER_OTLP_HEADERS` takes its values.
fn percent_decode(value: &str) -> Result<String, String> {
    let bytes = value.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            let hex = bytes
                .get(index + 1..index + 3)
                .and_then(|hex| std::str::from_utf8(hex).ok())
                .ok_or_else(|| format!("invalid percent-encoding: {value}"))?;
            let byte = u8::from_str_radix(hex, 16)
                .map_err(|_| format!("invalid percent-encoding: {value}"))?;
            decoded.push(byte);
            index += 3;
        } else {
            decoded.push(bytes[index]);
            index += 1;
        }
    }
    String::from_utf8(decoded).map_err(|_| format!("invalid percent-encoding: {value}"))
}

/// The TLS a `POST` uses: the roots of `--otlp-ca` or the webPKI ones, with the
/// client certificate of `--otlp-client-cert` when given.
fn client_config(
    ca_pem: Option<&[u8]>,
    identity: Option<&(Vec<u8>, Vec<u8>)>,
) -> Result<rustls::ClientConfig, String> {
    let mut roots = rustls::RootCertStore::empty();
    match ca_pem {
        Some(pem) => {
            for cert in CertificateDer::pem_slice_iter(pem) {
                let cert = cert.map_err(|e| format!("--otlp-ca: {e}"))?;
                roots.add(cert).map_err(|e| format!("--otlp-ca: {e}"))?;
            }
            if roots.is_empty() {
                return Err("--otlp-ca: no certificate in it".to_string());
            }
        }
        None => roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned()),
    }
    let builder = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .map_err(|e| format!("--otlp tls: {e}"))?
    .with_root_certificates(roots);
    match identity {
        Some((cert_pem, key_pem)) => {
            let certs: Vec<CertificateDer<'static>> = CertificateDer::pem_slice_iter(cert_pem)
                .collect::<Result<_, _>>()
                .map_err(|e| format!("--otlp-client-cert: {e}"))?;
            if certs.is_empty() {
                return Err("--otlp-client-cert: no certificate in it".to_string());
            }
            let key = PrivateKeyDer::from_pem_slice(key_pem)
                .map_err(|e| format!("--otlp-client-key: {e}"))?;
            builder
                .with_client_auth_cert(certs, key)
                .map_err(|e| format!("--otlp-client-cert: {e}"))
        }
        None => Ok(builder.with_no_client_auth()),
    }
}

/// The running exporter; [`Exporter::shutdown`] sends what it still holds.
pub struct Exporter {
    stop: Option<watch::Sender<bool>>,
    task: Option<tokio::task::JoinHandle<()>>,
}

impl Exporter {
    /// Stops the exporter, sending its pending batch within a few seconds, and
    /// waits for it: the flume exits after.
    pub async fn shutdown(mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(true);
        }
        if let Some(mut task) = self.task.take()
            && tokio::time::timeout(SHUTDOWN_GRACE + Duration::from_secs(1), &mut task)
                .await
                .is_err()
        {
            task.abort();
        }
    }
}

/// Starts exporting every line the hub receives; the exporter is a standing
/// watcher, so the nodes feed it whether or not a console watches too.
pub fn start(hub: Arc<Hub>, settings: OtlpSettings) -> Exporter {
    let (stop, stopped) = watch::channel(false);
    let task = tokio::spawn(async move {
        if let Err(message) = run(hub, settings, stopped).await {
            eprintln!("otlp: {message}");
        }
    });
    Exporter {
        stop: Some(stop),
        task: Some(task),
    }
}

async fn run(
    hub: Arc<Hub>,
    settings: OtlpSettings,
    stop: watch::Receiver<bool>,
) -> Result<(), String> {
    eprintln!("otlp: exporting to {}", settings.url);
    let filter = settings.filter.clone();
    let watch = hub.watch(filter, None)?;
    let (sender, mut receiver) = mpsc::channel(WATCH_BUFFER);
    // dropped when the exporter stops: the nodes nobody else watches stop feeding
    tokio::spawn(crate::service::relay(hub, watch, sender));
    Export::new(settings, stop).run(&mut receiver).await
}

/// The exporter's state across batches.
struct Export {
    settings: OtlpSettings,
    stop: watch::Receiver<bool>,
    /// When a shutdown is to have sent the pending batch by.
    stop_deadline: Option<Instant>,
    /// A failure was reported and no success since.
    failing: bool,
    dropped: u64,
    rejected: u64,
    last_report: Instant,
}

impl Export {
    fn new(settings: OtlpSettings, stop: watch::Receiver<bool>) -> Self {
        Self {
            settings,
            stop,
            stop_deadline: None,
            failing: false,
            dropped: 0,
            rejected: 0,
            // the first count reports at once
            last_report: Instant::now()
                .checked_sub(COUNT_REPORT)
                .unwrap_or_else(Instant::now),
        }
    }

    async fn run(&mut self, receiver: &mut mpsc::Receiver<WatchEvent>) -> Result<(), String> {
        let mut batch = Batch::default();
        let mut deadline: Option<Instant> = None;
        loop {
            let mut watching = self.stop.clone();
            let stopped = async move {
                if *watching.borrow() {
                    return;
                }
                let _ = watching.changed().await;
            };
            let timer = async move {
                match deadline {
                    Some(at) => tokio::time::sleep_until(at.into()).await,
                    None => std::future::pending().await,
                }
            };
            tokio::select! {
                biased;
                () = stopped => break,
                maybe = receiver.recv() => {
                    let Some(event) = maybe else {
                        return Ok(());
                    };
                    if batch.add(&event) == 0 {
                        continue;
                    }
                    if deadline.is_none() {
                        deadline = Some(Instant::now() + self.settings.batch_delay);
                    }
                    if batch.full(&self.settings) {
                        self.deliver(&batch).await;
                        batch = Batch::default();
                        deadline = None;
                    }
                }
                _ = timer => {
                    self.deliver(&batch).await;
                    batch = Batch::default();
                    deadline = None;
                }
            }
        }
        // the stop: what is pending goes out within the grace
        self.stop_deadline = Some(Instant::now() + SHUTDOWN_GRACE);
        if !batch.is_empty() {
            self.deliver(&batch).await;
        }
        Ok(())
    }

    /// When a shutdown began: no attempt starts past it.
    fn stopping(&mut self) -> Option<Instant> {
        if self.stop_deadline.is_none() && *self.stop.borrow() {
            self.stop_deadline = Some(Instant::now() + SHUTDOWN_GRACE);
        }
        self.stop_deadline
    }

    fn remaining(&mut self) -> Option<Duration> {
        self.stopping()
            .map(|deadline| deadline.saturating_duration_since(Instant::now()))
    }

    /// Sends the batch, retrying what may succeed again for as long as the
    /// ring holds the lines it may lose meanwhile.
    async fn deliver(&mut self, batch: &Batch) {
        let payload = match self.encode(batch) {
            Ok(payload) => payload,
            Err(message) => {
                eprintln!("otlp: {message}");
                return;
            }
        };
        let mut backoff = RETRY_BASE;
        let mut attempts: u64 = 0;
        loop {
            if self
                .remaining()
                .is_some_and(|remaining| remaining.is_zero())
            {
                return;
            }
            attempts += 1;
            let timeout = self
                .remaining()
                .map_or(REQUEST_TIMEOUT, |remaining| remaining.min(REQUEST_TIMEOUT));
            let outcome = match tokio::time::timeout(timeout, self.post(&payload)).await {
                Ok(outcome) => outcome,
                Err(_) => Outcome::Retry {
                    reason: format!("POST {}: no answer in {timeout:?}", self.settings.url),
                    after: None,
                },
            };
            match outcome {
                Outcome::Delivered { rejected, message } => {
                    if self.failing {
                        eprintln!("otlp: recovered after {attempts} attempts");
                        self.failing = false;
                    }
                    if rejected > 0 {
                        self.rejected += rejected;
                        if let Some(message) = message {
                            eprintln!("otlp: {message}");
                        }
                        self.report();
                    }
                    return;
                }
                Outcome::Retry { reason, after } => {
                    if !self.failing {
                        eprintln!("otlp: {reason}, retrying");
                        self.failing = true;
                    }
                    let delay = jitter(after.unwrap_or(backoff));
                    backoff = (backoff * 2).min(RETRY_MAX);
                    let delay = self.remaining().map_or(delay, |left| delay.min(left));
                    if !delay.is_zero() {
                        tokio::time::sleep(delay).await;
                    }
                }
                Outcome::Dropped { reason } => {
                    if !self.failing {
                        eprintln!("otlp: {reason}, dropping the batch");
                    }
                    self.dropped += batch.count as u64;
                    self.report();
                    return;
                }
            }
        }
    }

    /// At most one line a minute summing what the endpoint lost.
    fn report(&mut self) {
        if self.dropped == 0 && self.rejected == 0 {
            return;
        }
        if self.last_report.elapsed() < COUNT_REPORT {
            return;
        }
        eprintln!(
            "otlp: {} lines dropped, {} records rejected",
            self.dropped, self.rejected
        );
        self.dropped = 0;
        self.rejected = 0;
        self.last_report = Instant::now();
    }

    fn encode(&self, batch: &Batch) -> Result<Vec<u8>, String> {
        let request = batch.request(&self.settings);
        let encoded = request.encode_to_vec();
        if self.settings.compression == Compression::Gzip {
            let mut encoder =
                flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
            encoder
                .write_all(&encoded)
                .and_then(|()| encoder.finish())
                .map_err(|e| format!("gzip: {e}"))
        } else {
            Ok(encoded)
        }
    }

    async fn post(&self, payload: &[u8]) -> Outcome {
        let mut request = self
            .settings
            .http
            .post(self.settings.url.clone())
            .header(CONTENT_TYPE, "application/x-protobuf")
            .body(payload.to_vec());
        if self.settings.compression == Compression::Gzip {
            request = request.header(CONTENT_ENCODING, "gzip");
        }
        for (name, value) in &self.settings.headers {
            request = request.header(name, value);
        }
        let response = match request.send().await {
            Ok(response) => response,
            Err(e) => {
                return Outcome::Retry {
                    reason: format!("POST {}: {}", self.settings.url, describe(&e)),
                    after: None,
                };
            }
        };
        let status = response.status();
        if status.is_success() {
            let bytes = response.bytes().await.unwrap_or_default();
            let (rejected, message) = partial_success(&bytes);
            return Outcome::Delivered { rejected, message };
        }
        let after = retry_after(response.headers());
        let reason = format!("POST {}: {status}", self.settings.url);
        if is_retryable(status) {
            Outcome::Retry {
                reason,
                after: after.map(Duration::from_secs),
            }
        } else {
            Outcome::Dropped { reason }
        }
    }
}

/// What one `POST` came to.
enum Outcome {
    Delivered {
        rejected: u64,
        message: Option<String>,
    },
    Retry {
        reason: String,
        after: Option<Duration>,
    },
    Dropped {
        reason: String,
    },
}

/// A status worth the same batch again.
fn is_retryable(status: reqwest::StatusCode) -> bool {
    matches!(status.as_u16(), 429 | 502 | 503 | 504)
}

fn retry_after(headers: &reqwest::header::HeaderMap) -> Option<u64> {
    headers
        .get(reqwest::header::RETRY_AFTER)?
        .to_str()
        .ok()?
        .trim()
        .parse()
        .ok()
}

/// The records a 2xx response rejected, and what it said about them.
fn partial_success(body: &[u8]) -> (u64, Option<String>) {
    let Ok(response) = ExportLogsServiceResponse::decode(body) else {
        return (0, None);
    };
    match response.partial_success {
        Some(partial) => (
            partial.rejected_log_records.max(0) as u64,
            (!partial.error_message.is_empty()).then_some(partial.error_message),
        ),
        None => (0, None),
    }
}

/// The error and what it is about: reqwest's own names the request only.
fn describe(error: &reqwest::Error) -> String {
    let mut described = error.to_string();
    let mut source = std::error::Error::source(error);
    while let Some(cause) = source {
        described.push_str(": ");
        described.push_str(&cause.to_string());
        source = cause.source();
    }
    described
}

/// A retry wait widened at random, so the exporters of a fleet spread.
fn jitter(delay: Duration) -> Duration {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.subsec_nanos())
        .unwrap_or(0);
    let spread = (delay.as_millis() as u64 / 4).max(1);
    delay + Duration::from_millis(u64::from(nanos) % spread)
}

/// The records waiting for their request, under the resource they go out with.
#[derive(Default)]
struct Batch {
    /// By `service.instance.id`: the node's id, or the flume's for a gap of
    /// the flume's own ring.
    records: BTreeMap<String, Vec<LogRecord>>,
    count: usize,
    /// The records' [`LogRecord::encoded_len`]: the request adds little around
    /// them.
    encoded: usize,
}

impl Batch {
    /// Adds what the event carries; nothing for a node state.
    fn add(&mut self, event: &WatchEvent) -> usize {
        let observed = now_nanos();
        match event.body.as_ref() {
            Some(watch_event::Body::Record(record)) => {
                let Some(access) = record.event.as_ref() else {
                    return 0;
                };
                self.push(&record.node_id, access_record(access, observed));
                1
            }
            Some(watch_event::Body::Gap(gap)) => {
                let cause = watch_gap::Cause::try_from(gap.cause).unwrap_or_default();
                // the flume's ring lost them, not a node
                let instance = match cause {
                    watch_gap::Cause::Behind => FLUME_INSTANCE,
                    _ => gap.node_id.as_str(),
                };
                self.push(instance, gap_record(gap, cause, observed));
                1
            }
            _ => 0,
        }
    }

    fn push(&mut self, instance: &str, record: LogRecord) {
        self.encoded += record.encoded_len();
        self.count += 1;
        self.records
            .entry(instance.to_string())
            .or_default()
            .push(record);
    }

    fn is_empty(&self) -> bool {
        self.count == 0
    }

    fn full(&self, settings: &OtlpSettings) -> bool {
        self.count >= settings.batch_size || self.encoded >= MAX_BATCH_BYTES
    }

    fn request(&self, settings: &OtlpSettings) -> ExportLogsServiceRequest {
        ExportLogsServiceRequest {
            resource_logs: self
                .records
                .iter()
                .map(|(instance, records)| ResourceLogs {
                    resource: Some(Resource {
                        attributes: resource_attributes(settings, instance),
                        ..Default::default()
                    }),
                    scope_logs: vec![ScopeLogs {
                        scope: Some(InstrumentationScope {
                            name: SCOPE.to_string(),
                            version: env!("CARGO_PKG_VERSION").to_string(),
                            attributes: Vec::new(),
                            dropped_attributes_count: 0,
                        }),
                        log_records: records.clone(),
                        schema_url: String::new(),
                    }],
                    schema_url: String::new(),
                })
                .collect(),
        }
    }
}

/// The scope the records carry.
const SCOPE: &str = "sluice.access";

/// The resource of a node's records: `service.name`, its id, then what
/// `--otlp-resource-attributes` gives, the last of a key winning.
fn resource_attributes(settings: &OtlpSettings, instance: &str) -> Vec<KeyValue> {
    let mut attributes = vec![
        ("service.name".to_string(), settings.service_name.clone()),
        ("service.instance.id".to_string(), instance.to_string()),
    ];
    for (key, value) in &settings.resource_attributes {
        match attributes.iter_mut().find(|(name, _)| name == key) {
            Some(attribute) => attribute.1 = value.clone(),
            None => attributes.push((key.clone(), value.clone())),
        }
    }
    attributes
        .into_iter()
        .map(|(key, value)| string_attribute(key, value))
        .collect()
}

/// One access log line as a record.
fn access_record(access: &crate::proto::AccessEvent, observed: u64) -> LogRecord {
    let mut attributes = vec![string_attribute(
        "sluice.kind".to_string(),
        access.kind.clone(),
    )];
    if !access.route_key.is_empty() {
        attributes.push(string_attribute(
            "sluice.route_key".to_string(),
            access.route_key.clone(),
        ));
    }
    attributes.push(KeyValue {
        key: "sluice.node_seq".to_string(),
        value: Some(AnyValue {
            value: Some(any_value::Value::IntValue(access.node_seq as i64)),
        }),
        ..Default::default()
    });
    for field in &access.fields {
        let key = format!("sluice.{}", field.name);
        // a field named like one of the line's own keeps the line's
        if attributes.iter().any(|attribute| attribute.key == key) {
            continue;
        }
        attributes.push(string_attribute(key, field.value.clone()));
    }
    let body = match access.kind.as_str() {
        // the semconv for a process's output
        "stdout" | "stderr" => {
            attributes.push(string_attribute(
                "log.iostream".to_string(),
                access.kind.clone(),
            ));
            access
                .fields
                .iter()
                .find(|field| field.name == "line")
                .map_or_else(String::new, |field| field.value.clone())
        }
        _ => logfmt(access),
    };
    LogRecord {
        time_unix_nano: (access.time_unix_millis.max(0) as u64) * 1_000_000,
        observed_time_unix_nano: observed,
        severity_number: SeverityNumber::Info as i32,
        severity_text: "INFO".to_string(),
        body: Some(string_value(body)),
        attributes,
        event_name: format!("sluice.access.{}", access.kind),
        ..Default::default()
    }
}

/// A gap the flume reports: a node's dropped lines or its own ring's.
fn gap_record(gap: &crate::proto::WatchGap, cause: watch_gap::Cause, observed: u64) -> LogRecord {
    let cause_name = match cause {
        watch_gap::Cause::NodeDropped => "node_dropped",
        _ => "behind",
    };
    let body = match cause {
        watch_gap::Cause::NodeDropped => format!("{} dropped {} lines", gap.node_id, gap.dropped),
        _ => format!("up to {} lines missed", gap.dropped),
    };
    LogRecord {
        observed_time_unix_nano: observed,
        severity_number: SeverityNumber::Warn as i32,
        severity_text: "WARN".to_string(),
        body: Some(string_value(body)),
        attributes: vec![
            string_attribute("sluice.gap.cause".to_string(), cause_name.to_string()),
            KeyValue {
                key: "sluice.gap.dropped".to_string(),
                value: Some(AnyValue {
                    value: Some(any_value::Value::IntValue(gap.dropped as i64)),
                }),
                ..Default::default()
            },
        ],
        event_name: "sluice.gap".to_string(),
        ..Default::default()
    }
}

/// The line as the node's `AccessEvent.quoted` builds it: the wire does not
/// carry which fields it quoted, so a value is quoted when it could not stand
/// bare.
fn logfmt(access: &crate::proto::AccessEvent) -> String {
    let mut line = String::new();
    for field in &access.fields {
        if !line.is_empty() {
            line.push(' ');
        }
        line.push_str(&field.name);
        line.push('=');
        line.push_str(&logfmt_value(&field.value));
    }
    line
}

fn logfmt_value(value: &str) -> String {
    let bare = !value.is_empty()
        && !value
            .chars()
            .any(|c| c == ' ' || c == '"' || c == '\\' || c == '=' || c.is_control());
    if bare {
        return value.to_string();
    }
    let mut quoted = String::with_capacity(value.len() + 2);
    quoted.push('"');
    for c in value.chars() {
        match c {
            '"' | '\\' => {
                quoted.push('\\');
                quoted.push(c);
            }
            c if c.is_control() => quoted.push_str(&format!("\\u{:04x}", c as u32)),
            c => quoted.push(c),
        }
    }
    quoted.push('"');
    quoted
}

fn string_value(value: String) -> AnyValue {
    AnyValue {
        value: Some(any_value::Value::StringValue(value)),
    }
}

fn string_attribute(key: String, value: String) -> KeyValue {
    KeyValue {
        key,
        value: Some(string_value(value)),
        ..Default::default()
    }
}

fn now_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_nanos() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::{access_event, watch_gap};

    fn settings() -> OtlpSettings {
        OtlpSettings::new(RawOtlp {
            endpoint: "http://127.0.0.1:4318".to_string(),
            resource_attributes: Some(
                "deployment.environment.name=test,service.name=override".into(),
            ),
            ..Default::default()
        })
        .expect("the settings parse")
    }

    fn event(kind: &str, fields: &[(&str, &str)]) -> crate::proto::AccessEvent {
        crate::proto::AccessEvent {
            node_seq: 7,
            time_unix_millis: 1_700_000_000_000,
            kind: kind.to_string(),
            route_key: "a.local".to_string(),
            fields: fields
                .iter()
                .map(|(name, value)| access_event::Field {
                    name: name.to_string(),
                    value: value.to_string(),
                })
                .collect(),
        }
    }

    /// The resource, the scope, the times and the attributes use the semconv
    /// names; a gap of the flume's own ring goes under the flume's instance.
    #[test]
    fn records_carry_the_semconv_names() {
        let settings = settings();
        assert_eq!(settings.endpoint(), "http://127.0.0.1:4318/v1/logs");
        let mut batch = Batch::default();
        batch.add(&record_event(event(
            "request",
            &[("method", "GET"), ("agent", "curl 8"), ("kind", "spoofed")],
        )));
        batch.add(&WatchEvent {
            body: Some(watch_event::Body::Gap(crate::proto::WatchGap {
                seq: 0,
                node_id: String::new(),
                dropped: 12,
                cause: watch_gap::Cause::Behind as i32,
            })),
        });
        let request = batch.request(&settings);
        let node = &request.resource_logs[0];
        let resource = node.resource.as_ref().expect("a resource");
        let attribute = |attributes: &[KeyValue], key: &str| {
            attributes
                .iter()
                .find(|attribute| attribute.key == key)
                .and_then(|attribute| attribute.value.as_ref())
                .and_then(|value| value.value.clone())
        };
        // the flags' key wins over the one the resource sets
        assert_eq!(
            attribute(&resource.attributes, "service.name"),
            Some(any_value::Value::StringValue("override".into()))
        );
        assert_eq!(
            attribute(&resource.attributes, "service.instance.id"),
            Some(any_value::Value::StringValue("node-1".into()))
        );
        let scope = &node.scope_logs[0];
        assert_eq!(scope.scope.as_ref().unwrap().name, SCOPE);
        assert_eq!(
            scope.scope.as_ref().unwrap().version,
            env!("CARGO_PKG_VERSION")
        );
        let access = &scope.log_records[0];
        assert_eq!(access.time_unix_nano, 1_700_000_000_000_000_000);
        assert_eq!(access.severity_number, SeverityNumber::Info as i32);
        assert_eq!(access.severity_text, "INFO");
        assert_eq!(access.event_name, "sluice.access.request");
        assert_eq!(
            attribute(&access.attributes, "sluice.node_seq"),
            Some(any_value::Value::IntValue(7))
        );
        // the field named like the line's own does not replace it
        assert_eq!(
            attribute(&access.attributes, "sluice.kind"),
            Some(any_value::Value::StringValue("request".into()))
        );
        assert_eq!(
            attribute(&access.attributes, "sluice.agent"),
            Some(any_value::Value::StringValue("curl 8".into()))
        );
        // the body is the logfmt line, the value that needs it quoted
        assert_eq!(
            access.body.as_ref().unwrap().value,
            Some(any_value::Value::StringValue(
                "method=GET agent=\"curl 8\" kind=spoofed".into()
            ))
        );
        let flume = &request.resource_logs[1];
        let gap = &flume.scope_logs[0].log_records[0];
        assert_eq!(
            attribute(
                &flume.resource.as_ref().unwrap().attributes,
                "service.instance.id"
            ),
            Some(any_value::Value::StringValue(FLUME_INSTANCE.into()))
        );
        assert_eq!(gap.severity_number, SeverityNumber::Warn as i32);
        assert_eq!(gap.severity_text, "WARN");
        assert_eq!(gap.event_name, "sluice.gap");
        assert_eq!(
            attribute(&gap.attributes, "sluice.gap.cause"),
            Some(any_value::Value::StringValue("behind".into()))
        );
        assert_eq!(
            attribute(&gap.attributes, "sluice.gap.dropped"),
            Some(any_value::Value::IntValue(12))
        );
        assert_eq!(
            gap.body.as_ref().unwrap().value,
            Some(any_value::Value::StringValue(
                "up to 12 lines missed".into()
            ))
        );
    }

    fn record_event(access: crate::proto::AccessEvent) -> WatchEvent {
        WatchEvent {
            body: Some(watch_event::Body::Record(crate::proto::WatchRecord {
                seq: 1,
                node_id: "node-1".to_string(),
                event: Some(access),
            })),
        }
    }
}
