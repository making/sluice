//! TLS for the control-plane connection (`grpcs://`).
//!
//! Three modes, mirroring the Java client's `NodeConnection#buildChannel`:
//! verification against a pinned CA (`--ca-cert`), verification against the
//! webPKI roots (default), or no verification at all (`--insecure`, for
//! self-signed setups).

use std::sync::Arc;

use rustls::client::danger::{
    HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier,
};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{DigitallySignedStruct, SignatureScheme};
use tonic::transport::{Certificate, ClientTlsConfig, Endpoint};

/// Wraps `endpoint` in the TLS mode implied by `--insecure` / `--ca-cert`.
pub fn configure(
    endpoint: Endpoint,
    insecure: bool,
    ca_pem: Option<&[u8]>,
) -> Result<Endpoint, tonic::transport::Error> {
    if insecure {
        let verifier: Arc<dyn ServerCertVerifier> = Arc::new(NoVerifier);
        return endpoint.tls_config_with_verifier(ClientTlsConfig::new(), verifier);
    }
    let tls = match ca_pem {
        Some(pem) => ClientTlsConfig::new().ca_certificate(Certificate::from_pem(pem)),
        // no CA pinned: fall back to the public roots for real certificates
        None => ClientTlsConfig::new().with_enabled_roots(),
    };
    endpoint.tls_config(tls)
}

/// Accepts any certificate chain and any signature; the `--insecure` mode.
#[derive(Debug)]
struct NoVerifier;

impl ServerCertVerifier for NoVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        rustls::crypto::ring::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}
