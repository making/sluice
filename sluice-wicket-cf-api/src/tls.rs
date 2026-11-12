//! The roots the store's and the bucket's TLS is verified against ([`client_config`]), and TLS
//! for the flume connection (`grpcs://`), as the wicket's: verification
//! against a pinned CA (`--flume-ca-cert`), against the webPKI roots
//! (default), or no verification at all (`--flume-insecure`, for self-signed
//! setups).

use std::sync::Arc;

use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{DigitallySignedStruct, SignatureScheme};
use tonic::transport::{Certificate, ClientTlsConfig, Endpoint};

/// A client's TLS verified against the PEM CA `ca_pem` (the option `flag`'s), else against the
/// webPKI roots.
pub fn client_config(ca_pem: Option<&[u8]>, flag: &str) -> Result<rustls::ClientConfig, String> {
    let mut roots = rustls::RootCertStore::empty();
    match ca_pem {
        Some(pem) => {
            use rustls::pki_types::pem::PemObject as _;
            for cert in CertificateDer::pem_slice_iter(pem) {
                let cert = cert.map_err(|e| format!("{flag}: {e}"))?;
                (roots.add(cert)).map_err(|e| format!("{flag}: {e}"))?;
            }
            if roots.is_empty() {
                return Err(format!("{flag}: no certificate in it"));
            }
        }
        None => roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned()),
    }
    let tls = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .map_err(|e| format!("{flag}: tls: {e}"))?
    .with_root_certificates(roots)
    .with_no_client_auth();
    Ok(tls)
}

/// Wraps `endpoint` in the TLS mode implied by `--flume-insecure` /
/// `--flume-ca-cert`.
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

/// Accepts any certificate chain and any signature; the `--flume-insecure`
/// mode.
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
