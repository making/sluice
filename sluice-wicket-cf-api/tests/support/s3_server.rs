//! An S3 bucket for the tests of the blobs: one a test binary shares, each test with a prefix
//! of its own. `SLUICE_CF_API_TEST_S3` names one there (`https://id:key@host:port/bucket`, with
//! `SLUICE_CF_API_TEST_S3_CA` the file of the PEM CA verifying it, if not the webPKI roots);
//! else a container of `versity/versitygw` over a directory (Docker on the PATH; CI's runner
//! has it), taking TLS with a certificate made here, which ends with the test process: its
//! stdin's end stops the gateway.

use std::process::{Command, Stdio};
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use crate::blobs::BlobSettings;

const IMAGE: &str = "versity/versitygw:v1.10.0";

/// Hang guard of a start, the image pulled.
const START: Duration = Duration::from_secs(120);

fn server() -> &'static BlobSettings {
    static SERVER: OnceLock<BlobSettings> = OnceLock::new();
    SERVER.get_or_init(|| match std::env::var("SLUICE_CF_API_TEST_S3") {
        Ok(url) => {
            let named = || {
                let (scheme, rest) = url.split_once("://")?;
                let (keys, rest) = rest.split_once('@')?;
                let (id, key) = keys.split_once(':')?;
                let (host, bucket) = rest.split_once('/')?;
                Some((format!("{scheme}://{host}"), id, key, bucket))
            };
            let (endpoint, id, key, bucket) = named().expect("https://id:key@host:port/bucket");
            let ca = std::env::var("SLUICE_CF_API_TEST_S3_CA").ok();
            BlobSettings {
                bucket: bucket.to_string(),
                prefix: String::new(),
                endpoint: Some(endpoint),
                region: "us-east-1".into(),
                access_key_id: id.to_string(),
                secret_access_key: key.to_string(),
                ca_pem: ca.map(|file| std::fs::read(file).expect("the CA's file")),
            }
        }
        Err(_) => container(),
    })
}

fn container() -> BlobSettings {
    let name = format!("sluice-cf-api-test-s3-{}", std::process::id());
    let tls = rcgen::generate_simple_self_signed(vec!["127.0.0.1".to_string()]).unwrap();
    let (cert, key) = (tls.cert.pem(), tls.signing_key.serialize_pem());
    // a bucket of the gateway is a directory of its root
    let run = "printf %s \"$TLS_CERT\" >/tls.crt; printf %s \"$TLS_KEY\" >/tls.key; \
               mkdir -p /data/blobs; \
               versitygw --cert /tls.crt --key /tls.key posix /data & \
               cat >/dev/null; kill $!; wait";
    let child = Command::new("docker")
        .args(["run", "--rm", "-i", "--name", &name, "--entrypoint", "sh"])
        .args([
            "-e",
            "ROOT_ACCESS_KEY=test",
            "-e",
            "ROOT_SECRET_KEY=test-secret",
        ])
        .args(["-p", "127.0.0.1::7070"])
        .args(["-e", &format!("TLS_CERT={cert}"), "-e"])
        .arg(format!("TLS_KEY={key}"))
        .args([IMAGE, "-c", run])
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("docker on the PATH");
    // held open until the process ends
    std::mem::forget(child);
    let began = Instant::now();
    loop {
        assert!(began.elapsed() < START, "{IMAGE} did not start");
        let port = Command::new("docker")
            .args(["port", &name, "7070"])
            .output();
        let port = String::from_utf8(port.expect("docker on the PATH").stdout).unwrap();
        let at = port.lines().next().and_then(|at| at.rsplit_once(':'));
        if let Some((_, port)) = at
            && std::net::TcpStream::connect(format!("127.0.0.1:{port}")).is_ok()
        {
            return BlobSettings {
                bucket: "blobs".into(),
                prefix: String::new(),
                endpoint: Some(format!("https://127.0.0.1:{port}")),
                region: "us-east-1".into(),
                access_key_id: "test".into(),
                secret_access_key: "test-secret".into(),
                ca_pem: Some(cert.into_bytes()),
            };
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// The bucket, under a prefix of its own, once it answers.
pub async fn bucket() -> BlobSettings {
    let bucket = BlobSettings {
        prefix: format!("t{}/", uuid::Uuid::new_v4()),
        ..server().clone()
    };
    let s3 = crate::s3::S3::new(&bucket).expect("the bucket's settings");
    let began = Instant::now();
    while let Err(e) = s3.list().await {
        assert!(began.elapsed() < START, "{e}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    bucket
}
