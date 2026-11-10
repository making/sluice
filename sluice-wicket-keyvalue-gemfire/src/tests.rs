use std::time::{Duration, Instant};

use super::*;
use crate::mock::{Cluster, MockTls, Options};

/// Hang guard of a wait.
const HANG: Duration = Duration::from_secs(30);

fn settings(endpoints: Endpoints, region: &str, timeout: Duration) -> Settings {
    Settings {
        label: "keyvalue store 'test'".to_string(),
        endpoints,
        region: region.to_string(),
        tls: None,
        token: None,
        timeout,
    }
}

/// Every key of `namespace`, a page of `page` keys at a time, and the pages.
async fn listed(store: &Store, namespace: &str, page: usize) -> (Vec<String>, usize) {
    let (mut keys, mut pages, mut cursor) = (Vec::new(), 0, None);
    loop {
        let (found, next) = store.list_keys(namespace, cursor, page).await.unwrap();
        keys.extend(found);
        pages += 1;
        cursor = next;
        if cursor.is_none() {
            return (keys, pages);
        }
    }
}

/// The namespaces stay apart in the region (a `:` or `\` in one reaches no other's keys,
/// listed or purged); a swap writes only over the value expected; list-keys pages in key
/// order through a key set streamed in batches; a batch reads, writes and deletes them all. A
/// value of another type is no value of the store's, swapped over neither; a region not there
/// fails the calls. A call timing out has the next one open a new connection.
#[tokio::test]
async fn a_store_keeps_its_namespaces_apart_in_its_region() {
    let cluster = Cluster::start(Options {
        batch: 7,
        ..Options::default()
    })
    .await;
    let servers = Endpoints::Servers(cluster.servers());
    let store = Store::open(settings(servers.clone(), "sluice", DEFAULT_TIMEOUT)).unwrap();

    for (namespace, key, value) in [("a", "b:c", "1"), ("a:b", "c", "2"), (r"a\", "k", "3")] {
        store.set(namespace, key, value.into()).await.unwrap();
    }
    assert_eq!(cluster.keys("sluice"), ["a:b:c", r"a\:b:c", r"a\\:k"]);
    assert_eq!(store.get("a:b", "c").await, Ok(Some(b"2".to_vec())));
    assert_eq!(store.get("a", "b:c").await, Ok(Some(b"1".to_vec())));
    assert_eq!(store.get("b", "c").await, Ok(None));
    for (namespace, key) in [("a", "b:c"), ("a:b", "c"), (r"a\", "k")] {
        let listed = listed(&store, namespace, 100).await;
        assert_eq!(listed, (vec![key.to_string()], 1), "{namespace}");
    }
    assert_eq!(store.purge(r"a\").await, Ok(1));
    assert_eq!(store.purge(r"a\").await, Ok(0));
    assert_eq!(
        cluster.keys("sluice"),
        ["a:b:c", r"a\:b:c"],
        "no other namespace's"
    );

    assert_eq!(
        store.swap("s", "k", None, b"1".into()).await,
        Ok(Swap::Done)
    );
    assert_eq!(
        store.swap("s", "k", None, b"2".into()).await,
        Ok(Swap::Stale(Some(b"1".into())))
    );
    assert_eq!(
        store.swap("s", "k", Some(b"1".into()), b"2".into()).await,
        Ok(Swap::Done)
    );
    assert_eq!(
        store.swap("s", "k", Some(b"1".into()), b"3".into()).await,
        Ok(Swap::Stale(Some(b"2".into())))
    );
    assert_eq!(store.exists("s", "k").await, Ok(true));
    store.delete("s", "k").await.unwrap();
    assert_eq!(store.exists("s", "k").await, Ok(false));
    assert_eq!(
        store.swap("s", "k", Some(b"2".into()), b"3".into()).await,
        Ok(Swap::Stale(None))
    );

    let pairs: Vec<_> = (0..250)
        .map(|n| (format!("key-{n:03}"), n.to_string().into_bytes()))
        .collect();
    store.set_many("big", pairs.clone()).await.unwrap();
    let keys: Vec<String> = pairs.iter().map(|(key, _)| key.clone()).collect();
    assert_eq!(listed(&store, "big", 100).await, (keys.clone(), 3));
    let asked = vec!["key-007".into(), "none".into(), "key-007".into()];
    assert_eq!(
        store.get_many("big", asked).await,
        Ok(vec![
            ("key-007".into(), Some(b"7".into())),
            ("none".into(), None),
            ("key-007".into(), Some(b"7".into())),
        ])
    );
    store.delete_many("big", keys).await.unwrap();
    assert_eq!(listed(&store, "big", 100).await, (Vec::new(), 1));
    assert_eq!(store.get_many("big", Vec::new()).await, Ok(Vec::new()));
    assert_eq!(store.set_many("big", Vec::new()).await, Ok(()));
    assert_eq!(store.delete_many("big", Vec::new()).await, Ok(()));

    let string = ValueValue::String("1".into());
    cluster.put(
        "sluice",
        "f:k",
        Value {
            value_value: Some(string),
        },
    );
    let foreign = Error("'k' holds a string value, not bytes: another client's".into());
    assert_eq!(store.get("f", "k").await, Err(foreign.clone()));
    assert_eq!(
        store.swap("f", "k", Some(b"1".into()), b"2".into()).await,
        Err(foreign)
    );
    let nowhere = Store::open(settings(servers.clone(), "nowhere", DEFAULT_TIMEOUT)).unwrap();
    let missing = nowhere.get("a", "b").await.unwrap_err();
    assert!(
        missing
            .0
            .ends_with("FailedPrecondition: Region not found: nowhere"),
        "{missing}"
    );

    let timeout = Duration::from_millis(300);
    let impatient = Store::open(settings(servers, "sluice", timeout)).unwrap();
    assert_eq!(impatient.get("a:b", "c").await, Ok(Some(b"2".to_vec())));
    let connections = cluster.connections(0);
    cluster.pause(0, true);
    let server = &cluster.servers()[0];
    assert_eq!(
        impatient.get("a:b", "c").await,
        Err(Error(format!("{server}: no answer within 300ms")))
    );
    cluster.pause(0, false);
    assert_eq!(impatient.get("a:b", "c").await, Ok(Some(b"2".to_vec())));
    assert_eq!(cluster.connections(0), connections + 1, "a new connection");
}

/// A CA, the servers' certificate (for `name` alone) and a client's, PEM each.
struct Pki {
    ca: String,
    cert: String,
    key: String,
    client_cert: String,
    client_key: String,
}

impl Pki {
    fn new(name: &str) -> Pki {
        let ca_key = rcgen::KeyPair::generate().unwrap();
        let mut ca = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
        ca.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        let ca_pem = ca.self_signed(&ca_key).unwrap().pem();
        let issuer = rcgen::Issuer::new(ca, ca_key);
        let issue = |name: &str| {
            let key = rcgen::KeyPair::generate().unwrap();
            let params = rcgen::CertificateParams::new(vec![name.to_string()]).unwrap();
            let cert = params.signed_by(&key, &issuer).unwrap();
            (cert.pem(), key.serialize_pem())
        };
        let ((cert, key), (client_cert, client_key)) = (issue(name), issue("client"));
        Pki {
            ca: ca_pem,
            cert,
            key,
            client_cert,
            client_key,
        }
    }

    fn tls(&self, client: bool, server_name: Option<&str>) -> Tls {
        Tls {
            ca_pem: Some(self.ca.clone().into_bytes()),
            client_pem: client.then(|| {
                let cert = self.client_cert.clone().into_bytes();
                (cert, self.client_key.clone().into_bytes())
            }),
            server_name: server_name.map(String::from),
        }
    }
}

/// Waits until the locator's answers acknowledged pass `acks`.
async fn acknowledged(cluster: &Cluster, acks: u64) {
    let deadline = Instant::now() + HANG;
    while cluster.acks().0 <= acks {
        assert!(Instant::now() < deadline, "never acknowledged");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Over mTLS, the servers' certificate for a name of theirs (not their address), with a
/// token: the locators (the first refusing) name the servers, the calls go round them, and
/// follow the locator's answer as it changes (one naming none keeps them). A server stopped,
/// the calls fail over, a swap too when its request never left. A wrong token, no client
/// certificate, the address as the name: each fails the calls.
#[tokio::test]
async fn the_locators_name_the_servers_the_calls_go_round() {
    let pki = Pki::new("gemfire.test");
    let cluster = Cluster::start(Options {
        servers: 2,
        tls: Some(MockTls {
            cert_pem: pki.cert.clone().into_bytes(),
            key_pem: pki.key.clone().into_bytes(),
            client_ca_pem: Some(pki.ca.clone().into_bytes()),
        }),
        token: Some("secret".into()),
        ..Options::default()
    })
    .await;
    let secured = |endpoints, tls, token: &str| Settings {
        tls: Some(tls),
        token: Some(token.to_string()),
        ..settings(endpoints, "sluice", DEFAULT_TIMEOUT)
    };
    let locators = Endpoints::Locators(vec!["127.0.0.1:1".into(), cluster.locator()]);
    let tls = pki.tls(true, Some("gemfire.test"));
    let store = Store::open(secured(locators, tls, "secret")).unwrap();

    store.set("n", "k", b"v".into()).await.unwrap();
    for _ in 0..10 {
        assert_eq!(store.get("n", "k").await, Ok(Some(b"v".to_vec())));
    }
    assert!(cluster.calls(0) > 0 && cluster.calls(1) > 0, "round both");
    for advertised in [&[1][..], &[]] {
        let acks = cluster.acks().0;
        cluster.advertise(advertised);
        acknowledged(&cluster, acks).await;
        let calls = cluster.calls(0);
        for _ in 0..10 {
            assert_eq!(store.get("n", "k").await, Ok(Some(b"v".to_vec())));
        }
        assert_eq!(cluster.calls(0), calls, "{advertised:?}: the second alone");
    }
    assert_eq!(cluster.acks().1, 0, "none refused");
    let acks = cluster.acks().0;
    cluster.advertise(&[0, 1]);
    acknowledged(&cluster, acks).await;

    cluster.stop(0).await;
    for _ in 0..10 {
        assert_eq!(store.get("n", "k").await, Ok(Some(b"v".to_vec())));
    }
    let servers = Endpoints::Servers(cluster.servers());
    let tls = pki.tls(true, Some("gemfire.test"));
    let fresh = Store::open(secured(servers.clone(), tls, "secret")).unwrap();
    assert_eq!(
        fresh.swap("n", "s", None, b"1".into()).await,
        Ok(Swap::Done),
        "the first server tried refused it"
    );
    cluster.restart(0).await;

    let failing = [
        (
            pki.tls(true, Some("gemfire.test")),
            "wrong",
            "Unauthenticated",
        ),
        (
            pki.tls(false, Some("gemfire.test")),
            "secret",
            "CertificateRequired",
        ),
        (
            pki.tls(true, None),
            "secret",
            r#"certificate not valid for name "127.0.0.1""#,
        ),
    ];
    for (tls, token, why) in failing {
        let store = Store::open(secured(servers.clone(), tls, token)).unwrap();
        let failed = store.get("n", "k").await.unwrap_err();
        assert!(failed.0.contains(why), "{failed}");
    }
}
