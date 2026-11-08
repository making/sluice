//! An Open Service Broker API broker for the test: the offering `db`, its plans `small` and
//! `large`. Each request needs its basic authentication and an API version. An operation is
//! asynchronous when its parameters say `"async": true` -- a deprovision or an unbind when
//! what it deletes was made so -- and its last operation is in progress for one poll (as
//! long as [`Broker::hold`] holds), then succeeded, or failed with the parameters'
//! `fail_async`; `fail` refuses it at once (400). A binding's credentials name it. Plain HTTP
//! on the loopback, one request per connection, on a thread of its own.

use std::collections::HashMap;
use std::io::{Read as _, Write as _};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use base64::Engine as _;
use serde_json::{Value, json};

pub const SERVICE: &str = "db-id";

pub struct Broker {
    /// `http://127.0.0.1:<port>`.
    pub url: String,
    content: Arc<Mutex<Content>>,
    stop: Arc<AtomicBool>,
}

/// What the broker has made, an instance or a binding.
#[derive(Clone, Debug)]
pub struct Made {
    pub plan: String,
    pub parameters: Value,
    /// Its operations are asynchronous.
    asynchronous: bool,
    /// What its last operation is: in progress, and how it ends.
    pending: Option<Pending>,
}

#[derive(Clone, Debug)]
struct Pending {
    /// Polls answered in progress before it ends.
    polls: u32,
    /// Its failure's description.
    fails: Option<String>,
    /// It deletes.
    deletes: bool,
}

struct Content {
    /// `user:password`, base64.
    authorization: String,
    plans: Vec<&'static str>,
    instances: HashMap<String, Made>,
    bindings: HashMap<String, Made>,
    /// Last operations answer in progress meanwhile.
    held: bool,
    /// Each request's method and path, and the X-Broker-API-Originating-Identity header.
    requests: Vec<(String, Option<String>)>,
    /// The last provision's body.
    provisioned: Value,
}

impl Broker {
    pub fn start(user: &str, password: &str) -> Broker {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let content = Arc::new(Mutex::new(Content {
            authorization: base64::engine::general_purpose::STANDARD
                .encode(format!("{user}:{password}")),
            plans: vec!["small", "large"],
            instances: HashMap::new(),
            bindings: HashMap::new(),
            held: false,
            requests: Vec::new(),
            provisioned: Value::Null,
        }));
        let stop = Arc::new(AtomicBool::new(false));
        let (serving, stopping) = (content.clone(), stop.clone());
        std::thread::spawn(move || {
            while !stopping.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((stream, _)) => {
                        stream.set_nonblocking(false).unwrap();
                        serve(&serving, stream);
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(10));
                    }
                    Err(e) => panic!("broker accept: {e}"),
                }
            }
        });
        Broker { url, content, stop }
    }

    /// Keeps the last operations in progress (`true`) or lets them end.
    pub fn hold(&self, held: bool) {
        self.content.lock().unwrap().held = held;
    }

    /// The plans its catalog offers.
    pub fn offer(&self, plans: &[&'static str]) {
        self.content.lock().unwrap().plans = plans.to_vec();
    }

    pub fn instances(&self) -> HashMap<String, Made> {
        self.content.lock().unwrap().instances.clone()
    }

    pub fn bindings(&self) -> HashMap<String, Made> {
        self.content.lock().unwrap().bindings.clone()
    }

    /// The requests so far: method and path, and the originating identity.
    pub fn requests(&self) -> Vec<(String, Option<String>)> {
        self.content.lock().unwrap().requests.clone()
    }

    /// The body of the last provision.
    pub fn provisioned(&self) -> Value {
        self.content.lock().unwrap().provisioned.clone()
    }
}

impl Drop for Broker {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
    }
}

fn serve(content: &Mutex<Content>, mut stream: TcpStream) {
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    let mut head = Vec::new();
    let mut byte = [0u8; 1];
    while !head.ends_with(b"\r\n\r\n") {
        if stream.read(&mut byte).unwrap_or(0) == 0 {
            return;
        }
        head.push(byte[0]);
    }
    let head = String::from_utf8_lossy(&head).to_string();
    let mut lines = head.lines();
    let request = lines.next().unwrap_or_default().to_string();
    let headers: HashMap<String, String> = lines
        .filter_map(|line| line.split_once(':'))
        .map(|(name, value)| (name.to_ascii_lowercase(), value.trim().to_string()))
        .collect();
    let length: usize = headers
        .get("content-length")
        .and_then(|length| length.parse().ok())
        .unwrap_or(0);
    let mut body = vec![0u8; length];
    stream.read_exact(&mut body).unwrap();
    let body: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
    let mut parts = request.split(' ');
    let (method, target) = (parts.next().unwrap(), parts.next().unwrap());
    let (path, query) = target.split_once('?').unwrap_or((target, ""));
    let query: HashMap<String, String> = form_urlencoded::parse(query.as_bytes())
        .into_owned()
        .collect();
    let (status, answer) = content
        .lock()
        .unwrap()
        .respond(method, path, &query, &headers, &body);
    let answer = answer.to_string();
    let retry = if path.ends_with("/last_operation") {
        "Retry-After: 1\r\n"
    } else {
        ""
    };
    let response = format!(
        "HTTP/1.1 {status} {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n{retry}Connection: close\r\n\r\n{answer}",
        if status < 400 { "OK" } else { "Error" },
        answer.len()
    );
    let _ = stream.write_all(response.as_bytes());
}

fn refused(status: u16, error: &str, description: &str) -> (u16, Value) {
    (status, json!({"error": error, "description": description}))
}

/// The operation `parameters` ask for: its pending state, if asynchronous; the failure to
/// answer at once.
fn asked(parameters: &Value, deletes: bool) -> Result<Option<Pending>, (u16, Value)> {
    if let Some(fail) = parameters["fail"].as_str() {
        return Err(refused(400, "BadRequest", fail));
    }
    Ok((parameters["async"] == true).then(|| Pending {
        polls: 1,
        fails: parameters["fail_async"].as_str().map(String::from),
        deletes,
    }))
}

impl Content {
    fn catalog(&self) -> Value {
        let plans: Vec<Value> = self
            .plans
            .iter()
            .map(|name| {
                let mut plan = json!({
                    "id": format!("{name}-id"), "name": name,
                    "description": format!("a {name} database"),
                    "free": *name == "small",
                    "metadata": {"costs": [{"amount": {"usd": 9.5}, "unit": "MONTHLY"}]},
                });
                if *name == "small" {
                    plan["maintenance_info"] = json!({"version": "1.0.0", "description": "first"});
                }
                plan
            })
            .collect();
        json!({"services": [{
            "id": SERVICE, "name": "db", "description": "a database for the test",
            "bindable": true, "plan_updateable": true, "tags": ["sql"],
            "instances_retrievable": true, "bindings_retrievable": true,
            "metadata": {"documentationUrl": "https://db.example.com/docs"},
            "plans": plans,
        }]})
    }

    fn respond(
        &mut self,
        method: &str,
        path: &str,
        query: &HashMap<String, String>,
        headers: &HashMap<String, String>,
        body: &Value,
    ) -> (u16, Value) {
        let identity = headers.get("x-broker-api-originating-identity").cloned();
        self.requests.push((format!("{method} {path}"), identity));
        if headers.get("authorization") != Some(&format!("Basic {}", self.authorization)) {
            return refused(401, "Unauthorized", "bad credentials");
        }
        if headers.get("x-broker-api-version").is_none() {
            return refused(412, "PreconditionFailed", "no X-Broker-API-Version");
        }
        let segments: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
        let incomplete = query.get("accepts_incomplete").map(String::as_str) == Some("true");
        match (method, segments.as_slice()) {
            ("GET", ["v2", "catalog"]) => (200, self.catalog()),
            ("PUT", ["v2", "service_instances", id]) => {
                if body["context"]["platform"] != "cloudfoundry" || !body["space_guid"].is_string()
                {
                    return refused(400, "BadRequest", "no context");
                }
                let pending = match asked(&body["parameters"], false) {
                    Ok(pending) => pending,
                    Err(refusal) => return refusal,
                };
                if pending.is_some() && !incomplete {
                    return refused(422, "AsyncRequired", "this plan needs accepts_incomplete");
                }
                self.provisioned = body.clone();
                let asynchronous = pending.is_some();
                self.instances.insert(
                    id.to_string(),
                    Made {
                        plan: body["plan_id"].as_str().unwrap_or_default().into(),
                        parameters: body["parameters"].clone(),
                        asynchronous,
                        pending,
                    },
                );
                match asynchronous {
                    true => (
                        202,
                        json!({"operation": format!("provision-{id}"), "dashboard_url": format!("https://db.example.com/{id}")}),
                    ),
                    false => (
                        201,
                        json!({"dashboard_url": format!("https://db.example.com/{id}")}),
                    ),
                }
            }
            ("PATCH", ["v2", "service_instances", id]) => {
                let pending = match asked(&body["parameters"], false) {
                    Ok(pending) => pending,
                    Err(refusal) => return refusal,
                };
                let Some(instance) = self.instances.get_mut(*id) else {
                    return refused(404, "NotFound", "no such instance");
                };
                instance.plan = body["plan_id"].as_str().unwrap_or_default().into();
                if !body["parameters"].is_null() {
                    instance.parameters = body["parameters"].clone();
                }
                let asynchronous = pending.is_some();
                instance.pending = pending;
                match asynchronous {
                    true => (202, json!({"operation": format!("update-{id}")})),
                    false => (200, json!({})),
                }
            }
            ("DELETE", ["v2", "service_instances", id]) => match self.instances.get_mut(*id) {
                None => (410, json!({})),
                Some(instance) if instance.asynchronous => {
                    instance.pending = Some(Pending {
                        polls: 1,
                        fails: None,
                        deletes: true,
                    });
                    (202, json!({"operation": format!("deprovision-{id}")}))
                }
                Some(_) => {
                    self.instances.remove(*id);
                    (200, json!({}))
                }
            },
            ("GET", ["v2", "service_instances", id]) => match self.instances.get(*id) {
                Some(instance) => (
                    200,
                    json!({"service_id": SERVICE, "plan_id": instance.plan, "parameters": instance.parameters}),
                ),
                None => refused(404, "NotFound", "no such instance"),
            },
            ("GET", ["v2", "service_instances", id, "last_operation"]) => {
                let held = self.held;
                Self::poll(&mut self.instances, id, held)
            }
            ("PUT", ["v2", "service_instances", id, "service_bindings", binding]) => {
                if body["bind_resource"]["app_guid"] != body["app_guid"] {
                    return refused(400, "BadRequest", "no bind_resource");
                }
                if !self.instances.contains_key(*id) {
                    return refused(400, "BadRequest", "no such instance");
                }
                let pending = match asked(&body["parameters"], false) {
                    Ok(pending) => pending,
                    Err(refusal) => return refusal,
                };
                let asynchronous = pending.is_some();
                self.bindings.insert(
                    binding.to_string(),
                    Made {
                        plan: body["plan_id"].as_str().unwrap_or_default().into(),
                        parameters: body["parameters"].clone(),
                        asynchronous,
                        pending,
                    },
                );
                match asynchronous {
                    true => (202, json!({"operation": format!("bind-{binding}")})),
                    false => (201, json!({"credentials": credentials(binding)})),
                }
            }
            ("GET", ["v2", "service_instances", _, "service_bindings", binding]) => {
                match self.bindings.get(*binding) {
                    Some(made) => (
                        200,
                        json!({"credentials": credentials(binding), "parameters": made.parameters}),
                    ),
                    None => refused(404, "NotFound", "no such binding"),
                }
            }
            ("DELETE", ["v2", "service_instances", _, "service_bindings", binding]) => {
                match self.bindings.get_mut(*binding) {
                    None => (410, json!({})),
                    Some(made) if made.asynchronous => {
                        made.pending = Some(Pending {
                            polls: 1,
                            fails: None,
                            deletes: true,
                        });
                        (202, json!({"operation": format!("unbind-{binding}")}))
                    }
                    Some(_) => {
                        self.bindings.remove(*binding);
                        (200, json!({}))
                    }
                }
            }
            (
                "GET",
                [
                    "v2",
                    "service_instances",
                    _,
                    "service_bindings",
                    binding,
                    "last_operation",
                ],
            ) => {
                let held = self.held;
                Self::poll(&mut self.bindings, binding, held)
            }
            _ => refused(404, "NotFound", &format!("{method} {path}")),
        }
    }

    /// A last operation: in progress for its polls (and while held), then as it ends; a
    /// deletion's end is 410.
    fn poll(made: &mut HashMap<String, Made>, id: &str, held: bool) -> (u16, Value) {
        let Some(found) = made.get_mut(id) else {
            return (410, json!({}));
        };
        let Some(pending) = found.pending.as_mut() else {
            return (200, json!({"state": "succeeded"}));
        };
        if held || pending.polls > 0 {
            pending.polls = pending.polls.saturating_sub(1);
            return (
                200,
                json!({"state": "in progress", "description": "working on it"}),
            );
        }
        let pending = found.pending.take().expect("pending");
        if let Some(fails) = pending.fails {
            made.remove(id);
            return (200, json!({"state": "failed", "description": fails}));
        }
        if pending.deletes {
            made.remove(id);
            return (410, json!({}));
        }
        (200, json!({"state": "succeeded", "description": "done"}))
    }
}

fn credentials(binding: &str) -> Value {
    json!({"uri": format!("db://db.example.com/{binding}"), "password": "s3cr3t-db"})
}
