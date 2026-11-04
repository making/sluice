//! A plugin for the host's tests: the request's path picks what it does.

use std::cell::RefCell;
use std::hint::black_box;
use std::time::Duration;

use proxy_wasm::hostcalls;
use proxy_wasm::traits::{Context, HttpContext, RootContext};
use proxy_wasm::types::{Action, ContextType, LogLevel, MetricType};

proxy_wasm::main! {{
    proxy_wasm::set_log_level(LogLevel::Info);
    proxy_wasm::set_root_context(|_| -> Box<dyn RootContext> { Box::new(Root) });
}}

#[link(wasm_import_module = "env")]
unsafe extern "C" {
    #[link_name = "proxy_set_header_map_pairs"]
    fn set_header_map_pairs_raw(map_type: u32, data: *const u8, size: usize) -> u32;
}

thread_local! {
    static CONFIGURATION: RefCell<String> = const { RefCell::new(String::new()) };
}

struct Root;

impl Context for Root {}

impl RootContext for Root {
    fn on_configure(&mut self, _: usize) -> bool {
        let configuration = self.get_plugin_configuration().unwrap_or_default();
        let text = String::from_utf8(configuration).unwrap();
        let accepted = text != "refuse";
        CONFIGURATION.with(|held| *held.borrow_mut() = text);
        accepted
    }

    fn get_type(&self) -> Option<ContextType> {
        Some(ContextType::HttpContext)
    }

    fn create_http_context(&self, _: u32) -> Option<Box<dyn HttpContext>> {
        Some(Box::new(Stream::default()))
    }
}

#[derive(Default)]
struct Stream {
    path: String,
}

fn spin() -> ! {
    loop {
        black_box(0);
    }
}

fn hog() -> ! {
    let mut held: Vec<Vec<u8>> = Vec::new();
    loop {
        held.push(vec![1; 1024 * 1024]);
        black_box(&held);
    }
}

fn count(name: &str, value: usize) {
    let id = hostcalls::define_metric(MetricType::Counter, name).unwrap();
    hostcalls::increment_metric(id, value as i64).unwrap();
}

impl Context for Stream {}

impl HttpContext for Stream {
    fn on_http_request_headers(&mut self, _: usize, end_of_stream: bool) -> Action {
        self.path = self.get_http_request_header(":path").unwrap_or_default();
        match self.path.as_str() {
            "/spin-request" => spin(),
            "/trap-request" => panic!(),
            "/host" => self.set_http_request_header("host", Some("elsewhere")),
            "/request-edit" => self.set_http_request_header("x-plugin", Some("request")),
            "/request-end" => {
                self.set_http_request_header("x-end", Some(if end_of_stream { "yes" } else { "no" }))
            }
            "/request-pairs" => {
                let mut pairs = self.get_http_request_headers();
                pairs.retain(|(name, _)| name != "x-gone");
                pairs.push(("x-pairs".to_owned(), "set".to_owned()));
                let pairs: Vec<(&str, &str)> = pairs.iter().map(|(n, v)| (n.as_str(), v.as_str())).collect();
                self.set_http_request_headers(pairs);
            }
            "/local" => {
                self.send_http_response(403, vec![("x-plugin", "local"), ("content-length", "1")], Some(b"denied\n"));
                return Action::Pause;
            }
            "/request-replace" | "/request-stream" | "/request-then-trap" => {
                self.set_http_request_header("content-length", None)
            }
            "/stdout" => println!("to stdout"),
            "/pairs-malformed" => {
                // a count no bytes follow
                let data = [0xff_u8, 0xff, 0xff, 0x7f];
                unsafe { set_header_map_pairs_raw(0, data.as_ptr(), data.len()) };
            }
            _ => {}
        }
        Action::Continue
    }

    fn on_http_request_body(&mut self, size: usize, end_of_stream: bool) -> Action {
        match self.path.as_str() {
            "/request-read" => {
                if let Some(bytes) = self.get_http_request_body(0, size) {
                    count("body.bytes;kind=request", bytes.len());
                }
            }
            "/request-replace" if !end_of_stream => return Action::Pause,
            "/request-replace" | "/request-stream" | "/request-then-trap" => {
                let bytes = self.get_http_request_body(0, size).unwrap_or_default();
                self.set_http_request_body(0, size, &bytes.to_ascii_uppercase());
            }
            "/local-body" => {
                if !end_of_stream {
                    return Action::Pause;
                }
                self.send_http_response(451, vec![], Some(b"read it\n"));
                return Action::Pause;
            }
            "/request-trap" => panic!(),
            _ => {}
        }
        Action::Continue
    }

    fn on_http_response_headers(&mut self, _: usize, _: bool) -> Action {
        match self.path.as_str() {
            "/spin-head" => spin(),
            "/trap-head" => panic!(),
            "/hog-head" => hog(),
            "/local-late" => {
                self.send_http_response(403, vec![], None);
            }
            "/framing" => self.set_http_response_header("transfer-encoding", Some("x")),
            "/smuggle" => self.set_http_response_header("x-plugin", Some("a\r\nx-smuggled: 1")),
            "/edit" => self.add_http_response_header("x-plugin", "edited"),
            "/hog-fields" => loop {
                self.add_http_response_header("x-plugin", "a value kept by the host");
            },
            "/meters" => {
                for i in 0..100 {
                    count(&format!("plugin.meters;i={i}"), 1);
                }
            }
            "/properties" => {
                let text = |bytes: Option<Vec<u8>>| String::from_utf8(bytes.unwrap_or_default()).unwrap();
                let route = text(self.get_property(vec!["route_name"]));
                let code = self.get_property(vec!["response", "code"]).unwrap_or_default();
                let code = u64::from_le_bytes(code.try_into().unwrap());
                let configuration = CONFIGURATION.with(|held| held.borrow().clone());
                self.add_http_response_header("x-route", &route);
                self.add_http_response_header("x-code", &code.to_string());
                self.add_http_response_header("x-configuration", &configuration);
            }
            "/outgoing" => {
                let refused = self
                    .dispatch_http_call("upstream", vec![], None, vec![], Duration::from_secs(1))
                    .is_err();
                self.add_http_response_header("x-outgoing", if refused { "refused" } else { "sent" });
            }
            "/replace" => self.set_http_response_header("content-length", None),
            _ => {}
        }
        Action::Continue
    }

    fn on_http_response_body(&mut self, size: usize, end_of_stream: bool) -> Action {
        match self.path.as_str() {
            "/spin-body" => spin(),
            "/trap-body" => panic!(),
            "/hog-body" => hog(),
            "/count" => {
                if let Some(bytes) = self.get_http_response_body(0, size) {
                    count("body.bytes;kind=response", bytes.len());
                }
            }
            // refused on a body that keeps its length: the call fails
            "/replace-sized" | "/replace" => {
                let bytes = self.get_http_response_body(0, size).unwrap_or_default();
                self.set_http_response_body(0, size, &bytes.to_ascii_uppercase());
                if end_of_stream {
                    self.set_http_response_body(usize::MAX, 0, b"<end>");
                }
            }
            "/hold" if !end_of_stream => return Action::Pause,
            "/request-then-trap" => panic!(),
            "/replace-trap" => {
                self.set_http_response_body(0, size, b"PART");
                panic!()
            }
            _ => {}
        }
        Action::Continue
    }
}
