//! What the wickets' own listeners and sluice-wicket-cf-api have alike:
//!
//! - [`accept`]: the accept loop, its connections' slots and TCP keepalive
//! - [`http`]: an HTTP/1.1 connection no client holds by going quiet

pub mod accept;
pub mod http;
