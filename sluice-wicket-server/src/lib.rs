//! What the wickets' own listeners and sluice-wicket-cf-api have alike:
//!
//! - [`accept`]: the accept loop, its connections' slots and TCP keepalive
//! - [`http`]: an HTTP/1.1 connection no client holds by going quiet, and its end at a stop
//! - [`health`]: the wickets' readiness probe

pub mod accept;
pub mod health;
pub mod http;
