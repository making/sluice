//! What the wickets' consoles (`--console <host:port>`) have alike: a page and
//! a JSON API over a wicket's routes, deploying uploaded files as routes
//! without a restart. Each wicket supplies its page and its API
//! ([`serve::Service`]); this crate serves them behind the same gate and keeps
//! the same directory layout:
//!
//! - [`gate`]: the credentials (`--console-token`, the sign-in through an
//!   OpenID Provider), else loopback only and no DNS rebinding; no edit from
//!   another origin
//! - [`serve`]: the console's HTTP/1.1 server, the page's shared files (its
//!   style, fonts and script helpers) and its headers
//! - [`http`]: the answers, the bodies read in memory or streamed to a file
//! - [`store`]: the console's directory (files kept under their sha256, never
//!   rewritten, written by a rename), the names and hosts it takes
//! - [`status`]: what the page shows of the reloads, and its long poll
//! - [`settings`]: the console's options, as both wickets take them

pub mod gate;
pub mod http;
pub mod serve;
pub mod settings;
pub mod status;
pub mod store;
