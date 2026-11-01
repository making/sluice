//! sluice-wicket-firecracker (spike): a tunnel client serving each route with
//! Firecracker microVMs, as sluice-wicket-wasi serves each with a wasi:http component.
//!
//! ```text
//! sluice server --tunnel--> wicket --vsock (raw bytes per connection)--> guest in a microVM
//! ```
//!
//! The wicket does not terminate HTTP: each tunnel connection is relayed verbatim to a
//! vsock stream of a guest (framed, see `wire`, to carry half-closes), as the Java client
//! relays to a TCP upstream; the guest's `/init` relays it on to the process's TCP port. A route's VMs
//! are either one shared VM, or a VM per connection cloned from a snapshot.

pub mod listener;
pub mod route;
pub mod tunnel;
pub mod vm;
pub mod wire;

pub mod proto {
    tonic::include_proto!("sluice.v1");
}

pub type BoxError = Box<dyn std::error::Error + Send + Sync>;
