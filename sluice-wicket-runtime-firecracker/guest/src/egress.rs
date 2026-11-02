//! The guest's egress: the connections its processes open to the destinations of its
//! route's egress (`wire::Rule`), which the kernel command line names
//! (`wire::EGRESS_PARAMETER`). The guest has no network device: /init relays each
//! connection over vsock (`wire::EGRESS_PORT`) to the wicket, which connects to the
//! destination if the route's rules have it.
//!
//! Before the process starts, /init makes the destinations reachable as named:
//!
//! - a name: an address of its own, 127.1.0.1 on, in `/etc/hosts` (the rootfs's, the
//!   names first, bound read-only over it); `localhost`, the host's, stays the guest's
//!   127.0.0.1 and ::1
//! - an address or a range: a local route to it on `lo`, as the kernel routes
//!   127.0.0.0/8: the guest's own
//! - a listener at each name's address and each address, at the rule's port; a range's at
//!   every address (`0.0.0.0`, `[::]`), its connections asking for the address they were
//!   made to: a range's port is the guest's on every address, no process's to listen on

use std::ffi::CString;
use std::fmt::Write as _;
use std::io;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::sync::Arc;
use std::time::Duration;

use tokio::net::{TcpListener, TcpSocket, TcpStream};
use tokio_vsock::{VMADDR_CID_HOST, VsockAddr, VsockStream};

use crate::log::Log;
use crate::wire::{self, Host, Rule, Stream};

/// The first name's address (127.1.0.1), the next name's the next.
const NAMES: u32 = 0x7f01_0001;

/// A listener's backlog.
const BACKLOG: u32 = 1024;

/// The route's egress, as the kernel command line names it.
pub struct Egress {
    rules: Vec<Rule>,
    /// Each name but `localhost`, and its address.
    names: Vec<(String, Ipv4Addr)>,
    /// What went wrong so far, for the log.
    errors: Vec<String>,
}

impl Egress {
    /// The rules the kernel command line `cmdline` names.
    pub fn of(cmdline: &str) -> Egress {
        let mut egress = Egress {
            rules: Vec::new(),
            names: Vec::new(),
            errors: Vec::new(),
        };
        let parameter = format!("{}=", wire::EGRESS_PARAMETER);
        for rules in cmdline
            .split_whitespace()
            .filter_map(|arg| arg.strip_prefix(&parameter))
        {
            for rule in rules.split(',').filter(|rule| !rule.is_empty()) {
                match rule.parse::<Rule>() {
                    Ok(rule) => egress.rules.push(rule),
                    Err(e) => egress.errors.push(e),
                }
            }
        }
        for rule in &egress.rules {
            if let Host::Name(name) = &rule.host
                && name != "localhost"
                && egress.address(name).is_none()
            {
                let address = Ipv4Addr::from(NAMES + egress.names.len() as u32);
                egress.names.push((name.clone(), address));
            }
        }
        egress
    }

    /// The address of `name`.
    fn address(&self, name: &str) -> Option<Ipv4Addr> {
        let named = self.names.iter().find(|(known, _)| known == name);
        named.map(|(_, address)| *address)
    }

    /// Puts the names into `/etc/hosts`, on a tmpfs mounted on `/tmp` a moment: before
    /// `/tmp`'s own is.
    pub fn name(&mut self) {
        if let Err(e) = hosts(&self.names) {
            self.errors
                .push(format!("/etc/hosts: {e}: the names resolve to nothing"));
        }
    }

    /// Routes the addresses to the guest and listens at the destinations, before the
    /// process starts (`own`: the port it listens on, launcher mode's); tells `log` what
    /// it did, and what went wrong. Whether /init itself then answers at the process's
    /// `127.0.0.1:own`, where a process counts as listening: it could not, and it would seem
    /// to.
    pub fn listen(mut self, own: Option<u16>, log: &Arc<Log>) -> bool {
        if self.rules.is_empty() && self.errors.is_empty() {
            return false;
        }
        // SAFETY: a NUL-terminated name
        let lo = unsafe { libc::if_nametoindex(c"lo".as_ptr()) };
        let mut named = Vec::new();
        // where the process connects, the rule its connections ask for (None: the address
        // each was made to), and the rule; `localhost`'s IPv6 apart: it may fail, and an
        // address's rule there comes first
        let mut at: Vec<(SocketAddr, Option<Rule>, &Rule)> = Vec::new();
        let mut localhost_v6 = Vec::new();
        for rule in &self.rules {
            let port = rule.port;
            if own == Some(port) {
                self.errors.push(format!(
                    "{rule}: port {port} is the process's own (PORT): it cannot listen there"
                ));
            }
            match &rule.host {
                Host::Name(name) if name == "localhost" => {
                    named.push(rule.to_string());
                    at.push(((Ipv4Addr::LOCALHOST, port).into(), Some(rule.clone()), rule));
                    localhost_v6.push((
                        (Ipv6Addr::LOCALHOST, port).into(),
                        Some(rule.clone()),
                        rule,
                    ));
                }
                Host::Name(name) => {
                    let address = self.address(name).expect("every name has one");
                    named.push(format!("{rule} at {address}"));
                    at.push(((address, port).into(), Some(rule.clone()), rule));
                }
                Host::Net(ip, prefix) => {
                    if let Err(e) = local_route(*ip, *prefix, lo) {
                        self.errors.push(format!("{rule}: its local route: {e}"));
                        continue;
                    }
                    named.push(rule.to_string());
                    match rule.is_one() {
                        true => at.push((SocketAddr::new(*ip, port), Some(rule.clone()), rule)),
                        false => {
                            let every = match ip {
                                IpAddr::V4(_) => IpAddr::V4(Ipv4Addr::UNSPECIFIED),
                                IpAddr::V6(_) => IpAddr::V6(Ipv6Addr::UNSPECIFIED),
                            };
                            at.push((SocketAddr::new(every, port), None, rule));
                        }
                    }
                }
            }
        }
        let required = at.len();
        let mut listening: Vec<SocketAddr> = Vec::new();
        for (n, (address, to, rule)) in at.into_iter().chain(localhost_v6).enumerate() {
            if listening.contains(&address) {
                continue;
            }
            match bind(address) {
                Ok(listener) => {
                    listening.push(address);
                    tokio::spawn(accept(listener, to));
                }
                Err(_) if n >= required => {}
                Err(e) => self
                    .errors
                    .push(format!("{rule}: listening at {address}: {e}")),
            }
        }
        let line = format!("guest: egress to {}", named.join(", "));
        log.line(Stream::Stdout, line.as_bytes());
        for error in self.errors {
            log.line(Stream::Stderr, format!("guest: egress {error}").as_bytes());
        }
        listening.iter().any(|address| {
            Some(address.port()) == own
                && (address.ip() == IpAddr::V4(Ipv4Addr::UNSPECIFIED)
                    || address.ip() == IpAddr::V4(Ipv4Addr::LOCALHOST))
        })
    }
}

/// Accepts the process's connections at `listener`, each relayed to the wicket for `to`
/// (None: the address it was made to).
async fn accept(listener: TcpListener, to: Option<Rule>) {
    loop {
        let app = match listener.accept().await {
            Ok((app, _)) => app,
            // out of files, for one: a moment for some to close
            Err(_) => {
                tokio::time::sleep(Duration::from_millis(10)).await;
                continue;
            }
        };
        let to = match &to {
            Some(to) => to.clone(),
            None => match app.local_addr() {
                Ok(address) => Rule::address(address),
                Err(_) => continue,
            },
        };
        tokio::spawn(forward(app, to));
    }
}

/// Relays a connection of the process to the wicket, which connects to `to`: reset when
/// it does not, or the relay fails.
async fn forward(mut app: TcpStream, to: Rule) {
    let _ = app.set_nodelay(true);
    let relayed = match open(&to).await {
        Ok(stream) => wire::relay(&mut app, stream, None).await,
        Err(e) => Err(e),
    };
    if relayed.is_err() {
        // reset, not ended: the process takes the connection as failed
        let _ = app.set_zero_linger();
    }
}

/// An egress stream to `to`, once the wicket connected to it.
async fn open(to: &Rule) -> io::Result<VsockStream> {
    let host = VsockAddr::new(VMADDR_CID_HOST, wire::EGRESS_PORT);
    let mut stream = VsockStream::connect(host).await?;
    wire::request_egress(&mut stream, to).await?;
    Ok(stream)
}

/// A listener at `address`: beside the others at its port (a range's is at every
/// address), at an address of a local route as well as of an interface.
fn bind(address: SocketAddr) -> io::Result<TcpListener> {
    let socket = match address {
        SocketAddr::V4(_) => TcpSocket::new_v4()?,
        SocketAddr::V6(_) => TcpSocket::new_v6()?,
    };
    socket.set_reuseport(true)?;
    let fd = socket.as_raw_fd();
    // IPv6 binds an address of an interface alone, else
    set_option(fd, libc::SOL_IP, libc::IP_FREEBIND)?;
    if address.is_ipv6() && address.ip().is_unspecified() {
        // a range of IPv6's: IPv4's ranges listen apart
        set_option(fd, libc::IPPROTO_IPV6, libc::IPV6_V6ONLY)?;
    }
    socket.bind(address)?;
    socket.listen(BACKLOG)
}

/// Turns on the socket option `name` of `level`.
fn set_option(fd: RawFd, level: libc::c_int, name: libc::c_int) -> io::Result<()> {
    let on: libc::c_int = 1;
    // SAFETY: setsockopt of an int, of its size, on an open socket
    let set = unsafe {
        libc::setsockopt(
            fd,
            level,
            name,
            (&raw const on).cast(),
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        )
    };
    match set {
        0 => Ok(()),
        _ => Err(io::Error::last_os_error()),
    }
}

/// Makes the range of `ip` and `prefix` the guest's own: a local route on `lo` (the
/// interface of index `lo`), in the local table, as the kernel routes 127.0.0.0/8 there.
/// One there already is kept.
fn local_route(ip: IpAddr, prefix: u8, lo: u32) -> io::Result<()> {
    let (family, address) = match ip {
        IpAddr::V4(ip) => (libc::AF_INET, ip.octets().to_vec()),
        IpAddr::V6(ip) => (libc::AF_INET6, ip.octets().to_vec()),
    };
    // the header (its length below), the route, its attributes
    let mut message = Vec::with_capacity(64);
    message.extend_from_slice(&0u32.to_ne_bytes());
    message.extend_from_slice(&libc::RTM_NEWROUTE.to_ne_bytes());
    let flags = libc::NLM_F_REQUEST | libc::NLM_F_ACK | libc::NLM_F_CREATE | libc::NLM_F_EXCL;
    message.extend_from_slice(&(flags as u16).to_ne_bytes());
    // the sequence number, the sender's port id (the kernel's to give)
    message.extend_from_slice(&1u32.to_ne_bytes());
    message.extend_from_slice(&0u32.to_ne_bytes());
    message.extend_from_slice(&[
        family as u8,
        prefix,
        0,
        0,
        libc::RT_TABLE_LOCAL,
        libc::RTPROT_BOOT,
        libc::RT_SCOPE_HOST,
        libc::RTN_LOCAL,
    ]);
    message.extend_from_slice(&0u32.to_ne_bytes());
    for (kind, data) in [
        (libc::RTA_DST, &address[..]),
        (libc::RTA_OIF, &lo.to_ne_bytes()[..]),
    ] {
        message.extend_from_slice(&((4 + data.len()) as u16).to_ne_bytes());
        message.extend_from_slice(&kind.to_ne_bytes());
        message.extend_from_slice(data);
        message.resize(message.len().next_multiple_of(4), 0);
    }
    let length = message.len() as u32;
    message[..4].copy_from_slice(&length.to_ne_bytes());
    // SAFETY: a socket of its own
    let fd = unsafe {
        libc::socket(
            libc::AF_NETLINK,
            libc::SOCK_RAW | libc::SOCK_CLOEXEC,
            libc::NETLINK_ROUTE,
        )
    };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: just opened, owned here alone
    let socket = unsafe { OwnedFd::from_raw_fd(fd) };
    // SAFETY: the message's bytes, to the kernel (a netlink socket's default peer)
    let sent = unsafe {
        libc::send(
            socket.as_raw_fd(),
            message.as_ptr().cast(),
            message.len(),
            0,
        )
    };
    if sent < 0 {
        return Err(io::Error::last_os_error());
    }
    let mut answer = [0u8; 256];
    // SAFETY: into a buffer of its length
    let read = unsafe {
        libc::recv(
            socket.as_raw_fd(),
            answer.as_mut_ptr().cast(),
            answer.len(),
            0,
        )
    };
    if read < 0 {
        return Err(io::Error::last_os_error());
    }
    // the acknowledgement: an error message, of error 0 or a negated errno
    if read < 20 || u16::from_ne_bytes([answer[4], answer[5]]) != libc::NLMSG_ERROR as u16 {
        return Err(io::Error::other("netlink: no acknowledgement"));
    }
    match i32::from_ne_bytes(answer[16..20].try_into().expect("4 bytes")) {
        0 => Ok(()),
        error if -error == libc::EEXIST => Ok(()),
        error => Err(io::Error::from_raw_os_error(-error)),
    }
}

/// Writes `/etc/hosts` anew -- `names` first, then the rootfs's lines -- into a file of a
/// tmpfs mounted on `/tmp` a moment, bound read-only over the rootfs's.
fn hosts(names: &[(String, Ipv4Addr)]) -> io::Result<()> {
    if names.is_empty() {
        return Ok(());
    }
    let mut text = String::new();
    for (name, address) in names {
        let _ = writeln!(text, "{address} {name}");
    }
    text.push_str(&std::fs::read_to_string("/etc/hosts")?);
    mount(Some("tmpfs"), "/tmp", Some("tmpfs"), 0)?;
    let bound = std::fs::write("/tmp/hosts", text)
        .and_then(|()| mount(Some("/tmp/hosts"), "/etc/hosts", None, libc::MS_BIND))
        .and_then(|()| {
            let read_only = libc::MS_REMOUNT | libc::MS_BIND | libc::MS_RDONLY;
            mount(None, "/etc/hosts", None, read_only)
        });
    // the tmpfs lives on in the bind alone
    let target = CString::new("/tmp").expect("no NUL");
    // SAFETY: a NUL-terminated path
    if unsafe { libc::umount2(target.as_ptr(), libc::MNT_DETACH) } != 0 {
        return Err(io::Error::last_os_error());
    }
    bound
}

fn mount(
    source: Option<&str>,
    target: &str,
    fstype: Option<&str>,
    flags: libc::c_ulong,
) -> io::Result<()> {
    let text = |text: &str| CString::new(text).map_err(|_| io::Error::other("a NUL byte"));
    let (source, fstype) = (source.map(text).transpose()?, fstype.map(text).transpose()?);
    let target = text(target)?;
    let pointer = |text: &Option<CString>| text.as_ref().map_or(std::ptr::null(), |t| t.as_ptr());
    // SAFETY: NUL-terminated strings or null, no data
    let mounted = unsafe {
        libc::mount(
            pointer(&source),
            target.as_ptr(),
            pointer(&fstype),
            flags,
            std::ptr::null(),
        )
    };
    match mounted {
        0 => Ok(()),
        _ => Err(io::Error::last_os_error()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The rules of the kernel command line, among its other parameters: a name each an
    /// address of its own (`localhost` none), once; what is not a rule, an error.
    #[test]
    fn the_command_line_names_the_rules() {
        let egress = Egress::of(
            "console=none quiet sluice.egress=db.internal:5432,localhost:6379,\
             10.0.0.0/24:5432,db.internal:5433,cache:6379,nonsense init=/init",
        );
        let rules: Vec<String> = egress.rules.iter().map(Rule::to_string).collect();
        assert_eq!(
            rules,
            [
                "db.internal:5432",
                "localhost:6379",
                "10.0.0.0/24:5432",
                "db.internal:5433",
                "cache:6379"
            ]
        );
        assert_eq!(
            egress.names,
            [
                ("db.internal".to_string(), Ipv4Addr::new(127, 1, 0, 1)),
                ("cache".to_string(), Ipv4Addr::new(127, 1, 0, 2)),
            ]
        );
        assert_eq!(egress.errors, ["nonsense: expected <host>:<port>"]);
        assert!(Egress::of("console=none init=/init").rules.is_empty());
    }
}
