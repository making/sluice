//! The VMs' output: what the processes of a guest write to their stdout / stderr, which
//! its `/init` sends as lines on the VM's log stream (`wire::LOG_PORT`). Each line goes
//! to the wicket's stdout / stderr with a prefix naming the route's hosts and the VM
//! (`stdout [app.local vm-3] :: ...`), and to the live access log's tap (`--flume`) as a
//! line of the route's first host.
//!
//! ```text
//! process --pipe--> /init --log stream (vsock)--> follow --Printer--> the wicket's stdout / stderr
//!                                                        --Tap--> sluice-flume
//! ```
//!
//! The lines are written from a thread of their own ([`Printer`]): a slow reader of the
//! wicket's output (a container's log driver) holds the log streams back -- and the
//! guests drop lines past their buffers, counted -- but never a runtime thread.

use std::io::Write;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use sluice_wicket_tunnel::flume::Tap;
use tokio::io::{AsyncWriteExt as _, BufReader};
use tokio::net::UnixStream;
use tokio::sync::{mpsc, watch};

use crate::wire::{self, LogFrame, Stream};

/// Lines waiting for the printer at most: past them the streams wait.
const PRINTER_LINES: usize = 1024;

/// Bytes the printer writes in one call at most (one line is written whole).
const PRINTER_BATCH: usize = 64 * 1024;

/// The hosts a route serves, as its log lines and its VMs' output name them: a reload
/// keeping the route's VMs may change them.
pub struct Hosts(RwLock<Arc<Named>>);

/// A route's hosts, named.
pub struct Named {
    /// All of them, for the log: `a.local, b.local`.
    pub label: String,
    /// The first: the route key of its VMs' lines in the live access log.
    pub key: String,
}

impl Hosts {
    pub fn new(hosts: &[String]) -> Arc<Hosts> {
        Arc::new(Hosts(RwLock::new(Self::named(hosts))))
    }

    fn named(hosts: &[String]) -> Arc<Named> {
        Arc::new(Named {
            label: hosts.join(", "),
            key: hosts.first().cloned().unwrap_or_default(),
        })
    }

    pub fn set(&self, hosts: &[String]) {
        *self.0.write().unwrap_or_else(|e| e.into_inner()) = Self::named(hosts);
    }

    pub fn current(&self) -> Arc<Named> {
        self.0.read().unwrap_or_else(|e| e.into_inner()).clone()
    }
}

/// Writes lines to the wicket's stdout / stderr, from a thread of its own, in order.
#[derive(Clone)]
pub struct Printer {
    lines: mpsc::Sender<(Stream, Vec<u8>)>,
}

impl Printer {
    /// A printer to the wicket's stdout and stderr.
    pub fn stdio() -> Printer {
        Printer::new(std::io::stdout(), std::io::stderr())
    }

    /// A printer to `stdout` and `stderr`.
    pub fn new(
        mut stdout: impl Write + Send + 'static,
        mut stderr: impl Write + Send + 'static,
    ) -> Printer {
        let (lines, mut queued) = mpsc::channel::<(Stream, Vec<u8>)>(PRINTER_LINES);
        std::thread::Builder::new()
            .name("vm-output".into())
            .spawn(move || {
                let mut write = |stream: Stream, batch: &[u8]| {
                    // nothing to tell a broken stdout or stderr
                    let _ = match stream {
                        Stream::Stdout => stdout.write_all(batch).and_then(|()| stdout.flush()),
                        Stream::Stderr => stderr.write_all(batch).and_then(|()| stderr.flush()),
                    };
                };
                let mut batch = Vec::new();
                while let Some((mut of, line)) = queued.blocking_recv() {
                    batch.extend_from_slice(&line);
                    // what came meanwhile in one write a stream, in the order it came
                    while let Ok((stream, line)) = queued.try_recv() {
                        if stream != of || batch.len() >= PRINTER_BATCH {
                            write(of, &batch);
                            batch.clear();
                            of = stream;
                        }
                        batch.extend_from_slice(&line);
                    }
                    write(of, &batch);
                    batch.clear();
                }
            })
            .expect("the printer's thread");
        Printer { lines }
    }

    /// Writes `line` (ended by its newline) to `stream`, once the lines before it were.
    pub async fn print(&self, stream: Stream, line: Vec<u8>) {
        let _ = self.lines.send((stream, line)).await;
    }
}

/// Where the VMs' lines go: the printer, and the live access log's tap.
pub struct Sink {
    pub printer: Printer,
    pub tap: Arc<Tap>,
}

impl Sink {
    /// A line of the VM `vm` of the route of `hosts`.
    async fn line(&self, hosts: &Hosts, vm: &str, stream: Stream, line: &[u8]) {
        let named = hosts.current();
        if self.tap.on() {
            let text = line.strip_suffix(b"\r").unwrap_or(line);
            self.tap.publish(
                stream.name(),
                named.key.clone(),
                vec![
                    ("vm", vm.to_string()),
                    ("line", String::from_utf8_lossy(text).into_owned()),
                ],
            );
        }
        let prefix = format!("{} [{} {vm}] :: ", stream.name(), named.label);
        let mut out = Vec::with_capacity(prefix.len() + line.len() + 1);
        out.extend_from_slice(prefix.as_bytes());
        out.extend_from_slice(line);
        out.push(b'\n');
        self.printer.print(stream, out).await;
    }

    /// The lines the guest dropped: its buffer of them was full.
    async fn gap(&self, hosts: &Hosts, vm: &str, stdout: u64, stderr: u64) {
        let named = hosts.current();
        self.tap.dropped(Stream::Stdout.name(), &named.key, stdout);
        self.tap.dropped(Stream::Stderr.name(), &named.key, stderr);
        let note = format!(
            "route {} {vm}: {} line(s) of its output dropped, its buffer of them full\n",
            named.label,
            stdout + stderr
        );
        self.printer.print(Stream::Stderr, note.into_bytes()).await;
    }
}

/// What ended, as a VM's log stream tells.
#[derive(Debug, PartialEq, Eq)]
pub enum Ended {
    /// The guest said its process did.
    Process,
    /// The stream did, the VM with it.
    Vm,
}

/// Resolves once `flag` is on; never if it never will be.
async fn turned_on(flag: &mut watch::Receiver<bool>) {
    if flag.wait_for(|on| *on).await.is_err() {
        std::future::pending::<()>().await;
    }
}

/// A VM's output being followed ([`follow`]).
pub struct Output {
    syncs: mpsc::Sender<()>,
    synced: watch::Receiver<u64>,
    exited: watch::Receiver<bool>,
    gone: watch::Receiver<bool>,
}

impl Output {
    /// Resolves once the guest said its process ended; never if the stream ends first.
    pub async fn exited(&mut self) {
        turned_on(&mut self.exited).await;
    }

    /// Resolves once the VM is gone, or its process ended unless that was `told` before; never
    /// on a stream given up with the VM alive.
    pub async fn ended(&mut self, told: bool) -> Ended {
        tokio::select! {
            biased;
            () = turned_on(&mut self.gone) => Ended::Vm,
            () = turned_on(&mut self.exited), if !told => Ended::Process,
        }
    }

    /// Waits for every line the guest's processes wrote so far, handed on (to the
    /// printer, and the tap), within `timeout`: whether they were.
    pub async fn sync(&mut self, timeout: Duration) -> bool {
        let before = *self.synced.borrow_and_update();
        let synced = async {
            self.syncs.send(()).await.ok()?;
            self.synced.wait_for(|count| *count > before).await.ok()
        };
        matches!(tokio::time::timeout(timeout, synced).await, Ok(Some(_)))
    }
}

/// Follows the log stream of the VM named `vm` of the route of `hosts` in a task of its
/// own, until the stream ends (the VM gone): its lines to `sink`. A frame the wicket
/// does not take (too long, of an unknown type) ends it too.
pub fn follow(mut stream: UnixStream, sink: Arc<Sink>, hosts: Arc<Hosts>, vm: String) -> Output {
    let (syncs, mut asked) = mpsc::channel(1);
    let (count, synced) = watch::channel(0u64);
    let (ended, exited) = watch::channel(false);
    let (vm_gone, gone) = watch::channel(false);
    tokio::spawn(async move {
        // borrowed halves: an owned write half shuts the socket down when dropped,
        // which Firecracker takes as the end of the whole stream
        let (reader, mut writer) = stream.split();
        let mut reader = BufReader::with_capacity(4096, reader);
        let reading = async {
            loop {
                match wire::read_log_frame(&mut reader).await {
                    Ok(Some(LogFrame::Line(stream, line))) => {
                        sink.line(&hosts, &vm, stream, &line).await;
                    }
                    Ok(Some(LogFrame::Gap { stdout, stderr })) => {
                        sink.gap(&hosts, &vm, stdout, stderr).await;
                    }
                    Ok(Some(LogFrame::Synced)) => count.send_modify(|count| *count += 1),
                    Ok(Some(LogFrame::Exited)) => drop(ended.send(true)),
                    Ok(None) => {
                        let _ = vm_gone.send(true);
                        return;
                    }
                    Err(e) => {
                        let note = format!(
                            "route {} {vm}: its output: {e}; no more of it is read\n",
                            hosts.current().label
                        );
                        sink.printer.print(Stream::Stderr, note.into_bytes()).await;
                        // a frame not taken: the VM lives on
                        if e.kind() != std::io::ErrorKind::InvalidData {
                            let _ = vm_gone.send(true);
                        }
                        return;
                    }
                }
            }
        };
        let asking = async {
            while asked.recv().await.is_some() {
                if writer.write_all(&[wire::SYNC]).await.is_err() {
                    break;
                }
            }
            // the reading alone ends the stream
            std::future::pending::<()>().await;
        };
        tokio::select! {
            () = reading => {}
            () = asking => {}
        }
    });
    Output {
        syncs,
        synced,
        exited,
        gone,
    }
}

/// What a printer wrote, for the tests.
#[cfg(test)]
#[derive(Clone, Default)]
pub(crate) struct Written(Arc<std::sync::Mutex<Vec<u8>>>);

#[cfg(test)]
impl Write for Written {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
impl Written {
    pub(crate) fn text(&self) -> String {
        String::from_utf8_lossy(&self.0.lock().unwrap()).into_owned()
    }
}

#[cfg(test)]
mod tests {
    use tokio::io::AsyncReadExt as _;

    use super::*;

    /// Syncs `output`, the guest double answering with `frames` before its answer.
    async fn synced(output: &mut Output, guest: &mut UnixStream, frames: &[LogFrame]) -> bool {
        let answered = async {
            assert_eq!(guest.read_u8().await.unwrap(), wire::SYNC);
            let mut out = Vec::new();
            for frame in frames.iter().chain([&LogFrame::Synced]) {
                wire::put_log_frame(&mut out, frame);
            }
            guest.write_all(&out).await.unwrap();
        };
        tokio::join!(output.sync(Duration::from_secs(10)), answered).0
    }

    /// Each line goes to the printer with the route's hosts of the moment and the VM,
    /// and to the tap as a line of the first host; the guest's drops are noted; a sync
    /// answers once the lines before it were handed on; a frame the wicket does not
    /// take ends the stream.
    #[tokio::test]
    async fn a_vms_lines_reach_the_printer_and_the_tap() {
        let (stdout, stderr) = (Written::default(), Written::default());
        let tap = Tap::new();
        let sink = Arc::new(Sink {
            printer: Printer::new(stdout.clone(), stderr.clone()),
            tap: tap.clone(),
        });
        let hosts = Hosts::new(&["a.local".into(), "b.local".into()]);
        let (wicket, mut guest) = UnixStream::pair().unwrap();
        let mut output = follow(wicket, sink, hosts.clone(), "vm-3".into());

        let line = |stream, text: &str| LogFrame::Line(stream, text.as_bytes().to_vec());
        let gap = LogFrame::Gap {
            stdout: 2,
            stderr: 1,
        };
        let frames = [
            line(Stream::Stdout, "hello\r"),
            line(Stream::Stderr, "oops"),
            gap,
        ];
        assert!(synced(&mut output, &mut guest, &frames).await);
        hosts.set(&["b.local".into()]);
        assert!(synced(&mut output, &mut guest, &[line(Stream::Stdout, "on")]).await);
        let tapped: Vec<_> = tap
            .recent()
            .iter()
            .map(|line| (line.kind, line.route_key.clone(), line.fields.clone()))
            .collect();
        let tapped_line = |kind, host: &str, text: &str| {
            let fields = vec![("vm", "vm-3".to_string()), ("line", text.to_string())];
            (kind, host.to_string(), fields)
        };
        assert_eq!(
            tapped,
            [
                tapped_line("stdout", "a.local", "hello"),
                tapped_line("stderr", "a.local", "oops"),
                tapped_line("stdout", "b.local", "on"),
            ]
        );

        // over the bound: the stream is given up
        let mut overlong = vec![2u8];
        overlong.extend_from_slice(&(wire::MAX_LINE as u32 + 1).to_be_bytes());
        guest.write_all(&overlong).await.unwrap();
        let mut rest = Vec::new();
        let ended = tokio::time::timeout(Duration::from_secs(10), guest.read_to_end(&mut rest));
        assert!(ended.await.is_ok(), "the stream is not given up");
        assert!(!output.sync(Duration::from_secs(1)).await);
        // the printer's thread wrote what it was handed by now
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            stdout.text(),
            "stdout [a.local, b.local vm-3] :: hello\r\nstdout [b.local vm-3] :: on\n"
        );
        assert_eq!(
            stderr.text(),
            "stderr [a.local, b.local vm-3] :: oops\n\
             route a.local, b.local vm-3: 3 line(s) of its output dropped, its buffer of them full\n\
             route b.local vm-3: its output: a frame of type 2 and 16385 bytes; no more of it is read\n"
        );
    }
}
