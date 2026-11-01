//! PID 1's reaping: every child that ends -- the process, a restore hook, an orphan
//! reparented to /init -- is reaped here. A child spawned through it gets its exit
//! status from it: a wait of its own would race the reaping, which takes any child.

use std::os::unix::process::ExitStatusExt as _;
use std::process::{Child, Command, ExitStatus};
use std::sync::{Mutex, MutexGuard};
use std::time::Duration;

use tokio::sync::oneshot;

/// Reaps the children of /init ([`Reaper::run`]).
pub struct Reaper {
    /// The children spawned through it, and where their status goes.
    waiting: Mutex<Vec<(libc::pid_t, oneshot::Sender<ExitStatus>)>>,
}

impl Reaper {
    pub const fn new() -> Reaper {
        Reaper {
            waiting: Mutex::new(Vec::new()),
        }
    }

    fn waiting(&self) -> MutexGuard<'_, Vec<(libc::pid_t, oneshot::Sender<ExitStatus>)>> {
        self.waiting.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Spawns `command`; its exit status comes through the receiver. The child is not
    /// to be waited for.
    pub fn spawn(
        &self,
        command: &mut Command,
    ) -> std::io::Result<(Child, oneshot::Receiver<ExitStatus>)> {
        // held across the spawn: the reaping takes the child's status once it is known
        // here, and a spawn whose exec fails reaps the child itself
        let mut waiting = self.waiting();
        let child = command.spawn()?;
        let (status, received) = oneshot::channel();
        waiting.push((child.id() as libc::pid_t, status));
        Ok((child, received))
    }

    /// Reaps every child for good; `exited` is told of each that was not spawned
    /// through it.
    pub fn run(&self, exited: impl Fn(libc::pid_t, ExitStatus)) -> ! {
        loop {
            // which child ended, left a zombie: a spawn under way may not know it yet,
            // or reap it itself
            // SAFETY: a zeroed siginfo_t is valid, filled by waitid
            let mut info: libc::siginfo_t = unsafe { std::mem::zeroed() };
            // SAFETY: waitid for any child into `info`
            let ended =
                unsafe { libc::waitid(libc::P_ALL, 0, &mut info, libc::WEXITED | libc::WNOWAIT) };
            if ended != 0 {
                // no child for now
                std::thread::sleep(Duration::from_millis(100));
                continue;
            }
            // SAFETY: waitid filled it for a child that ended
            let pid = unsafe { info.si_pid() };
            let mut waiting = self.waiting();
            let mut raw = 0;
            // SAFETY: a plain waitpid for the child that ended
            if unsafe { libc::waitpid(pid, &mut raw, libc::WNOHANG) } != pid {
                // its spawn reaped it
                continue;
            }
            let status = ExitStatus::from_raw(raw);
            match waiting.iter().position(|(child, _)| *child == pid) {
                Some(at) => {
                    let _ = waiting.swap_remove(at).1.send(status);
                }
                None => {
                    drop(waiting);
                    exited(pid, status);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Read as _;
    use std::process::Stdio;
    use std::time::Instant;

    use super::*;

    /// Set in the process running the test apart.
    const APART: &str = "SLUICE_GUEST_REAPER_TEST";

    /// A child spawned through the reaper gets its exit status while the reaper reaps
    /// every child, an exec failing is the spawn's error, and an orphan is reaped. Runs
    /// in a process of its own, the test binary again, as the subreaper of its
    /// descendants (as PID 1 is): reaping any child would take the other tests'.
    #[test]
    fn a_child_spawned_through_it_gets_its_status_while_it_reaps_every_child() {
        let name =
            "reaper::tests::a_child_spawned_through_it_gets_its_status_while_it_reaps_every_child";
        if std::env::var_os(APART).is_none() {
            let output = Command::new(std::env::current_exe().unwrap())
                .args(["--exact", name, "--nocapture"])
                .env(APART, "1")
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "{}{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            return;
        }
        // SAFETY: a plain prctl
        assert_eq!(unsafe { libc::prctl(libc::PR_SET_CHILD_SUBREAPER, 1) }, 0);
        static REAPER: Reaper = Reaper::new();
        // as the image's process: a child all along
        let (mut process, ended) = REAPER.spawn(Command::new("sleep").arg("60")).unwrap();
        let (others, reaped) = std::sync::mpsc::channel();
        std::thread::spawn(move || REAPER.run(move |pid, _| others.send(pid).unwrap()));
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        runtime.block_on(async {
            for n in 0..200 {
                let mut command = Command::new("sh");
                command.args(["-c", &format!("exit {}", n % 3)]);
                let (_child, status) = REAPER.spawn(&mut command).unwrap();
                assert_eq!(status.await.unwrap().code(), Some(n % 3), "child {n}");
            }
            let failed = REAPER.spawn(&mut Command::new("/nonexistent/program"));
            assert!(failed.is_err(), "an exec failing");

            // the shell exits at once: its child (sleep) is an orphan, the reaper's
            let mut command = Command::new("sh");
            command
                .args(["-c", "sleep 0.1 & echo $!"])
                .stdout(Stdio::piped());
            let (mut child, status) = REAPER.spawn(&mut command).unwrap();
            let mut orphan = String::new();
            let stdout = child.stdout.as_mut().unwrap();
            stdout.read_to_string(&mut orphan).unwrap();
            assert!(status.await.unwrap().success());
            let orphan: libc::pid_t = orphan.trim().parse().unwrap();
            let deadline = Instant::now() + Duration::from_secs(10);
            while reaped.recv_timeout(deadline.saturating_duration_since(Instant::now()))
                != Ok(orphan)
            {}

            process.kill().unwrap();
            let killed = ended.await.unwrap();
            assert_eq!(killed.signal(), Some(libc::SIGKILL), "{killed}");
        });
    }
}
