use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use sluice_wicket_runtime_firecracker::cgroup::Cgroups;
use sluice_wicket_runtime_firecracker::config::{self, Command};
use sluice_wicket_runtime_firecracker::forker::{self, Forker};
use sluice_wicket_runtime_firecracker::keeper::{self, Client};
use sluice_wicket_runtime_firecracker::seccomp::Filter;
use sluice_wicket_runtime_firecracker::state::StateDir;
use sluice_wicket_runtime_firecracker::vm::Launcher;
use sluice_wicket_tunnel::workers::Workers;

/// How long the VMs' cgroups are waited for at the end: a VM killed takes a moment to go.
const CGROUPS_WAIT: Duration = Duration::from_secs(2);

fn main() -> ExitCode {
    let mut args = std::env::args_os().skip(1);
    if args.next().is_some_and(|first| first == forker::FORK) {
        return match forker::serve(args) {
            Ok(()) => ExitCode::SUCCESS,
            Err(e) => {
                eprintln!("{}: {e}", forker::FORK);
                ExitCode::FAILURE
            }
        };
    }
    // the keeper's end, when it started this wicket (jail mode): taken from the
    // environment before anything else, single-threaded
    let keeper = Client::fd_from_env();
    let mut settings =
        match config::parse(std::env::args().skip(1), |name| std::env::var(name).ok()).and_then(
            |command| match command {
                Command::Run(_) => config::check_env(env_names()).map(|()| command),
                command => Ok(command),
            },
        ) {
            Ok(Command::Help) => {
                println!("{}", config::USAGE);
                return ExitCode::SUCCESS;
            }
            Ok(Command::Version) => {
                println!("{}", sluice_wicket_runtime_firecracker::describe_build());
                return ExitCode::SUCCESS;
            }
            Ok(Command::Probe(address)) => {
                return match sluice_wicket_server::health::probe(&address) {
                    Ok(ready) => {
                        print!("{ready}");
                        ExitCode::SUCCESS
                    }
                    Err(not) => {
                        eprintln!("{}", not.trim_end());
                        ExitCode::FAILURE
                    }
                };
            }
            Ok(Command::Run(settings)) => *settings,
            Err(message) => {
                eprintln!("{message}\n\n{}", config::USAGE);
                return ExitCode::from(2);
            }
        };
    let mut cgroups = None;
    let (launcher, state) = match (&settings.jail, keeper) {
        (Some(_), Some(Ok(fd))) => {
            let firecracker = match settings.routes.firecracker() {
                Ok(firecracker) => firecracker,
                Err(e) => {
                    eprintln!("{e}");
                    return ExitCode::FAILURE;
                }
            };
            (Launcher::Jailed { fd, firecracker }, None)
        }
        (_, Some(Err(e))) => {
            eprintln!("{e}");
            return ExitCode::FAILURE;
        }
        // SAFETY: geteuid
        (Some(jail), None) if unsafe { libc::geteuid() } == 0 => {
            let firecracker = match settings.routes.firecracker() {
                Ok(firecracker) => firecracker,
                Err(e) => {
                    eprintln!("{e}");
                    return ExitCode::FAILURE;
                }
            };
            return keeper::run(
                jail,
                firecracker,
                settings.state_dir,
                settings.max_instances,
            );
        }
        (Some(_), None) => {
            eprintln!(
                "jail mode (--jail-ids) starts as root: the keeper stays root to spawn the VMs \
                 and runs the wicket as --jail-user"
            );
            return ExitCode::FAILURE;
        }
        (None, _) => {
            // the VMs' cgroups, where the wicket's own is delegated to it: before the forker
            // (it forks from the wicket's leaf), the routes checked against what they found
            let delegated = match Cgroups::delegated() {
                Ok(delegated) => delegated,
                Err(e) => {
                    eprintln!("the VMs' cgroups: {e}");
                    return ExitCode::FAILURE;
                }
            };
            // what a route's `cpu` needs, where the VMs have it not
            let (found, no_cpu) = match &delegated {
                Ok(cgroups) => {
                    let dir = cgroups.dir().display();
                    let found = format!(
                        "the VMs' cgroups in {dir}, the wicket's own, delegated to it: each \
                         VM's memory and threads bound by its own"
                    );
                    match cgroups.cpu() {
                        true => (format!("{found}, the CPUs shared by what each holds"), None),
                        false => (
                            format!("{found}; no cpu controller there: a route's cpu is refused"),
                            Some(format!("the cpu controller, which {dir} lacks")),
                        ),
                    }
                }
                Err(why) => (
                    format!(
                        "no cgroup of the VMs' own: the wicket's is not delegated to it ({why}); \
                         it bounds them all, and a route's cpu is refused"
                    ),
                    Some(format!(
                        "a cgroup of its VMs' own: jail mode (--jail-ids), or the wicket's \
                         cgroup delegated to it ({why})"
                    )),
                ),
            };
            println!("{found}");
            cgroups = delegated.ok();
            let failed = |e: String| {
                eprintln!("{e}");
                if let Some(cgroups) = &cgroups {
                    cgroups.close(Duration::ZERO);
                }
                ExitCode::FAILURE
            };
            if let Some(why) = no_cpu {
                settings.routes.refuse_cpu(why);
                if let Err(e) = settings.routes.read() {
                    return failed(e);
                }
            }
            let state = match StateDir::open(settings.state_dir.clone()) {
                Ok(state) => state,
                Err(e) => return failed(format!("--state-dir: {e}")),
            };
            let forker = match Forker::start(std::path::Path::new("/proc/self/exe")) {
                Ok(forker) => forker,
                Err(e) => return failed(e.to_string()),
            };
            let launcher = Launcher::Direct {
                state: state.dir.clone(),
                forker,
                cgroups: cgroups.clone(),
            };
            (launcher, Some(state))
        }
    };
    // the wicket reads what the guests send: under its seccomp filter from here on, its
    // VMs too (single-threaded: before the runtime)
    if let Err(e) = Filter::unprivileged().install() {
        eprintln!("the seccomp filter: {e}");
        return ExitCode::FAILURE;
    }
    // the main thread's runtime boots, restores and reloads; the workers relay
    let (runtime, workers) = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .and_then(|runtime| Ok((runtime, Workers::start(settings.workers, "relay")?)))
    {
        Ok((runtime, workers)) => (runtime, Arc::new(workers)),
        Err(e) => {
            eprintln!("cannot start the runtime: {e}");
            return ExitCode::FAILURE;
        }
    };
    let ran = runtime.block_on(sluice_wicket_runtime_firecracker::run(
        settings,
        launcher,
        workers.clone(),
    ));
    // the tasks left are dropped, and with them the VMs they hold (killed): the
    // connections' first, the main runtime's reaping theirs
    workers.shutdown();
    runtime.shutdown_timeout(Duration::from_secs(5));
    if let Some(state) = state {
        state.close();
    }
    if let Some(cgroups) = cgroups {
        cgroups.close(CGROUPS_WAIT);
    }
    match ran {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("sluice-wicket-runtime-firecracker failed: {e}");
            ExitCode::FAILURE
        }
    }
}

/// The names of the process's environment variables.
fn env_names() -> impl Iterator<Item = String> {
    std::env::vars_os().map(|(name, _)| name.to_string_lossy().into_owned())
}
