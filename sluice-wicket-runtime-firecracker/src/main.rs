use std::process::ExitCode;
use std::time::Duration;

use sluice_wicket_runtime_firecracker::config::{self, Command};
use sluice_wicket_runtime_firecracker::keeper::{self, Client};
use sluice_wicket_runtime_firecracker::state::StateDir;
use sluice_wicket_runtime_firecracker::vm::{self, Launcher};

fn main() -> ExitCode {
    let mut args = std::env::args_os().skip(1);
    if args.next().is_some_and(|first| first == vm::LAUNCH) {
        let error = vm::launch(args);
        eprintln!(
            "{} {}: {error}",
            vm::LAUNCH,
            std::env::args().nth(4).unwrap_or_default()
        );
        return ExitCode::from(127);
    }
    // the keeper's end, when it started this wicket (jail mode): taken from the
    // environment before anything else, single-threaded
    let keeper = Client::fd_from_env();
    let settings = match config::parse(std::env::args().skip(1), |name| std::env::var(name).ok()) {
        Ok(Command::Help) => {
            println!("{}", config::USAGE);
            return ExitCode::SUCCESS;
        }
        Ok(Command::Version) => {
            println!("{}", sluice_wicket_runtime_firecracker::describe_build());
            return ExitCode::SUCCESS;
        }
        Ok(Command::Run(settings)) => *settings,
        Err(message) => {
            eprintln!("{message}\n\n{}", config::USAGE);
            return ExitCode::from(2);
        }
    };
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
            return keeper::run(jail, firecracker, settings.state_dir, settings.max_vms);
        }
        (Some(_), None) => {
            eprintln!(
                "jail mode (--jail-ids) starts as root: the keeper stays root to spawn the VMs \
                 and runs the wicket as --jail-user"
            );
            return ExitCode::FAILURE;
        }
        (None, _) => match StateDir::open(settings.state_dir.clone()) {
            Ok(state) => (
                Launcher::Direct {
                    state: state.dir.clone(),
                },
                Some(state),
            ),
            Err(e) => {
                eprintln!("--state-dir: {e}");
                return ExitCode::FAILURE;
            }
        },
    };
    let runtime = match tokio::runtime::Runtime::new() {
        Ok(runtime) => runtime,
        Err(e) => {
            eprintln!("cannot start the runtime: {e}");
            return ExitCode::FAILURE;
        }
    };
    let ran = runtime.block_on(sluice_wicket_runtime_firecracker::run(settings, launcher));
    // the tasks left are dropped, and with them the VMs they hold (killed)
    runtime.shutdown_timeout(Duration::from_secs(5));
    if let Some(state) = state {
        state.close();
    }
    match ran {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("sluice-wicket-runtime-firecracker failed: {e}");
            ExitCode::FAILURE
        }
    }
}
