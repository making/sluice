use std::process::ExitCode;

use sluice_wicket_runtime_wasi::config::{self, Command};

fn main() -> ExitCode {
    let settings = match config::parse(std::env::args().skip(1), |name| std::env::var(name).ok())
        .and_then(|command| match command {
            Command::Run(_) => config::check_env(env_names()).map(|()| command),
            command => Ok(command),
        }) {
        Ok(Command::Help) => {
            println!("{}", config::USAGE);
            return ExitCode::SUCCESS;
        }
        Ok(Command::Version) => {
            println!("{}", sluice_wicket_runtime_wasi::describe_build());
            return ExitCode::SUCCESS;
        }
        Ok(Command::Run(settings)) => *settings,
        Err(message) => {
            eprintln!("{message}\n\n{}", config::USAGE);
            return ExitCode::from(2);
        }
    };
    let runtime = match tokio::runtime::Runtime::new() {
        Ok(runtime) => runtime,
        Err(e) => {
            eprintln!("cannot start the runtime: {e}");
            return ExitCode::FAILURE;
        }
    };
    let ran = runtime.block_on(sluice_wicket_runtime_wasi::run(settings));
    // drained: the tasks left (guest instances among them) are not waited for,
    // as dropping the runtime would
    runtime.shutdown_background();
    match ran {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("sluice-wicket-runtime-wasi failed: {e}");
            ExitCode::FAILURE
        }
    }
}

/// The names of the process's environment variables.
fn env_names() -> impl Iterator<Item = String> {
    std::env::vars_os().map(|(name, _)| name.to_string_lossy().into_owned())
}
