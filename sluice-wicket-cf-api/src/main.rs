use std::process::ExitCode;

use sluice_wicket_cf_api::config::{self, Command};
use tokio::io::AsyncReadExt;

#[tokio::main]
async fn main() -> ExitCode {
    let config = match config::parse(std::env::args().skip(1), |name| std::env::var(name).ok())
        .and_then(|command| match command {
            Command::Serve(_) => config::check_env(env_names()).map(|()| command),
            command => Ok(command),
        }) {
        Ok(Command::Help) => {
            println!("{}", config::USAGE);
            return ExitCode::SUCCESS;
        }
        Ok(Command::Version) => {
            println!("{}", sluice_wicket_cf_api::describe_build());
            return ExitCode::SUCCESS;
        }
        Ok(Command::Serve(config)) => *config,
        Err(message) => {
            eprintln!("{message}\n\n{}", config::USAGE);
            return ExitCode::from(2);
        }
    };
    if let Err(e) = sluice_wicket_cf_api::check_data_dir(&config.settings.data_dir) {
        eprintln!("{e}");
        return ExitCode::FAILURE;
    }
    let listener =
        match sluice_wicket_cf_api::bind(&config.listen, config.settings.oidc.is_none()).await {
            Ok(listener) => listener,
            Err(e) => {
                eprintln!("{e}");
                return ExitCode::FAILURE;
            }
        };
    let address = listener
        .local_addr()
        .map_or_else(|_| config.listen.clone(), |a| a.to_string());
    let wickets: Vec<String> = (config.settings.wickets.iter())
        .map(|wicket| format!("{}={}", wicket.name, wicket.url))
        .collect();
    // a parent process waits for this line before it dials
    println!(
        "{} listening on {}://{address} (wickets {}, domains {}, data {}, auth {}, flume {})",
        sluice_wicket_cf_api::describe_build(),
        if config.settings.tls.is_some() {
            "https"
        } else {
            "http"
        },
        wickets.join(" "),
        config.settings.domains.join(", "),
        config.settings.data_dir.display(),
        config
            .settings
            .oidc
            .as_ref()
            .map_or("any credentials".to_string(), |oidc| oidc.issuer.clone()),
        config
            .settings
            .flume
            .as_ref()
            .map_or("none", |flume| flume.url.as_str())
    );
    let exit_on_stdin_close = config.exit_on_stdin_close;
    match sluice_wicket_cf_api::serve(listener, config.settings, shutdown(exit_on_stdin_close))
        .await
    {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("sluice-wicket-cf-api failed: {e}");
            ExitCode::FAILURE
        }
    }
}

/// SIGINT, SIGTERM or, when asked, the end of stdin.
async fn shutdown(exit_on_stdin_close: bool) {
    let stdin_closed = async {
        if !exit_on_stdin_close {
            return std::future::pending().await;
        }
        let mut stdin = tokio::io::stdin();
        let mut buffer = [0u8; 256];
        while matches!(stdin.read(&mut buffer).await, Ok(n) if n > 0) {}
    };
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        () = terminated() => {}
        () = stdin_closed => {}
    }
}

#[cfg(unix)]
async fn terminated() {
    match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
        Ok(mut signal) => {
            signal.recv().await;
        }
        Err(_) => std::future::pending().await,
    }
}

#[cfg(not(unix))]
async fn terminated() {
    std::future::pending().await
}

/// The names of the process's environment variables.
fn env_names() -> impl Iterator<Item = String> {
    std::env::vars_os().map(|(name, _)| name.to_string_lossy().into_owned())
}
