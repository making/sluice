use std::process::ExitCode;

use sluice_flume::config::{self, Command};
use tokio::io::AsyncReadExt;

#[tokio::main]
async fn main() -> ExitCode {
    let config = match config::parse(std::env::args().skip(1), |name| std::env::var(name).ok()) {
        Ok(Command::Help) => {
            println!("{}", config::USAGE);
            return ExitCode::SUCCESS;
        }
        Ok(Command::Version) => {
            println!("{}", sluice_flume::describe_build());
            return ExitCode::SUCCESS;
        }
        Ok(Command::Serve(config)) => config,
        Err(message) => {
            eprintln!("{message}\n\n{}", config::USAGE);
            return ExitCode::from(2);
        }
    };
    let listener = match tokio::net::TcpListener::bind((config.host.as_str(), config.port)).await {
        Ok(listener) => listener,
        Err(e) => {
            eprintln!("cannot listen on {}:{}: {e}", config.host, config.port);
            return ExitCode::FAILURE;
        }
    };
    let address = listener.local_addr().map_or_else(
        |_| format!("{}:{}", config.host, config.port),
        |a| a.to_string(),
    );
    let scheme = if config.settings.tls.is_some() {
        "grpcs"
    } else {
        "grpc"
    };
    // a parent process waits for this line before it dials
    println!(
        "{} listening on {scheme}://{address} (ring {} lines)",
        sluice_flume::describe_build(),
        config.settings.ring_size
    );
    let exit_on_stdin_close = config.exit_on_stdin_close;
    match sluice_flume::serve(listener, config.settings, shutdown(exit_on_stdin_close)).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("sluice-flume failed: {e}");
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
