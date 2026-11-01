//! `--listen`: the sole route served over plain TCP as well, relayed as the tunnel's
//! connections are (no sluice server needed: a local loop for trying a rootfs).

use std::sync::Arc;

use tokio::net::TcpListener;

use crate::route::Route;
use crate::wire;

pub async fn run(listener: TcpListener, route: Arc<Route>) {
    loop {
        let Ok((client, _)) = listener.accept().await else {
            continue;
        };
        let route = route.clone();
        tokio::spawn(async move {
            match route.open().await {
                Ok(lease) => {
                    if let Err(e) = wire::relay(client, lease.stream).await {
                        eprintln!("listener ({}): {e}", route.spec.host);
                    }
                }
                Err(e) => eprintln!("listener ({}): {e}", route.spec.host),
            }
        });
    }
}
