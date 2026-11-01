//! `--listen`: the sole route served over plain TCP as well, relayed as the tunnel's
//! connections are (no sluice server needed: a local loop for trying a rootfs).

use std::sync::Arc;

use sluice_wicket_tunnel::routes::Routes;
use tokio::net::TcpListener;

use crate::route::Lease;
use crate::routes::Table;
use crate::wire;

/// Serves each connection with the sole route of the table of the moment; a
/// connection while the routes are not one is closed at once.
pub async fn run(listener: TcpListener, routes: Arc<Routes<Table>>) {
    loop {
        let Ok((client, _)) = listener.accept().await else {
            continue;
        };
        let Some(route) = routes.current().sole() else {
            eprintln!("listener: the routes are not one; closing the connection");
            continue;
        };
        tokio::spawn(async move {
            match route.open().await {
                Ok(Lease { stream, vm }) => {
                    if let Err(e) = wire::relay(client, stream).await {
                        eprintln!("listener ({}): {e}", route.label());
                    }
                    if let Some(vm) = vm {
                        vm.end().await;
                    }
                }
                Err(e) => eprintln!("listener ({}): {e}", route.label()),
            }
        });
    }
}
