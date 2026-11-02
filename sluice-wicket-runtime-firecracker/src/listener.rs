//! `--listen`: the sole route served over plain TCP as well, relayed as the tunnel's
//! connections are (no sluice server needed: a local loop for trying a rootfs).

use std::sync::Arc;

use sluice_wicket_tunnel::routes::Routes;
use sluice_wicket_tunnel::workers::Workers;
use tokio::net::{TcpListener, TcpStream};

use crate::route::Lease;
use crate::routes::Table;
use crate::wire;

/// Serves each connection with the sole route of the table of the moment; a
/// connection while the routes are not one is closed at once.
pub async fn run(listener: TcpListener, routes: Arc<Routes<Table>>, workers: Arc<Workers>) {
    loop {
        let Ok((client, peer)) = listener.accept().await else {
            continue;
        };
        let Some(route) = routes.current().sole() else {
            eprintln!("listener: the routes are not one; closing the connection");
            continue;
        };
        let Ok(client) = client.into_std() else {
            continue;
        };
        workers.place().spawn(async move {
            // the worker's from here on
            let Ok(client) = TcpStream::from_std(client) else {
                return;
            };
            match route.open(&peer.ip().to_string()).await {
                Ok(Lease { stream, hold }) => {
                    let mut client = client;
                    let relayed = wire::relay(&mut client, stream, route.spec.idle).await;
                    // a reused clone kept before the client sees the end
                    let ended = hold.end(relayed.is_ok());
                    drop(client);
                    if let Err(e) = relayed {
                        eprintln!("listener ({}): {e}", route.label());
                    }
                    ended.await;
                }
                Err(e) => eprintln!("listener ({}): {e}", route.label()),
            }
        });
    }
}
