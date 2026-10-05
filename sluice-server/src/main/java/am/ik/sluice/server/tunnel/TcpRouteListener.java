package am.ik.sluice.server.tunnel;

import java.util.Set;

/**
 * Callback invoked whenever the tcp route ports advertised by a client change; the data
 * plane side binds a listener per port and releases them when the session ends.
 */
public interface TcpRouteListener {

	/**
	 * Reconciles the listen ports owned by the client: ports missing from the given set
	 * are unbound, new ones are bound. An empty set releases every listener of the
	 * client.
	 * @param clientId owner of the ports
	 * @param listenPorts the ports currently advertised by the client
	 * @return the ports that were not bound (outside the configured range, owned by
	 * another live client, or failed to bind)
	 */
	Set<Integer> reconcile(String clientId, Set<Integer> listenPorts);

}
