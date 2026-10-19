package am.ik.sluice.server.route;

/**
 * Open / shut state of a route's gate. Routes default to open; the console toggles the
 * gate in memory and every node starts from its configured default again on restart.
 */
public enum GateState {

	/** The route serves traffic. */
	OPEN,

	/** The route serves no traffic; requests fall through to the next candidate. */
	SHUT

}
