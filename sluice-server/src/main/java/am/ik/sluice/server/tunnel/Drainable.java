package am.ik.sluice.server.tunnel;

/**
 * Implemented by the data plane listeners; the cluster lifecycle calls it at drain start
 * so they stop accepting new connections while the in-flight ones finish.
 */
public interface Drainable {

	/** Stops accepting new connections; the ones in flight keep running. */
	void beginDrain();

}
