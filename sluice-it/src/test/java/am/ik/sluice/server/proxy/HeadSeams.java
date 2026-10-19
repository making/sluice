package am.ik.sluice.server.proxy;

/**
 * Sets the head seam of the HTTP/1.1 relays ({@link ReroutingRelay.HeadSeam}) from tests
 * outside the package.
 */
public final class HeadSeams {

	private HeadSeams() {
	}

	/**
	 * Runs {@code seam} between the choice of each request head's leg and the head's
	 * relaying, in the relays the server starts from now on.
	 */
	public static void set(DataProxyServer server, Runnable seam) {
		server.headSeam(relay -> seam.run());
	}

	/** Takes the seam out of the relays the server starts from now on. */
	public static void clear(DataProxyServer server) {
		server.headSeam(ReroutingRelay.HeadSeam.NONE);
	}

}
