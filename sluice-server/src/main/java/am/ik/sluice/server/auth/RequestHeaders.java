package am.ik.sluice.server.auth;

import java.util.List;

/**
 * The parts of a request head {@link RouteAuthenticator} reads: its method, target and
 * header values, whether of an HTTP/1.1 head or an h2 header block.
 */
public interface RequestHeaders {

	/** The request method. */
	String method();

	/**
	 * The request target as sent: the path and query of an HTTP/1.1 request line or of an
	 * h2 {@code :path}; empty when none.
	 */
	String target();

	/**
	 * Every value of the header, in the order they came; empty when absent.
	 * @param name the header name in lowercase
	 */
	List<String> values(String name);

}
