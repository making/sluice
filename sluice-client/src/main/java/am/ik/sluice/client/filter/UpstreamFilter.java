package am.ik.sluice.client.filter;

import java.util.List;
import java.util.Objects;

import org.jspecify.annotations.Nullable;

/**
 * Sees the HTTP/1.1 exchanges of one route on the tunnel client, and may change them. A
 * call runs on the virtual thread relaying its direction: it may block, holding that
 * connection only, and the tunnel's flow control holds the other end meanwhile. The
 * request and the response side may run at once.
 */
@FunctionalInterface
public interface UpstreamFilter {

	/**
	 * A request head on its way to the upstream; its edits reach the upstream.
	 * @return the filter's part in this exchange; null leaves the exchange alone
	 */
	@Nullable Exchange onRequest(Request request, Headers headers);

	/**
	 * A request as it reached the client.
	 *
	 * @param route the route's key
	 * @param address the upstream's dial address
	 * @param method the method
	 * @param target the request target, as sent
	 * @param version the version, {@code HTTP/1.1}
	 * @param host the value of the Host header; null when absent
	 * @param bodyFollows whether a body follows the head
	 */
	record Request(String route, String address, String method, String target, String version, @Nullable String host,
			boolean bodyFollows) {

		/** The target's path: an absolute form's authority and any query left out. */
		public String path() {
			String path = this.target;
			int scheme = path.indexOf("://");
			if (scheme > 0 && !path.startsWith("/")) {
				int slash = path.indexOf('/', scheme + 3);
				path = slash < 0 ? "/" : path.substring(slash);
			}
			int query = path.indexOf('?');
			return query < 0 ? path : path.substring(0, query);
		}

		public static Builder builder() {
			return new Builder();
		}

		public static final class Builder {

			private String route = "";

			private String address = "";

			private @Nullable String method;

			private @Nullable String target;

			private String version = "HTTP/1.1";

			private @Nullable String host;

			private boolean bodyFollows;

			private Builder() {
			}

			public Builder route(String route) {
				this.route = route;
				return this;
			}

			public Builder address(String address) {
				this.address = address;
				return this;
			}

			public Builder method(String method) {
				this.method = method;
				return this;
			}

			public Builder target(String target) {
				this.target = target;
				return this;
			}

			public Builder version(String version) {
				this.version = version;
				return this;
			}

			public Builder host(@Nullable String host) {
				this.host = host;
				return this;
			}

			public Builder bodyFollows(boolean bodyFollows) {
				this.bodyFollows = bodyFollows;
				return this;
			}

			public Request build() {
				return new Request(this.route, this.address, Objects.requireNonNull(this.method, "method is required"),
						Objects.requireNonNull(this.target, "target is required"), this.version, this.host,
						this.bodyFollows);
			}

		}

	}

	/**
	 * The fields of a head. What frames its message cannot be changed, nor the Host of a
	 * request, which the server routed by.
	 */
	interface Headers {

		/** Every value of the header, in order; empty when absent. */
		List<String> values(String name);

		/**
		 * Replaces the header's values with this one, the name as given.
		 * @throws IllegalArgumentException when the header cannot be changed, or the name
		 * or value would make a line of its own
		 */
		void set(String name, String value);

		/**
		 * Adds this value of the header after its others, the name as given.
		 * @throws IllegalArgumentException when the header cannot be changed, or the name
		 * or value would make a line of its own
		 */
		void add(String name, String value);

		/**
		 * Removes the header.
		 * @throws IllegalArgumentException when the header cannot be changed
		 */
		void remove(String name);

		/** The fields as lines, {@code name: value}, in order. */
		List<String> lines();

		/**
		 * Replaces every field by these lines; the caller validated them. A plugin may
		 * take a {@code Content-Length} out with this, to replace the body.
		 */
		void replaceLines(List<String> lines);

	}

	/** What a filter does with a body: {@link BodyRewriter} or {@link BodyObserver}. */
	sealed interface BodyHandler permits BodyRewriter, BodyObserver {

	}

	/**
	 * Reads a body relayed as it was framed, on its payload.
	 */
	non-sealed interface BodyObserver extends BodyHandler {

		void onData(byte[] bytes, int offset, int length);

		/**
		 * The end of the observation, called once.
		 * @param complete false when the body was cut: the connection ended before its
		 * end
		 */
		void onEnd(boolean complete);

	}

	/**
	 * Rewrites a body as it streams, on its payload: what comes back goes on at once.
	 * <p>
	 * A response goes out chunked from its head on. A request's head waits for the first
	 * bytes to come back: chunked from there, or with a {@code Content-Length} when the
	 * first come back at the end, so a filter holding a whole body changes no framing.
	 */
	non-sealed interface BodyRewriter extends BodyHandler {

		byte[] onData(byte[] bytes, int offset, int length);

		/** The end of the body; what comes back is its last part. */
		byte[] onEnd();

		/** The exchange is over, whether {@link #onEnd()} was called or not. */
		default void onClose() {
		}

		/**
		 * Whether what came back and will come back is the body as it was: a request's
		 * head waiting for the first bytes then keeps its framing.
		 */
		default boolean unchanged() {
			return false;
		}

	}

	/**
	 * An answer in the upstream's place: the connection ends behind it, as after a
	 * refusal on the server. No filter sees it.
	 *
	 * @param status the status code
	 * @param lines the fields but the framing, as lines {@code name: value}
	 * @param body the body
	 */
	record LocalResponse(int status, List<String> lines, byte[] body) {

		public LocalResponse {
			lines = List.copyOf(lines);
		}

	}

	/** A filter's part in one request, from its head on, until its response ended. */
	interface Exchange {

		/**
		 * The request's body, when one follows.
		 * @return how to handle it; null relays it as it is, unseen
		 */
		default @Nullable BodyHandler onRequestBody() {
			return null;
		}

		/**
		 * The answer to give in the upstream's place, if any; asked after the request
		 * head and after each call on its body. An upstream that has begun answering
		 * keeps the answer: the response is cut.
		 */
		default @Nullable LocalResponse localResponse() {
			return null;
		}

		/**
		 * A final response head; 1xx heads and what follows a 101 pass unseen.
		 * @param bodiless whether no body follows it
		 * @return how to handle the body; null relays it as it is, unseen
		 */
		@Nullable BodyHandler onResponse(int status, Headers headers, boolean bodiless);

		/** No response will be offered: the connection ended, or left HTTP. */
		default void abandon() {
		}

	}

}
