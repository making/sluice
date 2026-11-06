package am.ik.sluice.client.filter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

import org.jspecify.annotations.Nullable;

/**
 * Sets, adds and removes the headers of a route's requests and responses, as its
 * {@code args} say: {@code <request|response>.<set|add|remove>.<header>}, the header's
 * value as the arg's ({@code true} or empty removes, {@code false} keeps). Removals go
 * first, then what is set, then what is added. Every edit is checked at the start, so
 * none fails on an exchange; a value is never part of a message, as it may be a secret.
 */
final class HeadersFilter implements UpstreamFilter {

	/** The name routes use this filter by. */
	static final String NAME = "headers";

	private final Edits request;

	private final Edits response;

	private HeadersFilter(Edits request, Edits response) {
		this.request = request;
		this.response = response;
	}

	/** The factory of this filter. */
	static UpstreamFilterFactory factory() {
		return new UpstreamFilterFactory() {

			@Override
			public String name() {
				return NAME;
			}

			@Override
			public UpstreamFilter create(Context context) {
				return of(context.settings().args());
			}

		};
	}

	/**
	 * The filter of these args.
	 * @throws IllegalArgumentException when an arg is not an edit, or an edit one a head
	 * cannot take
	 */
	static HeadersFilter of(Map<String, String> args) {
		Edits.Builder request = new Edits.Builder();
		Edits.Builder response = new Edits.Builder();
		// in the order of their keys, whatever the map's
		for (Map.Entry<String, String> arg : new TreeMap<>(args).entrySet()) {
			String key = arg.getKey();
			String[] parts = key.split("\\.", 3);
			Edits.Builder edits = parts.length < 3 ? null : switch (parts[0]) {
				case "request" -> request;
				case "response" -> response;
				default -> null;
			};
			if (edits == null || !List.of("set", "add", "remove").contains(parts[1])) {
				throw new IllegalArgumentException(
						"filter " + NAME + " takes args <request|response>.<set|add|remove>.<header>, not " + key);
			}
			String name = parts[2];
			if (name.indexOf('.') >= 0) {
				// an environment variable's '_' binds as '.': X_API_KEY is not x-api-key
				throw new IllegalArgumentException("filter " + NAME + " takes no header name with '.': " + key);
			}
			if (name.equalsIgnoreCase("host")) {
				throw new IllegalArgumentException("header " + name + " cannot be changed by a filter");
			}
			String value = arg.getValue();
			if (parts[1].equals("remove")) {
				UpstreamFilterPipe.checkEdit(name, null);
				edits.remove(key, name, removes(key, value));
			}
			else {
				UpstreamFilterPipe.checkEdit(name, value);
				edits.put(parts[1], key, name, value);
			}
		}
		return new HeadersFilter(request.build(), response.build());
	}

	private static boolean removes(String key, String value) {
		return switch (value.trim().toLowerCase(Locale.ROOT)) {
			case "", "true" -> true;
			case "false" -> false;
			default -> throw new IllegalArgumentException("filter " + NAME + " takes true or false for " + key);
		};
	}

	@Override
	public @Nullable Exchange onRequest(Request request, Headers headers) {
		this.request.apply(headers);
		if (this.response.isEmpty()) {
			return null;
		}
		return (status, responseHeaders, bodiless) -> {
			this.response.apply(responseHeaders);
			return null;
		};
	}

	/** The edits of one direction. */
	private record Edits(List<String> removed, List<Header> set, List<Header> added) {

		boolean isEmpty() {
			return this.removed.isEmpty() && this.set.isEmpty() && this.added.isEmpty();
		}

		void apply(Headers headers) {
			this.removed.forEach(headers::remove);
			this.set.forEach(header -> headers.set(header.name(), header.value()));
			this.added.forEach(header -> headers.add(header.name(), header.value()));
		}

		static final class Builder {

			private final List<String> removed = new ArrayList<>();

			private final List<Header> set = new ArrayList<>();

			private final List<Header> added = new ArrayList<>();

			/**
			 * The key of each header's edit, by operation: one each, whatever its case.
			 */
			private final Map<String, String> keys = new HashMap<>();

			void remove(String key, String name, boolean removes) {
				once("remove", key, name);
				if (removes) {
					this.removed.add(name);
				}
			}

			void put(String operation, String key, String name, String value) {
				once(operation, key, name);
				(operation.equals("set") ? this.set : this.added).add(new Header(name, value));
			}

			private void once(String operation, String key, String name) {
				String earlier = this.keys.putIfAbsent(operation + "." + name.toLowerCase(Locale.ROOT), key);
				if (earlier != null) {
					throw new IllegalArgumentException(
							"filter " + NAME + " is given " + earlier + " and " + key + ": header names have no case");
				}
			}

			Edits build() {
				return new Edits(List.copyOf(this.removed), List.copyOf(this.set), List.copyOf(this.added));
			}

		}

	}

	private record Header(String name, String value) {

	}

}
