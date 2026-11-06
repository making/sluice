package am.ik.sluice.client.filter;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.jspecify.annotations.Nullable;
import org.springframework.boot.convert.DurationStyle;

/**
 * CORS for a route's allowed origins, on the client: a preflight from one is answered in
 * the upstream's place, a response to one carries the CORS headers, the upstream's
 * replaced. A preflight or request from another origin passes as it is. Every response
 * the upstream gives gets {@code Vary: Origin}.
 */
final class CorsFilter implements UpstreamFilter {

	/** The name routes use this filter by. */
	static final String NAME = "cors";

	private static final Pattern TOKEN = Pattern.compile("[!#$%&'*+.^_`|~0-9A-Za-z-]+");

	private static final Pattern ORIGIN = Pattern.compile("[A-Za-z][A-Za-z0-9+.-]*://[^/?#\\s,]+");

	private static final Set<String> ARGS = Set.of("allowed-origins", "allowed-methods", "allowed-headers",
			"exposed-headers", "allow-credentials", "max-age");

	/** What a response to an allowed origin carries only as this filter says. */
	private static final List<String> RESPONSE_HEADERS = List.of("access-control-allow-origin",
			"access-control-allow-credentials", "access-control-expose-headers");

	private static final List<String> PREFLIGHT_VARY = List.of("Origin", "Access-Control-Request-Method",
			"Access-Control-Request-Headers");

	/** Lower case; null for any. */
	private final @Nullable Set<String> origins;

	/** As given; null for any requested. */
	private final @Nullable List<String> methods;

	/** Lower case; null for any requested. */
	private final @Nullable Set<String> headers;

	private final List<String> exposed;

	private final boolean credentials;

	private final @Nullable Duration maxAge;

	private CorsFilter(@Nullable Set<String> origins, @Nullable List<String> methods, @Nullable Set<String> headers,
			List<String> exposed, boolean credentials, @Nullable Duration maxAge) {
		this.origins = origins;
		this.methods = methods;
		this.headers = headers;
		this.exposed = exposed;
		this.credentials = credentials;
		this.maxAge = maxAge;
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
	 * @throws IllegalArgumentException when an arg is unknown or malformed, or the args
	 * ask for what browsers reject
	 */
	static CorsFilter of(Map<String, String> args) {
		for (String arg : args.keySet()) {
			if (!ARGS.contains(arg)) {
				throw new IllegalArgumentException("filter " + NAME + " takes no argument " + arg);
			}
		}
		boolean credentials = switch (args.getOrDefault("allow-credentials", "").trim().toLowerCase(Locale.ROOT)) {
			case "", "false" -> false;
			case "true" -> true;
			default ->
				throw new IllegalArgumentException("filter " + NAME + " takes true or false for allow-credentials");
		};
		List<String> origins = list(args, "allowed-origins", "");
		if (origins.isEmpty()) {
			throw new IllegalArgumentException("filter " + NAME + " needs allowed-origins");
		}
		boolean anyOrigin = wildcard(origins, "allowed-origins");
		if (anyOrigin && credentials) {
			throw new IllegalArgumentException(
					"filter " + NAME + " cannot allow credentials to any origin (*): browsers reject it");
		}
		for (String origin : origins) {
			if (!anyOrigin && !origin.equals("null") && !ORIGIN.matcher(origin).matches()) {
				throw new IllegalArgumentException("filter " + NAME
						+ " takes origins as scheme://host[:port], with no path, null or *, not " + origin);
			}
		}
		List<String> methods = tokens(args, "allowed-methods", "GET,HEAD,POST");
		List<String> headers = tokens(args, "allowed-headers", "");
		List<String> exposed = tokens(args, "exposed-headers", "");
		if (wildcard(exposed, "exposed-headers") && credentials) {
			throw new IllegalArgumentException(
					"filter " + NAME + " cannot expose * with credentials: browsers read it as a header named *");
		}
		Duration maxAge = null;
		String age = args.getOrDefault("max-age", "").trim();
		if (!age.isEmpty()) {
			maxAge = DurationStyle.detectAndParse(age, ChronoUnit.SECONDS);
			if (maxAge.isNegative()) {
				throw new IllegalArgumentException("filter " + NAME + " takes no negative max-age");
			}
		}
		return new CorsFilter(anyOrigin ? null : lowerCase(origins),
				wildcard(methods, "allowed-methods") ? null : methods,
				wildcard(headers, "allowed-headers") ? null : lowerCase(headers), exposed, credentials, maxAge);
	}

	private static List<String> list(Map<String, String> args, String key, String defaults) {
		List<String> values = new ArrayList<>();
		for (String value : args.getOrDefault(key, defaults).split(",")) {
			if (!value.isBlank()) {
				values.add(value.trim());
			}
		}
		return values;
	}

	private static List<String> tokens(Map<String, String> args, String key, String defaults) {
		List<String> values = list(args, key, defaults);
		for (String value : values) {
			if (!value.equals("*") && !TOKEN.matcher(value).matches()) {
				throw new IllegalArgumentException("filter " + NAME + " takes names in " + key + ", not " + value);
			}
		}
		return values;
	}

	/** Whether the values are {@code *}: alone, as one with others is a mistake. */
	private static boolean wildcard(List<String> values, String key) {
		if (!values.contains("*")) {
			return false;
		}
		if (values.size() > 1) {
			throw new IllegalArgumentException("filter " + NAME + " takes * alone in " + key);
		}
		return true;
	}

	private static Set<String> lowerCase(List<String> values) {
		Set<String> lowered = new LinkedHashSet<>();
		values.forEach(value -> lowered.add(value.toLowerCase(Locale.ROOT)));
		return Set.copyOf(lowered);
	}

	@Override
	public Exchange onRequest(Request request, Headers headers) {
		String origin = origin(headers);
		if (origin != null && request.method().equals("OPTIONS")) {
			List<String> requested = headers.values("access-control-request-method");
			if (requested.size() == 1) {
				LocalResponse answer = preflight(origin, requested.get(0).trim(),
						list(headers.values("access-control-request-headers")));
				return new Exchange() {

					@Override
					public LocalResponse localResponse() {
						return answer;
					}

					@Override
					public @Nullable BodyHandler onResponse(int status, Headers responseHeaders, boolean bodiless) {
						return null;
					}

				};
			}
		}
		return (status, responseHeaders, bodiless) -> {
			if (origin != null) {
				RESPONSE_HEADERS.forEach(responseHeaders::remove);
				responseHeaders.set("Access-Control-Allow-Origin", allowOrigin(origin));
				if (this.credentials) {
					responseHeaders.set("Access-Control-Allow-Credentials", "true");
				}
				if (!this.exposed.isEmpty()) {
					responseHeaders.set("Access-Control-Expose-Headers", String.join(", ", this.exposed));
				}
			}
			responseHeaders.vary("Origin");
			return null;
		};
	}

	/** The request's origin when it is one allowed; null otherwise. */
	private @Nullable String origin(Headers headers) {
		List<String> values = headers.values("origin");
		if (values.size() != 1) {
			return null;
		}
		String origin = values.get(0).trim();
		return this.origins == null || this.origins.contains(origin.toLowerCase(Locale.ROOT)) ? origin : null;
	}

	private String allowOrigin(String origin) {
		return this.origins == null ? "*" : origin;
	}

	private static List<String> list(List<String> values) {
		List<String> names = new ArrayList<>();
		for (String value : values) {
			for (String name : value.split(",")) {
				if (!name.isBlank()) {
					names.add(name.trim());
				}
			}
		}
		return names;
	}

	/** The answer to an allowed origin's preflight: 403 for what it may not ask. */
	private LocalResponse preflight(String origin, String method, List<String> headers) {
		List<String> lines = new ArrayList<>();
		lines.add("Vary: " + String.join(", ", PREFLIGHT_VARY));
		boolean allowed = (this.methods == null || this.methods.contains(method)) && TOKEN.matcher(method).matches();
		for (String header : headers) {
			allowed &= TOKEN.matcher(header).matches()
					&& (this.headers == null || this.headers.contains(header.toLowerCase(Locale.ROOT)));
		}
		if (!allowed) {
			return new LocalResponse(403, lines, new byte[0]);
		}
		lines.add("Access-Control-Allow-Origin: " + allowOrigin(origin));
		lines.add("Access-Control-Allow-Methods: " + (this.methods == null ? method : String.join(", ", this.methods)));
		if (!headers.isEmpty()) {
			lines.add("Access-Control-Allow-Headers: " + String.join(", ", headers));
		}
		if (this.credentials) {
			lines.add("Access-Control-Allow-Credentials: true");
		}
		if (this.maxAge != null) {
			lines.add("Access-Control-Max-Age: " + this.maxAge.toSeconds());
		}
		return new LocalResponse(204, lines, new byte[0]);
	}

}
