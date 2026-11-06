package am.ik.sluice.client.filter;

import java.io.ByteArrayOutputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.jspecify.annotations.Nullable;
import tools.jackson.core.JacksonException;
import tools.jackson.core.JsonParser;
import tools.jackson.core.JsonToken;
import tools.jackson.core.ObjectReadContext;
import tools.jackson.core.json.JsonFactory;

import am.ik.sluice.client.config.FilterConfig;
import am.ik.sluice.tunnel.TunnelBudget;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

import org.springframework.util.unit.DataSize;

/**
 * Limits what a request of the APIs {@code llm-usage} knows may ask for: the models
 * allowed, and a ceiling on the output tokens, which a larger value is refused or lowered
 * to. A request past them is answered in the upstream's place, in the API's error format,
 * before any of it reaches the upstream: its body is held, read by a strict parser as the
 * upstream reads it, and refused when it cannot be read. With {@code other-paths} set, a
 * request of any other path but those is refused too.
 */
final class LlmGuardFilter implements UpstreamFilter {

	/** The name routes use this filter by. */
	static final String NAME = "llm-guard";

	private static final Set<String> ARGS = Set.of("models", "max-output-tokens", "over-max-output-tokens",
			"other-paths");

	/** Methods that spend no tokens. */
	private static final Set<String> READS = Set.of("GET", "HEAD", "OPTIONS");

	private static final DataSize DEFAULT_MAX_BODY = DataSize.ofMegabytes(16);

	private static final JsonFactory JSON = JsonFactory.builder().build();

	private static final byte[] EMPTY = new byte[0];

	private static final byte[] NULL = "null".getBytes(StandardCharsets.US_ASCII);

	private static final String UNREADABLE = "the request body is not a JSON object this route can read";

	private final String route;

	private final MeterRegistry meters;

	private final TunnelBudget budget;

	/** Exact, or a prefix ending in {@code *}; empty for any. */
	private final List<String> models;

	/** The most output tokens; null for no ceiling. */
	private final @Nullable BigDecimal ceiling;

	private final boolean lower;

	/** The paths but the APIs' that pass; null for every one. */
	private final @Nullable List<String> otherPaths;

	private final long maxBody;

	private LlmGuardFilter(UpstreamFilterFactory.Context context) {
		this.route = context.route();
		this.meters = context.meters();
		this.budget = context.budget();
		FilterConfig settings = context.settings();
		Map<String, String> args = settings.args();
		for (String arg : args.keySet()) {
			if (!ARGS.contains(arg)) {
				throw new IllegalArgumentException("filter " + NAME + " takes no argument " + arg);
			}
		}
		this.models = list(args.get("models"));
		String max = args.getOrDefault("max-output-tokens", "").trim();
		if (!max.isEmpty()) {
			long parsed;
			try {
				parsed = Long.parseLong(max);
			}
			catch (NumberFormatException e) {
				parsed = 0;
			}
			if (parsed < 1) {
				throw new IllegalArgumentException(
						"filter " + NAME + " takes a positive max-output-tokens, not " + max);
			}
			this.ceiling = BigDecimal.valueOf(parsed);
		}
		else {
			this.ceiling = null;
		}
		if (this.models.isEmpty() && this.ceiling == null) {
			throw new IllegalArgumentException("filter " + NAME + " needs models or max-output-tokens");
		}
		String over = args.getOrDefault("over-max-output-tokens", "reject").trim().toLowerCase(Locale.ROOT);
		this.lower = switch (over) {
			case "reject" -> false;
			case "lower" -> true;
			default -> throw new IllegalArgumentException(
					"filter " + NAME + " takes reject or lower for over-max-output-tokens, not " + over);
		};
		if (this.lower && this.ceiling == null) {
			throw new IllegalArgumentException("filter " + NAME + " lowers nothing without max-output-tokens");
		}
		String others = args.get("other-paths");
		this.otherPaths = others == null ? null : list(others);
		DataSize limit = settings.maxBody();
		this.maxBody = (limit == null ? DEFAULT_MAX_BODY : limit).toBytes();
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
				return new LlmGuardFilter(context);
			}

		};
	}

	private static List<String> list(@Nullable String values) {
		List<String> listed = new ArrayList<>();
		if (values != null) {
			for (String value : values.split(",")) {
				if (!value.isBlank()) {
					listed.add(value.trim());
				}
			}
		}
		return List.copyOf(listed);
	}

	@Override
	public @Nullable Exchange onRequest(Request request, Headers headers) {
		String path = request.path();
		LlmApi api = LlmApi.ofLoosePath(path);
		if (api == null) {
			if (this.otherPaths == null || FilterChain.matches(this.otherPaths, path) && !this.otherPaths.isEmpty()) {
				return null;
			}
			count("none", "path");
			return new Refusal(new LocalResponse(403, List.of("Content-Type: application/json"),
					anthropicError("permission_error", path + " is not allowed on this route")));
		}
		if (READS.contains(request.method())) {
			return null;
		}
		List<String> encodings = headers.values("content-encoding");
		if (!encodings.isEmpty() && !(encodings.size() == 1 && encodings.get(0).trim().equalsIgnoreCase("identity"))) {
			return refused(api, "unreadable", 400, null, "the request body is encoded");
		}
		if (!request.bodyFollows()) {
			return refused(api, "unreadable", 400, null, UNREADABLE);
		}
		return new Held(api);
	}

	private Refusal refused(LlmApi api, String outcome, int status, @Nullable String param, String message) {
		count(api.tag, outcome);
		return new Refusal(error(api, status, param, message));
	}

	private void count(String api, String outcome) {
		Counter.builder("sluice.client.filter.llm.guard.requests")
			.description("Requests of an LLM API a route's llm-guard filter decided on, by what it decided")
			.tag("route", this.route)
			.tag("api", api)
			.tag("outcome", outcome)
			.register(this.meters)
			.increment();
	}

	/** An error as the API gives it. */
	private static LocalResponse error(LlmApi api, int status, @Nullable String param, String message) {
		byte[] body;
		if (api.openAi()) {
			body = ("{\"error\":{\"message\":" + quoted(message) + ",\"type\":\"invalid_request_error\",\"param\":"
					+ (param == null ? "null" : quoted(param)) + ",\"code\":null}}")
				.getBytes(StandardCharsets.UTF_8);
		}
		else {
			body = anthropicError(status == 413 ? "request_too_large" : "invalid_request_error", message);
		}
		return new LocalResponse(status, List.of("Content-Type: application/json"), body);
	}

	private static byte[] anthropicError(String type, String message) {
		return ("{\"type\":\"error\",\"error\":{\"type\":\"" + type + "\",\"message\":" + quoted(message) + "}}")
			.getBytes(StandardCharsets.UTF_8);
	}

	private static String quoted(String text) {
		StringBuilder quoted = new StringBuilder(text.length() + 2).append('"');
		for (int i = 0; i < text.length(); i++) {
			char c = text.charAt(i);
			if (c == '"' || c == '\\') {
				quoted.append('\\').append(c);
			}
			else if (c < 0x20) {
				quoted.append(String.format("\\u%04x", (int) c));
			}
			else {
				quoted.append(c);
			}
		}
		return quoted.append('"').toString();
	}

	private boolean allowed(String model) {
		if (this.models.isEmpty()) {
			return true;
		}
		for (String pattern : this.models) {
			if (pattern.endsWith("*") ? model.startsWith(pattern.substring(0, pattern.length() - 1))
					: model.equals(pattern)) {
				return true;
			}
		}
		return false;
	}

	/** A request answered at its head. */
	private record Refusal(LocalResponse answer) implements Exchange {

		@Override
		public LocalResponse localResponse() {
			return this.answer;
		}

		@Override
		public @Nullable BodyHandler onResponse(int status, Headers headers, boolean bodiless) {
			return null;
		}

	}

	/** A replacement of the bytes from {@code start} to {@code end} of a body. */
	private record Edit(int start, int end, byte[] bytes) {
	}

	/** What a body was found to be: refused with an answer, or passed with edits. */
	private record Decision(@Nullable LocalResponse answer, List<Edit> edits) {
	}

	/**
	 * One request of an API: its body held whole, decided on at its end, before any of it
	 * goes on.
	 */
	private final class Held implements Exchange, BodyRewriter {

		private final LlmApi api;

		private final TunnelBudget.Account account = LlmGuardFilter.this.budget.account();

		private ByteArrayOutputStream held = new ByteArrayOutputStream();

		private volatile @Nullable LocalResponse answer;

		private boolean edited;

		Held(LlmApi api) {
			this.api = api;
		}

		@Override
		public BodyHandler onRequestBody() {
			return this;
		}

		@Override
		public @Nullable LocalResponse localResponse() {
			return this.answer;
		}

		@Override
		public @Nullable BodyHandler onResponse(int status, Headers headers, boolean bodiless) {
			return null;
		}

		@Override
		public byte[] onData(byte[] bytes, int offset, int length) {
			if (this.answer != null) {
				return EMPTY;
			}
			long wanted = (long) this.held.size() + length;
			if (wanted <= LlmGuardFilter.this.maxBody && this.account.tryGrow(wanted)) {
				this.held.write(bytes, offset, length);
				return EMPTY;
			}
			release();
			refuse("unreadable", 413, null,
					"the request body is past the " + LlmGuardFilter.this.maxBody + " bytes this route holds");
			return EMPTY;
		}

		@Override
		public byte[] onEnd() {
			if (this.answer != null) {
				return EMPTY;
			}
			byte[] body = this.held.toByteArray();
			release();
			Decision decision = decide(body);
			if (decision.answer() != null) {
				this.answer = decision.answer();
				return EMPTY;
			}
			if (decision.edits().isEmpty()) {
				count(this.api.tag, "allowed");
				return body;
			}
			count(this.api.tag, "lowered");
			this.edited = true;
			return edited(body, decision.edits());
		}

		@Override
		public void onClose() {
			release();
		}

		@Override
		public boolean unchanged() {
			return !this.edited;
		}

		private void release() {
			this.held = new ByteArrayOutputStream(0);
			this.account.close();
		}

		private void refuse(String outcome, int status, @Nullable String param, String message) {
			count(this.api.tag, outcome);
			this.answer = error(this.api, status, param, message);
		}

		private Decision refusal(String outcome, @Nullable String param, String message) {
			count(this.api.tag, outcome);
			return new Decision(error(this.api, 400, param, message), List.of());
		}

		/** The top level fields that decide, read as the upstream's parser reads them. */
		private Decision decide(byte[] body) {
			String model = null;
			Set<String> seen = new HashSet<>();
			List<Limit> limits = new ArrayList<>();
			int properties = 0;
			int closing;
			try (JsonParser parser = JSON.createParser(ObjectReadContext.empty(), body)) {
				if (parser.nextToken() != JsonToken.START_OBJECT) {
					return refusal("unreadable", null, UNREADABLE);
				}
				JsonToken token;
				while ((token = parser.nextToken()) == JsonToken.PROPERTY_NAME) {
					properties++;
					String name = parser.currentName();
					JsonToken value = parser.nextToken();
					boolean decides = name.equals("model") || LlmApi.OUTPUT_LIMITS.contains(name);
					if (decides && !seen.add(name)) {
						// parsers differ on which of two counts
						return refusal("unreadable", null, UNREADABLE);
					}
					if (name.equals("model")) {
						model = value == JsonToken.VALUE_STRING ? parser.getString() : null;
					}
					else if (decides) {
						Limit limit = limit(name, value, parser, body);
						if (limit == null) {
							return refusal("unreadable", null, UNREADABLE);
						}
						limits.add(limit);
					}
					parser.skipChildren();
				}
				closing = (int) parser.currentTokenLocation().getByteOffset();
				if (token != JsonToken.END_OBJECT || parser.nextToken() != null || closing < 0 || closing >= body.length
						|| body[closing] != '}') {
					return refusal("unreadable", null, UNREADABLE);
				}
			}
			catch (JacksonException e) {
				return refusal("unreadable", null, UNREADABLE);
			}
			if (!LlmGuardFilter.this.models.isEmpty()) {
				if (model == null) {
					return refusal("model", "model", "model is required on this route");
				}
				if (!allowed(model)) {
					return refusal("model", "model", "model " + model + " is not allowed on this route");
				}
			}
			BigDecimal ceiling = LlmGuardFilter.this.ceiling;
			return ceiling == null ? new Decision(null, List.of()) : limited(ceiling, limits, properties, closing);
		}

		/** The output limits against the ceiling: refused, or lowered to it. */
		private Decision limited(BigDecimal ceiling, List<Limit> limits, int properties, int closing) {
			List<Edit> edits = new ArrayList<>();
			byte[] lowered = ceiling.toPlainString().getBytes(StandardCharsets.US_ASCII);
			boolean bounded = false;
			for (Limit limit : limits) {
				if (limit.number() == null && !limit.isNull()) {
					return refusal("max_tokens", limit.name(), limit.name() + " must be a number on this route");
				}
				BigDecimal number = limit.number();
				boolean over = number == null || number.compareTo(ceiling) > 0;
				if (over && (number != null || LlmGuardFilter.this.lower)) {
					if (!LlmGuardFilter.this.lower) {
						return refusal("max_tokens", limit.name(),
								limit.name() + " " + limit.text() + " is past this route's " + ceiling.toPlainString());
					}
					edits.add(new Edit(limit.start(), limit.end(), lowered));
				}
				bounded |= this.api.outputLimits.contains(limit.name())
						&& (number != null || LlmGuardFilter.this.lower);
			}
			if (!bounded) {
				String field = this.api.outputLimits.get(0);
				if (!LlmGuardFilter.this.lower) {
					return refusal("max_tokens", field,
							field + " up to " + ceiling.toPlainString() + " is required on this route");
				}
				String added = (properties > 0 ? "," : "") + "\"" + field + "\":" + ceiling.toPlainString();
				edits.add(new Edit(closing, closing, added.getBytes(StandardCharsets.US_ASCII)));
			}
			return new Decision(null, edits);
		}

	}

	/**
	 * An output limit as the body has it: a number, null, or something else.
	 *
	 * @param name the field
	 * @param number its value when a number
	 * @param isNull whether it is null
	 * @param text the value as written, when a number
	 * @param start where the value begins in the body
	 * @param end where it ends
	 */
	private record Limit(String name, @Nullable BigDecimal number, boolean isNull, String text, int start, int end) {
	}

	/** The limit at the parser's value; null when its place in the body is not found. */
	private static @Nullable Limit limit(String name, JsonToken value, JsonParser parser, byte[] body) {
		if (value != JsonToken.VALUE_NUMBER_INT && value != JsonToken.VALUE_NUMBER_FLOAT
				&& value != JsonToken.VALUE_NULL) {
			return new Limit(name, null, false, "", -1, -1);
		}
		long offset = parser.currentTokenLocation().getByteOffset();
		if (offset < 0 || offset >= body.length) {
			return null;
		}
		int start = (int) offset;
		if (value == JsonToken.VALUE_NULL) {
			boolean found = start + NULL.length <= body.length;
			for (int i = 0; found && i < NULL.length; i++) {
				found = body[start + i] == NULL[i];
			}
			return found ? new Limit(name, null, true, "null", start, start + NULL.length) : null;
		}
		int end = start;
		while (end < body.length && isNumberByte(body[end])) {
			end++;
		}
		if (end == start) {
			return null;
		}
		String text = new String(body, start, end - start, StandardCharsets.US_ASCII);
		BigDecimal number = parser.getDecimalValue();
		return new BigDecimal(text).compareTo(number) == 0 ? new Limit(name, number, false, text, start, end) : null;
	}

	private static boolean isNumberByte(byte b) {
		return b >= '0' && b <= '9' || b == '-' || b == '+' || b == '.' || b == 'e' || b == 'E';
	}

	private static byte[] edited(byte[] body, List<Edit> edits) {
		List<Edit> ordered = new ArrayList<>(edits);
		ordered.sort((a, b) -> Integer.compare(a.start(), b.start()));
		ByteArrayOutputStream out = new ByteArrayOutputStream(body.length + 32);
		int at = 0;
		for (Edit edit : ordered) {
			out.write(body, at, edit.start() - at);
			out.write(edit.bytes(), 0, edit.bytes().length);
			at = edit.end();
		}
		out.write(body, at, body.length - at);
		return out.toByteArray();
	}

}
