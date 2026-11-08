package am.ik.sluice.client.filter;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.jspecify.annotations.Nullable;

import am.ik.sluice.client.config.FilterConfig;
import am.ik.sluice.tunnel.TunnelBudget;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

import org.springframework.util.unit.DataSize;

/**
 * The token usage of the OpenAI compatible APIs and of the Anthropic API, read from their
 * responses as they pass: one scanner of JSON and server-sent events, a dialect per API
 * chosen by exact request path (the {@code /v1} prefix aside, as some upstreams serve the
 * endpoints without it), the same counters out. It holds nothing of a response, so a
 * filtered connection costs no more for it.
 * <p>
 * {@code include-usage} ({@code args}) adds {@code stream_options.include_usage} to a
 * request of a stream that does not set it: an OpenAI compatible stream reports no usage
 * without it, and its client gets one chunk more, with no choices.
 */
public final class LlmUsageFilter implements UpstreamFilter {

	/** The name routes use this filter by. */
	public static final String NAME = "llm-usage";

	/** Models counted apart; the others count as {@code other}. */
	private static final int MAX_MODELS = 256;

	private static final DataSize DEFAULT_MAX_BODY = DataSize.ofMegabytes(16);

	private static final String UNKNOWN = "unknown";

	private static final byte[] EMPTY = new byte[0];

	private static final byte[] DATA = "data:".getBytes(StandardCharsets.US_ASCII);

	private static final byte[] DONE = "[DONE]".getBytes(StandardCharsets.US_ASCII);

	private static final byte[] INCLUDE_USAGE = ",\"stream_options\":{\"include_usage\":true}}"
		.getBytes(StandardCharsets.US_ASCII);

	private static final Set<String> USAGE = Set.of("input_tokens", "output_tokens", "prompt_tokens",
			"completion_tokens", "cache_read_input_tokens", "cache_creation_input_tokens",
			"prompt_tokens_details.cached_tokens", "prompt_tokens_details.cache_write_tokens",
			"input_tokens_details.cached_tokens", "input_tokens_details.cache_write_tokens");

	private final String route;

	private final MeterRegistry meters;

	private final boolean includeUsage;

	private final long maxBody;

	private final TunnelBudget budget;

	private final Set<String> models = ConcurrentHashMap.newKeySet();

	LlmUsageFilter(UpstreamFilterFactory.Context context) {
		this.route = context.route();
		this.meters = context.meters();
		this.budget = context.budget();
		FilterConfig settings = context.settings();
		for (String arg : settings.args().keySet()) {
			if (!arg.equals("include-usage")) {
				throw new IllegalArgumentException("filter " + NAME + " takes no argument " + arg);
			}
		}
		this.includeUsage = Boolean.parseBoolean(settings.args().get("include-usage"));
		DataSize limit = settings.maxBody();
		this.maxBody = (limit == null ? DEFAULT_MAX_BODY : limit).toBytes();
	}

	/** The factory of this filter. */
	public static UpstreamFilterFactory factory() {
		return new UpstreamFilterFactory() {

			@Override
			public String name() {
				return NAME;
			}

			@Override
			public UpstreamFilter create(Context context) {
				return new LlmUsageFilter(context);
			}

		};
	}

	@Override
	public @Nullable Exchange onRequest(Request request, Headers headers) {
		LlmApi api = LlmApi.of(request.method(), request.path());
		if (api == null) {
			return null;
		}
		// a compressed body cannot be read
		headers.remove("accept-encoding");
		return new UsageExchange(api);
	}

	private void requests(LlmApi api, String model, String stream, String outcome) {
		Counter.builder("sluice.client.filter.llm.requests")
			.description("Requests of an LLM API a route's filter saw the responses of")
			.tag("route", this.route)
			.tag("api", api.tag)
			.tag("model", model)
			.tag("stream", stream)
			.tag("outcome", outcome)
			.register(this.meters)
			.increment();
	}

	private void tokens(LlmApi api, String model, String type, @Nullable Long value) {
		if (value != null && value > 0) {
			Counter.builder("sluice.client.filter.llm.tokens")
				.description("Tokens the responses of an LLM API report, by type")
				.tag("route", this.route)
				.tag("api", api.tag)
				.tag("model", model)
				.tag("type", type)
				.register(this.meters)
				.increment(value);
		}
	}

	/** A model as a tag: it comes from a response, so its values are bounded. */
	private String modelTag(@Nullable String model) {
		if (model == null || model.isBlank()) {
			return UNKNOWN;
		}
		if (this.models.contains(model)) {
			return model;
		}
		if (this.models.size() >= MAX_MODELS) {
			return "other";
		}
		this.models.add(model);
		return model;
	}

	/** One request of an API. */
	private final class UsageExchange implements Exchange {

		private final LlmApi api;

		UsageExchange(LlmApi api) {
			this.api = api;
		}

		@Override
		public @Nullable BodyHandler onRequestBody() {
			return LlmUsageFilter.this.includeUsage && this.api.completions() ? new IncludeUsage() : null;
		}

		@Override
		public @Nullable BodyHandler onResponse(int status, Headers headers, boolean bodiless) {
			if (status < 200 || status >= 300) {
				requests(this.api, UNKNOWN, UNKNOWN, "status_" + status / 100);
				return null;
			}
			List<String> encodings = headers.values("content-encoding");
			if (!encodings.isEmpty() && !encodings.get(0).equalsIgnoreCase("identity")) {
				requests(this.api, UNKNOWN, UNKNOWN, "encoded");
				return null;
			}
			List<String> types = headers.values("content-type");
			String type = types.isEmpty() ? "" : types.get(0).toLowerCase(Locale.ROOT);
			boolean stream = type.startsWith("text/event-stream");
			if (bodiless || !stream && !type.startsWith("application/json")) {
				requests(this.api, UNKNOWN, UNKNOWN, "other_type");
				return null;
			}
			return new Reading(this.api, stream);
		}

	}

	/**
	 * Holds a request's body, charging the budget, to add the option when it is a stream
	 * that does not set it: the upstream gets a length as it would have. A body past the
	 * limit or the budget goes on unchanged, its head as it was.
	 */
	private final class IncludeUsage implements BodyRewriter, JsonFields.Sink {

		private ByteArrayOutputStream held = new ByteArrayOutputStream();

		private final TunnelBudget.Account account = LlmUsageFilter.this.budget.account();

		private boolean gaveUp;

		private boolean stream;

		private boolean optionSet;

		@Override
		public byte[] onData(byte[] bytes, int offset, int length) {
			if (!this.gaveUp) {
				long wanted = (long) this.held.size() + length;
				if (wanted <= LlmUsageFilter.this.maxBody && this.account.tryGrow(wanted)) {
					this.held.write(bytes, offset, length);
					return EMPTY;
				}
				this.gaveUp = true;
				this.held.write(bytes, offset, length);
				byte[] all = this.held.toByteArray();
				// charged no more: its array must go as well
				this.held = new ByteArrayOutputStream(0);
				this.account.close();
				return all;
			}
			byte[] part = new byte[length];
			System.arraycopy(bytes, offset, part, 0, length);
			return part;
		}

		@Override
		public byte[] onEnd() {
			if (this.gaveUp) {
				return EMPTY;
			}
			byte[] body = this.held.toByteArray();
			JsonFields fields = new JsonFields(this);
			fields.feed(body, 0, body.length);
			int last = body.length - 1;
			while (last >= 0 && body[last] != '}') {
				last--;
			}
			if (!this.stream || this.optionSet || last < 0) {
				return body;
			}
			byte[] added = new byte[last + INCLUDE_USAGE.length];
			System.arraycopy(body, 0, added, 0, last);
			System.arraycopy(INCLUDE_USAGE, 0, added, last, INCLUDE_USAGE.length);
			return added;
		}

		@Override
		public void onClose() {
			this.account.close();
		}

		@Override
		public boolean unchanged() {
			return this.gaveUp;
		}

		@Override
		public boolean wants(String path) {
			return path.equals("stream") || path.equals("stream_options");
		}

		@Override
		public void field(String path, String text) {
			if (path.equals("stream")) {
				this.stream = text.equals("true");
			}
			else {
				this.optionSet = true;
			}
		}

		@Override
		public void documentEnd() {
		}

	}

	/** Reads one response as it passes. */
	private final class Reading implements BodyObserver, JsonFields.Sink {

		private final LlmApi api;

		private final boolean stream;

		private final JsonFields fields = new JsonFields(this);

		/** The usage numbers of the document being read. */
		private final Map<String, Long> numbers = new HashMap<>();

		/** The event type of the document being read. */
		private String type = "";

		private @Nullable String model;

		private @Nullable Long input;

		private @Nullable Long output;

		private @Nullable Long cacheRead;

		private @Nullable Long cacheCreation;

		private boolean terminal;

		private boolean error;

		/** The bytes of {@code data:} the line begins with so far. */
		private int lineMatched;

		/** This line is something else than a {@code data:} one, read to its end. */
		private boolean otherLine;

		/** The bytes of the line's payload compared to {@code [DONE]}; -1 once not. */
		private int done;

		private boolean atPayloadStart;

		/** Nothing but a CR read of this line. */
		private boolean blank = true;

		Reading(LlmApi api, boolean stream) {
			this.api = api;
			this.stream = stream;
		}

		@Override
		public void onData(byte[] bytes, int offset, int length) {
			if (this.stream) {
				events(bytes, offset, length);
			}
			else {
				this.fields.feed(bytes, offset, length);
			}
		}

		/** The lines of server-sent events: only those of {@code data:} are parsed. */
		private void events(byte[] bytes, int offset, int length) {
			int i = offset;
			int end = offset + length;
			while (i < end) {
				byte b = bytes[i];
				if (b == '\n') {
					endLine();
					i++;
				}
				else if (this.otherLine) {
					i++;
				}
				else if (this.lineMatched < DATA.length) {
					this.blank &= b == '\r';
					if (b == DATA[this.lineMatched]) {
						this.lineMatched++;
						this.atPayloadStart = this.lineMatched == DATA.length;
					}
					else {
						this.otherLine = !this.blank;
					}
					i++;
				}
				else {
					if (this.atPayloadStart) {
						this.atPayloadStart = false;
						if (b == ' ') {
							i++;
							continue;
						}
					}
					int stop = i;
					while (stop < end && bytes[stop] != '\n') {
						matchDone(bytes[stop]);
						stop++;
					}
					this.fields.feed(bytes, i, stop - i);
					i = stop;
				}
			}
		}

		private void matchDone(byte b) {
			if (this.done >= 0) {
				if (this.done < DONE.length && b == DONE[this.done]) {
					this.done++;
				}
				else if (b != '\r') {
					this.done = -1;
				}
			}
		}

		private void endLine() {
			if (this.done == DONE.length) {
				this.terminal = true;
				this.fields.reset();
			}
			else if (this.blank) {
				// a blank line ends an event, whatever was read of it
				this.fields.reset();
			}
			this.lineMatched = 0;
			this.blank = true;
			this.otherLine = false;
			this.atPayloadStart = false;
			this.done = 0;
		}

		@Override
		public boolean wants(String path) {
			if (path.equals("type") || path.equals("error") || path.equals("message") || path.equals("response")) {
				return true;
			}
			String local = local(path);
			return local.equals("model") || local.equals("usage") || local.equals("usage.prompt_tokens_details")
					|| local.equals("usage.input_tokens_details")
					|| local.startsWith("usage.") && USAGE.contains(local.substring("usage.".length()));
		}

		/** A stream's events wrap the usage and the model in an object. */
		private String local(String path) {
			if (path.startsWith("message.")) {
				return path.substring("message.".length());
			}
			return path.startsWith("response.") ? path.substring("response.".length()) : path;
		}

		@Override
		public void field(String path, String text) {
			if (path.equals("type")) {
				this.type = text;
				return;
			}
			if (path.equals("error")) {
				this.error |= !text.equals("null");
				return;
			}
			String local = local(path);
			if (local.equals("model")) {
				if (this.model == null && !text.isEmpty() && !text.equals("null")) {
					this.model = text;
				}
			}
			else if (local.startsWith("usage.") && !text.isEmpty()) {
				try {
					this.numbers.put(local.substring("usage.".length()), Long.parseLong(text));
				}
				catch (NumberFormatException e) {
					// not a count: left out
				}
			}
		}

		@Override
		public void documentEnd() {
			updateTokens();
			this.numbers.clear();
			switch (this.type) {
				case "message_stop", "response.completed" -> this.terminal = true;
				case "response.failed", "response.incomplete" -> {
					this.terminal = true;
					this.error = true;
				}
				case "error" -> this.error = true;
				default -> {
				}
			}
			this.type = "";
			if (!this.stream) {
				this.terminal = true;
			}
		}

		/**
		 * Later values overwrite, field by field: they are cumulative, never to be added.
		 * The input of the OpenAI APIs is a total, what the cache served and took within
		 * it; the Anthropic API reports them apart.
		 */
		private void updateTokens() {
			if (this.api == LlmApi.ANTHROPIC) {
				this.input = this.numbers.getOrDefault("input_tokens", this.input);
				this.output = this.numbers.getOrDefault("output_tokens", this.output);
				this.cacheRead = this.numbers.getOrDefault("cache_read_input_tokens", this.cacheRead);
				this.cacheCreation = this.numbers.getOrDefault("cache_creation_input_tokens", this.cacheCreation);
				return;
			}
			boolean chat = this.api.completions();
			String details = chat ? "prompt_tokens_details." : "input_tokens_details.";
			Long total = this.numbers.get(chat ? "prompt_tokens" : "input_tokens");
			if (total != null) {
				long cached = this.numbers.getOrDefault(details + "cached_tokens", 0L);
				long written = this.numbers.getOrDefault(details + "cache_write_tokens", 0L);
				this.input = Math.max(0, total - cached - written);
				this.cacheRead = cached;
				this.cacheCreation = written;
			}
			this.output = this.numbers.getOrDefault(chat ? "completion_tokens" : "output_tokens", this.output);
		}

		@Override
		public void onEnd(boolean complete) {
			String outcome;
			if (this.error) {
				outcome = "error";
			}
			else if (!complete || this.stream && !this.terminal) {
				outcome = "cut";
			}
			else if (this.input == null && this.output == null) {
				outcome = "no_usage";
			}
			else {
				outcome = "complete";
			}
			String tag = modelTag(this.model);
			requests(this.api, tag, String.valueOf(this.stream), outcome);
			tokens(this.api, tag, "input", this.input);
			tokens(this.api, tag, "output", this.output);
			tokens(this.api, tag, "cache_read", this.cacheRead);
			tokens(this.api, tag, "cache_creation", this.cacheCreation);
		}

	}

}
