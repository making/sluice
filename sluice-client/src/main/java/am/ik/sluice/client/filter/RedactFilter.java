package am.ik.sluice.client.filter;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.jspecify.annotations.Nullable;

import am.ik.sluice.tunnel.TunnelBudget;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

/**
 * Replaces what a route's text bodies match as they stream, on its responses, its
 * requests or both: literals, and regexes whose matches take a bounded number of bytes.
 * <p>
 * What the next bytes could still make a match is held, the least that can be: a match's
 * bytes at most, so a stream whose parts end in nothing that begins a match passes as it
 * comes. A body passes byte for byte but for its matches. A coded body passes as it came
 * ({@code compression} before it decodes one).
 */
final class RedactFilter implements UpstreamFilter {

	/** The name routes use this filter by. */
	static final String NAME = "redact";

	/** The most bytes a match may take. */
	static final int MAX_MATCH = 4096;

	private static final String DEFAULT_TYPES = "text/*,application/json,application/*+json,application/x-ndjson";

	private static final String DEFAULT_REPLACEMENT = "[REDACTED]";

	/**
	 * What a match attempt may read past the bytes of its longest match: {@code \b} and
	 * {@code $} look at what follows.
	 */
	private static final int LOOK_AHEAD = 4;

	/** What is kept of the bytes before those held, for {@code \b} and {@code ^}. */
	private static final int LOOK_BEHIND = 2;

	private static final byte[] EMPTY = new byte[0];

	private final String route;

	private final MeterRegistry meters;

	private final TunnelBudget budget;

	private final boolean requests;

	private final boolean responses;

	private final MediaTypes types;

	/**
	 * The rules in one alternation, each its own named group; none when every rule is a
	 * literal.
	 */
	private final @Nullable Pattern pattern;

	/** Whether a byte begins a literal, when every rule is one. */
	private final boolean[] first = new boolean[256];

	private final List<Rule> rules;

	/** The most a match attempt reads from where it starts. */
	private final int reach;

	/** What is kept before the bytes held: none when no rule looks behind a match. */
	private final int behind;

	/**
	 * One rule.
	 *
	 * @param name its name in the args
	 * @param literal its bytes, when a literal
	 * @param group the pattern's group of it, if any
	 * @param replacement what replaces a match
	 */
	private record Rule(String name, byte @Nullable [] literal, int group, byte[] replacement) {
	}

	private RedactFilter(UpstreamFilterFactory.Context context) {
		this.route = context.route();
		this.meters = context.meters();
		this.budget = context.budget();
		String side = "response";
		String types = DEFAULT_TYPES;
		Map<String, Map<String, String>> rules = new TreeMap<>();
		// in the order of their keys, whatever the map's
		for (Map.Entry<String, String> arg : new TreeMap<>(context.settings().args()).entrySet()) {
			String key = arg.getKey();
			switch (key) {
				case "side" -> side = arg.getValue().trim();
				case "types" -> types = arg.getValue();
				default -> {
					int dot = key.lastIndexOf('.');
					String field = key.substring(dot + 1);
					if (dot <= 0 || !List.of("literal", "regex", "replacement").contains(field)) {
						throw new IllegalArgumentException("filter " + NAME
								+ " takes args side, types and <rule>.<literal|regex|replacement>, not " + key);
					}
					rules.computeIfAbsent(key.substring(0, dot), name -> new TreeMap<>()).put(field, arg.getValue());
				}
			}
		}
		switch (side) {
			case "request" -> {
				this.requests = true;
				this.responses = false;
			}
			case "response" -> {
				this.requests = false;
				this.responses = true;
			}
			case "both" -> {
				this.requests = true;
				this.responses = true;
			}
			default -> throw new IllegalArgumentException(
					"filter " + NAME + " takes request, response or both as its side, not " + side);
		}
		this.types = MediaTypes.parse(NAME, types);
		if (rules.isEmpty()) {
			throw new IllegalArgumentException("filter " + NAME + " needs a rule: args <rule>.literal or <rule>.regex");
		}
		StringBuilder alternation = new StringBuilder();
		List<String> names = new ArrayList<>();
		List<byte @Nullable []> literals = new ArrayList<>();
		List<byte[]> replacements = new ArrayList<>();
		long longest = 0;
		boolean looksBehind = false;
		for (Map.Entry<String, Map<String, String>> rule : rules.entrySet()) {
			String name = rule.getKey();
			String literal = rule.getValue().get("literal");
			String regex = rule.getValue().get("regex");
			if ((literal == null) == (regex == null)) {
				throw new IllegalArgumentException("filter " + NAME + " rule " + name + " takes a literal or a regex");
			}
			String expression;
			if (literal != null) {
				// a literal may be a secret: no message tells it
				byte[] bytes = literal.getBytes(StandardCharsets.UTF_8);
				if (bytes.length == 0 || bytes.length > MAX_MATCH) {
					throw new IllegalArgumentException("filter " + NAME + " rule " + name + " takes a literal of 1 to "
							+ MAX_MATCH + " bytes, not " + bytes.length);
				}
				expression = Pattern.quote(new String(bytes, StandardCharsets.ISO_8859_1));
				longest = Math.max(longest, bytes.length);
				literals.add(bytes);
				this.first[bytes[0] & 0xff] = true;
			}
			else {
				literals.add(null);
				Bounds bounds = Bounds.of(name, regex);
				expression = regex;
				longest = Math.max(longest, bounds.max);
				looksBehind |= bounds.looksBehind;
			}
			String group = "sluiceRule" + names.size();
			alternation.append(names.isEmpty() ? "" : "|")
				.append("(?<")
				.append(group)
				.append('>')
				.append(expression)
				.append(')');
			names.add(name);
			replacements
				.add(rule.getValue().getOrDefault("replacement", DEFAULT_REPLACEMENT).getBytes(StandardCharsets.UTF_8));
		}
		// literals alone are found without a regex: several times faster
		Pattern compiled = null;
		if (literals.contains(null)) {
			try {
				compiled = Pattern.compile(alternation.toString());
			}
			catch (PatternSyntaxException e) {
				throw new IllegalArgumentException(
						"filter " + NAME + "'s rules do not compile together: " + e.getDescription());
			}
		}
		this.pattern = compiled;
		Map<String, Integer> groups = compiled == null ? Map.of() : compiled.namedGroups();
		List<Rule> made = new ArrayList<>();
		for (int i = 0; i < names.size(); i++) {
			made.add(new Rule(names.get(i), literals.get(i), groups.getOrDefault("sluiceRule" + i, -1),
					replacements.get(i)));
		}
		this.rules = List.copyOf(made);
		this.reach = (int) longest + LOOK_AHEAD;
		this.behind = looksBehind ? LOOK_BEHIND : 0;
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
				return new RedactFilter(context);
			}

		};
	}

	@Override
	public @Nullable Exchange onRequest(Request request, Headers headers) {
		boolean requestBody = this.requests && request.bodyFollows() && redacts("request", headers);
		if (this.responses) {
			// what is redacted is read whole: not coded, not in parts a match may
			// straddle
			headers.remove("accept-encoding");
			headers.remove("range");
			headers.remove("if-range");
		}
		else if (!requestBody) {
			return null;
		}
		return new Exchange() {

			@Override
			public @Nullable BodyHandler onRequestBody() {
				return requestBody ? new Redaction("request") : null;
			}

			@Override
			public @Nullable BodyHandler onResponse(int status, Headers responseHeaders, boolean bodiless) {
				if (!RedactFilter.this.responses || bodiless || !redacts("response", responseHeaders)) {
					return null;
				}
				List<String> tags = responseHeaders.values("etag");
				if (tags.size() == 1 && !tags.get(0).startsWith("W/")) {
					// the bytes may differ from the upstream's
					responseHeaders.set("ETag", "W/" + tags.get(0));
				}
				return new Redaction("response");
			}

		};
	}

	/** Whether a body of these headers is read; counted when it is not. */
	private boolean redacts(String side, Headers headers) {
		if (!this.types.matches(headers)) {
			bodies(side, "other_type");
			return false;
		}
		if (!MediaTypes.codings(headers).isEmpty()) {
			bodies(side, "encoded");
			return false;
		}
		return true;
	}

	private void bodies(String side, String outcome) {
		Counter.builder("sluice.client.filter.redact.bodies")
			.description("Bodies a route's redact filter saw, by what came of them")
			.tag("route", this.route)
			.tag("side", side)
			.tag("outcome", outcome)
			.register(this.meters)
			.increment();
	}

	/**
	 * One body: what can no longer be part of a match goes on, its matches replaced; the
	 * rest is held for the next bytes, with the few before it that a match's start looks
	 * at.
	 */
	private final class Redaction implements BodyRewriter {

		private final String side;

		/** The replacements made, by rule. */
		private final long[] made = new long[RedactFilter.this.rules.size()];

		/** The bytes held, after {@link #kept} bytes gone on already. */
		private byte[] carry = EMPTY;

		private int kept;

		private TunnelBudget.@Nullable Account account;

		/** What the last scan gave to go on. */
		private byte[] output = EMPTY;

		private boolean ended;

		private boolean closed;

		Redaction(String side) {
			this.side = side;
		}

		@Override
		public byte[] onData(byte[] bytes, int offset, int length) {
			if (this.account == null) {
				TunnelBudget.Account charged = RedactFilter.this.budget.account();
				this.account = charged;
				if (!charged.tryGrow(RedactFilter.this.reach + RedactFilter.this.behind)) {
					// a body unread would pass what it should not
					throw new IllegalStateException("filter " + NAME + " has no tunnel budget left to hold a match");
				}
			}
			byte[] buffer = bytes;
			int base = offset;
			int end = length;
			if (this.carry.length > 0) {
				buffer = Arrays.copyOf(this.carry, this.carry.length + length);
				System.arraycopy(bytes, offset, buffer, this.carry.length, length);
				base = 0;
				end = buffer.length;
			}
			int held = scan(buffer, base, end, false);
			int keep = Math.min(RedactFilter.this.behind, held);
			this.carry = held - keep == end ? EMPTY : Arrays.copyOfRange(buffer, base + held - keep, base + end);
			this.kept = keep;
			return this.output;
		}

		@Override
		public byte[] onEnd() {
			this.ended = true;
			if (this.carry.length > this.kept) {
				scan(this.carry, 0, this.carry.length, true);
			}
			else {
				this.output = EMPTY;
			}
			this.carry = EMPTY;
			return this.output;
		}

		/**
		 * Replaces the matches of {@code buffer[base + kept, base + end)} in
		 * {@link #output}, up to where what follows could still change a match: at the
		 * end of the body, none could.
		 * @return where what is held begins, from {@code base}
		 */
		private int scan(byte[] buffer, int base, int end, boolean last) {
			Pattern regex = RedactFilter.this.pattern;
			Finder finder = regex == null ? new LiteralFinder(buffer, base, end, last)
					: new RegexFinder(regex, new String(buffer, base, end, StandardCharsets.ISO_8859_1), last);
			ByteArrayOutputStream replaced = null;
			int from = this.kept;
			int stop = end;
			while (from < end) {
				if (!finder.find(from)) {
					stop = finder.stop();
					break;
				}
				if (replaced == null) {
					replaced = new ByteArrayOutputStream(end - from + 64);
				}
				replaced.write(buffer, base + from, finder.start() - from);
				byte[] replacement = RedactFilter.this.rules.get(finder.rule()).replacement;
				replaced.write(replacement, 0, replacement.length);
				this.made[finder.rule()]++;
				from = finder.end();
			}
			if (replaced == null) {
				this.output = Arrays.copyOfRange(buffer, base + from, base + stop);
			}
			else {
				replaced.write(buffer, base + from, stop - from);
				this.output = replaced.toByteArray();
			}
			return stop;
		}

		@Override
		public void onClose() {
			if (this.closed) {
				return;
			}
			this.closed = true;
			this.carry = EMPTY;
			if (this.account != null) {
				this.account.close();
			}
			long replacements = 0;
			for (int i = 0; i < this.made.length; i++) {
				replacements += this.made[i];
				if (this.made[i] > 0) {
					Counter.builder("sluice.client.filter.redact.replacements")
						.description("Matches a route's redact filter replaced, by rule")
						.tag("route", RedactFilter.this.route)
						.tag("side", this.side)
						.tag("rule", RedactFilter.this.rules.get(i).name)
						.register(RedactFilter.this.meters)
						.increment(this.made[i]);
				}
			}
			bodies(this.side, !this.ended ? "cut" : replacements > 0 ? "redacted" : "clean");
		}

	}

	/** The matches of one scan, in order, but those the next bytes could change. */
	private interface Finder {

		/**
		 * Finds the first match from {@code at} that the next bytes cannot change.
		 * @return false when there is none: {@link #stop()} tells where a match may yet
		 * begin
		 */
		boolean find(int at);

		int start();

		int end();

		/** The index of the rule matched. */
		int rule();

		/** Where what may begin a match begins; the end when nothing may. */
		int stop();

	}

	/**
	 * Finds the rules in their alternation, on the bytes as ISO-8859-1 chars: the matcher
	 * tells when it read the end, which is when the next bytes could change what it
	 * found.
	 */
	private final class RegexFinder implements Finder {

		private final Matcher matcher;

		private final String text;

		private final boolean last;

		private @Nullable Matcher probe;

		/** Where a match may begin that the next bytes could change, once looked for. */
		private int hold = -1;

		private int stop;

		private int rule;

		RegexFinder(Pattern pattern, String text, boolean last) {
			this.matcher = matcher(pattern, text);
			this.text = text;
			this.last = last;
		}

		private static Matcher matcher(Pattern pattern, String text) {
			// what is before and after the region is seen, and no bound is an anchor
			return pattern.matcher(text).useTransparentBounds(true).useAnchoringBounds(false);
		}

		@Override
		public boolean find(int at) {
			int end = this.text.length();
			this.matcher.region(at, end);
			boolean found = this.matcher.find();
			if (!this.last && (this.matcher.hitEnd() || found && this.matcher.requireEnd())) {
				// a match may begin, or this one change, with the next bytes
				if (this.hold < at) {
					this.hold = holdPoint(at, end);
				}
				if (!found || this.matcher.start() >= this.hold) {
					this.stop = this.hold;
					return false;
				}
			}
			if (!found) {
				this.stop = end;
				return false;
			}
			List<Rule> rules = RedactFilter.this.rules;
			for (int i = 0; i < rules.size(); i++) {
				if (this.matcher.start(rules.get(i).group) >= 0) {
					this.rule = i;
					return true;
				}
			}
			throw new IllegalStateException("a match of no rule");
		}

		/**
		 * The first place a match may begin that the next bytes could change: none before
		 * the most a match attempt reads.
		 */
		private int holdPoint(int from, int end) {
			Matcher probe = this.probe;
			if (probe == null) {
				probe = matcher(this.matcher.pattern(), this.text);
				this.probe = probe;
			}
			for (int at = Math.max(from, end - RedactFilter.this.reach); at < end; at++) {
				probe.region(at, end);
				probe.lookingAt();
				if (probe.hitEnd()) {
					return at;
				}
			}
			return end;
		}

		@Override
		public int start() {
			return this.matcher.start();
		}

		@Override
		public int end() {
			return this.matcher.end();
		}

		@Override
		public int rule() {
			return this.rule;
		}

		@Override
		public int stop() {
			return this.stop;
		}

	}

	/**
	 * Finds literals byte by byte: at a place, the first rule whose literal is there,
	 * unless one before it is begun by the bytes up to the end.
	 */
	private final class LiteralFinder implements Finder {

		private final byte[] buffer;

		private final int base;

		private final int end;

		private final boolean last;

		private int start;

		private int rule;

		private int stop;

		LiteralFinder(byte[] buffer, int base, int end, boolean last) {
			this.buffer = buffer;
			this.base = base;
			this.end = end;
			this.last = last;
		}

		@Override
		public boolean find(int at) {
			List<Rule> rules = RedactFilter.this.rules;
			boolean[] first = RedactFilter.this.first;
			for (int place = at; place < this.end; place++) {
				if (!first[this.buffer[this.base + place] & 0xff]) {
					continue;
				}
				boolean begun = false;
				for (int i = 0; i < rules.size(); i++) {
					byte[] literal = Objects.requireNonNull(rules.get(i).literal);
					int length = Math.min(literal.length, this.end - place);
					if (!Arrays.equals(this.buffer, this.base + place, this.base + place + length, literal, 0,
							length)) {
						continue;
					}
					if (length < literal.length) {
						// at the end of the body, no match
						begun |= !this.last;
					}
					else if (begun) {
						break;
					}
					else {
						this.start = place;
						this.rule = i;
						return true;
					}
				}
				if (begun) {
					this.stop = place;
					return false;
				}
			}
			this.stop = this.end;
			return false;
		}

		@Override
		public int start() {
			return this.start;
		}

		@Override
		public int end() {
			return this.start + Objects.requireNonNull(RedactFilter.this.rules.get(this.rule).literal).length;
		}

		@Override
		public int rule() {
			return this.rule;
		}

		@Override
		public int stop() {
			return this.stop;
		}

	}

	/**
	 * The fewest and the most chars of a regex's match, read from its syntax, and whether
	 * it looks at what comes before its start. What it cannot bound is refused: a
	 * quantifier without an upper bound, lookaround, a backreference.
	 */
	private static final class Bounds {

		/** Past any match this filter takes, short of an overflow. */
		private static final long LIMIT = 1L << 30;

		private static final long[] NONE = { 0, 0 };

		private static final long[] ONE = { 1, 1 };

		private final String rule;

		private final String regex;

		private int at;

		private long max;

		private boolean looksBehind;

		private Bounds(String rule, String regex) {
			this.rule = rule;
			this.regex = regex;
		}

		/**
		 * The bounds of a rule's regex.
		 * @throws IllegalArgumentException when it does not compile, or is not bounded
		 * from 1 to {@link #MAX_MATCH}
		 */
		static Bounds of(String rule, String regex) {
			Bounds bounds = new Bounds(rule, regex);
			try {
				Pattern.compile(regex);
			}
			catch (PatternSyntaxException e) {
				throw bounds.refused("does not compile: " + e.getDescription() + " at " + e.getIndex());
			}
			for (int i = 0; i < regex.length(); i++) {
				if (regex.charAt(i) > 0x7f) {
					throw bounds.refused("is not ASCII: it reads bytes, a literal takes any text");
				}
			}
			long[] lengths = bounds.alternation();
			if (bounds.at != regex.length()) {
				throw bounds.refused("has a ')' of no group");
			}
			if (lengths[0] == 0) {
				throw bounds.refused("may match nothing");
			}
			if (lengths[1] > MAX_MATCH) {
				throw bounds.refused("matches up to " + (lengths[1] >= LIMIT ? "more than " + LIMIT : lengths[1])
						+ " bytes, past " + MAX_MATCH);
			}
			bounds.max = lengths[1];
			return bounds;
		}

		private IllegalArgumentException refused(String why) {
			return new IllegalArgumentException("filter " + NAME + " rule " + this.rule + ": the regex " + why);
		}

		private boolean next(char c) {
			return this.at < this.regex.length() && this.regex.charAt(this.at) == c;
		}

		private char take() {
			return this.regex.charAt(this.at++);
		}

		private long[] alternation() {
			long[] bounds = sequence();
			while (next('|')) {
				this.at++;
				long[] other = sequence();
				bounds = new long[] { Math.min(bounds[0], other[0]), Math.max(bounds[1], other[1]) };
			}
			return bounds;
		}

		private long[] sequence() {
			long min = 0;
			long max = 0;
			while (this.at < this.regex.length() && !next('|') && !next(')')) {
				long[] atom = quantified(atom());
				min = Math.min(min + atom[0], LIMIT);
				max = Math.min(max + atom[1], LIMIT);
			}
			return new long[] { min, max };
		}

		private long[] atom() {
			return switch (take()) {
				case '(' -> group();
				case '[' -> {
					characterClass();
					yield ONE;
				}
				case '\\' -> escape();
				case '^' -> {
					this.looksBehind = true;
					yield NONE;
				}
				case '$' -> NONE;
				default -> ONE;
			};
		}

		private long[] group() {
			if (next('?')) {
				this.at++;
				char kind = take();
				switch (kind) {
					case ':', '>' -> {
						// non-capturing, atomic
					}
					case '=', '!' -> throw refused("looks ahead: not bounded by its match");
					case '<' -> {
						if (next('=') || next('!')) {
							throw refused("looks behind: not bounded by its match");
						}
						this.at = this.regex.indexOf('>', this.at) + 1;
					}
					default -> {
						this.at--;
						int start = this.at;
						while (Character.isLetter(this.regex.charAt(this.at)) || next('-')) {
							this.at++;
						}
						if (this.regex.substring(start, this.at).indexOf('x') >= 0) {
							throw refused("sets x: comments are not read");
						}
						if (take() == ')') {
							// flags for the rest of the group around
							return NONE;
						}
					}
				}
			}
			long[] inner = alternation();
			// what compiled closes its groups
			this.at++;
			return inner;
		}

		/** Skips a class, nested ones and escapes in it: a char. */
		private void characterClass() {
			int depth = 1;
			while (depth > 0) {
				char c = take();
				if (c == '\\') {
					if (next('Q')) {
						quoted();
					}
					else {
						this.at++;
					}
				}
				else if (c == '[') {
					depth++;
				}
				else if (c == ']') {
					depth--;
				}
			}
		}

		/** The chars of {@code \Q...\E}, from its {@code Q}. */
		private int quoted() {
			int end = this.regex.indexOf("\\E", this.at);
			if (end < 0) {
				throw refused("has \\Q without \\E");
			}
			int length = end - this.at - 1;
			this.at = end + 2;
			return length;
		}

		private long[] escape() {
			char c = this.regex.charAt(this.at);
			if (c == 'Q') {
				int length = quoted();
				return new long[] { length, length };
			}
			this.at++;
			switch (c) {
				case 'b', 'B' -> {
					if (next('{')) {
						throw refused("has \\b{...}: not bounded");
					}
					this.looksBehind = true;
					return NONE;
				}
				case 'A' -> {
					this.looksBehind = true;
					return NONE;
				}
				case 'Z', 'z' -> {
					return NONE;
				}
				case 'G', 'X' -> throw refused("has \\" + c + ": not supported");
				case 'R' -> {
					return new long[] { 1, 2 };
				}
				case 'k', '1', '2', '3', '4', '5', '6', '7', '8', '9' ->
					throw refused("has a backreference: not bounded by its syntax");
				case 'x', 'p', 'P', 'N' -> {
					if (next('{')) {
						this.at = this.regex.indexOf('}', this.at) + 1;
					}
					else {
						this.at += c == 'x' ? 2 : c == 'N' ? 0 : 1;
					}
				}
				case 'u' -> this.at += 4;
				case 'c' -> this.at++;
				case '0' -> {
					int start = this.at;
					while (this.at < start + 3 && this.at < this.regex.length() && this.regex.charAt(this.at) >= '0'
							&& this.regex.charAt(this.at) <= '7') {
						this.at++;
					}
				}
				default -> {
					// a class (\d, \w, \s, ...), a control char or a char escaped
				}
			}
			return ONE;
		}

		private long[] quantified(long[] atom) {
			if (this.at >= this.regex.length()) {
				return atom;
			}
			long[] bounds;
			switch (this.regex.charAt(this.at)) {
				case '?' -> {
					this.at++;
					bounds = new long[] { 0, atom[1] };
				}
				case '*', '+' -> throw refused(
						"has " + this.regex.charAt(this.at) + ": its match is not bounded, write {0,n} or {1,n}");
				case '{' -> {
					int close = this.regex.indexOf('}', this.at);
					String[] counts = this.regex.substring(this.at + 1, close).split(",", -1);
					if (counts.length > 1 && counts[1].isBlank()) {
						throw refused("has {n,}: its match is not bounded, write {n,m}");
					}
					long least = Math.min(Long.parseLong(counts[0].trim()), LIMIT);
					long most = counts.length > 1 ? Math.min(Long.parseLong(counts[1].trim()), LIMIT) : least;
					this.at = close + 1;
					bounds = new long[] { Math.min(atom[0] * least, LIMIT), Math.min(atom[1] * most, LIMIT) };
				}
				default -> {
					return atom;
				}
			}
			if (next('?') || next('+')) {
				// lazy, possessive
				this.at++;
			}
			return bounds;
		}

	}

}
