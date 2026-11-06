package am.ik.sluice.client.filter;

import java.io.ByteArrayOutputStream;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.zip.CRC32;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

import org.jspecify.annotations.Nullable;

import am.ik.sluice.tunnel.TunnelBudget;

import org.springframework.util.unit.DataSize;

/**
 * Codes the responses of the filters after it on a route: a body the upstream compressed
 * (gzip, deflate) is decoded for them, and what they leave is compressed with gzip when
 * the client takes it, so they read a body the tunnel still carries small. A body that
 * passes unseen and that the client takes as it is stays as the upstream sent it.
 * <p>
 * It owns {@code Accept-Encoding} towards the upstream: what the client takes of what it
 * decodes, whatever the filters after it left. What it holds charges the tunnel budget: a
 * coder's state, and what one read decodes to, which is bounded by the bytes read.
 */
final class CompressionFilter implements UpstreamFilter {

	/** The name routes use this filter by. */
	static final String NAME = "compression";

	private static final Set<String> ARGS = Set.of("types", "min-length", "level", "max-ratio");

	private static final String DEFAULT_TYPES = "text/*,application/json,application/*+json,application/xml,"
			+ "application/*+xml,application/javascript,application/x-ndjson,application/wasm,image/svg+xml";

	private static final DataSize DEFAULT_MIN_LENGTH = DataSize.ofKilobytes(1);

	private static final int DEFAULT_MAX_RATIO = 100;

	/** What a body may decode to whatever its ratio: a small one may well be past it. */
	private static final long FREE_EXPANSION = 1024 * 1024;

	/** What a coder hands on at once. */
	private static final int PIECE = 16 * 1024;

	/** A decoder's state: zlib's window and its own, and the piece. */
	private static final long DECODER_BYTES = 32 * 1024 + 8 * 1024 + PIECE;

	/**
	 * An encoder's state at zlib's defaults: its window and hash chains, and the piece.
	 */
	private static final long ENCODER_BYTES = 256 * 1024 + 8 * 1024 + PIECE;

	/** The most a gzip header's optional fields may take: they are only skipped. */
	private static final int MAX_GZIP_HEADER = 64 * 1024;

	private static final byte[] GZIP_HEADER = { 0x1f, (byte) 0x8b, 8, 0, 0, 0, 0, 0, 0, (byte) 0xff };

	private final MediaTypes types;

	private final long minLength;

	private final int level;

	private final int maxRatio;

	private final List<String> paths;

	private final TunnelBudget budget;

	private final UpstreamFilter inner;

	private final @Nullable FilterFailures failures;

	private CompressionFilter(CompressionFilter settings, UpstreamFilter inner, FilterFailures failures) {
		this.types = settings.types;
		this.minLength = settings.minLength;
		this.level = settings.level;
		this.maxRatio = settings.maxRatio;
		this.paths = settings.paths;
		this.budget = settings.budget;
		this.inner = inner;
		this.failures = failures;
	}

	private CompressionFilter(UpstreamFilterFactory.Context context) {
		Map<String, String> args = context.settings().args();
		for (String arg : args.keySet()) {
			if (!ARGS.contains(arg)) {
				throw new IllegalArgumentException("filter " + NAME + " takes no argument " + arg);
			}
		}
		this.types = MediaTypes.parse(NAME, args.getOrDefault("types", DEFAULT_TYPES));
		String minLength = args.get("min-length");
		this.minLength = (minLength == null ? DEFAULT_MIN_LENGTH : DataSize.parse(minLength.trim())).toBytes();
		this.level = intArg(args, "level", Deflater.DEFAULT_COMPRESSION, 1, 9);
		this.maxRatio = intArg(args, "max-ratio", DEFAULT_MAX_RATIO, 1, Integer.MAX_VALUE);
		this.paths = context.settings().paths();
		this.budget = context.budget();
		this.inner = (request, headers) -> null;
		this.failures = null;
	}

	private static int intArg(Map<String, String> args, String key, int defaults, int min, int max) {
		String value = args.get(key);
		if (value == null) {
			return defaults;
		}
		try {
			int parsed = Integer.parseInt(value.trim());
			if (parsed >= min && parsed <= max) {
				return parsed;
			}
		}
		catch (NumberFormatException e) {
			// refused below
		}
		throw new IllegalArgumentException("filter " + NAME + " takes " + key + " from " + min
				+ (max == Integer.MAX_VALUE ? " up" : " to " + max));
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
				return new CompressionFilter(context);
			}

		};
	}

	/** This filter around the filters after it on the route, its failures counted. */
	UpstreamFilter around(UpstreamFilter inner, FilterFailures failures) {
		return new CompressionFilter(this, inner, failures);
	}

	@Override
	public @Nullable Exchange onRequest(Request request, Headers headers) {
		if (!FilterChain.matches(this.paths, request.path())) {
			return this.inner.onRequest(request, headers);
		}
		Accepted accepted = Accepted.of(headers.values("accept-encoding"));
		Exchange exchange = this.inner.onRequest(request, headers);
		// the filters after it read what it decodes: one the client takes may come
		String wanted = accepted.gzip() ? "gzip" : accepted.deflate() ? "deflate" : null;
		List<String> sent = headers.values("accept-encoding");
		if (wanted == null && !sent.isEmpty()) {
			headers.remove("accept-encoding");
		}
		else if (wanted != null && !sent.equals(List.of(wanted))) {
			headers.set("Accept-Encoding", wanted);
		}
		return new Coding(exchange, accepted);
	}

	/** Whether the body is told to be shorter than what pays for compressing it. */
	private boolean small(Headers headers) {
		List<String> lengths = headers.values("content-length");
		if (lengths.isEmpty()) {
			return false;
		}
		try {
			return Long.parseLong(lengths.get(0)) < this.minLength;
		}
		catch (NumberFormatException e) {
			return false;
		}
	}

	/**
	 * What may not be coded anew: what the upstream said not to transform, a part of a
	 * representation, a body with a transfer coding of its own.
	 */
	private static boolean untouchable(int status, Headers headers) {
		if (status == 206 || MediaTypes.elements(headers.values("cache-control")).contains("no-transform")) {
			return true;
		}
		List<String> transfer = MediaTypes.elements(headers.values("transfer-encoding"));
		transfer.removeIf("chunked"::equals);
		return !transfer.isEmpty();
	}

	/** The codings this filter decodes. */
	private enum Codec {

		GZIP, DEFLATE;

		static @Nullable Codec of(List<String> codings) {
			if (codings.size() != 1) {
				return null;
			}
			return switch (codings.get(0)) {
				case "gzip", "x-gzip" -> GZIP;
				case "deflate" -> DEFLATE;
				default -> null;
			};
		}

	}

	/**
	 * What a request's client takes, by its {@code Accept-Encoding}.
	 *
	 * @param told whether it said: without the header any coding is acceptable (RFC
	 * 9110), which is no reason to compress
	 * @param gzip whether it takes gzip
	 * @param deflate whether it takes deflate
	 */
	record Accepted(boolean told, boolean gzip, boolean deflate) {

		static Accepted of(List<String> values) {
			if (values.isEmpty()) {
				return new Accepted(false, false, false);
			}
			double gzip = -1;
			double deflate = -1;
			double any = -1;
			for (String element : MediaTypes.elements(values)) {
				String[] parts = element.split(";");
				double quality = 1;
				for (int i = 1; i < parts.length; i++) {
					String parameter = parts[i].trim();
					if (parameter.startsWith("q=")) {
						try {
							quality = Double.parseDouble(parameter.substring(2).trim());
						}
						catch (NumberFormatException e) {
							quality = 0;
						}
					}
				}
				switch (parts[0].trim()) {
					case "gzip", "x-gzip" -> gzip = Math.max(gzip, quality);
					case "deflate" -> deflate = Math.max(deflate, quality);
					case "*" -> any = Math.max(any, quality);
					default -> {
					}
				}
			}
			return new Accepted(true, (gzip < 0 ? any : gzip) > 0, (deflate < 0 ? any : deflate) > 0);
		}

		boolean takes(Codec codec) {
			return !this.told || (codec == Codec.GZIP ? this.gzip : this.deflate);
		}

	}

	/** One exchange of the filters it wraps, its response coded. */
	private final class Coding implements Exchange {

		private final @Nullable Exchange inner;

		private final Accepted accepted;

		Coding(@Nullable Exchange inner, Accepted accepted) {
			this.inner = inner;
			this.accepted = accepted;
		}

		@Override
		public @Nullable BodyHandler onRequestBody() {
			return this.inner == null ? null : this.inner.onRequestBody();
		}

		@Override
		public @Nullable LocalResponse localResponse() {
			return this.inner == null ? null : this.inner.localResponse();
		}

		@Override
		public void abandon() {
			if (this.inner != null) {
				this.inner.abandon();
			}
		}

		private @Nullable BodyHandler inner(int status, Headers headers, boolean bodiless) {
			return this.inner == null ? null : this.inner.onResponse(status, headers, bodiless);
		}

		@Override
		public @Nullable BodyHandler onResponse(int status, Headers headers, boolean bodiless) {
			List<String> codings = MediaTypes.codings(headers);
			Codec upstream = Codec.of(codings);
			if (bodiless || untouchable(status, headers) || !codings.isEmpty() && upstream == null) {
				return inner(status, headers, bodiless);
			}
			TunnelBudget.Account account = CompressionFilter.this.budget.account();
			if (upstream != null && !account.tryGrow(DECODER_BYTES)) {
				// past the budget the filters after it see the body as it came
				account.close();
				return inner(status, headers, false);
			}
			List<String> before = headers.lines();
			List<String> encoding = headers.values("content-encoding");
			if (upstream != null) {
				headers.remove("content-encoding");
			}
			List<String> decoded = headers.lines();
			BodyHandler handler;
			try {
				handler = inner(status, headers, false);
			}
			catch (RuntimeException e) {
				account.close();
				throw e;
			}
			boolean compressible = upstream != null || CompressionFilter.this.types.matches(headers) && !small(headers);
			if (upstream != null && handler == null && this.accepted.takes(upstream)) {
				// unseen, and taken as it came
				account.close();
				if (headers.lines().equals(decoded)) {
					headers.replaceLines(before);
				}
				else {
					headers.set("Content-Encoding", String.join(", ", encoding));
				}
				headers.vary("Accept-Encoding");
				return null;
			}
			if (compressible) {
				headers.vary("Accept-Encoding");
			}
			boolean encode = this.accepted.gzip() && compressible && account.tryGrow(account.charged() + ENCODER_BYTES);
			if (upstream == null && !encode) {
				account.close();
				return handler;
			}
			if (encode) {
				headers.set("Content-Encoding", "gzip");
			}
			List<String> tags = headers.values("etag");
			if (tags.size() == 1 && !tags.get(0).startsWith("W/")) {
				headers.set("ETag", "W/" + tags.get(0));
			}
			// a body of no told length may be a stream: what came of each read goes at
			// once
			boolean live = headers.values("content-length").isEmpty();
			return new Recoding(upstream == null ? null : new Decoder(upstream), handler,
					encode ? new Encoder(CompressionFilter.this.level, live) : null, account);
		}

	}

	/** Where a coder hands what it made. */
	@FunctionalInterface
	private interface Sink {

		void accept(byte[] bytes, int offset, int length);

	}

	/** A failure of this filter's own, not of a filter it wraps. */
	private static final class CodingException extends IllegalStateException {

		CodingException(String message) {
			super(message);
		}

		CodingException(String message, Throwable cause) {
			super(message, cause);
		}

	}

	/** One body: decoded, through what the filters after it make of it, encoded. */
	private final class Recoding implements BodyRewriter {

		private static final byte[] EMPTY = new byte[0];

		private final @Nullable Decoder decoder;

		private final @Nullable BodyHandler handler;

		private final @Nullable Encoder encoder;

		private final TunnelBudget.Account account;

		/** What the coders hold. */
		private final long state;

		private ByteArrayOutputStream out = new ByteArrayOutputStream();

		private long read;

		private long decoded;

		private boolean ended;

		private boolean closed;

		Recoding(@Nullable Decoder decoder, @Nullable BodyHandler handler, @Nullable Encoder encoder,
				TunnelBudget.Account account) {
			this.decoder = decoder;
			this.handler = handler;
			this.encoder = encoder;
			this.account = account;
			this.state = account.charged();
		}

		@Override
		public byte[] onData(byte[] bytes, int offset, int length) {
			try {
				this.read += length;
				if (this.decoder != null) {
					this.decoder.feed(bytes, offset, length, this::decoded);
				}
				else {
					handled(bytes, offset, length);
				}
				if (this.encoder != null) {
					this.encoder.flush(this::write);
				}
				return taken();
			}
			catch (CodingException e) {
				counted(e);
				throw e;
			}
		}

		@Override
		public byte[] onEnd() {
			try {
				if (this.decoder != null) {
					this.decoder.finish();
				}
				if (this.handler instanceof BodyRewriter rewriter) {
					byte[] last = rewriter.onEnd();
					encoded(last, 0, last.length);
				}
				else if (this.handler instanceof BodyObserver observer) {
					this.ended = true;
					observer.onEnd(true);
				}
				if (this.encoder != null) {
					this.encoder.finish(this::write);
				}
				return taken();
			}
			catch (CodingException e) {
				counted(e);
				throw e;
			}
		}

		@Override
		public void onClose() {
			if (this.closed) {
				return;
			}
			this.closed = true;
			try {
				if (this.handler instanceof BodyRewriter rewriter) {
					rewriter.onClose();
				}
				else if (this.handler instanceof BodyObserver observer && !this.ended) {
					observer.onEnd(false);
				}
			}
			finally {
				if (this.decoder != null) {
					this.decoder.end();
				}
				if (this.encoder != null) {
					this.encoder.end();
				}
				this.out = new ByteArrayOutputStream(0);
				this.account.close();
			}
		}

		private void counted(CodingException e) {
			FilterFailures counting = CompressionFilter.this.failures;
			if (counting != null) {
				counting.count("body", e);
			}
		}

		/** A piece of the body decoded: bounded by what was read of it. */
		private void decoded(byte[] bytes, int offset, int length) {
			this.decoded += length;
			int ratio = CompressionFilter.this.maxRatio;
			long most = Math.max(FREE_EXPANSION,
					this.read > Long.MAX_VALUE / ratio ? Long.MAX_VALUE : this.read * ratio);
			if (this.decoded > most) {
				throw new CodingException(
						"a body decoded past " + CompressionFilter.this.maxRatio + " times the bytes it came in");
			}
			handled(bytes, offset, length);
		}

		/** A piece of the body as it is, through the filters after this one. */
		private void handled(byte[] bytes, int offset, int length) {
			switch (this.handler) {
				case null -> encoded(bytes, offset, length);
				case BodyObserver observer -> {
					observer.onData(bytes, offset, length);
					encoded(bytes, offset, length);
				}
				case BodyRewriter rewriter -> {
					byte[] part = rewriter.onData(bytes, offset, length);
					encoded(part, 0, part.length);
				}
			}
		}

		private void encoded(byte[] bytes, int offset, int length) {
			if (this.encoder != null) {
				this.encoder.feed(bytes, offset, length, this::write);
			}
			else {
				write(bytes, offset, length);
			}
		}

		/** What goes on, charged until it went. */
		private void write(byte[] bytes, int offset, int length) {
			if (length == 0) {
				return;
			}
			if (!this.account.tryGrow(this.state + this.out.size() + length)) {
				throw new CodingException("a body decoded past the tunnel budget");
			}
			this.out.write(bytes, offset, length);
		}

		private byte[] taken() {
			if (this.out.size() == 0) {
				return EMPTY;
			}
			byte[] taken = this.out.toByteArray();
			// its array must go with its charge
			this.out = taken.length > PIECE ? new ByteArrayOutputStream() : this.out;
			this.out.reset();
			this.account.resize(this.state);
			return taken;
		}

	}

	/**
	 * Decodes gzip, members one after another, or deflate, zlib's format or raw, as it
	 * comes: what follows a gzip member that is not one is left out, as the JDK's
	 * {@code GZIPInputStream} does.
	 */
	private static final class Decoder {

		private static final int FEXTRA = 4;

		private static final int FNAME = 8;

		private static final int FCOMMENT = 16;

		private static final int FHCRC = 2;

		private enum State {

			/** A gzip member's fixed header, or deflate's first two bytes. */
			START, EXTRA_LENGTH, EXTRA, NAME, COMMENT, HEADER_CRC, BODY, TRAILER, DONE

		}

		private final Codec codec;

		private final byte[] piece = new byte[PIECE];

		private final CRC32 crc = new CRC32();

		/** The fixed fields being read: a header's, a trailer's, deflate's start. */
		private final byte[] fields = new byte[10];

		private int got;

		private int flags;

		/** What is left of a field being skipped. */
		private long skip;

		private long headerBytes;

		private boolean member;

		private boolean fed;

		private State state = State.START;

		private @Nullable Inflater inflater;

		Decoder(Codec codec) {
			this.codec = codec;
		}

		void feed(byte[] bytes, int offset, int length, Sink sink) {
			int at = offset;
			int end = offset + length;
			this.fed |= length > 0;
			while (at < end) {
				at = switch (this.state) {
					case START -> start(bytes, at, end, sink);
					case EXTRA_LENGTH, EXTRA, NAME, COMMENT, HEADER_CRC -> header(bytes, at, end);
					case BODY -> inflate(bytes, at, end, sink);
					case TRAILER -> trailer(bytes, at, end);
					case DONE -> end;
				};
			}
		}

		/**
		 * Whether the body ended where its coding does: an empty one has nothing to end.
		 */
		void finish() {
			// a few bytes after a gzip member are no member
			boolean complete = !this.fed || this.state == State.DONE
					|| this.codec == Codec.GZIP && this.member && this.state == State.START;
			if (!complete) {
				throw new CodingException("a body ended inside its " + this.codec.name().toLowerCase(Locale.ROOT));
			}
		}

		void end() {
			if (this.inflater != null) {
				this.inflater.end();
			}
		}

		private int fill(byte[] bytes, int at, int end, int wanted) {
			int n = Math.min(wanted - this.got, end - at);
			System.arraycopy(bytes, at, this.fields, this.got, n);
			this.got += n;
			return at + n;
		}

		private int start(byte[] bytes, int at, int end, Sink sink) {
			if (this.codec == Codec.DEFLATE) {
				int next = fill(bytes, at, end, 2);
				if (this.got < 2) {
					return next;
				}
				int first = this.fields[0] & 0xff;
				boolean zlib = (first & 0x0f) == 8 && (first >> 4) <= 7
						&& ((first << 8) | (this.fields[1] & 0xff)) % 31 == 0;
				this.inflater = new Inflater(!zlib);
				this.state = State.BODY;
				inflate(this.fields, 0, 2, sink);
				return next;
			}
			int next = fill(bytes, at, end, 10);
			if (this.got < 10) {
				return next;
			}
			this.got = 0;
			if ((this.fields[0] & 0xff) != 0x1f || (this.fields[1] & 0xff) != 0x8b || this.fields[2] != 8
					|| (this.fields[3] & 0xe0) != 0) {
				if (this.member) {
					this.state = State.DONE;
					return end;
				}
				throw new CodingException("a body that is not gzip");
			}
			this.flags = this.fields[3];
			this.headerBytes = 10;
			this.state = State.EXTRA_LENGTH;
			advance();
			return next;
		}

		/** The next of a header's optional fields it has, or its body. */
		private void advance() {
			while (true) {
				switch (this.state) {
					case EXTRA_LENGTH -> {
						if ((this.flags & FEXTRA) != 0) {
							return;
						}
						this.state = State.NAME;
					}
					case EXTRA -> this.state = State.NAME;
					case NAME -> {
						if ((this.flags & FNAME) != 0) {
							return;
						}
						this.state = State.COMMENT;
					}
					case COMMENT -> {
						if ((this.flags & FCOMMENT) != 0) {
							return;
						}
						this.state = State.HEADER_CRC;
					}
					case HEADER_CRC -> {
						if ((this.flags & FHCRC) != 0) {
							this.skip = 2;
							return;
						}
						this.state = State.BODY;
					}
					default -> {
						if (this.inflater == null) {
							this.inflater = new Inflater(true);
						}
						this.inflater.reset();
						this.crc.reset();
						return;
					}
				}
			}
		}

		private int header(byte[] bytes, int at, int end) {
			int next = at;
			switch (this.state) {
				case EXTRA_LENGTH -> {
					next = fill(bytes, at, end, 2);
					if (this.got == 2) {
						this.got = 0;
						this.skip = (this.fields[0] & 0xff) | (this.fields[1] & 0xff) << 8;
						this.state = State.EXTRA;
						if (this.skip == 0) {
							advance();
						}
					}
				}
				case EXTRA, HEADER_CRC -> {
					int n = (int) Math.min(this.skip, end - at);
					this.skip -= n;
					next = at + n;
					if (this.skip == 0) {
						this.state = this.state == State.EXTRA ? State.NAME : State.BODY;
						advance();
					}
				}
				default -> {
					// a name or a comment, to its zero byte
					while (next < end && bytes[next] != 0) {
						next++;
					}
					if (next < end) {
						next++;
						this.state = this.state == State.NAME ? State.COMMENT : State.HEADER_CRC;
						advance();
					}
				}
			}
			this.headerBytes += next - at;
			if (this.headerBytes > MAX_GZIP_HEADER) {
				throw new CodingException("a gzip header past " + MAX_GZIP_HEADER + " bytes");
			}
			return next;
		}

		private int inflate(byte[] bytes, int at, int end, Sink sink) {
			Inflater inflating = this.inflater;
			if (inflating == null) {
				throw new IllegalStateException("no inflater");
			}
			inflating.setInput(bytes, at, end - at);
			try {
				while (true) {
					int n = inflating.inflate(this.piece);
					if (n > 0) {
						if (this.codec == Codec.GZIP) {
							this.crc.update(this.piece, 0, n);
						}
						sink.accept(this.piece, 0, n);
					}
					if (inflating.finished()) {
						this.state = this.codec == Codec.GZIP ? State.TRAILER : State.DONE;
						return end - inflating.getRemaining();
					}
					if (n == 0) {
						if (inflating.needsDictionary()) {
							throw new CodingException("a deflate body needing a dictionary");
						}
						if (inflating.needsInput()) {
							return end;
						}
						throw new CodingException("a body its inflater stalled on");
					}
				}
			}
			catch (DataFormatException e) {
				throw new CodingException("a body that is not " + this.codec.name().toLowerCase(Locale.ROOT), e);
			}
		}

		private int trailer(byte[] bytes, int at, int end) {
			int next = fill(bytes, at, end, 8);
			if (this.got < 8) {
				return next;
			}
			this.got = 0;
			long crc = littleEndian(0);
			long size = littleEndian(4);
			Inflater inflating = this.inflater;
			if (inflating == null || crc != this.crc.getValue()
					|| size != (inflating.getBytesWritten() & 0xffffffffL)) {
				throw new CodingException("a gzip member whose trailer does not match it");
			}
			this.member = true;
			this.state = State.START;
			return next;
		}

		private long littleEndian(int at) {
			return (this.fields[at] & 0xffL) | (this.fields[at + 1] & 0xffL) << 8 | (this.fields[at + 2] & 0xffL) << 16
					| (this.fields[at + 3] & 0xffL) << 24;
		}

	}

	/**
	 * Encodes gzip as it comes. A live one is flushed on each call, so that a stream
	 * stays live: each flush costs a few bytes and what the window had of the input.
	 */
	private static final class Encoder {

		private final Deflater deflater;

		private final boolean live;

		private final CRC32 crc = new CRC32();

		private final byte[] piece = new byte[PIECE];

		private boolean started;

		/** Input came since the last flush. */
		private boolean pending;

		Encoder(int level, boolean live) {
			this.deflater = new Deflater(level, true);
			this.live = live;
		}

		void feed(byte[] bytes, int offset, int length, Sink sink) {
			start(sink);
			if (length == 0) {
				return;
			}
			this.crc.update(bytes, offset, length);
			this.deflater.setInput(bytes, offset, length);
			while (!this.deflater.needsInput()) {
				drain(Deflater.NO_FLUSH, sink);
			}
			this.pending = true;
		}

		void flush(Sink sink) {
			if (this.live && this.pending) {
				this.pending = false;
				while (drain(Deflater.SYNC_FLUSH, sink) == this.piece.length) {
					// more of the flush
				}
			}
		}

		void finish(Sink sink) {
			start(sink);
			this.deflater.finish();
			while (!this.deflater.finished()) {
				drain(Deflater.NO_FLUSH, sink);
			}
			long crc = this.crc.getValue();
			long size = this.deflater.getBytesRead();
			byte[] trailer = new byte[8];
			for (int i = 0; i < 4; i++) {
				trailer[i] = (byte) (crc >>> (8 * i));
				trailer[4 + i] = (byte) (size >>> (8 * i));
			}
			sink.accept(trailer, 0, trailer.length);
		}

		void end() {
			this.deflater.end();
		}

		private void start(Sink sink) {
			if (!this.started) {
				this.started = true;
				sink.accept(GZIP_HEADER, 0, GZIP_HEADER.length);
			}
		}

		private int drain(int flush, Sink sink) {
			int n = this.deflater.deflate(this.piece, 0, this.piece.length, flush);
			if (n > 0) {
				sink.accept(this.piece, 0, n);
			}
			return n;
		}

	}

}
