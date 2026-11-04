package am.ik.sluice.client.filter;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;

import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import am.ik.sluice.client.filter.UpstreamFilter.BodyHandler;
import am.ik.sluice.client.filter.UpstreamFilter.BodyObserver;
import am.ik.sluice.client.filter.UpstreamFilter.BodyRewriter;
import am.ik.sluice.client.filter.UpstreamFilter.Exchange;
import am.ik.sluice.client.filter.UpstreamFilter.LocalResponse;
import am.ik.sluice.client.filter.UpstreamFilter.Request;
import am.ik.sluice.http1.Http1Heads;
import am.ik.sluice.http1.Http1RequestParser;
import am.ik.sluice.http1.Http1ResponseParser;
import am.ik.sluice.tunnel.DuplexPipe;
import am.ik.sluice.tunnel.TunnelBudget;

import org.springframework.http.HttpStatus;

/**
 * An upstream connection the server speaks HTTP/1.1 on, its exchanges passing an
 * {@link UpstreamFilter}: both directions are parsed as the relay moves them, and changed
 * as the filter says. The server's relay sees an ordinary upstream: whatever comes out
 * here is framed, or ends as the upstream ended. A response the parse cannot follow, and
 * a connection past a 101, are relayed as they are.
 * <p>
 * A filter failing, or a request the parse cannot follow, fails the relay's call: the
 * server answers 502, or cuts the response begun.
 */
public final class UpstreamFilterPipe implements DuplexPipe {

	/**
	 * The most a filtered connection buffers on top of an unfiltered one's
	 * ({@code ConnectionLimit.CONNECTION_BYTES}), a filter's held bytes aside, which
	 * charge the budget: a head being read each way, the heads of the requests awaiting
	 * their responses, and the output waiting for the relay.
	 */
	public static final long CONNECTION_BYTES = 4L * Http1RequestParser.MAX_HEAD;

	/** The heads of the requests awaiting their responses; one larger may be alone. */
	private static final int MAX_PENDING_HEADS = Http1RequestParser.MAX_HEAD;

	/**
	 * The requests awaiting their responses. Few: pipelining is rare, a next request only
	 * waits, and each costs a filter a stream's state, which a client must not be able to
	 * pile up.
	 */
	public static final int MAX_PENDING_REQUESTS = 4;

	/** What the output holds without charging the budget. */
	private static final int OUTPUT_ALLOWANCE = Http1RequestParser.MAX_HEAD;

	/**
	 * What is read of the upstream at once: what merely passes stays within the output's
	 * allowance, its heads' edits and a rewrite's framing added, so only a filter writing
	 * more depends on the budget.
	 */
	private static final int MAX_READ = 48 * 1024;

	private static final Logger log = LoggerFactory.getLogger(UpstreamFilterPipe.class);

	private static final Pattern TOKEN = Pattern.compile("[!#$%&'*+.^_`|~0-9A-Za-z-]+");

	/** Framing, and what the relay follows: a filter cannot change them. */
	private static final Set<String> PROTECTED = Set.of("content-length", "transfer-encoding", "connection", "upgrade",
			"keep-alive", "proxy-connection", "te", "trailer");

	private static final byte[] CRLF = { '\r', '\n' };

	private static final byte[] LAST_CHUNK = { '0', '\r', '\n' };

	private final DuplexPipe upstream;

	private final UpstreamFilter filter;

	private final String route;

	private final String address;

	private final Requests requests = new Requests();

	private final TunnelBudget.Account outputAccount;

	private final RequestSide sink;

	private final ResponseSide source;

	private final AtomicBoolean released = new AtomicBoolean();

	/** A request asking for an upgrade has no answer yet; the first answer stands. */
	private volatile boolean upgradeAwaited;

	/** The answer to the request asking for an upgrade, for the request side. */
	private volatile @Nullable Boolean upgradeAnswer;

	/** The pipe itself closed the upstream: what still comes for it is dropped. */
	private volatile boolean upstreamClosed;

	private UpstreamFilterPipe(Builder builder) {
		this.upstream = Objects.requireNonNull(builder.upstream, "upstream is required");
		this.filter = Objects.requireNonNull(builder.filter, "filter is required");
		this.route = builder.route;
		this.address = builder.address;
		this.outputAccount = Objects.requireNonNull(builder.budget, "budget is required").account();
		this.sink = new RequestSide(this.upstream.sink());
		this.source = new ResponseSide(this.upstream.source());
	}

	public static Builder builder() {
		return new Builder();
	}

	public static final class Builder {

		private @Nullable DuplexPipe upstream;

		private @Nullable UpstreamFilter filter;

		private String route = "";

		private String address = "";

		private @Nullable TunnelBudget budget;

		private Builder() {
		}

		public Builder upstream(DuplexPipe upstream) {
			this.upstream = upstream;
			return this;
		}

		public Builder filter(UpstreamFilter filter) {
			this.filter = filter;
			return this;
		}

		public Builder route(String route) {
			this.route = route;
			return this;
		}

		public Builder address(String address) {
			this.address = address;
			return this;
		}

		public Builder budget(TunnelBudget budget) {
			this.budget = budget;
			return this;
		}

		public UpstreamFilterPipe build() {
			return new UpstreamFilterPipe(this);
		}

	}

	@Override
	public InputStream source() {
		return this.source;
	}

	@Override
	public OutputStream sink() {
		return this.sink;
	}

	@Override
	public void shutdownOutput() throws Exception {
		if (this.sink.ended()) {
			this.upstream.shutdownOutput();
		}
	}

	/**
	 * Closes the upstream; what the filter is told of it runs on a thread of its own, as
	 * the caller may be the tunnel's receive path, which must not wait on a filter.
	 */
	@Override
	public void close() throws Exception {
		try {
			this.upstream.close();
		}
		finally {
			if (this.released.compareAndSet(false, true)) {
				Thread.ofVirtual().name("sluice-filter-release").start(this::release);
			}
		}
	}

	private void release() {
		this.requests.close();
		this.sink.release();
		this.source.release();
		for (Pending pending : this.requests.drain()) {
			abandon(pending.exchange);
		}
		this.outputAccount.close();
	}

	private static void abandon(@Nullable Exchange exchange) {
		if (exchange != null) {
			try {
				exchange.abandon();
			}
			catch (RuntimeException e) {
				log.warn("a filter failed on an exchange abandoned", e);
			}
		}
	}

	/** As the server's relay takes it: the first answer to the upgrade awaited stands. */
	private void answerUpgrade(boolean switched) {
		if (this.upgradeAwaited) {
			this.upgradeAwaited = false;
			this.upgradeAnswer = switched;
		}
	}

	private void closeUpstreamQuietly() {
		this.upstreamClosed = true;
		try {
			this.upstream.close();
		}
		catch (Exception e) {
			log.debug("closing the upstream failed", e);
		}
	}

	/**
	 * Refuses an edit a head cannot take, a filter's name or value being untrusted: the
	 * framing the relay follows, or what would make a line of its own.
	 * @param value null for a removal
	 * @throws IllegalArgumentException when the edit is refused
	 */
	static void checkEdit(String name, @Nullable String value) {
		if (!TOKEN.matcher(name).matches() || PROTECTED.contains(name.toLowerCase(Locale.ROOT))) {
			throw new IllegalArgumentException("header " + name + " cannot be changed by a filter");
		}
		checkValue(name, value);
	}

	private static void checkValue(String name, @Nullable String value) {
		if (value != null) {
			for (int i = 0; i < value.length(); i++) {
				char c = value.charAt(i);
				if (c < 0x20 && c != '\t' || c == 0x7f || c > 0xff) {
					throw new IllegalArgumentException("invalid value of header " + name);
				}
			}
		}
	}

	/** Whether a filter's answer may not set the header: the pipe frames the answer. */
	static boolean framingHeader(String name) {
		return PROTECTED.contains(name.toLowerCase(Locale.ROOT));
	}

	private static List<String> lines(byte[] head) {
		String text = new String(head, 0, head.length - 4, StandardCharsets.ISO_8859_1);
		return new ArrayList<>(List.of(text.split("\r\n", -1)));
	}

	/**
	 * A head on the wire. Past the limit the server's relay would stop following what the
	 * pipe goes on following.
	 */
	private static byte[] joined(List<String> lines) {
		byte[] head = (String.join("\r\n", lines) + "\r\n\r\n").getBytes(StandardCharsets.ISO_8859_1);
		if (head.length > Http1RequestParser.MAX_HEAD) {
			throw new IllegalArgumentException("a head edited past " + Http1RequestParser.MAX_HEAD + " bytes");
		}
		return head;
	}

	private static String nameOf(String line) {
		int colon = line.indexOf(':');
		return colon <= 0 ? "" : line.substring(0, colon).trim().toLowerCase(Locale.ROOT);
	}

	private static boolean framing(String line) {
		String name = nameOf(line);
		return name.equals("content-length") || name.equals("transfer-encoding");
	}

	/**
	 * A body not rewritten goes as it was framed, whatever a filter left of the framing
	 * of its head: without it the body would read as the next message.
	 */
	private static void keepFraming(List<String> lines, byte[] head) {
		List<String> framing = lines(head).stream().filter(UpstreamFilterPipe::framing).toList();
		if (!lines.containsAll(framing)) {
			lines.removeIf(UpstreamFilterPipe::framing);
			lines.addAll(framing);
		}
	}

	/** The codings of the body but the framing's: what it stays coded with. */
	private static List<String> codings(List<String> lines) {
		List<String> codings = new ArrayList<>();
		for (String line : lines) {
			if (nameOf(line).equals("transfer-encoding")) {
				for (String coding : line.substring(line.indexOf(':') + 1).split(",")) {
					String trimmed = coding.trim();
					if (!trimmed.isEmpty() && !trimmed.equalsIgnoreCase("chunked")) {
						codings.add(trimmed);
					}
				}
			}
		}
		return codings;
	}

	/** Frames the head's message anew: chunked, whatever codings it has before. */
	private static void frameChunked(List<String> lines) {
		List<String> codings = new ArrayList<>(codings(lines));
		codings.add("chunked");
		lines.removeIf(UpstreamFilterPipe::framing);
		lines.add("Transfer-Encoding: " + String.join(", ", codings));
	}

	/**
	 * h2 past a {@code 101} could not be read: asked for it, the upstream sees an
	 * ordinary request, which RFC 9113 lets it answer as one. The relay awaits the answer
	 * as for any upgrade, and takes this one as declined.
	 * @return whether the head changed
	 */
	private static boolean dropH2cUpgrade(List<String> lines) {
		boolean h2c = lines.stream()
			.anyMatch(line -> nameOf(line).equals("upgrade") && line.toLowerCase(Locale.ROOT).contains("h2c"));
		if (!h2c) {
			return false;
		}
		lines.removeIf(line -> nameOf(line).equals("upgrade") || nameOf(line).equals("http2-settings"));
		List<String> kept = new ArrayList<>();
		for (String line : lines) {
			if (!nameOf(line).equals("connection")) {
				kept.add(line);
				continue;
			}
			List<String> options = new ArrayList<>();
			for (String option : line.substring(line.indexOf(':') + 1).split(",")) {
				String trimmed = option.trim();
				if (!trimmed.equalsIgnoreCase("upgrade") && !trimmed.equalsIgnoreCase("http2-settings")) {
					options.add(trimmed);
				}
			}
			if (!options.isEmpty()) {
				kept.add(line.substring(0, line.indexOf(':') + 1) + " " + String.join(", ", options));
			}
		}
		lines.clear();
		lines.addAll(kept);
		return true;
	}

	/** A request whose response has not begun. */
	private static final class Pending {

		final Request request;

		final @Nullable Exchange exchange;

		final boolean upgrade;

		final int headBytes;

		/** The answer in the upstream's place; guarded by the queue's lock. */
		@Nullable LocalResponse local;

		Pending(Request request, @Nullable Exchange exchange, boolean upgrade, int headBytes) {
			this.request = request;
			this.exchange = exchange;
			this.upgrade = upgrade;
			this.headBytes = headBytes;
		}

	}

	/**
	 * The requests whose response has not begun, in order. A client pipelining past
	 * {@link #MAX_PENDING_REQUESTS} or {@link #MAX_PENDING_HEADS} waits for the
	 * upstream's answers: the wait holds the tunnel's flow control, and the response side
	 * goes on regardless, until it ends, which ends the wait too.
	 */
	private static final class Requests {

		private final ReentrantLock lock = new ReentrantLock();

		private final Condition room = this.lock.newCondition();

		private final ArrayDeque<Pending> queue = new ArrayDeque<>();

		private int headBytes;

		private boolean closed;

		/**
		 * Waits until a head of that size may go on; false once none will be answered.
		 */
		boolean awaitRoom(int bytes) {
			this.lock.lock();
			try {
				while (!this.closed && !this.queue.isEmpty()
						&& (this.queue.size() >= MAX_PENDING_REQUESTS || this.headBytes + bytes > MAX_PENDING_HEADS)) {
					this.room.awaitUninterruptibly();
				}
				return !this.closed;
			}
			finally {
				this.lock.unlock();
			}
		}

		void add(Pending pending) {
			this.lock.lock();
			try {
				this.queue.add(pending);
				this.headBytes += pending.headBytes;
			}
			finally {
				this.lock.unlock();
			}
		}

		@Nullable Pending poll() {
			this.lock.lock();
			try {
				Pending pending = this.queue.poll();
				if (pending != null) {
					this.headBytes -= pending.headBytes;
					this.room.signalAll();
				}
				return pending;
			}
			finally {
				this.lock.unlock();
			}
		}

		/** The next one when it is to be answered in the upstream's place. */
		@Nullable Pending nextAnswered() {
			this.lock.lock();
			try {
				Pending pending = this.queue.peek();
				return pending != null && pending.local != null ? pending : null;
			}
			finally {
				this.lock.unlock();
			}
		}

		/**
		 * Sets the answer in the upstream's place; false once the response has begun.
		 */
		boolean answer(Pending pending, LocalResponse local) {
			this.lock.lock();
			try {
				if (!this.queue.contains(pending)) {
					return false;
				}
				pending.local = local;
				return true;
			}
			finally {
				this.lock.unlock();
			}
		}

		/** Whether it is the next to get a response. */
		boolean first(Pending pending) {
			this.lock.lock();
			try {
				return this.queue.peek() == pending;
			}
			finally {
				this.lock.unlock();
			}
		}

		void close() {
			this.lock.lock();
			try {
				this.closed = true;
				this.room.signalAll();
			}
			finally {
				this.lock.unlock();
			}
		}

		List<Pending> drain() {
			this.lock.lock();
			try {
				List<Pending> drained = List.copyOf(this.queue);
				this.queue.clear();
				this.headBytes = 0;
				return drained;
			}
			finally {
				this.lock.unlock();
			}
		}

	}

	/**
	 * One body on its way, as its handler has it: relayed as framed while read, or framed
	 * anew from what a rewriter gives back.
	 */
	private static final class Flow {

		private final OutputStream out;

		private final @Nullable BodyObserver observer;

		private final @Nullable BodyRewriter rewriter;

		/** A request head waiting for the rewriter's first bytes; else null. */
		private @Nullable HeldHead held;

		/** What goes out is chunked; else it is as long as the head tells, or the end. */
		private boolean chunked;

		/** A rewrite's last part, from the last chunk to the trailers. */
		private byte @Nullable [] lastPart;

		private boolean lastWritten;

		private boolean closed;

		Flow(OutputStream out, @Nullable BodyHandler handler, boolean chunked) {
			this.out = out;
			this.observer = handler instanceof BodyObserver reading ? reading : null;
			this.rewriter = handler instanceof BodyRewriter rewriting ? rewriting : null;
			this.chunked = chunked;
		}

		void hold(HeldHead head) {
			this.held = head;
		}

		void payload(byte[] bytes, int offset, int length) throws IOException {
			BodyRewriter rewriting = this.rewriter;
			if (rewriting != null) {
				write(rewriting.onData(bytes, offset, length));
				return;
			}
			this.out.write(bytes, offset, length);
			if (this.observer != null) {
				this.observer.onData(bytes, offset, length);
			}
		}

		void framing(byte[] bytes, int offset, int length) throws IOException {
			if (this.rewriter == null) {
				this.out.write(bytes, offset, length);
			}
		}

		void lastChunk(byte[] line) throws IOException {
			BodyRewriter rewriting = this.rewriter;
			if (rewriting == null) {
				this.out.write(line);
			}
			else {
				this.lastPart = rewriting.onEnd();
			}
		}

		/**
		 * Trailers, whole or line by line; the rewrite's end goes before them, and what
		 * is not chunked has no place for them.
		 */
		void trailers(byte[] section) throws IOException {
			if (this.rewriter != null && !this.lastWritten) {
				writeLast(section);
			}
			else if (this.rewriter == null || this.chunked) {
				this.out.write(section);
			}
		}

		/**
		 * The message ended: a rewrite that met no last chunk ends here. However that
		 * goes, its handler is told: what it holds is given back.
		 */
		void completed() throws IOException {
			boolean complete = false;
			try {
				BodyRewriter rewriting = this.rewriter;
				if (rewriting != null && !this.lastWritten) {
					this.lastPart = rewriting.onEnd();
					writeLast(CRLF);
				}
				complete = true;
			}
			finally {
				close(complete);
			}
		}

		void close(boolean complete) {
			if (this.closed) {
				return;
			}
			this.closed = true;
			if (this.rewriter != null) {
				this.rewriter.onClose();
			}
			if (this.observer != null) {
				this.observer.onEnd(complete);
			}
		}

		private void write(byte[] part) throws IOException {
			if (part.length == 0) {
				return;
			}
			HeldHead waiting = this.held;
			if (waiting != null) {
				this.held = null;
				BodyRewriter rewriting = Objects.requireNonNull(this.rewriter);
				this.chunked = waiting.release(this.out, rewriting.unchanged());
			}
			if (this.chunked) {
				this.out.write(Integer.toHexString(part.length).getBytes(StandardCharsets.US_ASCII));
				this.out.write(CRLF);
				this.out.write(part);
				this.out.write(CRLF);
			}
			else {
				this.out.write(part);
			}
		}

		/**
		 * The rewrite's last part. A request whose head still waits goes with the length
		 * of it, unless trailers follow; what is chunked gets its last chunk.
		 */
		private void writeLast(byte[] section) throws IOException {
			this.lastWritten = true;
			byte[] last = Objects.requireNonNull(this.lastPart);
			HeldHead waiting = this.held;
			if (waiting != null && section.length == CRLF.length && waiting.sizable()) {
				this.held = null;
				this.chunked = false;
				waiting.releaseSized(this.out, last.length);
				this.out.write(last);
				return;
			}
			write(last);
			if (waiting != null && this.held != null) {
				// nothing came back, and trailers follow
				this.held = null;
				this.chunked = waiting.release(this.out, false);
			}
			if (this.chunked) {
				this.out.write(LAST_CHUNK);
				this.out.write(section);
			}
		}

	}

	/** A request head waiting for its rewriter's first bytes. */
	private static final class HeldHead {

		private final List<String> lines;

		private final boolean sized;

		private final boolean http10;

		HeldHead(List<String> lines, boolean sized, boolean http10) {
			this.lines = lines;
			this.sized = sized;
			this.http10 = http10;
		}

		/** Whether a length can frame it: a coding of its body would need chunks. */
		boolean sizable() {
			return codings(this.lines).isEmpty();
		}

		/**
		 * The head goes out chunked, or as it was when the body stays as it was and tells
		 * its length.
		 * @return whether the body goes out chunked
		 * @throws IOException when it cannot be framed: an HTTP/1.0 request takes no
		 * chunks
		 */
		boolean release(OutputStream out, boolean unchanged) throws IOException {
			if (unchanged && this.sized) {
				out.write(joined(this.lines));
				return false;
			}
			if (this.http10) {
				throw new IOException("an HTTP/1.0 request rewritten in pieces");
			}
			frameChunked(this.lines);
			out.write(joined(this.lines));
			return true;
		}

		void releaseSized(OutputStream out, int length) throws IOException {
			this.lines.removeIf(UpstreamFilterPipe::framing);
			this.lines.add("Content-Length: " + length);
			out.write(joined(this.lines));
		}

	}

	/** Follows the requests the relay writes towards the upstream. */
	private final class RequestSide extends OutputStream implements Http1RequestParser.Listener {

		private final OutputStream out;

		private final ReentrantLock lock = new ReentrantLock();

		private final Http1RequestParser parser = new Http1RequestParser(this);

		private @Nullable Flow flow;

		private @Nullable Pending current;

		/** An answer in the upstream's place was given: nothing more goes on. */
		private boolean discarding;

		private boolean released;

		RequestSide(OutputStream out) {
			// a head, framing and payload in one segment rather than one each
			this.out = new BufferedOutputStream(out, 8 * 1024);
		}

		@Override
		public void write(int b) throws IOException {
			write(new byte[] { (byte) b }, 0, 1);
		}

		@Override
		public void write(byte[] bytes, int offset, int length) throws IOException {
			this.lock.lock();
			try {
				if (this.released) {
					throw new IOException("the connection ended");
				}
				if (this.discarding || UpstreamFilterPipe.this.upstreamClosed) {
					return;
				}
				applyUpgradeAnswer();
				this.parser.feed(bytes, offset, length);
				if (this.parser.position() == Http1RequestParser.Position.MALFORMED) {
					// never reached: the server relays what its own parser accepted
					throw new IOException("a request the filter cannot follow");
				}
				applyUpgradeAnswer();
				this.out.flush();
			}
			catch (UncheckedIOException e) {
				throw e.getCause();
			}
			catch (RuntimeException e) {
				throw new IOException("the filter failed", e);
			}
			finally {
				this.lock.unlock();
			}
		}

		@Override
		public void flush() {
			// flushed with each write
		}

		@Override
		public void close() throws IOException {
			this.out.close();
		}

		/**
		 * The relay's source ended: what was read of a head goes on as it came, a body
		 * not ended is told as cut. False once an answer in the upstream's place was
		 * given: the upstream is gone.
		 */
		boolean ended() throws IOException {
			this.lock.lock();
			try {
				if (this.discarding || this.released || UpstreamFilterPipe.this.upstreamClosed) {
					return false;
				}
				this.parser.finish();
				Flow ending = this.flow;
				if (ending != null) {
					this.flow = null;
					ending.close(false);
				}
				this.out.flush();
				return true;
			}
			catch (UncheckedIOException e) {
				throw e.getCause();
			}
			catch (RuntimeException e) {
				throw new IOException("the filter failed", e);
			}
			finally {
				this.lock.unlock();
			}
		}

		void release() {
			this.lock.lock();
			try {
				this.released = true;
				Flow ending = this.flow;
				if (ending != null) {
					this.flow = null;
					ending.close(false);
				}
			}
			catch (RuntimeException e) {
				log.warn("a filter failed on a request cut", e);
			}
			finally {
				this.lock.unlock();
			}
		}

		/** The server's relay sends nothing until the answer reached it. */
		private void applyUpgradeAnswer() {
			Boolean answer = UpstreamFilterPipe.this.upgradeAnswer;
			if (answer != null && this.parser.position() == Http1RequestParser.Position.AWAITING_UPGRADE) {
				UpstreamFilterPipe.this.upgradeAnswer = null;
				this.parser.upgradeAnswered(answer);
			}
		}

		@Override
		public void head(byte[] head, @Nullable String host) {
			try {
				onHead(head, host);
			}
			catch (IOException e) {
				throw new UncheckedIOException(e);
			}
		}

		private void onHead(byte[] head, @Nullable String host) throws IOException {
			if (this.discarding) {
				return;
			}
			// what came before may be what the upstream waits for to answer
			this.out.flush();
			if (!UpstreamFilterPipe.this.requests.awaitRoom(head.length)) {
				// the responses ended: nothing goes on, and the relay ends with them
				this.discarding = true;
				return;
			}
			Http1Heads.RequestLine line = Objects.requireNonNull(Http1Heads.requestLine(head), "a request line");
			Request request = Request.builder()
				.route(UpstreamFilterPipe.this.route)
				.address(UpstreamFilterPipe.this.address)
				.method(line.method())
				.target(line.target())
				.version("HTTP/" + line.version())
				.host(host)
				.bodyFollows(this.parser.bodyFollows())
				.build();
			List<String> lines = lines(head);
			EditedHeaders headers = new EditedHeaders(lines, true, dropH2cUpgrade(lines));
			Exchange exchange = UpstreamFilterPipe.this.filter.onRequest(request, headers);
			boolean upgrade = Http1Heads.isUpgrade(head);
			Pending pending = new Pending(request, exchange, upgrade, head.length);
			UpstreamFilterPipe.this.requests.add(pending);
			if (upgrade) {
				UpstreamFilterPipe.this.upgradeAwaited = true;
			}
			this.current = pending;
			LocalResponse local = exchange == null ? null : exchange.localResponse();
			if (local != null) {
				answerLocally(pending, local);
				return;
			}
			BodyHandler handler = exchange != null && this.parser.bodyFollows() ? exchange.onRequestBody() : null;
			if (handler instanceof BodyRewriter) {
				boolean http10 = "HTTP/1.0".equals(request.version());
				if (Http1Heads.first(head, "expect") == null || http10) {
					Flow rewriting = new Flow(this.out, handler, true);
					rewriting.hold(new HeldHead(lines, !headers.values("content-length").isEmpty(), http10));
					this.flow = rewriting;
					return;
				}
				// its client waits for the upstream's 100 before sending the body
				frameChunked(lines);
				this.out.write(joined(lines));
				this.flow = new Flow(this.out, handler, true);
				return;
			}
			keepFraming(lines, head);
			this.out.write(headers.edited() ? joined(lines) : head);
			if (this.parser.bodyFollows()) {
				this.flow = new Flow(this.out, handler, true);
			}
		}

		/**
		 * Gives the answer in the upstream's place, once the responses before it ended.
		 * With none of them left the response side may be waiting on an idle upstream,
		 * which closing ends: the connection ends behind the answer anyway.
		 */
		private void answerLocally(Pending pending, LocalResponse local) throws IOException {
			if (!UpstreamFilterPipe.this.requests.answer(pending, local)) {
				throw new IOException("a filter answered a request whose response has begun");
			}
			this.discarding = true;
			Flow ending = this.flow;
			if (ending != null) {
				this.flow = null;
				ending.close(false);
			}
			if (UpstreamFilterPipe.this.source.idle(pending)) {
				closeUpstreamQuietly();
			}
		}

		/** A filter may answer after a call on the body. */
		private void answerIfAsked() throws IOException {
			Pending pending = this.current;
			if (pending != null && pending.exchange != null) {
				LocalResponse local = pending.exchange.localResponse();
				if (local != null) {
					answerLocally(pending, local);
				}
			}
		}

		@Override
		public void data(byte[] bytes, int offset, int length) {
			if (this.discarding) {
				return;
			}
			try {
				this.out.write(bytes, offset, length);
			}
			catch (IOException e) {
				throw new UncheckedIOException(e);
			}
		}

		@Override
		public void payload(byte[] bytes, int offset, int length) {
			Flow body = this.flow;
			if (this.discarding || body == null) {
				return;
			}
			try {
				body.payload(bytes, offset, length);
				answerIfAsked();
			}
			catch (IOException e) {
				throw new UncheckedIOException(e);
			}
		}

		@Override
		public void framing(byte[] bytes, int offset, int length) {
			Flow body = this.flow;
			if (this.discarding || body == null) {
				return;
			}
			try {
				body.framing(bytes, offset, length);
			}
			catch (IOException e) {
				throw new UncheckedIOException(e);
			}
		}

		@Override
		public void lastChunk(byte[] line) {
			Flow body = this.flow;
			if (this.discarding || body == null) {
				return;
			}
			try {
				body.lastChunk(line);
				answerIfAsked();
			}
			catch (IOException e) {
				throw new UncheckedIOException(e);
			}
		}

		@Override
		public void trailers(byte[] section) {
			Flow body = this.flow;
			if (this.discarding || body == null) {
				return;
			}
			try {
				body.trailers(section);
			}
			catch (IOException e) {
				throw new UncheckedIOException(e);
			}
		}

		@Override
		public void messageEnd() {
			Flow body = this.flow;
			if (this.discarding) {
				return;
			}
			this.flow = null;
			if (body != null) {
				try {
					body.completed();
					answerIfAsked();
				}
				catch (IOException e) {
					throw new UncheckedIOException(e);
				}
			}
			this.current = null;
		}

	}

	/** What waits for the relay; past its allowance it charges the budget. */
	private final class Output extends ByteArrayOutputStream {

		private int position;

		Output() {
			super(1024);
		}

		boolean hasRemaining() {
			return this.position < this.count;
		}

		int drain(byte[] bytes, int offset, int length) {
			int n = Math.min(length, this.count - this.position);
			System.arraycopy(this.buf, this.position, bytes, offset, n);
			this.position += n;
			if (this.position == this.count) {
				reset();
				this.position = 0;
				if (this.buf.length > 2 * OUTPUT_ALLOWANCE) {
					this.buf = new byte[OUTPUT_ALLOWANCE];
				}
			}
			UpstreamFilterPipe.this.outputAccount.resize(Math.max(0, this.count - this.position - OUTPUT_ALLOWANCE));
			return n;
		}

		/** Charged as it is written: one read may cause many writes of a filter's. */
		@Override
		public void write(byte[] bytes, int offset, int length) {
			long over = (long) this.count - this.position + length - OUTPUT_ALLOWANCE;
			if (over > 0 && !UpstreamFilterPipe.this.outputAccount.tryGrow(over)) {
				throw new UncheckedIOException(new IOException("a filter's output past the tunnel budget"));
			}
			super.write(bytes, offset, length);
		}

	}

	/** Parses what the upstream sends as it is read, and hands out what it became. */
	private final class ResponseSide extends InputStream implements Http1ResponseParser.Listener {

		private final InputStream in;

		private final Output out = new Output();

		/** Held while parsing, which writes to memory and calls the filter only. */
		private final ReentrantLock lock = new ReentrantLock();

		private final Http1ResponseParser parser = new Http1ResponseParser(this);

		private @Nullable Flow flow;

		/** The body in flow is what ends the connection: an HTTP/1.0 client's rewrite. */
		private boolean closeAfterBody;

		private boolean ended;

		ResponseSide(InputStream in) {
			this.in = in;
		}

		@Override
		public int read() throws IOException {
			byte[] one = new byte[1];
			return read(one, 0, 1) < 0 ? -1 : one[0] & 0xff;
		}

		@Override
		public int read(byte[] bytes, int offset, int length) throws IOException {
			if (length == 0) {
				return 0;
			}
			while (true) {
				this.lock.lock();
				try {
					if (this.out.hasRemaining()) {
						return this.out.drain(bytes, offset, length);
					}
					if (this.ended) {
						return -1;
					}
					if (giveAnswerIfDue()) {
						continue;
					}
				}
				finally {
					this.lock.unlock();
				}
				int n = readUpstream(bytes, offset, Math.min(length, MAX_READ));
				this.lock.lock();
				try {
					if (this.ended) {
						return -1;
					}
					if (giveAnswerIfDue()) {
						continue;
					}
					if (n < 0) {
						end();
						this.parser.finish();
					}
					else {
						this.parser.feed(bytes, offset, n);
					}
				}
				catch (UncheckedIOException e) {
					end();
					throw e.getCause();
				}
				catch (RuntimeException e) {
					// what the filter did not finish must not look like the upstream's:
					// the server's relay answers 502, or cuts the response begun
					end();
					throw new IOException("the filter failed", e);
				}
				finally {
					this.lock.unlock();
				}
			}
		}

		/** No response will come from here: no request is to wait for one. */
		private void end() {
			this.ended = true;
			UpstreamFilterPipe.this.requests.close();
		}

		private int readUpstream(byte[] bytes, int offset, int length) throws IOException {
			try {
				return this.in.read(bytes, offset, length);
			}
			catch (IOException e) {
				this.lock.lock();
				try {
					if (this.parser.atResponseStart() && UpstreamFilterPipe.this.requests.nextAnswered() != null) {
						// closed behind the answer in the upstream's place
						return -1;
					}
					end();
					closeFlow();
				}
				finally {
					this.lock.unlock();
				}
				throw e;
			}
		}

		private boolean giveAnswerIfDue() {
			if (!this.parser.atResponseStart()) {
				return false;
			}
			Pending pending = UpstreamFilterPipe.this.requests.nextAnswered();
			if (pending == null) {
				return false;
			}
			UpstreamFilterPipe.this.requests.poll();
			giveAnswer(pending);
			return true;
		}

		private void giveAnswer(Pending pending) {
			LocalResponse local = Objects.requireNonNull(pending.local);
			this.out.writeBytes(answer(local, "HEAD".equals(pending.request.method())));
			end();
			abandon(pending.exchange);
			closeUpstreamQuietly();
		}

		private byte[] answer(LocalResponse local, boolean bodiless) {
			HttpStatus known = HttpStatus.resolve(local.status());
			List<String> lines = new ArrayList<>();
			lines.add("HTTP/1.1 " + local.status() + " " + (known == null ? "" : known.getReasonPhrase()));
			lines.addAll(local.lines());
			boolean noBody = local.status() == 204 || local.status() == 304;
			if (!noBody) {
				lines.add("Content-Length: " + local.body().length);
			}
			lines.add("Connection: close");
			byte[] head = joined(lines);
			if (bodiless || noBody) {
				return head;
			}
			byte[] whole = new byte[head.length + local.body().length];
			System.arraycopy(head, 0, whole, 0, head.length);
			System.arraycopy(local.body(), 0, whole, head.length, local.body().length);
			return whole;
		}

		/** Whether the answer to the request can go at once: nothing else is awaited. */
		boolean idle(Pending pending) {
			this.lock.lock();
			try {
				return this.parser.atResponseStart() && UpstreamFilterPipe.this.requests.first(pending);
			}
			finally {
				this.lock.unlock();
			}
		}

		void release() {
			this.lock.lock();
			try {
				end();
				closeFlow();
			}
			catch (RuntimeException e) {
				log.warn("a filter failed on a response cut", e);
			}
			finally {
				this.lock.unlock();
			}
		}

		private void closeFlow() {
			Flow ending = this.flow;
			if (ending != null) {
				this.flow = null;
				ending.close(false);
			}
		}

		@Override
		public void interimHead(byte[] head) {
			this.out.writeBytes(head);
		}

		@Override
		public Http1ResponseParser.Follows finalHead(byte[] head, int status) {
			Pending pending = UpstreamFilterPipe.this.requests.poll();
			if (pending != null && pending.local != null) {
				// a filter answered, the upstream answering too: what follows is dropped
				giveAnswer(pending);
				return Http1ResponseParser.Follows.SWITCH;
			}
			String method = pending == null ? "" : pending.request.method();
			boolean switched = status == 101 || "CONNECT".equals(method) && status >= 200 && status < 300;
			if (pending != null && pending.upgrade) {
				answerUpgrade(switched);
			}
			Exchange exchange = pending == null ? null : pending.exchange;
			if (switched) {
				answerUpgrade(true);
				abandon(exchange);
				this.out.writeBytes(head);
				return Http1ResponseParser.Follows.SWITCH;
			}
			boolean bodiless = "HEAD".equals(method) || status == 204 || status == 304;
			if (exchange == null || pending == null || !Http1ResponseParser.followable(head)) {
				abandon(exchange);
				this.out.writeBytes(head);
				if (bodiless) {
					return Http1ResponseParser.Follows.NOTHING;
				}
				this.flow = new Flow(this.out, null, false);
				return Http1ResponseParser.Follows.BODY;
			}
			List<String> lines = lines(head);
			EditedHeaders headers = new EditedHeaders(lines, false, false);
			BodyHandler handler;
			try {
				handler = exchange.onResponse(status, headers, bodiless);
			}
			catch (RuntimeException e) {
				abandon(exchange);
				throw e;
			}
			boolean http10 = "HTTP/1.0".equals(pending.request.version());
			if (handler instanceof BodyRewriter) {
				if (http10) {
					// it cannot take chunks: the connection's end ends the body
					lines.removeIf(UpstreamFilterPipe::framing);
					lines.removeIf(line -> nameOf(line).equals("connection"));
					lines.add("Connection: close");
					this.closeAfterBody = !bodiless;
				}
				// as a GET's would be; a 304 and a 204 carry no framing
				else if (status != 204 && status != 304) {
					frameChunked(lines);
				}
				else {
					lines.removeIf(UpstreamFilterPipe::framing);
				}
			}
			else {
				keepFraming(lines, head);
			}
			this.out.writeBytes(handler instanceof BodyRewriter || headers.edited() ? joined(lines) : head);
			if (bodiless) {
				new Flow(this.out, handler, false).close(true);
				return Http1ResponseParser.Follows.NOTHING;
			}
			this.flow = new Flow(this.out, handler, !http10);
			return Http1ResponseParser.Follows.BODY;
		}

		@Override
		public void payload(byte[] bytes, int offset, int length) {
			try {
				Objects.requireNonNull(this.flow).payload(bytes, offset, length);
			}
			catch (IOException e) {
				throw new UncheckedIOException(e);
			}
		}

		@Override
		public void framing(byte[] bytes, int offset, int length) {
			try {
				Objects.requireNonNull(this.flow).framing(bytes, offset, length);
			}
			catch (IOException e) {
				throw new UncheckedIOException(e);
			}
		}

		@Override
		public void lastChunk(byte[] line) {
			try {
				Objects.requireNonNull(this.flow).lastChunk(line);
			}
			catch (IOException e) {
				throw new UncheckedIOException(e);
			}
		}

		@Override
		public void trailer(byte[] line) {
			try {
				Objects.requireNonNull(this.flow).trailers(line);
			}
			catch (IOException e) {
				throw new UncheckedIOException(e);
			}
		}

		@Override
		public void completed() {
			Flow body = this.flow;
			this.flow = null;
			if (body != null) {
				try {
					body.completed();
				}
				catch (IOException e) {
					throw new UncheckedIOException(e);
				}
			}
			if (this.closeAfterBody) {
				end();
				closeUpstreamQuietly();
			}
		}

		@Override
		public void untracked(byte[] raw) {
			// a request awaiting an upgrade's answer is relayed verbatim, as the relay
			// does
			answerUpgrade(true);
			closeFlow();
			this.out.writeBytes(raw);
		}

		@Override
		public void verbatim(byte[] bytes, int offset, int length) {
			if (!this.ended) {
				this.out.write(bytes, offset, length);
			}
		}

		@Override
		public void cut() {
			closeFlow();
		}

	}

	/** The fields of a head being offered, edited in place. */
	private static final class EditedHeaders implements UpstreamFilter.Headers {

		private final List<String> lines;

		private final boolean ofRequest;

		private boolean edited;

		EditedHeaders(List<String> lines, boolean ofRequest, boolean edited) {
			this.lines = lines;
			this.ofRequest = ofRequest;
			this.edited = edited;
		}

		boolean edited() {
			return this.edited;
		}

		private void check(String name, @Nullable String value) {
			checkEdit(name, value);
			// the relay routed by it
			if (this.ofRequest && name.equalsIgnoreCase("host")) {
				throw new IllegalArgumentException("header " + name + " cannot be changed by a filter");
			}
		}

		@Override
		public List<String> values(String name) {
			String lower = name.toLowerCase(Locale.ROOT);
			List<String> values = new ArrayList<>();
			for (int i = 1; i < this.lines.size(); i++) {
				String line = this.lines.get(i);
				if (nameOf(line).equals(lower)) {
					values.add(line.substring(line.indexOf(':') + 1).trim());
				}
			}
			return values;
		}

		@Override
		public void set(String name, String value) {
			check(name, value);
			remove(name);
			this.lines.add(name + ": " + value);
			this.edited = true;
		}

		@Override
		public void remove(String name) {
			check(name, null);
			String lower = name.toLowerCase(Locale.ROOT);
			this.edited |= this.lines.subList(1, this.lines.size()).removeIf(line -> nameOf(line).equals(lower));
		}

		@Override
		public List<String> lines() {
			return List.copyOf(this.lines.subList(1, this.lines.size()));
		}

		@Override
		public void replaceLines(List<String> replaced) {
			String start = this.lines.get(0);
			this.lines.clear();
			this.lines.add(start);
			this.lines.addAll(replaced);
			this.edited = true;
		}

	}

}
