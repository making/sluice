package am.ik.sluice.server.proxy;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import org.jspecify.annotations.Nullable;

import am.ik.sluice.server.route.Router;
import am.ik.sluice.server.tunnel.SessionRegistry;
import am.ik.sluice.server.tunnel.TunnelSession;
import am.ik.sluice.tunnel.DuplexPipe;
import am.ik.sluice.tunnel.StreamRelay;
import am.ik.sluice.tunnel.VirtualConnection;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

/**
 * Relays one plaintext HTTP/2 client connection over per-route legs: the
 * {@code :authority} of every request HEADERS block is resolved through the router and
 * the stream is relayed, frame for frame with its stream id unchanged, to a virtual
 * connection of the matching route -- concurrent streams of one connection (h2
 * multiplexing, connection coalescing) each reach their own upstream. Streams sharing a
 * route share the route's leg. The relay is the HTTP/2 endpoint of the client connection
 * for everything connection-level: settings are answered and re-announced to the legs,
 * pings are answered, and the connection-level flow-control windows are regenerated from
 * the bytes relayed (bytes forwarded over a hop release that hop's connection window for
 * the sender), while stream-level WINDOW_UPDATE frames and all other stream traffic pass
 * through verbatim. The end of the client stream half-closes every leg; the end of a leg
 * that still has streams awaiting their response ends the whole relay, as does any
 * failure.
 * <p>
 * The initial connection window is the spec default on every connection and the relay
 * advertises no settings of its own, so a stream can transiently exceed a leg's tighter
 * window; every hop in between is buffered (socket, tunnel queue, gRPC), which absorbs
 * it. HPACK blocks are relayed verbatim, so each side's dynamic table only ever sees a
 * subsequence of one connection's blocks, which decodes identically.
 */
final class Http2DemuxRelay {

	private static final int BUFFER_SIZE = 64 * 1024;

	private static final String METRIC_NAME = "sluice.tunnel.bytes";

	private static final int FRAME_DATA = 0x0;

	private static final int FRAME_HEADERS = 0x1;

	private static final int FRAME_RST_STREAM = 0x3;

	private static final int FRAME_SETTINGS = 0x4;

	private static final int FRAME_PUSH_PROMISE = 0x5;

	private static final int FRAME_PING = 0x6;

	private static final int FRAME_WINDOW_UPDATE = 0x8;

	private static final int FRAME_CONTINUATION = 0x9;

	private static final int FLAG_END_STREAM = 0x1;

	private static final int FLAG_ACK = 0x1;

	private static final int FLAG_END_HEADERS = 0x4;

	private static final int FLAG_PADDED = 0x8;

	private static final int FLAG_PRIORITY = 0x20;

	private static final int FRAME_HEADER_LENGTH = 9;

	/**
	 * SETTINGS_MAX_FRAME_SIZE every endpoint accepts without announcing more (RFC 9113).
	 */
	private static final int MAX_FRAME_SIZE = 16 * 1024;

	private final DuplexPipe pipe;

	private final byte[] seedHead;

	private final Router router;

	private final SessionRegistry sessions;

	private final AccessControl accessControl;

	private final ErrorResponse errorResponse;

	private final AccessLogger.Connection access;

	private final MeterRegistry meterRegistry;

	private final Router.Route initialRoute;

	private final TunnelSession initialSession;

	private final @Nullable InetAddress peer;

	private final Runnable onFinish;

	/** Serializes the writes of the shared client stream. */
	private final Object sinkLock = new Object();

	/** Stream id to the leg relaying it; entries live for the life of the connection. */
	private final Map<Integer, Leg> streams = new ConcurrentHashMap<>();

	private final List<Leg> legs = new CopyOnWriteArrayList<>();

	/** The outbound side plus one side per leg; the relay ends when all drain. */
	private final java.util.concurrent.atomic.AtomicInteger pending = new java.util.concurrent.atomic.AtomicInteger(1);

	private final java.util.concurrent.atomic.AtomicBoolean finished = new java.util.concurrent.atomic.AtomicBoolean();

	/** Client bytes read but not yet parsed into complete frames. */
	private final ByteArrayOutputStream buffer = new ByteArrayOutputStream(BUFFER_SIZE);

	/** Assembles a split request head, with the flags of its head frame. */
	private final ByteArrayOutputStream assembling = new ByteArrayOutputStream(256);

	private int assemblingStream = -1;

	private boolean assemblingEndStream;

	/** SETTINGS payloads announced by the client, replayed to every new leg. */
	private final ByteArrayOutputStream clientSettings = new ByteArrayOutputStream(128);

	private final io.netty.handler.codec.http2.DefaultHttp2HeadersDecoder headersDecoder = new io.netty.handler.codec.http2.DefaultHttp2HeadersDecoder(
			true);

	private final io.netty.handler.codec.http2.DefaultHttp2HeadersEncoder headersEncoder = new io.netty.handler.codec.http2.DefaultHttp2HeadersEncoder();

	private boolean firstHead = true;

	Http2DemuxRelay(DuplexPipe pipe, ConnectionHeadParser.Head head, Router router, SessionRegistry sessions,
			AccessControl accessControl, ErrorResponse errorResponse, AccessLogger.Connection access,
			MeterRegistry meterRegistry, Router.Route initialRoute, TunnelSession initialSession,
			@Nullable InetAddress peer, Runnable onFinish) {
		this.pipe = pipe;
		this.seedHead = head.bytes();
		this.router = router;
		this.sessions = sessions;
		this.accessControl = accessControl;
		this.errorResponse = errorResponse;
		this.access = access;
		this.meterRegistry = meterRegistry;
		this.initialRoute = initialRoute;
		this.initialSession = initialSession;
		this.peer = peer;
		this.onFinish = onFinish;
		try {
			// mirror the connection-head limit; the encoder keeps no dynamic table so the
			// blocks re-encoded with a rewritten authority are self-contained
			long maxHeaderListSize = 64 * 1024;
			this.headersEncoder.maxHeaderListSize(maxHeaderListSize);
			this.headersEncoder.maxHeaderTableSize(0);
			this.headersDecoder.configuration().maxHeaderListSize(maxHeaderListSize, maxHeaderListSize);
		}
		catch (io.netty.handler.codec.http2.Http2Exception e) {
			throw new IllegalStateException(e);
		}
	}

	/**
	 * Starts the outbound side; the leg of the head consumed with the connection is
	 * opened by its event, further legs on demand.
	 */
	void start() {
		Thread.ofVirtual().name("sluice-h2demux-out").start(this::pumpOutbound);
	}

	/**
	 * The outbound side: parses the frames the client sends, resolves the route of every
	 * request head, and relays each stream to the leg of its route.
	 */
	private void pumpOutbound() {
		try {
			// the connection head ends with the magic; the frames follow it
			int magic = ConnectionHeadParser.H2_MAGIC.length();
			feed(this.seedHead, magic, this.seedHead.length - magic);
			InputStream in = this.pipe.source();
			byte[] buffer = new byte[BUFFER_SIZE];
			int n;
			while ((n = in.read(buffer)) > 0) {
				feed(buffer, 0, n);
			}
			// the client is done sending: no further request on any leg
			for (Leg leg : this.legs) {
				leg.halfClose();
			}
		}
		catch (Exception e) {
			abort();
		}
		finally {
			settle();
		}
	}

	private void feed(byte[] bytes, int offset, int length) {
		this.buffer.write(bytes, offset, length);
		byte[] all = this.buffer.toByteArray();
		int position = 0;
		while (all.length - position >= FRAME_HEADER_LENGTH) {
			ByteBuffer header = ByteBuffer.wrap(all, position, FRAME_HEADER_LENGTH);
			int frameLength = unsigned24(header);
			if (all.length - position < FRAME_HEADER_LENGTH + frameLength) {
				break;
			}
			int type = header.get() & 0xff;
			int flags = header.get() & 0xff;
			int streamId = header.getInt() & 0x7fffffff;
			onFrame(type, flags, streamId, all, position + FRAME_HEADER_LENGTH, frameLength);
			position += FRAME_HEADER_LENGTH + frameLength;
		}
		if (position > 0) {
			byte[] remainder = new byte[all.length - position];
			System.arraycopy(all, position, remainder, 0, remainder.length);
			this.buffer.reset();
			this.buffer.write(remainder, 0, remainder.length);
		}
	}

	private void onFrame(int type, int flags, int streamId, byte[] bytes, int payload, int length) {
		switch (type) {
			case FRAME_SETTINGS -> onSettings(flags, bytes, payload, length);
			case FRAME_HEADERS -> onHeaders(flags, streamId, bytes, payload, length, true);
			case FRAME_CONTINUATION -> onHeaders(flags, streamId, bytes, payload, length, false);
			case FRAME_DATA -> {
				if (streamId == 0) {
					return; // DATA is stream-scoped; an unexpected connection-level frame
				}
				Leg leg = this.streams.get(Integer.valueOf(streamId));
				if (leg == null) {
					return; // frames of streams never routed (or already answered)
				}
				leg.relayFrame(frame(FRAME_DATA, flags, streamId, bytes, payload, length));
				int dataLength = dataPayloadLength(flags, bytes, payload, length);
				if (dataLength > 0) {
					// the payload left towards the leg: release the client's connection
					// window, whichever leg it went to
					grantClient(dataLength);
				}
			}
			case FRAME_RST_STREAM -> {
				Leg leg = this.streams.get(Integer.valueOf(streamId));
				if (leg != null) {
					leg.relayFrame(frame(FRAME_RST_STREAM, flags, streamId, bytes, payload, length));
					leg.streamClosedByClient(streamId);
				}
			}
			case FRAME_WINDOW_UPDATE -> {
				// stream-level grants pair stream for stream; connection-level grants are
				// regenerated from the bytes relayed
				Leg leg = streamId == 0 ? null : this.streams.get(Integer.valueOf(streamId));
				if (leg != null) {
					leg.relayFrame(frame(FRAME_WINDOW_UPDATE, 0, streamId, bytes, payload, length));
				}
			}
			case FRAME_PING -> {
				if ((flags & FLAG_ACK) == 0) {
					writeToClient(frame(FRAME_PING, FLAG_ACK, 0, bytes, payload, length));
				}
			}
			default -> {
				// priority, push promise, and unknown frames follow their stream
				Leg leg = streamId == 0 ? null : this.streams.get(Integer.valueOf(streamId));
				if (leg != null) {
					leg.relayFrame(frame(type, flags, streamId, bytes, payload, length));
				}
			}
		}
	}

	private void onSettings(int flags, byte[] bytes, int payload, int length) {
		if ((flags & FLAG_ACK) != 0) {
			return; // the ack of the settings announced towards the client
		}
		if (length > 0) {
			byte[] settings = new byte[length];
			System.arraycopy(bytes, payload, settings, 0, length);
			this.clientSettings.write(settings, 0, settings.length);
			// re-announce the peer's settings to the legs already serving the connection
			for (Leg leg : this.legs) {
				leg.relaySettings(settings);
			}
		}
		writeToClient(frame(FRAME_SETTINGS, FLAG_ACK, 0, null, 0, 0));
	}

	/**
	 * The head frame of a possibly split HEADERS; streams already routed receive their
	 * block verbatim (trailers), new streams are routed by the decoded
	 * {@code :authority}.
	 */
	private void onHeaders(int flags, int streamId, byte[] bytes, int payload, int length, boolean head) {
		if (streamId == 0) {
			return;
		}
		if (this.assemblingStream != -1 && streamId != this.assemblingStream) {
			return; // a head is already being assembled; heads never interleave
		}
		if (this.assemblingStream == -1) {
			this.assemblingStream = streamId;
			this.assemblingEndStream = (flags & FLAG_END_STREAM) != 0;
			this.assembling.reset();
		}
		appendFragment(flags, bytes, payload, length, head);
		if ((flags & FLAG_END_HEADERS) != 0) {
			onHeadBlock();
		}
	}

	/** Strips padding and priority fields, keeping the bare header block fragment. */
	private void appendFragment(int flags, byte[] bytes, int payload, int length, boolean headers) {
		int start = payload;
		int fragmentLength = length;
		if ((flags & FLAG_PADDED) != 0 && length > 0) {
			int padLength = bytes[start] & 0xff;
			start += 1;
			fragmentLength -= 1 + padLength;
		}
		if ((flags & FLAG_PRIORITY) != 0 && headers && fragmentLength > 0) {
			start += 5;
			fragmentLength -= 5;
		}
		if (fragmentLength > 0) {
			this.assembling.write(bytes, start, fragmentLength);
		}
	}

	private void onHeadBlock() {
		int streamId = this.assemblingStream;
		boolean endStream = this.assemblingEndStream;
		byte[] block = this.assembling.toByteArray();
		this.assemblingStream = -1;
		Leg leg = this.streams.get(Integer.valueOf(streamId));
		if (leg != null) {
			// trailers of a started stream: same leg, block untouched
			leg.relayFrame(headFrame(streamId, FLAG_END_HEADERS | (endStream ? FLAG_END_STREAM : 0), block));
			return;
		}
		onRequestHead(streamId, endStream, block);
	}

	private void onRequestHead(int streamId, boolean endStream, byte[] block) {
		io.netty.handler.codec.http2.Http2Headers headers = decode(streamId, block);
		String host = hostOf(headers);
		ConnectionHeadParser.Head.@Nullable Request request = requestOf(headers);
		Resolved resolved;
		if (this.firstHead) {
			// resolved with the connection; the lookup must not run twice (it advances
			// the load balancing state)
			this.firstHead = false;
			resolved = new Resolved(this.initialRoute, this.initialSession);
		}
		else {
			resolved = resolveOrRespond(streamId, host, request, headers);
			if (resolved == null) {
				return; // the error response was written on the stream
			}
		}
		Router.Route route = resolved.route();
		Leg leg = legFor(route, resolved.session());
		this.streams.put(Integer.valueOf(streamId), leg);
		leg.streamOpened(streamId);
		if (route.rewriteHost()) {
			leg.relayFrames(rewrittenHead(streamId, endStream, headers, route.address()));
		}
		else {
			leg.relayFrame(headFrame(streamId, FLAG_END_HEADERS | (endStream ? FLAG_END_STREAM : 0), block));
		}
		if (request != null) {
			this.access.request(route.routeTag(), request.method(), request.path(), "2");
		}
	}

	private io.netty.handler.codec.http2.Http2Headers decode(int streamId, byte[] block) {
		try {
			return this.headersDecoder.decodeHeaders(streamId, io.netty.buffer.Unpooled.wrappedBuffer(block));
		}
		catch (Exception e) {
			// an undecodable block carries no routable authority
			return new io.netty.handler.codec.http2.DefaultHttp2Headers();
		}
	}

	private static @Nullable String hostOf(io.netty.handler.codec.http2.@Nullable Http2Headers headers) {
		if (headers == null) {
			return null;
		}
		CharSequence authority = headers.authority();
		if (authority != null && !authority.isEmpty()) {
			return authority.toString();
		}
		CharSequence host = headers.get(io.netty.util.AsciiString.cached("host"));
		return host == null || host.isEmpty() ? null : host.toString();
	}

	private static ConnectionHeadParser.Head.@Nullable Request requestOf(
			io.netty.handler.codec.http2.@Nullable Http2Headers headers) {
		if (headers == null) {
			return null;
		}
		CharSequence method = headers.method();
		CharSequence path = headers.path();
		if (method == null || path == null) {
			return null;
		}
		return new ConnectionHeadParser.Head.Request(method.toString(), path.toString(), "2");
	}

	private static @Nullable String headerOf(io.netty.handler.codec.http2.Http2Headers headers, String name) {
		CharSequence value = headers.get(io.netty.util.AsciiString.of(name));
		return value == null || value.isEmpty() ? null : value.toString();
	}

	private record Resolved(Router.Route route, TunnelSession session) {
	}

	/**
	 * Resolves the route of a request head; on any refusal the request is logged, an
	 * error response is written on the stream, and {@code null} returned, leaving the
	 * connection alive.
	 */
	private @Nullable Resolved resolveOrRespond(int streamId, @Nullable String host,
			ConnectionHeadParser.Head.@Nullable Request request, io.netty.handler.codec.http2.Http2Headers headers) {
		Optional<Router.Route> route = this.router.lookup(host);
		if (route.isEmpty()) {
			return this.refuse(streamId, request, this.errorResponse.noRouteStream(streamId, host, request));
		}
		TunnelSession session = this.sessions.find(route.get().clientId()).orElse(null);
		if (session == null) {
			return this.refuse(streamId, request, this.errorResponse.noRouteStream(streamId, host, request));
		}
		String forwardedFor = headerOf(headers, ConnectionHeadParser.Head.FORWARDED_FOR);
		String forwarded = headerOf(headers, ConnectionHeadParser.Head.FORWARDED);
		if (!this.accessControl.allowed(route.get(), this.peer, forwardedFor, forwarded)) {
			return this.refuse(streamId, request, this.errorResponse.forbiddenStream(streamId, host, request));
		}
		return new Resolved(route.get(), session);
	}

	/** Logs a refused stream, mirroring the connection-level refusals. */
	private @Nullable Resolved refuse(int streamId, ConnectionHeadParser.Head.@Nullable Request request,
			byte[] response) {
		this.access.request("-", request == null ? "-" : request.method(), request == null ? "-" : request.path(), "2");
		writeToClient(response);
		return null;
	}

	/** The leg relaying the route: the alive leg serving it, or a new one. */
	private Leg legFor(Router.Route route, TunnelSession session) {
		for (Leg leg : this.legs) {
			if (!leg.ended && leg.route.equals(route)) {
				return leg;
			}
		}
		return new Leg(route, session);
	}

	/**
	 * The request head re-encoded with the route authority; the block is split to the
	 * default maximum frame size.
	 */
	private List<byte[]> rewrittenHead(int streamId, boolean endStream,
			io.netty.handler.codec.http2.Http2Headers headers, String address) {
		headers.authority(io.netty.util.AsciiString.of(address));
		io.netty.buffer.ByteBuf encoded = io.netty.buffer.Unpooled.buffer(256);
		byte[] block;
		try {
			this.headersEncoder.encodeHeaders(streamId, headers, encoded);
			block = new byte[encoded.readableBytes()];
			encoded.readBytes(block);
		}
		catch (io.netty.handler.codec.http2.Http2Exception e) {
			// unreachable for well-formed heads; the empty block keeps the stream alive
			block = new byte[0];
		}
		finally {
			encoded.release();
		}
		List<byte[]> frames = new ArrayList<>();
		int offset = 0;
		boolean first = true;
		do {
			int length = Math.min(MAX_FRAME_SIZE, block.length - offset);
			boolean last = offset + length >= block.length;
			int type = first ? FRAME_HEADERS : FRAME_CONTINUATION;
			int flags = first && endStream ? FLAG_END_STREAM : 0;
			if (last) {
				flags |= FLAG_END_HEADERS;
			}
			frames.add(frame(type, flags, streamId, block, offset, length));
			first = false;
			offset += length;
		}
		while (offset < block.length);
		return frames;
	}

	private static byte[] headFrame(int streamId, int flags, byte[] block) {
		return frame(FRAME_HEADERS, flags, streamId, block, 0, block.length);
	}

	private static byte[] frame(int type, int flags, int streamId, byte @Nullable [] bytes, int payload, int length) {
		byte[] frame = new byte[FRAME_HEADER_LENGTH + Math.max(0, length)];
		frame[0] = (byte) (length >>> 16 & 0xff);
		frame[1] = (byte) (length >>> 8 & 0xff);
		frame[2] = (byte) (length & 0xff);
		frame[3] = (byte) type;
		frame[4] = (byte) flags;
		frame[5] = (byte) (streamId >>> 24 & 0x7f);
		frame[6] = (byte) (streamId >>> 16 & 0xff);
		frame[7] = (byte) (streamId >>> 8 & 0xff);
		frame[8] = (byte) (streamId & 0xff);
		if (length > 0 && bytes != null) {
			System.arraycopy(bytes, payload, frame, FRAME_HEADER_LENGTH, length);
		}
		return frame;
	}

	private static int dataPayloadLength(int flags, byte[] bytes, int payload, int length) {
		int dataLength = length;
		if ((flags & FLAG_PADDED) != 0 && length > 0) {
			int padLength = bytes[payload] & 0xff;
			dataLength -= 1 + padLength;
		}
		return Math.max(0, dataLength);
	}

	/**
	 * Grants the client connection window; called as payload bytes leave towards legs.
	 */
	private void grantClient(int units) {
		writeToClient(windowUpdate(0, units));
	}

	private static byte[] windowUpdate(int streamId, int units) {
		byte[] payload = { (byte) (units >>> 24 & 0x7f), (byte) (units >>> 16 & 0xff), (byte) (units >>> 8 & 0xff),
				(byte) (units & 0xff) };
		return frame(FRAME_WINDOW_UPDATE, 0, streamId, payload, 0, 4);
	}

	private void writeToClient(byte[] bytes) {
		OutputStream sink = this.pipe.sink();
		synchronized (this.sinkLock) {
			try {
				sink.write(bytes);
				sink.flush();
			}
			catch (Exception e) {
				throw new IllegalStateException(e);
			}
		}
	}

	/**
	 * Relays response bytes to the client and accounts them to the leg and the access
	 * log; the single reader per leg keeps the writes in stream order.
	 */
	private void writeToClient(byte[] bytes, int offset, int length, Leg leg) {
		synchronized (this.sinkLock) {
			try {
				this.pipe.sink().write(bytes, offset, length);
				this.pipe.sink().flush();
			}
			catch (Exception e) {
				throw new IllegalStateException(e);
			}
		}
		leg.outbound.increment(length);
		leg.session.recordRelayed(length, StreamRelay.Direction.TO_LOCAL);
		this.access.bytes(length, StreamRelay.Direction.TO_LOCAL);
	}

	/**
	 * A leg: the virtual connection of one route, carrying every stream routed to it,
	 * alive until the relay ends or its upstream goes away.
	 */
	private final class Leg {

		final Router.Route route;

		final TunnelSession session;

		final VirtualConnection connection;

		private final Counter inbound;

		private final Counter outbound;

		private final Object upstreamWriteLock = new Object();

		/** The streams relayed whose response has not ended. */
		private final Set<Integer> openStreams = ConcurrentHashMap.newKeySet();

		private volatile boolean ended;

		Leg(Router.Route route, TunnelSession session) {
			this.route = route;
			this.session = session;
			this.connection = session.open(route.address());
			this.inbound = meterRegistry.counter(METRIC_NAME, "direction", "inbound", "route", route.routeTag());
			this.outbound = meterRegistry.counter(METRIC_NAME, "direction", "outbound", "route", route.routeTag());
			try {
				OutputStream sink = this.connection.sink();
				synchronized (this.upstreamWriteLock) {
					// the client role of the upstream connection: preface and the peer's
					// settings, so its encoder and flow control mirror the client's
					sink.write(ConnectionHeadParser.H2_MAGIC.getBytes(StandardCharsets.US_ASCII));
					sink.write(frame(FRAME_SETTINGS, 0, 0, null, 0, 0));
					byte[] settings = Http2DemuxRelay.this.clientSettings.toByteArray();
					if (settings.length > 0) {
						sink.write(frame(FRAME_SETTINGS, 0, 0, settings, 0, settings.length));
					}
					sink.flush();
				}
			}
			catch (Exception e) {
				this.connection.close();
				throw new IllegalStateException(e);
			}
			if (Http2DemuxRelay.this.legs.isEmpty()) {
				Http2DemuxRelay.this.access.connectionId(this.connection.connectionId());
			}
			Http2DemuxRelay.this.legs.add(this);
			pending.incrementAndGet();
			Thread.ofVirtual()
				.name("sluice-h2demux-in-" + this.connection.connectionId())
				.start(() -> pumpInbound(this));
		}

		/** Relays one frame towards the upstream. */
		void relayFrame(byte[] frameBytes) {
			try {
				OutputStream sink = this.connection.sink();
				synchronized (this.upstreamWriteLock) {
					sink.write(frameBytes);
					sink.flush();
				}
			}
			catch (Exception e) {
				throw new IllegalStateException(e);
			}
			this.outbound.increment(frameBytes.length);
			this.session.recordRelayed(frameBytes.length, StreamRelay.Direction.TO_REMOTE);
			access.bytes(frameBytes.length, StreamRelay.Direction.TO_REMOTE);
		}

		void relayFrames(List<byte[]> frames) {
			for (byte[] frame : frames) {
				relayFrame(frame);
			}
		}

		/** A late SETTINGS payload from the client, re-announced to the upstream. */
		void relaySettings(byte[] payload) {
			relayFrame(frame(FRAME_SETTINGS, 0, 0, payload, 0, payload.length));
		}

		void streamOpened(int streamId) {
			this.openStreams.add(Integer.valueOf(streamId));
		}

		/** The client reset the stream: no response follows on it. */
		void streamClosedByClient(int streamId) {
			this.openStreams.remove(Integer.valueOf(streamId));
		}

		/** The response of a stream ended on the leg's inbound side. */
		void responseEnded(int streamId) {
			this.openStreams.remove(Integer.valueOf(streamId));
		}

		boolean hasOpenStreams() {
			return !this.openStreams.isEmpty();
		}

		/** Half-closes the client side: no further requests follow. */
		void halfClose() {
			try {
				this.connection.sink().close();
			}
			catch (Exception e) {
				// the relay is ending anyway
			}
		}

		/** Closes the virtual connection, releasing the upstream. */
		void close() {
			this.connection.close();
		}

	}

	/**
	 * The inbound side of a leg: relays response frames to the client, releases the leg's
	 * connection window for the bytes forwarded, and watches the relayed frames for the
	 * stream endings and the settings and pings to answer.
	 */
	private void pumpInbound(Leg leg) {
		try {
			InputStream in = leg.connection.source();
			ByteArrayOutputStream scan = new ByteArrayOutputStream(2048);
			byte[] buffer = new byte[BUFFER_SIZE];
			int n;
			while ((n = in.read(buffer)) > 0) {
				writeToClient(buffer, 0, n, leg);
				writeToLeg(leg, windowUpdate(0, n)); // the bytes left towards the client
				scanInbound(leg, scan, buffer, n);
			}
			// the upstream half-closed: owed responses end the relay, an idle leg retires
			if (leg.hasOpenStreams()) {
				abort();
			}
		}
		catch (Exception e) {
			abort();
		}
		finally {
			leg.ended = true;
			settle();
		}
	}

	private void scanInbound(Leg leg, ByteArrayOutputStream scan, byte[] chunk, int length) {
		scan.write(chunk, 0, length);
		byte[] all = scan.toByteArray();
		int position = 0;
		while (all.length - position >= FRAME_HEADER_LENGTH) {
			ByteBuffer header = ByteBuffer.wrap(all, position, FRAME_HEADER_LENGTH);
			int frameLength = unsigned24(header);
			if (all.length - position < FRAME_HEADER_LENGTH + frameLength) {
				break;
			}
			int type = header.get() & 0xff;
			int flags = header.get() & 0xff;
			int streamId = header.getInt() & 0x7fffffff;
			if ((type == FRAME_DATA || type == FRAME_HEADERS) && (flags & FLAG_END_STREAM) != 0 && streamId != 0) {
				leg.responseEnded(streamId);
			}
			else if (type == FRAME_SETTINGS && (flags & FLAG_ACK) == 0) {
				writeToLeg(leg, frame(FRAME_SETTINGS, FLAG_ACK, 0, null, 0, 0));
			}
			else if (type == FRAME_PING && (flags & FLAG_ACK) == 0 && frameLength == 8) {
				writeToLeg(leg, frame(FRAME_PING, FLAG_ACK, 0, all, position + FRAME_HEADER_LENGTH, 8));
			}
			position += FRAME_HEADER_LENGTH + frameLength;
		}
		byte[] remainder = new byte[all.length - position];
		System.arraycopy(all, position, remainder, 0, remainder.length);
		scan.reset();
		scan.write(remainder, 0, remainder.length);
	}

	private void writeToLeg(Leg leg, byte[] frameBytes) {
		try {
			OutputStream sink = leg.connection.sink();
			synchronized (leg.upstreamWriteLock) {
				sink.write(frameBytes);
				sink.flush();
			}
		}
		catch (Exception e) {
			throw new IllegalStateException(e);
		}
	}

	private void settle() {
		if (pending.decrementAndGet() == 0) {
			finishRelay();
		}
	}

	private void finishRelay() {
		if (!this.finished.compareAndSet(false, true)) {
			return;
		}
		for (Leg leg : this.legs) {
			leg.close();
			leg.session.remove(leg.connection.connectionId());
		}
		try {
			this.pipe.close();
		}
		catch (Exception e) {
			// ignore
		}
		this.access.close();
		this.onFinish.run();
	}

	/** Tears the relay down after a failure or the loss of an owed response. */
	private void abort() {
		for (Leg leg : this.legs) {
			leg.close();
		}
		try {
			this.pipe.close();
		}
		catch (Exception e) {
			// ignore
		}
	}

	private static int unsigned24(ByteBuffer header) {
		return (header.get() & 0xff) << 16 | (header.get() & 0xff) << 8 | (header.get() & 0xff);
	}

}
