package am.ik.sluice.server.console.web;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import am.ik.sluice.tunnel.PeerBuilds;
import am.ik.sluice.v1.proto.WatchEvent;
import am.ik.sluice.v1.proto.WatchHello;
import am.ik.sluice.v1.proto.WatchRequest;

/**
 * One browser's live access log: the flume's {@code Watch} stream relayed to an SSE
 * stream, a line at a time -- the next is requested from the flume once the previous one
 * is written, so a slow browser holds the flume back instead of piling lines up here. The
 * SSE event id is the flume's position, so a browser reconnecting (to this node or
 * another) resumes from it.
 * <p>
 * Events: {@code row} and {@code gap} (a table row), {@code nodes} (the strip of nodes),
 * {@code flume} ({@code up}, or {@code down} before the stream ends and the browser
 * retries), {@code build} (the flume's).
 */
final class LiveLogStream implements ClientResponseObserver<WatchRequest, WatchEvent> {

	private static final Logger log = LoggerFactory.getLogger(LiveLogStream.class);

	/** How long the browser waits before it reconnects, in milliseconds. */
	static final long RETRY_MILLIS = 2000;

	private final SseEmitter emitter;

	private final LiveLogView view;

	private final Map<String, Boolean> nodes = new TreeMap<>();

	private final AtomicBoolean ended = new AtomicBoolean();

	private volatile @Nullable ClientCallStreamObserver<WatchRequest> call;

	private volatile @Nullable ScheduledFuture<?> heartbeat;

	private long epoch;

	LiveLogStream(SseEmitter emitter, LiveLogView view) {
		this.emitter = emitter;
		this.view = view;
		emitter.onCompletion(this::end);
		emitter.onTimeout(this::end);
		emitter.onError(error -> this.end());
	}

	void heartbeat(ScheduledFuture<?> heartbeat) {
		this.heartbeat = heartbeat;
		if (this.ended.get()) {
			heartbeat.cancel(false);
		}
	}

	/** A comment line, keeping proxies and the browser from timing the stream out. */
	void ping() {
		this.send(SseEmitter.event().comment("keep-alive"));
	}

	@Override
	public void beforeStart(ClientCallStreamObserver<WatchRequest> call) {
		this.call = call;
		call.disableAutoRequestWithInitial(1);
	}

	@Override
	public void onNext(WatchEvent event) {
		boolean sent = switch (event.getBodyCase()) {
			case HELLO -> {
				WatchHello hello = event.getHello();
				this.epoch = hello.getEpoch();
				yield this.send(SseEmitter.event().name("flume").data("up").reconnectTime(RETRY_MILLIS))
						&& this.send(SseEmitter.event()
							.name("build")
							.data(this.view.build(hello.hasBuild() ? PeerBuilds.sanitize(hello.getBuild()) : null)))
						&& (!hello.getReset() || this.send(SseEmitter.event().name("gap").data(this.view.restarted())));
			}
			case RECORD -> this.send(SseEmitter.event()
				.id(LiveLogView.idOf(this.epoch, event.getRecord().getSeq()))
				.name("row")
				.data(this.view.row(event.getRecord())));
			case GAP -> {
				SseEmitter.SseEventBuilder gap = SseEmitter.event().name("gap").data(this.view.gap(event.getGap()));
				yield this.send(event.getGap().getSeq() > 0
						? gap.id(LiveLogView.idOf(this.epoch, event.getGap().getSeq())) : gap);
			}
			case NODE -> {
				this.nodes.put(event.getNode().getNodeId(), event.getNode().getConnected());
				yield this.send(SseEmitter.event().name("nodes").data(this.view.nodes(this.nodes)));
			}
			case BODY_NOT_SET -> true;
		};
		ClientCallStreamObserver<WatchRequest> current = this.call;
		if (sent && current != null) {
			current.request(1);
		}
	}

	@Override
	public void onError(Throwable t) {
		if (this.ended.get()) {
			return;
		}
		log.debug("live access log: the watch ended: {}", t.toString());
		this.send(SseEmitter.event().name("flume").data("down").reconnectTime(RETRY_MILLIS));
		this.emitter.complete();
	}

	@Override
	public void onCompleted() {
		this.onError(new IllegalStateException("the flume ended the watch"));
	}

	/** Writes the event; on a browser gone, ends the watch and tells so. */
	private boolean send(SseEmitter.SseEventBuilder event) {
		if (this.ended.get()) {
			return false;
		}
		try {
			this.emitter.send(event);
			return true;
		}
		catch (IOException | IllegalStateException e) {
			this.end();
			return false;
		}
	}

	private void end() {
		if (!this.ended.compareAndSet(false, true)) {
			return;
		}
		ScheduledFuture<?> beat = this.heartbeat;
		if (beat != null) {
			beat.cancel(false);
		}
		ClientCallStreamObserver<WatchRequest> current = this.call;
		if (current != null) {
			current.cancel("the browser left", null);
		}
	}

}
