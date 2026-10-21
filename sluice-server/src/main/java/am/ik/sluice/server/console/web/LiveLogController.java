package am.ik.sluice.server.console.web;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.jspecify.annotations.Nullable;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.http.CacheControl;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import am.ik.sluice.server.livelog.FlumeConnection;
import am.ik.sluice.v1.proto.AccessFilter;
import am.ik.sluice.v1.proto.Cursor;
import am.ik.sluice.v1.proto.WatchRequest;

/**
 * The live access log: the lines of every node as the flume of
 * {@code sluice.live-log.url} streams them, filtered there by route, kind and node.
 */
@Controller
class LiveLogController implements DisposableBean {

	/** The kinds of lines, in the order the filter lists them. */
	static final List<String> KINDS = List.of("request", "accept", "close", "reject", "ratelimit");

	/** How often an idle stream carries a comment line. */
	static final Duration HEARTBEAT = Duration.ofSeconds(15);

	private final FlumeConnection flume;

	private final ConsoleView consoleView;

	private final LiveLogView view;

	private final ScheduledExecutorService heartbeats = Executors
		.newSingleThreadScheduledExecutor(Thread.ofPlatform().name("live-log-heartbeat").daemon().factory());

	/** Runs the watch callbacks, which block on writing to the browser. */
	private final ExecutorService callbacks = Executors
		.newThreadPerTaskExecutor(Thread.ofVirtual().name("live-log-", 0).factory());

	LiveLogController(FlumeConnection flume, ConsoleView consoleView, LiveLogView view) {
		this.flume = flume;
		this.consoleView = consoleView;
		this.view = view;
	}

	@GetMapping("/console/access-log")
	String page(@RequestParam(name = "route", required = false) @Nullable List<String> routes,
			@RequestParam(name = "kind", required = false) @Nullable List<String> kinds,
			@RequestParam(name = "node", required = false) @Nullable List<String> nodes, Authentication authentication,
			Model model) {
		model.addAttribute("user", ConsoleController.displayName(authentication));
		model.addAttribute("enabled", this.flume.enabled());
		model.addAttribute("flumeUrl", this.flume.url());
		Set<String> routeKeys = new LinkedHashSet<>();
		this.consoleView.httpRoutes().forEach(route -> routeKeys.add(route.gateKey()));
		this.consoleView.tcpRoutes().forEach(route -> routeKeys.add(route.gateKey()));
		model.addAttribute("routes", choices(routeKeys, routes));
		model.addAttribute("kinds", choices(KINDS, kinds));
		model.addAttribute("nodes",
				choices(this.consoleView.cluster().nodes().stream().map(ConsoleView.NodeRow::nodeId).toList(), nodes));
		model.addAttribute("rowCap", 1000);
		return "console/access-log";
	}

	/**
	 * The SSE stream of the lines passing the filter, resuming after the position of
	 * {@code Last-Event-ID} (a browser reconnecting) or else {@code after} (a view
	 * resuming from pause).
	 */
	@GetMapping("/console/access-log/stream")
	ResponseEntity<SseEmitter> stream(@RequestParam(name = "route", required = false) @Nullable List<String> routes,
			@RequestParam(name = "kind", required = false) @Nullable List<String> kinds,
			@RequestParam(name = "node", required = false) @Nullable List<String> nodes,
			@RequestParam(required = false) @Nullable String after,
			@RequestHeader(name = "Last-Event-ID", required = false) @Nullable String lastEventId) {
		if (!this.flume.enabled()) {
			return ResponseEntity.notFound().build();
		}
		WatchRequest.Builder request = WatchRequest.newBuilder()
			.setFilter(AccessFilter.newBuilder()
				.addAllRouteKeys(present(routes))
				.addAllKinds(present(kinds))
				.addAllNodeIds(present(nodes)));
		Cursor cursor = LiveLogView.cursorOf(lastEventId != null ? lastEventId : after);
		if (cursor != null) {
			request.setAfter(cursor);
		}
		SseEmitter emitter = new SseEmitter(0L);
		LiveLogStream stream = new LiveLogStream(emitter, this.view);
		stream.heartbeat(this.heartbeats.scheduleAtFixedRate(stream::ping, HEARTBEAT.toMillis(), HEARTBEAT.toMillis(),
				TimeUnit.MILLISECONDS));
		this.flume.stub().withExecutor(this.callbacks).watch(request.build(), stream);
		return ResponseEntity.ok()
			.cacheControl(CacheControl.noStore())
			// nginx would hold the events back to fill its buffer
			.header("X-Accel-Buffering", "no")
			.body(emitter);
	}

	private static List<String> present(@Nullable List<String> values) {
		return values == null ? List.of() : values.stream().map(String::strip).filter(v -> !v.isEmpty()).toList();
	}

	private static List<LiveLogView.Choice> choices(Iterable<String> values, @Nullable List<String> selected) {
		List<String> chosen = present(selected);
		List<LiveLogView.Choice> choices = new ArrayList<>();
		for (String value : values) {
			choices.add(new LiveLogView.Choice(value, value, chosen.contains(value)));
		}
		return choices;
	}

	@Override
	public void destroy() {
		this.heartbeats.shutdownNow();
		this.callbacks.shutdownNow();
	}

}
