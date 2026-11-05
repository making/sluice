package am.ik.sluice.client.filter;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Tags;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import run.endive.runtime.HostFunction;
import run.endive.runtime.ImportFunction;
import run.endive.runtime.ImportValues;
import run.endive.runtime.Instance;
import run.endive.runtime.Memory;
import run.endive.runtime.WasmFunctionHandle;
import run.endive.wasm.WasmModule;
import run.endive.wasm.types.FunctionImport;
import run.endive.wasm.types.FunctionType;
import run.endive.wasm.types.MemoryLimits;

import am.ik.sluice.client.config.FilterConfig;
import am.ik.sluice.tunnel.TunnelBudget;

import org.springframework.util.unit.DataSize;

/**
 * A filter that is a proxy-wasm plugin (ABI 0.2.1), interpreted by Endive: what a user
 * compiles with the proxy-wasm SDKs, as for Envoy, and adds to the released native image.
 * What it offers of the ABI, and why not the rest, is in {@code proxy-wasm-plugins.md}.
 * <p>
 * As in a proxy, an instance serves many streams, one call at a time on its own thread: a
 * stream costs a context, not an instance, and stays on its VM. A connection's virtual
 * thread waits unmounted meanwhile, so a plugin computing holds no carrier. A VM failing
 * is discarded with its streams, and started anew for the next stream.
 * <p>
 * The plugin is not trusted: it reaches nothing but the imports below, its memory and
 * each call's time are bounded, and what it sends is validated. A body it holds charges
 * the tunnel budget.
 */
final class ProxyWasmFilter implements UpstreamFilter {

	/** What one VM holds at most by default. */
	static final DataSize DEFAULT_MAX_MEMORY = DataSize.ofMegabytes(16);

	static final Duration DEFAULT_CALL_TIMEOUT = Duration.ofSeconds(1);

	static final DataSize DEFAULT_MAX_BODY = DataSize.ofMegabytes(16);

	static final int DEFAULT_MAX_METERS = 1024;

	/**
	 * The fields of a head a plugin is offered, or may leave: what the host keeps of a
	 * head grows with them more than with its bytes, about 200 bytes each.
	 */
	static final int MAX_FIELDS = 256;

	/**
	 * What a connection's streams hold in the host, at most: their heads' fields, of
	 * {@link UpstreamFilterPipe#MAX_PENDING_REQUESTS} requests awaiting responses and the
	 * one in progress.
	 */
	static final long CONNECTION_BYTES = 256 * 1024;

	private static final Logger log = LoggerFactory.getLogger(ProxyWasmFilter.class);

	static final int PAGE_SIZE = 64 * 1024;

	private static final int ROOT_CONTEXT = 1;

	private static final int OK = 0;

	private static final int NOT_FOUND = 1;

	private static final int BAD_ARGUMENT = 2;

	private static final int INTERNAL_FAILURE = 10;

	private static final int UNIMPLEMENTED = 12;

	private static final int WASI_ENOSYS = 52;

	private static final int WASI_SUCCESS = 0;

	private static final int BUFFER_REQUEST_BODY = 0;

	private static final int BUFFER_RESPONSE_BODY = 1;

	private static final int BUFFER_PLUGIN_CONFIGURATION = 7;

	private static final int MAP_REQUEST_HEADERS = 0;

	private static final int MAP_RESPONSE_HEADERS = 2;

	private static final int ACTION_PAUSE = 1;

	private static final int METRIC_COUNTER = 0;

	private static final int MAX_NAME = 64 * 1024;

	private static final int MAX_TAG_VALUE = 128;

	/** A metric's definition: kept for good, so a plugin must not grow them at will. */
	private static final int MAX_METRIC_DEFINITION = 1024;

	/** The vectors one WASI write may have, as Linux's IOV_MAX. */
	private static final int MAX_IOVS = 1024;

	private static final int WASI_EINVAL = 28;

	private static final int MAX_LOG = 1024;

	private static final byte[] EMPTY = new byte[0];

	private static final SecureRandom RANDOM = new SecureRandom();

	private static final Pattern METER_NAME = Pattern.compile("[a-z][a-z0-9_.]{0,63}");

	private static final Pattern TAG_KEY = Pattern.compile("[a-z][a-z0-9_]{0,31}");

	private static final Set<String> RESERVED_TAGS = Set.of("route", "filter");

	private static final List<String> REQUIRED_EXPORTS = List.of("proxy_abi_version_0_2_1", "proxy_on_context_create",
			"proxy_on_vm_start", "proxy_on_configure", "proxy_on_request_headers", "proxy_on_request_body",
			"proxy_on_response_headers", "proxy_on_response_body", "proxy_on_done", "proxy_on_log", "proxy_on_delete");

	private final String name;

	private final String route;

	private final WasmModule module;

	private final String allocator;

	private final boolean heapCharged;

	private final int initialPages;

	private final int maxPages;

	private final byte[] configuration;

	private final Duration callTimeout;

	private final int maxBody;

	private final int maxMeters;

	private final boolean failClosed;

	private final UpstreamFilterFactory.Context context;

	private final FilterFailures failures;

	private final Vm[] vms;

	private final AtomicInteger nextVm = new AtomicInteger();

	private final Map<String, Integer> metricIds = new ConcurrentHashMap<>();

	private final List<Counter> metrics = new CopyOnWriteArrayList<>();

	private final Set<String> refusedCalls = ConcurrentHashMap.newKeySet();

	private final Counter dropped;

	ProxyWasmFilter(String name, WasmModule module, UpstreamFilterFactory.Context context, FilterFailures failures) {
		this.name = name;
		this.module = module;
		this.context = context;
		this.failures = failures;
		this.route = context.route();
		FilterConfig settings = context.settings();
		if (!settings.args().isEmpty()) {
			throw new IllegalArgumentException("plugin " + name + " takes its settings as configuration, not args");
		}
		String text = settings.configuration();
		this.configuration = (text == null ? "" : text).getBytes(StandardCharsets.UTF_8);
		this.maxPages = maxPages(settings);
		this.callTimeout = callTimeout(settings);
		DataSize body = settings.maxBody();
		this.maxBody = (int) Math.min((body == null ? DEFAULT_MAX_BODY : body).toBytes(), Integer.MAX_VALUE - 8);
		Integer meters = settings.maxMeters();
		this.maxMeters = meters == null ? DEFAULT_MAX_METERS : meters;
		this.failClosed = Boolean.TRUE.equals(settings.failClosed());
		this.dropped = Counter.builder("sluice.client.filter.plugin.meters.dropped")
			.description("Counter updates of a plugin dropped past its limit of counters")
			.tag("route", this.route)
			.tag("filter", name)
			.register(context.meters());
		this.initialPages = module.memorySection()
			.filter(memories -> memories.memoryCount() == 1)
			.map(memories -> memories.getMemory(0).limits().initialPages())
			.orElseThrow(() -> new IllegalArgumentException("plugin " + name + " must define one memory"));
		long needed = (long) this.initialPages * PAGE_SIZE + PluginHeap.tableBytes(module);
		if (needed > (long) this.maxPages * PAGE_SIZE) {
			throw new IllegalArgumentException("plugin " + name + " needs " + needed
					+ " bytes of memory and tables, past its limit of " + (long) this.maxPages * PAGE_SIZE);
		}
		for (String export : REQUIRED_EXPORTS) {
			if (!exports(export)) {
				throw new IllegalArgumentException("plugin " + name + " is no proxy-wasm 0.2.1 plugin: no " + export);
			}
		}
		// the Rust SDK exports the latter on wasm32-unknown-unknown, as Envoy accepts
		this.allocator = exports("proxy_on_memory_allocate") ? "proxy_on_memory_allocate" : "malloc";
		if (!exports(this.allocator)) {
			throw new IllegalArgumentException("plugin " + name + " is no proxy-wasm 0.2.1 plugin: no allocator");
		}
		this.heapCharged = PluginHeap.needed(module);
		module.importSection().stream().forEach(i -> {
			boolean known = i.module().equals("env") || i.module().equals("wasi_snapshot_preview1");
			if (!known || !(i instanceof FunctionImport)) {
				throw new IllegalArgumentException("plugin " + name + " imports " + i.module() + "." + i.name());
			}
		});
		this.vms = new Vm[vms(settings)];
		for (int i = 0; i < this.vms.length; i++) {
			this.vms[i] = new Vm(i);
		}
		// refused at the start, not on the first request
		this.vms[0].run(() -> null);
	}

	private static int vms(FilterConfig settings) {
		Integer vms = settings.vms();
		return vms == null ? Math.min(Runtime.getRuntime().availableProcessors(), 4) : vms;
	}

	private static int maxPages(FilterConfig settings) {
		DataSize memory = settings.maxMemory();
		long bytes = (memory == null ? DEFAULT_MAX_MEMORY : memory).toBytes();
		return (int) Math.clamp(bytes / PAGE_SIZE, 1, 65536);
	}

	private static Duration callTimeout(FilterConfig settings) {
		Duration timeout = settings.callTimeout();
		return timeout == null ? DEFAULT_CALL_TIMEOUT : timeout;
	}

	/** The most the VMs of a plugin set so hold: counted out of the connection limit. */
	static long reservedBytes(FilterConfig settings) {
		return (long) vms(settings) * maxPages(settings) * PAGE_SIZE;
	}

	private boolean exports(String export) {
		for (int i = 0; i < this.module.exportSection().exportCount(); i++) {
			if (this.module.exportSection().getExport(i).name().equals(export)) {
				return true;
			}
		}
		return false;
	}

	@Override
	public Exchange onRequest(Request request, Headers headers) {
		checkFields(headers);
		Vm vm = this.vms[Math.floorMod(this.nextVm.getAndIncrement(), this.vms.length)];
		Stream stream = new Stream(vm, request);
		stream.requestHead(headers);
		return stream;
	}

	/** Not a plugin's doing: its VM stays, its stream is not started. */
	private static void checkFields(Headers headers) {
		int fields = headers.lines().size();
		if (fields > MAX_FIELDS) {
			throw new PluginFailure("a head of " + fields + " fields, past " + MAX_FIELDS);
		}
	}

	/** {@code name;key=value;...}: a metric's tags are in its name. */
	private int defineMetric(String definition) {
		Integer known = this.metricIds.get(definition);
		if (known != null) {
			return known;
		}
		if (definition.length() > MAX_METRIC_DEFINITION) {
			return -2;
		}
		if (this.metricIds.size() >= this.maxMeters) {
			return -1;
		}
		String[] parts = definition.split(";");
		if (!METER_NAME.matcher(parts[0]).matches()) {
			return -2;
		}
		Tags tags = Tags.of("route", this.route, "filter", this.name);
		for (int i = 1; i < parts.length; i++) {
			int equals = parts[i].indexOf('=');
			String key = equals < 0 ? "" : parts[i].substring(0, equals);
			if (!TAG_KEY.matcher(key).matches() || RESERVED_TAGS.contains(key)) {
				return -2;
			}
			String value = parts[i].substring(equals + 1);
			tags = tags.and(key, value.length() > MAX_TAG_VALUE ? value.substring(0, MAX_TAG_VALUE) : value);
		}
		Counter counter = Counter.builder("sluice.client.filter." + parts[0])
			.tags(tags)
			.register(this.context.meters());
		synchronized (this.metricIds) {
			return this.metricIds.computeIfAbsent(definition, key -> {
				this.metrics.add(counter);
				return this.metrics.size() - 1;
			});
		}
	}

	/** A plugin did what it may not, trapped, or ran out of its time. */
	static final class PluginFailure extends RuntimeException {

		PluginFailure(String message) {
			super(message);
		}

		PluginFailure(String message, Throwable cause) {
			super(message, cause);
		}

	}

	/** A call for a stream its VM no longer has: no fault of the VM it has now. */
	private static final class StreamGone extends RuntimeException {

		StreamGone() {
			super("gone");
		}

	}

	/** Interrupts the calls past their time: the interpreter checks at each branch. */
	static final class Watchdog {

		private static final Set<Call> calls = ConcurrentHashMap.newKeySet();

		static {
			Thread.ofPlatform().daemon().name("sluice-plugin-watchdog").start(Watchdog::run);
		}

		private static void run() {
			while (true) {
				try {
					Thread.sleep(20);
					long now = System.nanoTime();
					for (Call call : calls) {
						if (now - call.deadline > 0) {
							call.interrupt();
						}
					}
				}
				catch (InterruptedException e) {
					return;
				}
				catch (Throwable e) {
					// out of memory, say: without this thread nothing would stop a plugin
					report(e);
				}
			}
		}

		private static void report(Throwable e) {
			try {
				log.warn("the plugins' watchdog goes on past: {}", e.toString());
			}
			catch (Throwable reporting) {
				// out of memory again: nothing is left to tell it with
			}
		}

		/** One call into a plugin. */
		static final class Call {

			private final Thread thread = Thread.currentThread();

			private final long deadline;

			private boolean ended;

			private boolean interrupted;

			Call(Duration timeout) {
				this.deadline = System.nanoTime() + timeout.toNanos();
				calls.add(this);
			}

			private synchronized void interrupt() {
				if (!this.ended && !this.interrupted) {
					this.interrupted = true;
					this.thread.interrupt();
				}
			}

			/** After it no interrupt comes, for the thread's next task. */
			synchronized void end() {
				this.ended = true;
				calls.remove(this);
				if (this.interrupted) {
					Thread.interrupted();
				}
			}

		}

	}

	/** A plugin instance and the thread it runs on. */
	private final class Vm {

		private final ExecutorService thread;

		private @Nullable Instance instance;

		private @Nullable PluginHeap heap;

		private volatile int generation;

		private int nextContext = ROOT_CONTEXT + 1;

		private @Nullable Stream current;

		Vm(int index) {
			ThreadPoolExecutor pool = new ThreadPoolExecutor(1, 1, 30, TimeUnit.SECONDS, new LinkedBlockingQueue<>(),
					Thread.ofPlatform()
						.daemon()
						.name("sluice-plugin-" + ProxyWasmFilter.this.name + "-" + index)
						.factory());
			pool.allowCoreThreadTimeOut(true);
			this.thread = pool;
		}

		/** Runs a task in the VM, started if it is not; a failure discards the VM. */
		<T> T run(Supplier<T> task) {
			Future<T> future = this.thread.submit(() -> {
				PluginHeap counted = this.heap;
				if (counted != null) {
					counted.betweenCalls();
				}
				Watchdog.Call call = new Watchdog.Call(ProxyWasmFilter.this.callTimeout);
				try {
					if (this.instance == null) {
						start();
					}
					return task.get();
				}
				catch (StreamGone e) {
					throw e;
				}
				catch (RuntimeException | StackOverflowError | OutOfMemoryError e) {
					// contexts keep counting: an old one must not be taken for a new
					// one's
					this.instance = null;
					this.heap = null;
					this.generation++;
					throw e instanceof PluginFailure failure ? failure : new PluginFailure(e.toString(), e);
				}
				finally {
					this.current = null;
					call.end();
				}
			});
			try {
				return future.get();
			}
			catch (ExecutionException e) {
				if (e.getCause() instanceof StreamGone gone) {
					throw new PluginFailure("the VM of this stream was discarded", gone);
				}
				throw e.getCause() instanceof PluginFailure failure ? failure
						: new PluginFailure(String.valueOf(e.getCause()), e);
			}
			catch (InterruptedException e) {
				future.cancel(true);
				Thread.currentThread().interrupt();
				throw new PluginFailure("interrupted", e);
			}
		}

		private void start() {
			ProxyWasmFilter filter = ProxyWasmFilter.this;
			Instance.Builder builder = Instance.builder(filter.module)
				.withImportValues(imports())
				.withMemoryLimits(new MemoryLimits(filter.initialPages, filter.maxPages))
				// it would call _start, on a reactor too: called below instead
				.withStart(false);
			PluginHeap charged = filter.heapCharged ? new PluginHeap(filter.module, (long) filter.maxPages * PAGE_SIZE)
					: null;
			if (charged != null) {
				builder.withUnsafeExecutionListener(charged);
			}
			Instance started = builder.build();
			if (charged != null) {
				charged.attach(started);
			}
			if (filter.exports("_initialize")) {
				started.export("_initialize").apply();
			}
			else if (filter.exports("_start")) {
				started.export("_start").apply();
			}
			started.export("proxy_on_context_create").apply(ROOT_CONTEXT, 0);
			if (started.export("proxy_on_vm_start").apply(ROOT_CONTEXT, 0)[0] == 0
					|| started.export("proxy_on_configure").apply(ROOT_CONTEXT, filter.configuration.length)[0] == 0) {
				throw new PluginFailure("the plugin refused its start");
			}
			this.instance = started;
			this.heap = charged;
		}

		long[] call(@Nullable Stream stream, String export, long... args) {
			this.current = stream;
			return Objects.requireNonNull(this.instance, "the VM").export(export).apply(args);
		}

		private ImportValues imports() {
			List<ImportFunction> functions = new ArrayList<>();
			WasmModule module = ProxyWasmFilter.this.module;
			module.importSection().stream().forEach(i -> {
				FunctionImport function = (FunctionImport) i;
				FunctionType type = module.typeSection().getType(function.typeIndex());
				functions.add(new HostFunction(i.module(), i.name(), type, handle(i.module(), i.name(), type)));
			});
			return ImportValues.builder().withFunctions(functions).build();
		}

		private WasmFunctionHandle handle(String importModule, String importName, FunctionType type) {
			return importModule.equals("wasi_snapshot_preview1") ? wasi(importName, type) : proxy(importName, type);
		}

		private WasmFunctionHandle wasi(String importName, FunctionType type) {
			return switch (importName) {
				case "clock_time_get" -> (instance, args) -> {
					instance.memory().writeLong((int) args[2], System.currentTimeMillis() * 1_000_000L);
					return status(WASI_SUCCESS);
				};
				case "random_get" -> (instance, args) -> {
					byte[] random = new byte[Math.min((int) args[1], 1024 * 1024)];
					RANDOM.nextBytes(random);
					instance.memory().write((int) args[0], random);
					return status(WASI_SUCCESS);
				};
				case "environ_sizes_get", "args_sizes_get" -> (instance, args) -> {
					instance.memory().writeI32((int) args[0], 0);
					instance.memory().writeI32((int) args[1], 0);
					return status(WASI_SUCCESS);
				};
				case "environ_get", "args_get" -> (instance, args) -> status(WASI_SUCCESS);
				case "fd_write" -> this::fdWrite;
				case "proc_exit" -> (instance, args) -> {
					throw new PluginFailure("the plugin exited with " + args[0]);
				};
				default -> (instance, args) -> stub(type, WASI_ENOSYS);
			};
		}

		private WasmFunctionHandle proxy(String importName, FunctionType type) {
			return switch (importName) {
				case "proxy_log" -> (instance, args) -> {
					pluginLog((int) args[0], text(instance, args[1], Math.min(args[2] & 0xffffffffL, MAX_LOG)));
					return status(OK);
				};
				case "proxy_get_log_level" -> (instance, args) -> {
					instance.memory().writeI32((int) args[0], 2);
					return status(OK);
				};
				case "proxy_get_current_time_nanoseconds" -> (instance, args) -> {
					instance.memory().writeLong((int) args[0], System.currentTimeMillis() * 1_000_000L);
					return status(OK);
				};
				case "proxy_set_effective_context", "proxy_done", "proxy_continue_stream" ->
					(instance, args) -> status(OK);
				case "proxy_get_buffer_bytes" -> this::getBuffer;
				case "proxy_set_buffer_bytes" -> this::setBuffer;
				case "proxy_get_buffer_status" -> (instance, args) -> {
					Buffer buffer = buffer((int) args[0]);
					if (buffer == null) {
						return status(NOT_FOUND);
					}
					instance.memory().writeI32((int) args[1], buffer.size());
					instance.memory().writeI32((int) args[2], 0);
					return status(OK);
				};
				case "proxy_get_header_map_size" -> (instance, args) -> {
					List<Field> map = map((int) args[0]);
					if (map == null) {
						return status(NOT_FOUND);
					}
					instance.memory().writeI32((int) args[1], serialized(map).length);
					return status(OK);
				};
				case "proxy_get_header_map_pairs" -> (instance, args) -> {
					List<Field> map = map((int) args[0]);
					if (map == null) {
						return status(NOT_FOUND);
					}
					giveBack(instance, serialized(map), args[1], args[2]);
					return status(OK);
				};
				case "proxy_set_header_map_pairs" -> (instance, args) -> status(
						setPairs((int) args[0], deserialized(bytes(instance, args[1], args[2], MAX_NAME))));
				case "proxy_get_header_map_value" -> (instance, args) -> {
					List<Field> map = map((int) args[0]);
					String key = text(instance, args[1], args[2]).toLowerCase(Locale.ROOT);
					if (map != null) {
						for (Field field : map) {
							if (field.name().equals(key)) {
								giveBack(instance, field.value().getBytes(StandardCharsets.UTF_8), args[3], args[4]);
								return status(OK);
							}
						}
					}
					return status(NOT_FOUND);
				};
				case "proxy_replace_header_map_value", "proxy_add_header_map_value" -> (instance, args) -> {
					boolean add = importName.equals("proxy_add_header_map_value");
					return status(edit((int) args[0], text(instance, args[1], args[2]),
							text(instance, args[3], args[4]), add));
				};
				case "proxy_remove_header_map_value" ->
					(instance, args) -> status(edit((int) args[0], text(instance, args[1], args[2]), null, false));
				case "proxy_define_metric" -> (instance, args) -> {
					if (args[0] != METRIC_COUNTER) {
						return status(BAD_ARGUMENT);
					}
					int id = defineMetric(text(instance, args[1], args[2]));
					if (id == -2) {
						return status(BAD_ARGUMENT);
					}
					instance.memory().writeI32((int) args[3], id);
					return status(OK);
				};
				case "proxy_increment_metric" -> (instance, args) -> {
					int id = (int) args[0];
					if (id == -1) {
						ProxyWasmFilter.this.dropped.increment();
						return status(OK);
					}
					if (id < 0 || id >= ProxyWasmFilter.this.metrics.size()) {
						return status(NOT_FOUND);
					}
					if (args[1] < 0) {
						return status(BAD_ARGUMENT);
					}
					ProxyWasmFilter.this.metrics.get(id).increment(args[1]);
					return status(OK);
				};
				case "proxy_get_property" -> this::getProperty;
				case "proxy_send_local_response" -> this::sendLocalResponse;
				// what the SDKs let a plugin handle: reaching nothing outside is an
				// answer
				case "proxy_http_call", "proxy_grpc_call", "proxy_grpc_stream" ->
					(instance, args) -> stub(type, INTERNAL_FAILURE);
				default -> (instance, args) -> {
					if (ProxyWasmFilter.this.refusedCalls.add(importName)) {
						log.warn("filter {}: the plugin called {}, which this host does not offer",
								ProxyWasmFilter.this.name, importName);
					}
					return stub(type, UNIMPLEMENTED);
				};
			};
		}

		private long @Nullable [] stub(FunctionType type, int status) {
			return type.returns().isEmpty() ? null : status(status);
		}

		/** {@code fd, iovs, iovs length, written}; of what is written, a line of log. */
		private long[] fdWrite(Instance instance, long... args) {
			Memory memory = instance.memory();
			int vectors = (int) args[2];
			if (vectors < 0 || vectors > MAX_IOVS) {
				return status(WASI_EINVAL);
			}
			long written = 0;
			ByteArrayOutputStream text = new ByteArrayOutputStream();
			for (int i = 0; i < vectors; i++) {
				int at = (int) args[1] + i * 8;
				int pointer = memory.readInt(at);
				long length = memory.readInt(at + 4) & 0xffffffffL;
				int kept = (int) Math.min(length, MAX_LOG - text.size());
				if (kept > 0) {
					text.writeBytes(bytes(instance, pointer, kept, MAX_LOG));
				}
				written += length;
			}
			memory.writeI32((int) args[3], (int) written);
			pluginLog(2, text.toString(StandardCharsets.UTF_8));
			return status(WASI_SUCCESS);
		}

		private void pluginLog(int level, String message) {
			String clean = message.replaceAll("\\p{Cntrl}", "?");
			String trimmed = clean.length() > MAX_LOG ? clean.substring(0, MAX_LOG) : clean;
			if (level >= 3) {
				log.warn("filter {}: {}", ProxyWasmFilter.this.name, trimmed);
			}
			else if (level == 2) {
				log.info("filter {}: {}", ProxyWasmFilter.this.name, trimmed);
			}
			else {
				log.debug("filter {}: {}", ProxyWasmFilter.this.name, trimmed);
			}
		}

		private @Nullable Buffer buffer(int type) {
			if (type == BUFFER_PLUGIN_CONFIGURATION) {
				return Buffer.of(ProxyWasmFilter.this.configuration, 0, ProxyWasmFilter.this.configuration.length);
			}
			Stream stream = this.current;
			return stream != null && type == stream.bodyType ? stream.body : null;
		}

		private long[] getBuffer(Instance instance, long... args) {
			Buffer buffer = buffer((int) args[0]);
			if (buffer == null) {
				return status(NOT_FOUND);
			}
			long start = args[1] & 0xffffffffL;
			long max = args[2] & 0xffffffffL;
			if (start > buffer.size()) {
				return status(BAD_ARGUMENT);
			}
			int length = (int) Math.min(max, buffer.size() - start);
			giveBack(instance, buffer.copy((int) start, length), args[3], args[4]);
			return status(OK);
		}

		private long[] setBuffer(Instance instance, long... args) {
			Stream stream = this.current;
			Buffer body = stream == null ? null : stream.body;
			if (stream == null || body == null || args[0] != stream.bodyType || !stream.replaceable) {
				return status(NOT_FOUND);
			}
			int limit = ProxyWasmFilter.this.maxBody;
			byte[] value = bytes(instance, args[3], args[4], limit);
			long start = Math.min(args[1] & 0xffffffffL, body.size());
			long size = Math.min(args[2] & 0xffffffffL, body.size() - start);
			if (body.size() - size + value.length > limit) {
				return status(BAD_ARGUMENT);
			}
			body.replace((int) start, (int) size, value);
			if (stream.bodyType == BUFFER_REQUEST_BODY) {
				stream.requestModified = true;
			}
			else {
				stream.responseModified = true;
			}
			return status(OK);
		}

		private long[] getProperty(Instance instance, long... args) {
			Stream stream = this.current;
			String path = text(instance, args[0], args[1]).replace('\0', '.');
			byte[] value = property(stream, path);
			if (value == null) {
				return status(NOT_FOUND);
			}
			giveBack(instance, value, args[2], args[3]);
			return status(OK);
		}

		/** As Envoy serves them: a string as it is, a number as 8 little endian bytes. */
		private byte @Nullable [] property(@Nullable Stream stream, String path) {
			String value;
			switch (path) {
				case "plugin_name" -> value = ProxyWasmFilter.this.name;
				case "route_name" -> value = ProxyWasmFilter.this.route;
				default -> {
					if (stream == null) {
						return null;
					}
					Request request = stream.request;
					if (path.equals("response.code")) {
						return stream.status == 0 ? null
								: ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(stream.status).array();
					}
					value = switch (path) {
						case "request.path" -> request.target();
						case "request.url_path" -> request.path();
						case "request.method" -> request.method();
						case "request.host" -> request.host();
						case "request.scheme" -> "http";
						case "request.protocol" -> request.version();
						default -> null;
					};
				}
			}
			return value == null ? null : readable(value).getBytes(StandardCharsets.UTF_8);
		}

		/**
		 * {@code status, details, details length, body, body length, headers, headers
		 * length, grpc status}: while the request is offered only, an upstream's answer
		 * having begun to come otherwise.
		 */
		private long[] sendLocalResponse(Instance instance, long... args) {
			Stream stream = this.current;
			int code = (int) args[0];
			if (stream == null || !stream.answerable || code < 200 || code > 599) {
				return status(BAD_ARGUMENT);
			}
			byte[] body = bytes(instance, args[3], args[4], ProxyWasmFilter.this.maxBody);
			List<String> lines = new ArrayList<>();
			for (String[] pair : deserialized(bytes(instance, args[5], args[6], MAX_NAME))) {
				if (UpstreamFilterPipe.framingHeader(pair[0])) {
					continue;
				}
				String wire = new String(pair[1].getBytes(StandardCharsets.UTF_8), StandardCharsets.ISO_8859_1);
				try {
					UpstreamFilterPipe.checkEdit(pair[0], wire);
				}
				catch (IllegalArgumentException e) {
					return status(BAD_ARGUMENT);
				}
				lines.add(pair[0] + ": " + wire);
			}
			stream.localResponse = new LocalResponse(code, lines, body);
			return status(OK);
		}

		private @Nullable List<Field> map(int type) {
			Stream stream = this.current;
			if (stream == null) {
				return null;
			}
			return switch (type) {
				case MAP_REQUEST_HEADERS -> stream.requestHeaders;
				case MAP_RESPONSE_HEADERS -> stream.responseHeaders;
				default -> null;
			};
		}

		/**
		 * Only the head being offered can change, and takes effect when its call ends.
		 */
		private int edit(int type, String key, @Nullable String value, boolean add) {
			Stream stream = this.current;
			String lower = key.toLowerCase(Locale.ROOT);
			if (stream == null || type != stream.editable) {
				return BAD_ARGUMENT;
			}
			List<Field> map = type == MAP_REQUEST_HEADERS ? stream.requestHeaders : stream.responseHeaders;
			if (value == null && lower.equals("content-length")) {
				// what a plugin does to replace a body: its length goes with the rewrite
				stream.lengthRemoved = true;
				stream.edited |= map.removeIf(field -> field.name().equals(lower));
				return OK;
			}
			if (lower.startsWith(":") || type == MAP_REQUEST_HEADERS && lower.equals("host")) {
				return BAD_ARGUMENT;
			}
			String wire = value == null ? null
					: new String(value.getBytes(StandardCharsets.UTF_8), StandardCharsets.ISO_8859_1);
			try {
				UpstreamFilterPipe.checkEdit(lower, wire);
			}
			catch (IllegalArgumentException e) {
				return BAD_ARGUMENT;
			}
			if (!add) {
				stream.edited |= map.removeIf(field -> field.name().equals(lower));
			}
			if (value != null) {
				String line = lower + ": " + wire;
				if (map.size() >= MAX_FIELDS || linesLength(map) + line.length() > MAX_NAME) {
					return BAD_ARGUMENT;
				}
				map.add(new Field(lower, value, line));
				stream.edited = true;
			}
			return OK;
		}

		private static int linesLength(List<Field> map) {
			int length = 0;
			for (Field field : map) {
				length += field.line().length() + 2;
			}
			return length;
		}

		/**
		 * The whole map replaced: the difference is what is edited, so a plugin setting
		 * back what it read keeps what it may not change.
		 */
		private int setPairs(int type, List<String[]> pairs) {
			List<Field> map = map(type);
			if (map == null) {
				return NOT_FOUND;
			}
			for (Field field : List.copyOf(map)) {
				boolean kept = field.name().startsWith(":")
						|| pairs.stream().anyMatch(pair -> pair[0].equals(field.name()));
				if (!kept) {
					int status = edit(type, field.name(), null, false);
					if (status != OK) {
						return status;
					}
				}
			}
			for (String[] pair : pairs) {
				boolean known = pair[0].startsWith(":") || map.stream()
					.anyMatch(field -> field.name().equals(pair[0]) && field.value().equals(pair[1]));
				if (!known) {
					int status = edit(type, pair[0], pair[1], false);
					if (status != OK) {
						return status;
					}
				}
			}
			return OK;
		}

		/** Into memory the plugin allocated, as the ABI has it. */
		private void giveBack(Instance instance, byte[] bytes, long pointerAt, long sizeAt) {
			long pointer = instance.export(ProxyWasmFilter.this.allocator).apply(bytes.length)[0];
			if (pointer == 0 && bytes.length > 0) {
				throw new PluginFailure("the plugin allocated nothing");
			}
			instance.memory().write((int) pointer, bytes);
			instance.memory().writeI32((int) pointerAt, (int) pointer);
			instance.memory().writeI32((int) sizeAt, bytes.length);
		}

	}

	private static long[] status(int status) {
		return new long[] { status };
	}

	private static String text(Instance instance, long pointer, long length) {
		return new String(bytes(instance, pointer, length, MAX_NAME), StandardCharsets.UTF_8);
	}

	/** The plugin's pointers are not trusted; the memory checks the range. */
	private static byte[] bytes(Instance instance, long pointer, long length, int limit) {
		int len = (int) length;
		if (len < 0 || len > limit) {
			throw new PluginFailure("a length of " + (length & 0xffffffffL) + " past " + limit);
		}
		return instance.memory().readBytes((int) pointer, len);
	}

	/**
	 * {@code count, (name size, value size)..., (name NUL, value NUL)...}, little endian.
	 */
	private static byte[] serialized(List<Field> map) {
		List<byte[]> parts = new ArrayList<>();
		int size = 4 + map.size() * 8;
		for (Field field : map) {
			byte[] name = field.name().getBytes(StandardCharsets.UTF_8);
			byte[] value = field.value().getBytes(StandardCharsets.UTF_8);
			parts.add(name);
			parts.add(value);
			size += name.length + value.length + 2;
		}
		ByteBuffer out = ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN);
		out.putInt(map.size());
		for (byte[] part : parts) {
			out.putInt(part.length);
		}
		for (byte[] part : parts) {
			out.put(part).put((byte) 0);
		}
		return out.array();
	}

	/** The pairs of {@link #serialized}; the plugin's sizes are not trusted. */
	private static List<String[]> deserialized(byte[] bytes) {
		if (bytes.length == 0) {
			return List.of();
		}
		try {
			ByteBuffer in = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
			int count = in.getInt();
			if (count < 0 || count > MAX_FIELDS || count * 8L > in.remaining()) {
				throw new PluginFailure("a header map of " + count + " pairs");
			}
			int[] sizes = new int[count * 2];
			for (int i = 0; i < sizes.length; i++) {
				sizes[i] = in.getInt();
			}
			List<String[]> pairs = new ArrayList<>(count);
			for (int i = 0; i < count; i++) {
				String[] pair = new String[2];
				for (int j = 0; j < 2; j++) {
					int size = sizes[2 * i + j];
					if (size < 0 || size >= in.remaining()) {
						throw new PluginFailure("a header map past its bytes");
					}
					byte[] part = new byte[size];
					in.get(part);
					in.get();
					pair[j] = new String(part, StandardCharsets.UTF_8);
				}
				pair[0] = pair[0].toLowerCase(Locale.ROOT);
				pairs.add(pair);
			}
			return pairs;
		}
		catch (java.nio.BufferUnderflowException e) {
			throw new PluginFailure("a malformed header map", e);
		}
	}

	/**
	 * A head's text as a plugin reads it: the SDKs take a header as UTF-8 and fail on
	 * anything else, which a client must not be able to make happen. What is not UTF-8
	 * reads as replacement characters; the line itself stays as it came.
	 */
	private static String readable(String wire) {
		return new String(wire.getBytes(StandardCharsets.ISO_8859_1), StandardCharsets.UTF_8);
	}

	/** A field of a head: its name, its value as read, the line as it came. */
	private record Field(String name, String value, String line) {

		static Field pseudo(String name, String value) {
			return new Field(name, value, "");
		}

	}

	private static List<Field> fields(List<String> lines) {
		List<Field> fields = new ArrayList<>();
		for (String line : lines) {
			int colon = line.indexOf(':');
			if (colon > 0) {
				fields.add(new Field(line.substring(0, colon).trim().toLowerCase(Locale.ROOT),
						readable(line.substring(colon + 1).trim()), line));
			}
		}
		return fields;
	}

	/** Puts a head's fields back, the plugin's edits among them, but for the pseudo. */
	private static void apply(List<Field> fields, Headers headers) {
		List<String> lines = new ArrayList<>();
		for (Field field : fields) {
			if (!field.name().startsWith(":")) {
				lines.add(field.line());
			}
		}
		headers.replaceLines(lines);
	}

	/** The bytes of a body a call offers: a view of a piece, or what is held. */
	private static final class Buffer {

		private byte[] bytes;

		private int offset;

		private int size;

		private Buffer(byte[] bytes, int offset, int size) {
			this.bytes = bytes;
			this.offset = offset;
			this.size = size;
		}

		static Buffer of(byte[] bytes, int offset, int size) {
			return new Buffer(bytes, offset, size);
		}

		static Buffer held() {
			return new Buffer(EMPTY, 0, 0);
		}

		int size() {
			return this.size;
		}

		byte[] copy(int start, int length) {
			return Arrays.copyOfRange(this.bytes, this.offset + start, this.offset + start + length);
		}

		void append(byte[] more, int from, int length) {
			if (this.size + length > this.bytes.length) {
				this.bytes = Arrays.copyOf(this.bytes, Math.max(this.size + length, 2 * this.bytes.length));
			}
			System.arraycopy(more, from, this.bytes, this.size, length);
			this.size += length;
		}

		void replace(int start, int length, byte[] value) {
			byte[] replaced = new byte[this.size - length + value.length];
			System.arraycopy(this.bytes, this.offset, replaced, 0, start);
			System.arraycopy(value, 0, replaced, start, value.length);
			System.arraycopy(this.bytes, this.offset + start + length, replaced, start + value.length,
					this.size - start - length);
			this.bytes = replaced;
			this.offset = 0;
			this.size = replaced.length;
		}

		byte[] take() {
			byte[] taken = copy(0, this.size);
			this.bytes = EMPTY;
			this.offset = 0;
			this.size = 0;
			return taken;
		}

	}

	/** One request and its response: a context of a VM. */
	private final class Stream implements Exchange {

		private final Vm vm;

		final Request request;

		private final int generation;

		private final int context;

		/** The call ending the stream was made. */
		private volatile boolean closed;

		List<Field> requestHeaders = new ArrayList<>();

		List<Field> responseHeaders = new ArrayList<>();

		/** The map a call may edit; -1 for none. */
		int editable = -1;

		/** The plugin changed the head being offered: its fields are written back. */
		boolean edited;

		boolean lengthRemoved;

		int status;

		/** The buffer type the call in progress offers, and its bytes. */
		int bodyType = -1;

		@Nullable Buffer body;

		boolean replaceable;

		/**
		 * The plugin changed a body of that direction, which then cannot go on as it
		 * came.
		 */
		boolean requestModified;

		boolean responseModified;

		/** A call in progress may answer in the upstream's place. */
		boolean answerable;

		volatile @Nullable LocalResponse localResponse;

		Stream(Vm vm, Request request) {
			this.vm = vm;
			this.request = request;
			int[] started = vm.run(() -> {
				int id = vm.nextContext++;
				vm.call(null, "proxy_on_context_create", id, ROOT_CONTEXT);
				return new int[] { vm.generation, id };
			});
			this.generation = started[0];
			this.context = started[1];
		}

		private boolean gone() {
			return this.closed || this.generation != this.vm.generation;
		}

		/**
		 * Runs a call for this stream, unless its VM went since: another stream's failure
		 * is no reason to discard the VM started after it.
		 */
		private <T> T call(Supplier<T> task) {
			return this.vm.run(() -> {
				if (this.generation != this.vm.generation) {
					throw new StreamGone();
				}
				return task.get();
			});
		}

		void requestHead(Headers headers) {
			this.requestHeaders.add(Field.pseudo(":method", this.request.method()));
			this.requestHeaders.add(Field.pseudo(":path", readable(this.request.target())));
			this.requestHeaders.add(Field.pseudo(":scheme", "http"));
			String host = this.request.host();
			if (host != null) {
				this.requestHeaders.add(Field.pseudo(":authority", readable(host)));
			}
			this.requestHeaders.addAll(fields(headers.lines()));
			call(() -> {
				this.editable = MAP_REQUEST_HEADERS;
				this.answerable = true;
				this.vm.call(this, "proxy_on_request_headers", this.context, this.requestHeaders.size(),
						this.request.bodyFollows() ? 0 : 1);
				this.editable = -1;
				this.answerable = false;
				return null;
			});
			if (this.edited) {
				apply(this.requestHeaders, headers);
				this.edited = false;
			}
		}

		@Override
		public @Nullable BodyHandler onRequestBody() {
			if (gone()) {
				if (ProxyWasmFilter.this.failClosed) {
					throw new PluginFailure("the VM of this stream was discarded");
				}
				return null;
			}
			boolean sized = this.requestHeaders.stream().anyMatch(field -> field.name().equals("content-length"));
			boolean replacing = !sized || this.lengthRemoved;
			this.lengthRemoved = false;
			return replacing ? new Replacing(BUFFER_REQUEST_BODY, "proxy_on_request_body")
					: new Reading(BUFFER_REQUEST_BODY, "proxy_on_request_body");
		}

		@Override
		public @Nullable LocalResponse localResponse() {
			return this.localResponse;
		}

		@Override
		public @Nullable BodyHandler onResponse(int status, Headers headers, boolean bodiless) {
			if (gone()) {
				if (ProxyWasmFilter.this.failClosed) {
					throw new PluginFailure("the VM of this stream was discarded");
				}
				return null;
			}
			this.lengthRemoved = false;
			try {
				checkFields(headers);
			}
			catch (PluginFailure e) {
				close();
				throw e;
			}
			List<Field> offered = new ArrayList<>();
			offered.add(Field.pseudo(":status", Integer.toString(status)));
			offered.addAll(fields(headers.lines()));
			this.responseHeaders = offered;
			this.status = status;
			boolean sized = !headers.values("content-length").isEmpty();
			call(() -> {
				this.editable = MAP_RESPONSE_HEADERS;
				// a head's pause is not offered: it goes on as it would
				this.vm.call(this, "proxy_on_response_headers", this.context, offered.size(), bodiless ? 1 : 0);
				this.editable = -1;
				return null;
			});
			if (this.edited) {
				apply(offered, headers);
				this.edited = false;
			}
			if (bodiless) {
				close();
				return null;
			}
			return !sized || this.lengthRemoved ? new Replacing(BUFFER_RESPONSE_BODY, "proxy_on_response_body")
					: new Reading(BUFFER_RESPONSE_BODY, "proxy_on_response_body");
		}

		@Override
		public void abandon() {
			close();
		}

		/** The ABI's end of a stream, whatever its response came to. */
		private void close() {
			if (gone()) {
				return;
			}
			try {
				call(() -> {
					if (!this.closed) {
						this.closed = true;
						if (this.vm.call(this, "proxy_on_done", this.context)[0] != 0) {
							this.vm.call(this, "proxy_on_log", this.context);
							this.vm.call(this, "proxy_on_delete", this.context);
						}
					}
					return null;
				});
			}
			catch (PluginFailure e) {
				ProxyWasmFilter.this.failures.count("close", e);
			}
		}

		/**
		 * Offers a body as it stands; whether the plugin paused. Nothing is offered once
		 * the stream ended: its response may end before its request's body.
		 */
		private boolean offer(int type, String callback, Buffer buffer, boolean replaces, boolean end) {
			return call(() -> {
				if (this.closed) {
					return false;
				}
				this.bodyType = type;
				this.body = buffer;
				this.replaceable = replaces;
				this.answerable = type == BUFFER_REQUEST_BODY;
				long action = this.vm.call(this, callback, this.context, buffer.size(), end ? 1 : 0)[0];
				this.bodyType = -1;
				this.body = null;
				this.answerable = false;
				return action == ACTION_PAUSE;
			});
		}

		/** A body going on as it was framed: the plugin only reads it. */
		private final class Reading implements BodyObserver {

			private final int type;

			private final String callback;

			Reading(int type, String callback) {
				this.type = type;
				this.callback = callback;
			}

			@Override
			public void onData(byte[] bytes, int offset, int length) {
				if (!Stream.this.closed) {
					offer(this.type, this.callback, Buffer.of(bytes, offset, length), false, false);
				}
			}

			@Override
			public void onEnd(boolean complete) {
				try {
					if (complete && !Stream.this.closed) {
						offer(this.type, this.callback, Buffer.held(), false, true);
					}
				}
				finally {
					if (this.type == BUFFER_RESPONSE_BODY) {
						close();
					}
				}
			}

		}

		/**
		 * A body whose length may change: what the plugin left of it, when it stops
		 * pausing. What it holds meanwhile charges the budget.
		 */
		private final class Replacing implements BodyRewriter {

			private final int type;

			private final String callback;

			private final Buffer held = Buffer.held();

			private final TunnelBudget.Account account = ProxyWasmFilter.this.context.budget().account();

			/** The plugin failed, having changed nothing: the rest goes on unseen. */
			private boolean passing;

			Replacing(int type, String callback) {
				this.type = type;
				this.callback = callback;
			}

			private boolean modified() {
				return this.type == BUFFER_REQUEST_BODY ? Stream.this.requestModified : Stream.this.responseModified;
			}

			private byte[] step(byte[] bytes, int offset, int length, boolean end) {
				this.held.append(bytes, offset, length);
				if (!this.passing && !Stream.this.closed) {
					try {
						int size = this.held.size();
						if (size > ProxyWasmFilter.this.maxBody || !this.account.tryGrow(size)) {
							throw new PluginFailure("a body held past its limit");
						}
						if (offer(this.type, this.callback, this.held, true, end) && !end) {
							this.account.resize(this.held.size());
							return EMPTY;
						}
					}
					catch (PluginFailure e) {
						// what no one changed can go on; otherwise the message is cut
						if (modified() || ProxyWasmFilter.this.failClosed) {
							throw e;
						}
						ProxyWasmFilter.this.failures.count("body", e);
						this.passing = true;
					}
				}
				byte[] out = this.held.take();
				this.account.resize(0);
				return out;
			}

			@Override
			public byte[] onData(byte[] bytes, int offset, int length) {
				return step(bytes, offset, length, false);
			}

			@Override
			public byte[] onEnd() {
				return step(EMPTY, 0, 0, true);
			}

			@Override
			public void onClose() {
				this.account.close();
				if (this.type == BUFFER_RESPONSE_BODY) {
					close();
				}
			}

		}

	}

}
