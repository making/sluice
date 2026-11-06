package am.ik.sluice.client.filter;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.ref.Cleaner;

import org.jspecify.annotations.Nullable;

import am.ik.sluice.client.filter.ProxyWasmFilter.PluginFailure;

/**
 * An instance of a plugin, in a store of its own within its budget: the calls into it,
 * and its imports served by Java. Used by one thread at a time, its calls on a thread of
 * {@link Wasmtime#THREAD_STACK}.
 * <p>
 * What it holds natively is freed when it is closed, or when it is no longer reachable.
 */
final class PluginInstance implements AutoCloseable {

	private static final Cleaner CLEANER = Cleaner.create();

	/** The instance whose call is in progress on this thread: the imports' target. */
	private static final ThreadLocal<@Nullable PluginInstance> CURRENT = new ThreadLocal<>();

	private final PluginModule module;

	private final HostFunction[] functions;

	private final Native held;

	private final Cleaner.Cleanable cleanable;

	/** The memory as the import in progress was handed it, or as last asked for. */
	private @Nullable Memory memory;

	/** What an import threw, which its call rethrows. */
	private @Nullable Throwable failure;

	/**
	 * Instantiates a module and runs its start function, if it has one, within
	 * {@code ticks} of the epoch.
	 * @param functions what serves each of its imports, in their order
	 */
	PluginInstance(PluginModule module, HostFunction[] functions, long maxMemory, long ticks) {
		this.module = module;
		this.functions = functions;
		Arena arena = Arena.ofShared();
		MemorySegment values = arena.allocate(ValueLayout.JAVA_LONG, Wasmtime.MAX_VALUES + 1);
		PluginInstance previous = CURRENT.get();
		CURRENT.set(this);
		long vm;
		try {
			vm = Wasmtime.newVm(module.handle(), maxMemory, ticks);
		}
		finally {
			CURRENT.set(previous);
		}
		if (vm == 0) {
			arena.close();
			throw failed(Wasmtime.error());
		}
		this.held = new Native(vm, arena, values);
		this.cleanable = CLEANER.register(this, this.held);
	}

	/** Its time from now: {@code ticks} of the epoch, for all it runs until set again. */
	void deadline(long ticks) {
		Wasmtime.deadline(this.held.vm, ticks);
	}

	/** Calls an export of i32s and i64s; its result, or 0 for none. */
	long call(String export, long... args) {
		int index = this.module.function(export);
		if (index < 0) {
			throw new PluginFailure("the plugin has no function " + export);
		}
		if (args.length > Wasmtime.MAX_VALUES) {
			throw new PluginFailure("a call of " + args.length + " values");
		}
		MemorySegment values = this.held.values;
		// a call from an import's: its values come after the outer call's
		for (int i = 0; i < args.length; i++) {
			values.setAtIndex(ValueLayout.JAVA_LONG, i, args[i]);
		}
		PluginInstance previous = CURRENT.get();
		CURRENT.set(this);
		int status;
		try {
			status = Wasmtime.call(this.held.vm, index, values.address(), args.length);
		}
		finally {
			CURRENT.set(previous);
			// the call may have grown or moved it
			this.memory = null;
		}
		return switch (status) {
			case Wasmtime.OK -> values.getAtIndex(ValueLayout.JAVA_LONG, 0);
			case Wasmtime.HOST_FAILED -> throw hostFailure();
			default -> throw failed(Wasmtime.error());
		};
	}

	private RuntimeException hostFailure() {
		Throwable thrown = this.failure;
		this.failure = null;
		if (thrown instanceof RuntimeException runtime) {
			return runtime;
		}
		if (thrown == null) {
			return new PluginFailure("an import failed");
		}
		return new PluginFailure(thrown.toString(), thrown);
	}

	private static PluginFailure failed(String message) {
		return new PluginFailure(message);
	}

	/** The linear memory, during an import of a call. */
	Memory memory() {
		Memory known = this.memory;
		if (known != null) {
			return known;
		}
		MemorySegment size = this.held.values.asSlice(Wasmtime.MAX_VALUES * Long.BYTES, Long.BYTES);
		long base = Wasmtime.memory(this.held.vm, size.address());
		Memory current = new Memory(base, size.get(ValueLayout.JAVA_LONG, 0));
		this.memory = current;
		return current;
	}

	@Override
	public void close() {
		this.cleanable.clean();
	}

	/**
	 * Every import's entry, the library's callback: it must not throw, or the JVM ends.
	 * @param index the import's
	 * @param args its values, and the slot of its result
	 */
	static int host(int index, long args, int count, long base, long size) {
		PluginInstance instance = CURRENT.get();
		try {
			MemorySegment values = MemorySegment.ofAddress(args).reinterpret(Long.BYTES * Math.max(count, 1));
			long[] given = new long[count];
			for (int i = 0; i < count; i++) {
				given[i] = values.getAtIndex(ValueLayout.JAVA_LONG, i);
			}
			PluginInstance target = java.util.Objects.requireNonNull(instance, "no call in progress");
			target.memory = new Memory(base, size);
			long result = target.functions[index].apply(target, given);
			values.setAtIndex(ValueLayout.JAVA_LONG, 0, result);
			return Wasmtime.OK;
		}
		catch (Throwable ex) {
			if (instance != null) {
				instance.failure = ex;
			}
			return Wasmtime.HOST_FAILED;
		}
	}

	/** An import served by Java: its result, ignored when it has none. */
	@FunctionalInterface
	interface HostFunction {

		long apply(PluginInstance instance, long... args);

	}

	/** What is freed: the store, and the values passed to it. */
	private record Native(long vm, Arena arena, MemorySegment values) implements Runnable {

		@Override
		public void run() {
			Wasmtime.freeVm(this.vm);
			this.arena.close();
		}

	}

	/**
	 * A linear memory as it is during one import: the plugin's addresses are not trusted,
	 * the segment checks them.
	 */
	static final class Memory {

		private final MemorySegment segment;

		Memory(long base, long size) {
			this.segment = base == 0 ? MemorySegment.NULL : MemorySegment.ofAddress(base).reinterpret(size);
		}

		byte[] readBytes(int address, int length) {
			return this.segment.asSlice(Integer.toUnsignedLong(address), length).toArray(ValueLayout.JAVA_BYTE);
		}

		int readInt(int address) {
			return this.segment.get(ValueLayout.JAVA_INT_UNALIGNED, Integer.toUnsignedLong(address));
		}

		void write(int address, byte[] bytes) {
			MemorySegment.copy(bytes, 0, this.segment, ValueLayout.JAVA_BYTE, Integer.toUnsignedLong(address),
					bytes.length);
		}

		void writeI32(int address, int value) {
			this.segment.set(ValueLayout.JAVA_INT_UNALIGNED, Integer.toUnsignedLong(address), value);
		}

		void writeLong(int address, long value) {
			this.segment.set(ValueLayout.JAVA_LONG_UNALIGNED, Integer.toUnsignedLong(address), value);
		}

	}

}
