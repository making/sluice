package am.ik.sluice.client.filter;

import java.io.IOException;
import java.io.InputStream;
import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Locale;

/**
 * The binding of {@code sluice-client-wasm}, the library running the plugins on wasmtime,
 * through FFM: its C ABI of plain scalars and pointers. The library is a resource of the
 * client for its platform, extracted once to a temporary file, loaded, and removed.
 * <p>
 * The calls made per call into a plugin ({@link #call}, {@link #memory},
 * {@link #deadline}) take addresses as {@code long}s: a native image calls them through C
 * function pointers ({@code src/native/java}), as a downcall handle is interpreted there.
 */
final class Wasmtime {

	/** The most stack a call into a plugin takes. */
	static final long WASM_STACK = 1024 * 1024;

	/** The stack of a thread calling into a plugin: the call's, and the host's beyond. */
	static final long THREAD_STACK = 4 * WASM_STACK;

	/** The epoch's tick: the resolution of {@code call-timeout}. */
	static final long TICK_MILLIS = 10;

	static final int OK = 0;

	static final int FAILED = 1;

	static final int HOST_FAILED = 2;

	/** The values a call takes at most, as the library has it. */
	static final int MAX_VALUES = 16;

	private static final ValueLayout.OfLong J = ValueLayout.JAVA_LONG;

	private static final ValueLayout.OfInt I = ValueLayout.JAVA_INT;

	private static final ValueLayout P = ValueLayout.ADDRESS;

	private static final Linker LINKER = Linker.nativeLinker();

	private static final SymbolLookup LIBRARY = load();

	private static final MethodHandle INIT = handle("sw_init", FunctionDescriptor.of(I, P, J, J));

	private static final MethodHandle ERROR = handle("sw_error", FunctionDescriptor.of(J, P, J));

	private static final MethodHandle MODULE_NEW = handle("sw_module_new", FunctionDescriptor.of(J, P, J));

	private static final MethodHandle MODULE_DESCRIBE = handle("sw_module_describe", FunctionDescriptor.of(J, J, P, J));

	private static final MethodHandle VM_NEW = handle("sw_vm_new", FunctionDescriptor.of(J, J, J, J));

	private static final MethodHandle VM_FREE = handle("sw_vm_free", FunctionDescriptor.ofVoid(J));

	// they call back into the host: not critical

	private static final MethodHandle VM_CALL = handle("sw_vm_call", FunctionDescriptor.of(I, J, I, J, I));

	private static final MethodHandle VM_MEMORY = handle("sw_vm_memory", FunctionDescriptor.of(J, J, J),
			Linker.Option.critical(false));

	private static final MethodHandle VM_DEADLINE = handle("sw_vm_deadline", FunctionDescriptor.ofVoid(J, J),
			Linker.Option.critical(false));

	// the addresses of the calls per call, for a native image's function pointers

	static final long VM_CALL_ADDRESS = address("sw_vm_call");

	static final long VM_MEMORY_ADDRESS = address("sw_vm_memory");

	static final long VM_DEADLINE_ADDRESS = address("sw_vm_deadline");

	static {
		try {
			MethodHandle host = MethodHandles.lookup()
				.findStatic(PluginInstance.class, "host",
						MethodType.methodType(int.class, int.class, long.class, int.class, long.class, long.class));
			MemorySegment stub = LINKER.upcallStub(host, FunctionDescriptor.of(I, I, J, I, J, J), Arena.global());
			int status = (int) INIT.invokeExact(stub, WASM_STACK, TICK_MILLIS);
			if (status != OK) {
				throw new IllegalStateException("plugins cannot run: " + error());
			}
		}
		catch (RuntimeException | Error ex) {
			throw ex;
		}
		catch (Throwable ex) {
			throw new IllegalStateException(ex);
		}
	}

	private Wasmtime() {
	}

	/** {@code linux-x86_64}, {@code macos-aarch64}: where the library's resource is. */
	static String platform() {
		String os = System.getProperty("os.name", "").toLowerCase(Locale.ROOT);
		String arch = System.getProperty("os.arch", "").toLowerCase(Locale.ROOT);
		String system = os.startsWith("mac") ? "macos" : os.startsWith("linux") ? "linux" : os.replace(' ', '_');
		String machine = switch (arch) {
			case "amd64", "x86_64" -> "x86_64";
			case "aarch64", "arm64" -> "aarch64";
			default -> arch;
		};
		return system + "-" + machine;
	}

	private static SymbolLookup load() {
		String file = System.mapLibraryName("sluice_client_wasm");
		String resource = "META-INF/native/" + platform() + "/" + file;
		try (InputStream in = Wasmtime.class.getClassLoader().getResourceAsStream(resource)) {
			if (in == null) {
				throw new IllegalStateException(
						"plugins cannot run on " + platform() + ": this build of the client has no " + resource);
			}
			// a name of its own, only this user's: no file of another may be taken for it
			Path extracted = Files.createTempFile("sluice-client-wasm-", "-" + file);
			try {
				Files.copy(in, extracted, StandardCopyOption.REPLACE_EXISTING);
				return SymbolLookup.libraryLookup(extracted, Arena.global());
			}
			finally {
				Files.deleteIfExists(extracted);
			}
		}
		catch (IOException ex) {
			throw new IllegalStateException("plugins cannot run: " + resource + " not loaded: " + ex, ex);
		}
	}

	private static MemorySegment symbol(String name) {
		return LIBRARY.find(name).orElseThrow(() -> new IllegalStateException("no " + name + " in the library"));
	}

	private static MethodHandle handle(String name, FunctionDescriptor descriptor, Linker.Option... options) {
		return LINKER.downcallHandle(symbol(name), descriptor, options);
	}

	private static long address(String name) {
		return symbol(name).address();
	}

	/** This thread's last error of the library. */
	static String error() {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment out = arena.allocate(4096);
			long length = (long) ERROR.invokeExact(out, out.byteSize());
			if (length > out.byteSize()) {
				out = arena.allocate(length);
				length = (long) ERROR.invokeExact(out, out.byteSize());
			}
			return new String(out.asSlice(0, length).toArray(ValueLayout.JAVA_BYTE), StandardCharsets.UTF_8);
		}
		catch (Throwable ex) {
			throw rethrown(ex);
		}
	}

	/** A compiled module; its handle, never freed: a plugin is defined for good. */
	static long compile(byte[] bytes) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment wasm = arena.allocate(Math.max(bytes.length, 1));
			MemorySegment.copy(bytes, 0, wasm, ValueLayout.JAVA_BYTE, 0, bytes.length);
			long module = (long) MODULE_NEW.invokeExact(wasm, (long) bytes.length);
			if (module == 0) {
				throw new IllegalArgumentException(error());
			}
			return module;
		}
		catch (Throwable ex) {
			throw rethrown(ex);
		}
	}

	/** What the library tells of a module, as {@link PluginModule} reads it. */
	static byte[] describe(long module) {
		try (Arena arena = Arena.ofConfined()) {
			MemorySegment out = arena.allocate(64 * 1024);
			long length = (long) MODULE_DESCRIBE.invokeExact(module, out, out.byteSize());
			if (length > out.byteSize()) {
				out = arena.allocate(length);
				length = (long) MODULE_DESCRIBE.invokeExact(module, out, out.byteSize());
			}
			return out.asSlice(0, length).toArray(ValueLayout.JAVA_BYTE);
		}
		catch (Throwable ex) {
			throw rethrown(ex);
		}
	}

	/** A VM of a module, its start function run; 0 on failure, the reason in error. */
	static long newVm(long module, long maxMemory, long ticks) {
		try {
			return (long) VM_NEW.invokeExact(module, maxMemory, ticks);
		}
		catch (Throwable ex) {
			throw rethrown(ex);
		}
	}

	static void freeVm(long vm) {
		try {
			VM_FREE.invokeExact(vm);
		}
		catch (Throwable ex) {
			throw rethrown(ex);
		}
	}

	/** Calls an export with the values at {@code args}; its result at {@code args}. */
	static int call(long vm, int export, long args, int count) {
		try {
			return (int) VM_CALL.invokeExact(vm, export, args, count);
		}
		catch (Throwable ex) {
			throw rethrown(ex);
		}
	}

	/** The linear memory's base, its size at {@code size}. */
	static long memory(long vm, long size) {
		try {
			return (long) VM_MEMORY.invokeExact(vm, size);
		}
		catch (Throwable ex) {
			throw rethrown(ex);
		}
	}

	static void deadline(long vm, long ticks) {
		try {
			VM_DEADLINE.invokeExact(vm, ticks);
		}
		catch (Throwable ex) {
			throw rethrown(ex);
		}
	}

	private static RuntimeException rethrown(Throwable ex) {
		if (ex instanceof RuntimeException runtime) {
			return runtime;
		}
		if (ex instanceof Error error) {
			throw error;
		}
		return new IllegalStateException(ex);
	}

}
