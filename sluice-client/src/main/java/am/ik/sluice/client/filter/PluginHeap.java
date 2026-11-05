package am.ik.sluice.client.filter;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;

import org.jspecify.annotations.Nullable;
import run.endive.runtime.ExecutionListener;
import run.endive.runtime.Instance;
import run.endive.runtime.MStack;
import run.endive.runtime.TableInstance;
import run.endive.runtime.WasmArray;
import run.endive.runtime.WasmException;
import run.endive.runtime.WasmStruct;
import run.endive.wasm.WasmModule;
import run.endive.wasm.types.CompType;
import run.endive.wasm.types.FieldType;
import run.endive.wasm.types.Instruction;
import run.endive.wasm.types.StructType;
import run.endive.wasm.types.Table;
import run.endive.wasm.types.TableLimits;
import run.endive.wasm.types.TableSection;
import run.endive.wasm.types.TypeSection;

/**
 * What an instance of a plugin holds on the JVM's heap beside its linear memory, bounded
 * with it by {@code max-memory}: Wasm GC objects, exceptions, tables. Endive allocates
 * them as JVM objects and counts nothing, so each allocation is charged here before it
 * happens. That takes a listener on every instruction: a module allocating none of this
 * goes without.
 * <p>
 * All that was charged is taken as held until a census between two calls counts what the
 * globals and tables reach. The frames of a running call cannot be read, so one call may
 * allocate what is free, garbage or not.
 */
final class PluginHeap implements ExecutionListener {

	// What Endive's objects take on a 64-bit JVM, rounded up.

	private static final long OBJECT = 24;

	private static final long ARRAY = 16;

	private static final long LONG = 8;

	private static final long REFERENCE = 8;

	/** What a reference may hold: one at a time, so boxing in a loop charges nothing. */
	private static final long I31 = 16;

	private static final long EXCEPTION = 128;

	/** A table's entry: the index of a function, and its instance. */
	private static final long TABLE_ENTRY = 4 + REFERENCE;

	private final long limit;

	private final long[] structBytes;

	private final boolean[] arrayOfReferences;

	private final boolean[] tableOfReferences;

	private @Nullable Instance instance;

	private long live;

	private long since;

	PluginHeap(WasmModule module, long limit) {
		this.limit = limit;
		TypeSection types = module.typeSection();
		this.structBytes = new long[types.subTypeCount()];
		this.arrayOfReferences = new boolean[types.subTypeCount()];
		for (int i = 0; i < types.subTypeCount(); i++) {
			CompType type = types.getSubType(i).compType();
			StructType struct = type.structType();
			if (struct != null) {
				this.structBytes[i] = structBytes(struct);
			}
			else if (type.arrayType() != null) {
				this.arrayOfReferences[i] = type.arrayType().fieldType().storageType().isObjectRef();
			}
		}
		TableSection tables = module.tableSection();
		this.tableOfReferences = new boolean[tables.tableCount()];
		for (int i = 0; i < tables.tableCount(); i++) {
			this.tableOfReferences[i] = tables.getTable(i).elementType().isObjectRef();
		}
		this.live = tableBytes(module);
	}

	/** Whether a module can allocate any of this once instantiated. */
	static boolean needed(WasmModule module) {
		TypeSection types = module.typeSection();
		for (int i = 0; i < types.subTypeCount(); i++) {
			if (types.getSubType(i).compType().funcType() == null) {
				return true;
			}
		}
		TableSection tables = module.tableSection();
		for (int i = 0; i < tables.tableCount(); i++) {
			TableLimits limits = tables.getTable(i).limits();
			if (limits.max() > limits.min()) {
				return true;
			}
		}
		return false;
	}

	/** What a module's tables take at its instantiation. */
	static long tableBytes(WasmModule module) {
		long bytes = 0;
		TableSection tables = module.tableSection();
		for (int i = 0; i < tables.tableCount(); i++) {
			Table table = tables.getTable(i);
			bytes += table.limits().min() * tableEntry(table.elementType().isObjectRef());
		}
		return bytes;
	}

	void attach(Instance started) {
		this.instance = started;
	}

	@Override
	public void onExecution(Instruction instruction, MStack stack) {
		switch (instruction.opcode()) {
			case STRUCT_NEW, STRUCT_NEW_DEFAULT -> charge(this.structBytes[(int) instruction.operand(0)]);
			case ARRAY_NEW, ARRAY_NEW_DEFAULT, ARRAY_NEW_DATA, ARRAY_NEW_ELEM ->
				charge(arrayBytes(this.arrayOfReferences[(int) instruction.operand(0)], unsigned(stack.peek())));
			case ARRAY_NEW_FIXED ->
				charge(arrayBytes(this.arrayOfReferences[(int) instruction.operand(0)], instruction.operand(1)));
			case THROW -> charge(exceptionBytes(parameters((int) instruction.operand(0))));
			case TABLE_GROW ->
				charge(unsigned(stack.peek()) * tableEntry(this.tableOfReferences[(int) instruction.operand(0)]));
			case MEMORY_GROW -> check(unsigned(stack.peek()) * ProxyWasmFilter.PAGE_SIZE);
			default -> {
			}
		}
	}

	/**
	 * Counts anew when it pays: past what was found last, so the work stays in proportion
	 * to what was allocated, or when a call might not fit otherwise.
	 */
	void betweenCalls() {
		boolean due = this.since >= this.limit / 16
				&& (this.since >= this.live || linear() + this.live + this.since >= this.limit - this.limit / 4);
		if (due) {
			try {
				census();
			}
			catch (OutOfMemoryError ex) {
				// what was charged stands, and it is more than what is held
			}
		}
	}

	private void charge(long bytes) {
		this.since += bytes;
		check(0);
	}

	private void check(long growing) {
		long held = linear() + this.live + this.since + growing;
		if (held > this.limit) {
			throw new ProxyWasmFilter.PluginFailure(
					"the plugin would hold " + held + " bytes, past its limit of " + this.limit);
		}
	}

	private long linear() {
		Instance started = this.instance;
		return started == null ? 0 : (long) started.memory().pages() * ProxyWasmFilter.PAGE_SIZE;
	}

	private int parameters(int tag) {
		Instance started = this.instance;
		return started == null ? 0 : started.tag(tag).type().params().size();
	}

	private void census() {
		Instance started = this.instance;
		if (started == null) {
			return;
		}
		Set<Object> seen = Collections.newSetFromMap(new IdentityHashMap<>());
		ArrayDeque<Object> pending = new ArrayDeque<>();
		long bytes = 0;
		for (int i = 0; i < started.globalCount(); i++) {
			visit(started.global(i).getRefValue(), seen, pending);
		}
		for (int i = 0; i < started.tableCount(); i++) {
			TableInstance table = started.table(i);
			boolean references = table.elementType().isObjectRef();
			bytes += table.size() * tableEntry(references);
			for (int j = 0; references && j < table.size(); j++) {
				visit(table.objRef(j), seen, pending);
			}
		}
		for (Object object = pending.poll(); object != null; object = pending.poll()) {
			if (object instanceof WasmStruct struct) {
				bytes += this.structBytes[struct.typeIdx()];
				for (int i = 0; i < struct.fieldCount(); i++) {
					visit(struct.fieldRef(i), seen, pending);
				}
			}
			else if (object instanceof WasmArray array) {
				bytes += arrayBytes(array.elementRefs() != null, array.length());
				visitAll(array.elementRefs(), seen, pending);
			}
			else if (object instanceof WasmException exception) {
				bytes += exceptionBytes(exception.args() == null ? 0 : exception.args().length);
				visitAll(exception.refArgs(), seen, pending);
			}
		}
		this.live = bytes;
		this.since = 0;
	}

	private static void visitAll(Object @Nullable [] references, Set<Object> seen, ArrayDeque<Object> pending) {
		if (references != null) {
			for (Object reference : references) {
				visit(reference, seen, pending);
			}
		}
	}

	private static void visit(@Nullable Object reference, Set<Object> seen, ArrayDeque<Object> pending) {
		if (reference != null && seen.add(reference)) {
			pending.add(reference);
		}
	}

	private static long structBytes(StructType struct) {
		long bytes = OBJECT + ARRAY + ARRAY;
		for (FieldType field : struct.fieldTypes()) {
			bytes += LONG + REFERENCE + (field.storageType().isObjectRef() ? I31 : 0);
		}
		return bytes;
	}

	/** An element takes a long whatever its type: the interpreter does not pack them. */
	private static long arrayBytes(boolean references, long length) {
		return OBJECT + ARRAY + length * LONG + (references ? ARRAY + length * (REFERENCE + I31) : 0);
	}

	private static long exceptionBytes(int parameters) {
		return EXCEPTION + parameters * (LONG + REFERENCE + I31);
	}

	private static long tableEntry(boolean references) {
		return TABLE_ENTRY + (references ? REFERENCE + I31 : 0);
	}

	private static long unsigned(long length) {
		return Integer.toUnsignedLong((int) length);
	}

}
