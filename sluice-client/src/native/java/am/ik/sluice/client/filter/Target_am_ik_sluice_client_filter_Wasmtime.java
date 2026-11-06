package am.ik.sluice.client.filter;

import com.oracle.svm.core.annotate.Alias;
import com.oracle.svm.core.annotate.Substitute;
import com.oracle.svm.core.annotate.TargetClass;
import org.graalvm.nativeimage.c.function.CFunction.Transition;
import org.graalvm.nativeimage.c.function.CFunctionPointer;
import org.graalvm.nativeimage.c.function.InvokeCFunctionPointer;
import org.graalvm.word.WordFactory;

/**
 * On a native image a downcall handle made at run time is interpreted, some microseconds
 * a call: the calls made per call into a plugin go through C function pointers to the
 * addresses the FFM lookup found instead. Compiled only for {@code -Pnative} and
 * {@code -PnativeTest}. A pointer is made inside each method, from a {@code long}: a Word
 * in a static final field or a lambda fails the image's build.
 */
@TargetClass(Wasmtime.class)
final class Target_am_ik_sluice_client_filter_Wasmtime {

	@Alias
	static long VM_CALL_ADDRESS;

	@Alias
	static long VM_MEMORY_ADDRESS;

	@Alias
	static long VM_DEADLINE_ADDRESS;

	/** It calls back into Java: a transition to native, so the upcall can enter. */
	interface Call extends CFunctionPointer {

		@InvokeCFunctionPointer
		int invoke(long vm, int export, long args, int count);

	}

	interface Memory extends CFunctionPointer {

		@InvokeCFunctionPointer(transition = Transition.NO_TRANSITION)
		long invoke(long vm, long size);

	}

	interface Deadline extends CFunctionPointer {

		@InvokeCFunctionPointer(transition = Transition.NO_TRANSITION)
		void invoke(long vm, long ticks);

	}

	@Substitute
	static int call(long vm, int export, long args, int count) {
		Call call = WordFactory.pointer(VM_CALL_ADDRESS);
		return call.invoke(vm, export, args, count);
	}

	@Substitute
	static long memory(long vm, long size) {
		Memory memory = WordFactory.pointer(VM_MEMORY_ADDRESS);
		return memory.invoke(vm, size);
	}

	@Substitute
	static void deadline(long vm, long ticks) {
		Deadline deadline = WordFactory.pointer(VM_DEADLINE_ADDRESS);
		deadline.invoke(vm, ticks);
	}

}
