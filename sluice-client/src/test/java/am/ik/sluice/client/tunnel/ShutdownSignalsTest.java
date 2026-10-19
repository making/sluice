package am.ik.sluice.client.tunnel;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import sun.misc.Signal;
import sun.misc.SignalHandler;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The first SIGTERM / SIGINT goes to the handler installed before; a later one, either
 * signal, aborts the drain. An ignored signal stays ignored.
 */
class ShutdownSignalsTest {

	@Test
	void firstSignalStartsTheShutdownAndALaterOneAbortsTheDrain() {
		List<String> previousCalls = new ArrayList<>();
		AtomicInteger aborts = new AtomicInteger();
		Map<String, SignalHandler> installed = new LinkedHashMap<>();
		ShutdownSignals signals = new ShutdownSignals(aborts::incrementAndGet, (signal, handler) -> {
			installed.put(signal.getName(), handler);
			return received -> previousCalls.add(received.getName());
		});
		signals.start();
		assertThat(installed).containsOnlyKeys("TERM", "INT");

		Objects.requireNonNull(installed.get("TERM")).handle(new Signal("TERM"));
		assertThat(previousCalls).containsExactly("TERM");
		assertThat(aborts).hasValue(0);

		Objects.requireNonNull(installed.get("INT")).handle(new Signal("INT"));
		Objects.requireNonNull(installed.get("TERM")).handle(new Signal("TERM"));
		assertThat(previousCalls).containsExactly("TERM");
		assertThat(aborts).hasValue(2);
	}

	@Test
	void signalWithoutAJavaHandlerOrIgnoredIsLeftAsItWas() {
		Map<String, List<SignalHandler>> installs = new LinkedHashMap<>();
		ShutdownSignals signals = new ShutdownSignals(() -> {
		}, (signal, handler) -> {
			List<SignalHandler> history = installs.computeIfAbsent(signal.getName(), name -> new ArrayList<>());
			history.add(handler);
			return history.size() == 1
					? (signal.getName().equals("INT") ? SignalHandler.SIG_IGN : SignalHandler.SIG_DFL) : handler;
		});
		signals.start();
		assertThat(installs.get("INT")).hasSize(2).last().isSameAs(SignalHandler.SIG_IGN);
		assertThat(installs.get("TERM")).hasSize(2).last().isSameAs(SignalHandler.SIG_DFL);
	}

}
