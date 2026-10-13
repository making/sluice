package am.ik.sluice.server.proxy.hop;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/** An exception sink keeping what it was given. */
final class RecordingSink implements Loop.ExceptionSink {

	private final List<Failure> failures = new CopyOnWriteArrayList<>();

	record Failure(String source, Throwable cause) {

		@Override
		public String toString() {
			return this.source + ": " + this.cause;
		}

	}

	@Override
	public void exceptionCaught(String source, Throwable cause) {
		this.failures.add(new Failure(source, cause));
	}

	List<Failure> failures() {
		return List.copyOf(this.failures);
	}

}
