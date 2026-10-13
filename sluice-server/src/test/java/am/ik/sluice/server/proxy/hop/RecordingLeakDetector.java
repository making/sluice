package am.ik.sluice.server.proxy.hop;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import io.netty.util.ResourceLeakDetector;

/**
 * Netty's leak detector, recording every leak it reports. Installed for the JVM of this
 * package's tests by {@code -Dio.netty.customResourceLeakDetector} (see the surefire
 * execution {@code hop} in {@code pom.xml}), so it is in place before any ByteBuf class
 * creates its detector; {@link LeakCheck} reads the record.
 */
public final class RecordingLeakDetector<T> extends ResourceLeakDetector<T> {

	private static final List<String> LEAKS = new CopyOnWriteArrayList<>();

	private static volatile boolean installed;

	public RecordingLeakDetector(Class<?> resourceType, int samplingInterval) {
		super(resourceType, samplingInterval);
		installed = true;
	}

	@SuppressWarnings("deprecation")
	public RecordingLeakDetector(Class<?> resourceType, int samplingInterval, long maxActive) {
		super(resourceType, samplingInterval, maxActive);
		installed = true;
	}

	/** The leaks reported so far: the resource type and the access records. */
	static List<String> leaks() {
		return List.copyOf(LEAKS);
	}

	/** Whether netty created its detectors from this class. */
	static boolean installed() {
		return installed;
	}

	@Override
	protected boolean needReport() {
		// reported whatever the logging configuration
		return true;
	}

	@Override
	protected void reportTracedLeak(String resourceType, String records) {
		LEAKS.add(resourceType + records);
		super.reportTracedLeak(resourceType, records);
	}

	@Override
	protected void reportUntracedLeak(String resourceType) {
		LEAKS.add(resourceType);
		super.reportUntracedLeak(resourceType);
	}

}
