import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import jdk.jfr.consumer.RecordedEvent;
import jdk.jfr.consumer.RecordedFrame;
import jdk.jfr.consumer.RecordedStackTrace;
import jdk.jfr.consumer.RecordingFile;

/**
 * Aggregates a recording: ThreadPark by site (lock contention vs condition waits),
 * JavaMonitorEnter by site, ObjectAllocationSample weight by allocating site.
 * Usage: JfrAgg file.jfr [top]
 */
public final class JfrAgg {

	static final class Acc {

		long count;

		long nanos;

		long weight;

	}

	public static void main(String[] args) throws Exception {
		int top = args.length > 1 ? Integer.parseInt(args[1]) : 15;
		Map<String, Acc> parks = new HashMap<>();
		Map<String, Acc> monitors = new HashMap<>();
		Map<String, Acc> allocs = new HashMap<>();
		Map<String, Acc> allocClasses = new HashMap<>();
		long allocTotal = 0;
		long first = Long.MAX_VALUE;
		long last = 0;
		try (RecordingFile file = new RecordingFile(Path.of(args[0]))) {
			while (file.hasMoreEvents()) {
				RecordedEvent e = file.readEvent();
				String type = e.getEventType().getName();
				long t = e.getStartTime().toEpochMilli();
				first = Math.min(first, t);
				last = Math.max(last, t);
				switch (type) {
					case "jdk.ThreadPark" -> {
						Acc a = parks.computeIfAbsent(parkSite(e), k -> new Acc());
						a.count++;
						a.nanos += e.getDuration().toNanos();
					}
					case "jdk.JavaMonitorEnter" -> {
						String cls = e.getClass("monitorClass") == null ? "?" : e.getClass("monitorClass").getName();
						Acc a = monitors.computeIfAbsent(cls + " @ " + caller(e.getStackTrace(), 0), k -> new Acc());
						a.count++;
						a.nanos += e.getDuration().toNanos();
					}
					case "jdk.ObjectAllocationSample" -> {
						long w = e.getLong("weight");
						allocTotal += w;
						String cls = e.getClass("objectClass").getName();
						allocClasses.computeIfAbsent(cls, k -> new Acc()).weight += w;
						allocs.computeIfAbsent(cls + " @ " + allocSite(e.getStackTrace()), k -> new Acc()).weight += w;
					}
					default -> {
					}
				}
			}
		}
		double secs = (last - first) / 1000.0;
		System.out.printf("window %.1fs%n", secs);
		print("ThreadPark by site (count, total ms, ms per s of window)", parks, top, secs, false);
		print("JavaMonitorEnter by site", monitors, top, secs, false);
		System.out.printf("%nallocation samples: total weight %.1f MiB (%.1f MiB/s)%n", allocTotal / 1048576.0,
				allocTotal / 1048576.0 / secs);
		print("allocation by class (MiB)", allocClasses, top, secs, true);
		print("allocation by site (MiB)", allocs, top, secs, true);
	}

	static void print(String title, Map<String, Acc> map, int top, double secs, boolean weight) {
		System.out.println();
		System.out.println("== " + title);
		List<Map.Entry<String, Acc>> list = new ArrayList<>(map.entrySet());
		list.sort((a, b) -> weight ? Long.compare(b.getValue().weight, a.getValue().weight)
				: Long.compare(b.getValue().nanos, a.getValue().nanos));
		for (int i = 0; i < Math.min(top, list.size()); i++) {
			Acc a = list.get(i).getValue();
			if (weight) {
				System.out.printf("%10.1f  %s%n", a.weight / 1048576.0, list.get(i).getKey());
			}
			else {
				System.out.printf("%9d %10.1f %8.1f  %s%n", a.count, a.nanos / 1e6, a.nanos / 1e6 / secs,
						list.get(i).getKey());
			}
		}
	}

	static String threadKind(RecordedEvent e) {
		var thread = e.getThread();
		if (thread == null) {
			return "?";
		}
		String name = thread.getJavaName() == null ? "?" : thread.getJavaName();
		return name.replaceAll("[0-9]+", "#");
	}

	/** lock@caller for lock acquisition parks, await@caller for condition waits. */
	static String parkSite(RecordedEvent e) {
		RecordedStackTrace st = e.getStackTrace();
		String kind = "park";
		if (st != null) {
			for (RecordedFrame f : st.getFrames()) {
				String m = f.getMethod().getType().getName() + "." + f.getMethod().getName();
				if (m.endsWith("ReentrantLock.lock") || m.endsWith("ReentrantLock$Sync.lock")) {
					kind = "lock";
					break;
				}
				if (m.contains("ConditionObject.await")) {
					kind = "await";
					break;
				}
			}
		}
		return kind + " @ " + caller(st, 0) + "  [" + threadKind(e) + "]";
	}

	static boolean infra(String type) {
		return type.startsWith("java.") || type.startsWith("jdk.") || type.startsWith("sun.");
	}

	/** The first frame outside the JDK. */
	static String caller(RecordedStackTrace st, int skip) {
		if (st == null) {
			return "?";
		}
		for (RecordedFrame f : st.getFrames()) {
			String type = f.getMethod().getType().getName();
			if (!infra(type)) {
				return shorten(type) + "." + f.getMethod().getName();
			}
		}
		return "jdk-only";
	}

	/** The first frame outside the JDK, plus the first sluice frame if different. */
	static String allocSite(RecordedStackTrace st) {
		if (st == null) {
			return "?";
		}
		String firstOut = null;
		for (RecordedFrame f : st.getFrames()) {
			String type = f.getMethod().getType().getName();
			String m = shorten(type) + "." + f.getMethod().getName();
			if (firstOut == null && !infra(type)) {
				firstOut = m;
			}
			if (type.startsWith("am.ik.sluice")) {
				return firstOut.equals(m) ? m : firstOut + " <- " + m;
			}
		}
		return firstOut == null ? "jdk-only" : firstOut;
	}

	static String shorten(String type) {
		return type.replace("am.ik.sluice.", "").replace("io.netty.", "n.").replace("io.grpc.", "g.");
	}

}
