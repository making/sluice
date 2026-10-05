package am.ik.sluice.client.upstream;

import org.jspecify.annotations.Nullable;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Parses the {@code host=targetUrl,host2=targetUrl2} upstream notation (the port of
 * buildUpstreamMap). A missing {@code http://} / {@code https://} scheme is completed
 * with {@code http://}.
 */
public final class UpstreamParser {

	private UpstreamParser() {
	}

	/**
	 * Parses the comma separated upstream list preserving the declaration order.
	 */
	public static Map<String, String> parse(@Nullable String input) {
		Map<String, String> upstreams = new LinkedHashMap<>();
		if (input == null || input.isBlank()) {
			return upstreams;
		}
		for (String entry : input.split(",")) {
			String[] pair = entry.split("=", 2);
			if (pair.length == 1) {
				upstreams.put("", pair[0].trim());
			}
			else {
				upstreams.put(pair[0].trim(), pair[1].trim());
			}
		}
		Map<String, String> withScheme = new LinkedHashMap<>();
		for (Map.Entry<String, String> entry : upstreams.entrySet()) {
			String value = entry.getValue();
			boolean hasScheme = value.startsWith("http://") || value.startsWith("https://");
			withScheme.put(entry.getKey(), hasScheme ? value : "http://" + value);
		}
		return withScheme;
	}

}
