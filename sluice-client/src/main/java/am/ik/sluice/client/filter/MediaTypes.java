package am.ik.sluice.client.filter;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * The media types a filter's {@code types} setting lists, comma separated: exact,
 * {@code type/*}, or {@code type/*+suffix}.
 */
final class MediaTypes {

	private final List<String> patterns;

	private MediaTypes(List<String> patterns) {
		this.patterns = List.copyOf(patterns);
	}

	/**
	 * The types of a setting.
	 * @throws IllegalArgumentException when one is not a media type
	 */
	static MediaTypes parse(String filter, String list) {
		List<String> patterns = new ArrayList<>();
		for (String type : list.split(",")) {
			String trimmed = type.trim().toLowerCase(Locale.ROOT);
			if (!trimmed.isEmpty()) {
				if (trimmed.indexOf('/') <= 0) {
					throw new IllegalArgumentException("filter " + filter + " takes media types in types, not " + type);
				}
				patterns.add(trimmed);
			}
		}
		return new MediaTypes(patterns);
	}

	/** Whether the message's {@code Content-Type} is one of these; absent, it is not. */
	boolean matches(UpstreamFilter.Headers headers) {
		List<String> values = headers.values("content-type");
		if (values.isEmpty()) {
			return false;
		}
		String type = values.get(0);
		int parameters = type.indexOf(';');
		type = (parameters < 0 ? type : type.substring(0, parameters)).trim().toLowerCase(Locale.ROOT);
		int slash = type.indexOf('/');
		if (slash <= 0) {
			return false;
		}
		String main = type.substring(0, slash);
		String subtype = type.substring(slash + 1);
		for (String pattern : this.patterns) {
			int at = pattern.indexOf('/');
			if (!pattern.substring(0, at).equals(main)) {
				continue;
			}
			String wanted = pattern.substring(at + 1);
			if (wanted.equals("*") || wanted.equals(subtype)
					|| wanted.startsWith("*+") && subtype.endsWith(wanted.substring(1))) {
				return true;
			}
		}
		return false;
	}

	/** The elements of a list header's values, trimmed and in lower case. */
	static List<String> elements(List<String> values) {
		List<String> elements = new ArrayList<>();
		for (String value : values) {
			for (String element : value.split(",")) {
				String trimmed = element.trim().toLowerCase(Locale.ROOT);
				if (!trimmed.isEmpty()) {
					elements.add(trimmed);
				}
			}
		}
		return elements;
	}

	/** The content codings of a message, identity left out. */
	static List<String> codings(UpstreamFilter.Headers headers) {
		List<String> codings = elements(headers.values("content-encoding"));
		codings.removeIf("identity"::equals);
		return codings;
	}

}
