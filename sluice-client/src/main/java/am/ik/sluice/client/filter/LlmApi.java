package am.ik.sluice.client.filter;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Locale;

import org.jspecify.annotations.Nullable;

/**
 * The LLM APIs the {@code llm-*} filters know, by request path: the OpenAI compatible
 * ones and the Anthropic one.
 */
enum LlmApi {

	CHAT("openai", "max_completion_tokens", "max_tokens"), COMPLETIONS("openai", "max_tokens"),
	RESPONSES("openai", "max_output_tokens"), ANTHROPIC("anthropic", "max_tokens");

	/** Every field of a request the APIs bound their output by. */
	static final List<String> OUTPUT_LIMITS = List.of("max_tokens", "max_completion_tokens", "max_output_tokens");

	/** The API as a tag. */
	final String tag;

	/** This API's fields bounding the output; the first is the one it documents. */
	final List<String> outputLimits;

	LlmApi(String tag, String... outputLimits) {
		this.tag = tag;
		this.outputLimits = List.of(outputLimits);
	}

	/** Whether it is one of OpenAI's completions: prompt and completion tokens. */
	boolean completions() {
		return this == CHAT || this == COMPLETIONS;
	}

	boolean openAi() {
		return this != ANTHROPIC;
	}

	/** The API of a request, by its exact path; null for another request. */
	static @Nullable LlmApi of(String method, String path) {
		if (!method.equals("POST")) {
			return null;
		}
		return ofPath(path.endsWith("/") ? path.substring(0, path.length() - 1) : path);
	}

	/**
	 * The API a path may reach on an upstream reading it loosely: percent-decoded,
	 * {@code \} as {@code /}, segment parameters, empty and dot segments removed, in
	 * lower case. A limit takes every spelling an upstream may route as the API's.
	 */
	static @Nullable LlmApi ofLoosePath(String path) {
		Deque<String> segments = new ArrayDeque<>();
		for (String segment : decoded(path).replace('\\', '/').split("/")) {
			int parameters = segment.indexOf(';');
			String name = parameters < 0 ? segment : segment.substring(0, parameters);
			if (name.equals("..")) {
				segments.pollLast();
			}
			else if (!name.isEmpty() && !name.equals(".")) {
				segments.addLast(name);
			}
		}
		return ofPath("/" + String.join("/", segments).toLowerCase(Locale.ROOT));
	}

	private static @Nullable LlmApi ofPath(String path) {
		return switch (path) {
			case "/v1/chat/completions" -> CHAT;
			case "/v1/completions" -> COMPLETIONS;
			case "/v1/responses" -> RESPONSES;
			case "/v1/messages" -> ANTHROPIC;
			default -> null;
		};
	}

	/** Percent-decoded once, as bytes read as ISO-8859-1; a malformed escape kept. */
	private static String decoded(String path) {
		if (path.indexOf('%') < 0) {
			return path;
		}
		ByteArrayOutputStream bytes = new ByteArrayOutputStream(path.length());
		for (int i = 0; i < path.length(); i++) {
			char c = path.charAt(i);
			int high = i + 2 < path.length() ? Character.digit(path.charAt(i + 1), 16) : -1;
			int low = high < 0 ? -1 : Character.digit(path.charAt(i + 2), 16);
			if (c == '%' && low >= 0) {
				bytes.write(high << 4 | low);
				i += 2;
			}
			else {
				bytes.write(c);
			}
		}
		return bytes.toString(StandardCharsets.ISO_8859_1);
	}

}
