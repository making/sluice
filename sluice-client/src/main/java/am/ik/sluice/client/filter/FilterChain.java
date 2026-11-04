package am.ik.sluice.client.filter;

import java.util.ArrayList;
import java.util.List;

import org.jspecify.annotations.Nullable;

/**
 * The filters of one route, in request order: a request passes them first to last, its
 * response last to first, each body through what the filter before it left. A filter sees
 * only the paths it is set to, before anything of it runs. A filter failing is counted
 * and, unless it fails closed, left out of that exchange; a rewriter failing always fails
 * the message, part of which it may have changed.
 */
final class FilterChain implements UpstreamFilter {

	private static final byte[] EMPTY = new byte[0];

	private final List<Link> links;

	FilterChain(List<Link> links) {
		this.links = List.copyOf(links);
	}

	/**
	 * One filter of the chain.
	 *
	 * @param filter the filter
	 * @param paths the paths it sees, exact or a prefix ending in {@code *}; empty = all
	 * @param failClosed whether its failing fails the request or response
	 * @param failures where its failures are counted
	 */
	record Link(UpstreamFilter filter, List<String> paths, boolean failClosed, FilterFailures failures) {

		boolean sees(String path) {
			if (this.paths.isEmpty()) {
				return true;
			}
			for (String pattern : this.paths) {
				boolean matches = pattern.endsWith("*") ? path.startsWith(pattern.substring(0, pattern.length() - 1))
						: path.equals(pattern);
				if (matches) {
					return true;
				}
			}
			return false;
		}

		/** Counts a failure; goes on with what it returns, unless it fails closed. */
		void failed(String phase, RuntimeException failure) {
			this.failures.count(phase, failure);
			if (this.failClosed) {
				throw failure;
			}
		}

	}

	@Override
	public @Nullable Exchange onRequest(Request request, Headers headers) {
		String path = request.path();
		List<Exchange> taken = new ArrayList<>(this.links.size());
		for (Link link : this.links) {
			if (!link.sees(path)) {
				continue;
			}
			Exchange exchange;
			try {
				exchange = link.filter().onRequest(request, headers);
			}
			catch (RuntimeException e) {
				if (link.failClosed()) {
					abandonAll(taken);
				}
				link.failed("head", e);
				continue;
			}
			if (exchange != null) {
				Exchange guarded = new Guarded(link, exchange);
				taken.add(guarded);
				if (guarded.localResponse() != null) {
					break;
				}
			}
		}
		return switch (taken.size()) {
			case 0 -> null;
			case 1 -> taken.get(0);
			default -> new Chained(taken);
		};
	}

	private static void abandonAll(List<Exchange> exchanges) {
		for (Exchange exchange : exchanges) {
			exchange.abandon();
		}
	}

	/**
	 * One filter's part, under its failure policy. Once failed it is left out of the
	 * exchange, and told so; offered a response, it is not told of an abandon.
	 */
	private static final class Guarded implements Exchange {

		private final Link link;

		private final Exchange exchange;

		private volatile boolean over;

		Guarded(Link link, Exchange exchange) {
			this.link = link;
			this.exchange = exchange;
		}

		@Override
		public @Nullable BodyHandler onRequestBody() {
			if (this.over) {
				return null;
			}
			try {
				return guarded(this.exchange.onRequestBody());
			}
			catch (RuntimeException e) {
				abandon();
				this.link.failed("head", e);
				return null;
			}
		}

		@Override
		public @Nullable LocalResponse localResponse() {
			return this.over ? null : this.exchange.localResponse();
		}

		@Override
		public @Nullable BodyHandler onResponse(int status, Headers headers, boolean bodiless) {
			if (this.over) {
				return null;
			}
			this.over = true;
			try {
				return guarded(this.exchange.onResponse(status, headers, bodiless));
			}
			catch (RuntimeException e) {
				this.link.failed("head", e);
				return null;
			}
		}

		@Override
		public void abandon() {
			if (this.over) {
				return;
			}
			this.over = true;
			try {
				this.exchange.abandon();
			}
			catch (RuntimeException e) {
				this.link.failures().count("close", e);
			}
		}

		private @Nullable BodyHandler guarded(@Nullable BodyHandler handler) {
			return switch (handler) {
				case null -> null;
				case BodyObserver observer -> new GuardedObserver(this.link, observer);
				case BodyRewriter rewriter -> new GuardedRewriter(this.link, rewriter);
			};
		}

	}

	/** Left out once failed, or fails the message when it fails closed. */
	private static final class GuardedObserver implements BodyObserver {

		private final Link link;

		private final BodyObserver observer;

		private boolean failed;

		GuardedObserver(Link link, BodyObserver observer) {
			this.link = link;
			this.observer = observer;
		}

		@Override
		public void onData(byte[] bytes, int offset, int length) {
			if (!this.failed) {
				try {
					this.observer.onData(bytes, offset, length);
				}
				catch (RuntimeException e) {
					this.failed = true;
					this.link.failed("body", e);
				}
			}
		}

		@Override
		public void onEnd(boolean complete) {
			if (!this.failed) {
				try {
					this.observer.onEnd(complete);
				}
				catch (RuntimeException e) {
					this.failed = true;
					this.link.failed("body", e);
				}
			}
		}

	}

	/** Its failure fails the message whatever the policy: counted on the way. */
	private static final class GuardedRewriter implements BodyRewriter {

		private final Link link;

		private final BodyRewriter rewriter;

		GuardedRewriter(Link link, BodyRewriter rewriter) {
			this.link = link;
			this.rewriter = rewriter;
		}

		@Override
		public byte[] onData(byte[] bytes, int offset, int length) {
			try {
				return this.rewriter.onData(bytes, offset, length);
			}
			catch (RuntimeException e) {
				this.link.failures().count("body", e);
				throw e;
			}
		}

		@Override
		public byte[] onEnd() {
			try {
				return this.rewriter.onEnd();
			}
			catch (RuntimeException e) {
				this.link.failures().count("body", e);
				throw e;
			}
		}

		@Override
		public void onClose() {
			try {
				this.rewriter.onClose();
			}
			catch (RuntimeException e) {
				this.link.failures().count("close", e);
			}
		}

		@Override
		public boolean unchanged() {
			return this.rewriter.unchanged();
		}

	}

	/** The parts of several filters in one exchange. */
	private static final class Chained implements Exchange {

		private final List<Exchange> exchanges;

		Chained(List<Exchange> exchanges) {
			this.exchanges = exchanges;
		}

		@Override
		public @Nullable BodyHandler onRequestBody() {
			BodyHandler handler = null;
			try {
				for (Exchange exchange : this.exchanges) {
					handler = then(handler, exchange.onRequestBody());
				}
				return handler;
			}
			catch (RuntimeException e) {
				cut(handler);
				throw e;
			}
		}

		@Override
		public @Nullable LocalResponse localResponse() {
			for (Exchange exchange : this.exchanges) {
				LocalResponse local = exchange.localResponse();
				if (local != null) {
					return local;
				}
			}
			return null;
		}

		@Override
		public @Nullable BodyHandler onResponse(int status, Headers headers, boolean bodiless) {
			BodyHandler handler = null;
			try {
				for (Exchange exchange : this.exchanges.reversed()) {
					BodyHandler next = exchange.onResponse(status, headers, bodiless);
					if (bodiless) {
						endBodiless(next);
					}
					else {
						handler = then(handler, next);
					}
				}
				return handler;
			}
			catch (RuntimeException e) {
				cut(handler);
				throw e;
			}
		}

		private static void endBodiless(@Nullable BodyHandler handler) {
			if (handler instanceof BodyObserver observer) {
				observer.onEnd(true);
			}
			else if (handler instanceof BodyRewriter rewriter) {
				rewriter.onClose();
			}
		}

		/** A later filter failed closed: what the others took on ends here. */
		private static void cut(@Nullable BodyHandler handler) {
			if (handler instanceof BodyObserver observer) {
				observer.onEnd(false);
			}
			else if (handler instanceof BodyRewriter rewriter) {
				rewriter.onClose();
			}
		}

		@Override
		public void abandon() {
			abandonAll(this.exchanges);
		}

	}

	/** A body goes through {@code first}, what it left through {@code second}. */
	private static @Nullable BodyHandler then(@Nullable BodyHandler first, @Nullable BodyHandler second) {
		if (first == null || second == null) {
			return first == null ? second : first;
		}
		if (first instanceof BodyObserver one && second instanceof BodyObserver other) {
			return new BodyObserver() {

				@Override
				public void onData(byte[] bytes, int offset, int length) {
					one.onData(bytes, offset, length);
					other.onData(bytes, offset, length);
				}

				@Override
				public void onEnd(boolean complete) {
					one.onEnd(complete);
					other.onEnd(complete);
				}

			};
		}
		return new Piped(asRewriter(first), asRewriter(second));
	}

	private static BodyRewriter asRewriter(BodyHandler handler) {
		return switch (handler) {
			case BodyRewriter rewriter -> rewriter;
			case BodyObserver observer -> new ObservingRewriter(observer);
		};
	}

	/** An observer among rewriters: what comes in goes on. */
	private static final class ObservingRewriter implements BodyRewriter {

		private final BodyObserver observer;

		private boolean ended;

		ObservingRewriter(BodyObserver observer) {
			this.observer = observer;
		}

		@Override
		public byte[] onData(byte[] bytes, int offset, int length) {
			this.observer.onData(bytes, offset, length);
			byte[] copy = new byte[length];
			System.arraycopy(bytes, offset, copy, 0, length);
			return copy;
		}

		@Override
		public byte[] onEnd() {
			this.ended = true;
			this.observer.onEnd(true);
			return EMPTY;
		}

		@Override
		public void onClose() {
			if (!this.ended) {
				this.observer.onEnd(false);
			}
		}

		@Override
		public boolean unchanged() {
			return true;
		}

	}

	/** Two rewriters in a row. */
	private static final class Piped implements BodyRewriter {

		private final BodyRewriter first;

		private final BodyRewriter second;

		Piped(BodyRewriter first, BodyRewriter second) {
			this.first = first;
			this.second = second;
		}

		@Override
		public byte[] onData(byte[] bytes, int offset, int length) {
			byte[] part = this.first.onData(bytes, offset, length);
			return part.length == 0 ? EMPTY : this.second.onData(part, 0, part.length);
		}

		@Override
		public byte[] onEnd() {
			byte[] last = this.first.onEnd();
			byte[] part = last.length == 0 ? EMPTY : this.second.onData(last, 0, last.length);
			byte[] end = this.second.onEnd();
			byte[] whole = new byte[part.length + end.length];
			System.arraycopy(part, 0, whole, 0, part.length);
			System.arraycopy(end, 0, whole, part.length, end.length);
			return whole;
		}

		@Override
		public void onClose() {
			try {
				this.first.onClose();
			}
			finally {
				this.second.onClose();
			}
		}

		@Override
		public boolean unchanged() {
			return this.first.unchanged() && this.second.unchanged();
		}

	}

}
