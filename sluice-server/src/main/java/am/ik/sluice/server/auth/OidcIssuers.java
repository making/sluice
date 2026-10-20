package am.ik.sluice.server.auth;

import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.LongSupplier;

import org.jspecify.annotations.Nullable;

import org.springframework.boot.http.client.ClientHttpRequestFactoryBuilder;
import org.springframework.boot.http.client.HttpClientSettings;
import org.springframework.web.client.RestClient;

/**
 * The OIDC issuers the routes accept tokens of, one {@link IssuerKeys} each however many
 * routes name it, sharing a {@link RestClient} made with the first issuer from the
 * server's builder: its connect and read timeouts are the fetch timeout. The client and
 * every stage of the loads run on the executor (the server's virtual thread
 * {@code applicationTaskExecutor}).
 */
final class OidcIssuers implements AutoCloseable {

	private final Timing timing;

	private final Clock clock;

	private final LongSupplier nanos;

	private final Executor executor;

	private final RestClient.Builder restClientBuilder;

	private final Map<String, IssuerKeys> issuers = new TreeMap<>();

	private @Nullable RestClient http;

	/**
	 * @param clock the wall clock the tokens' times are judged by
	 * @param nanos the time in nanoseconds, for the bound on the loads
	 * @param executor what the HTTP client and the loads run on
	 * @param restClientBuilder the builder of the client fetching the documents
	 */
	OidcIssuers(Timing timing, Clock clock, LongSupplier nanos, Executor executor,
			RestClient.Builder restClientBuilder) {
		this.timing = timing;
		this.clock = clock;
		this.nanos = nanos;
		this.executor = executor;
		this.restClientBuilder = restClientBuilder;
	}

	Timing timing() {
		return this.timing;
	}

	Clock clock() {
		return this.clock;
	}

	/** The keys of the issuer, not loading until {@link #start}. */
	synchronized IssuerKeys keys(String issuer) {
		IssuerKeys keys = this.issuers.get(issuer);
		if (keys == null) {
			RestClient client = this.http;
			if (client == null) {
				client = this.restClientBuilder
					.requestFactory(ClientHttpRequestFactoryBuilder.jdk()
						.withExecutor(this.executor)
						.build(HttpClientSettings.defaults()
							.withTimeouts(this.timing.fetchTimeout(), this.timing.fetchTimeout())))
					.build();
				this.http = client;
			}
			keys = new IssuerKeys(issuer, client, this.executor, this.timing, this.nanos);
			this.issuers.put(issuer, keys);
		}
		return keys;
	}

	/**
	 * Loads every issuer's keys, waiting for the loads to end -- each fetches two
	 * documents, so about twice the fetch timeout at most. An issuer whose keys did not
	 * load goes on trying in the background.
	 */
	void start() {
		CompletableFuture<?>[] loads;
		synchronized (this) {
			loads = this.issuers.values().stream().map(IssuerKeys::refresh).toArray(CompletableFuture[]::new);
		}
		try {
			CompletableFuture.allOf(loads)
				.get(this.timing.fetchTimeout().multipliedBy(2).plusSeconds(1).toNanos(), TimeUnit.NANOSECONDS);
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
		catch (ExecutionException | TimeoutException e) {
			// the loads never complete exceptionally; one past the deadline logs itself
		}
	}

	@Override
	public synchronized void close() {
		this.issuers.values().forEach(IssuerKeys::close);
	}

	/**
	 * How the keys are loaded.
	 *
	 * @param fetchTimeout the deadline of each document's fetch, from the request to the
	 * last byte of its body; also the connect timeout
	 * @param refreshInterval how long after a load the keys load anew
	 * @param minRefreshInterval the least time between two loads, as for a token naming a
	 * key not loaded; also how long after a failed load the next one runs
	 */
	record Timing(Duration fetchTimeout, Duration refreshInterval, Duration minRefreshInterval) {

		static final Timing DEFAULT = new Timing(Duration.ofSeconds(5), Duration.ofMinutes(5), Duration.ofSeconds(10));

	}

}
