package am.ik.sluice.server.auth;

import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

import com.nimbusds.jose.jwk.JWK;
import com.nimbusds.jose.jwk.JWKSelector;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.jwk.source.JWKSource;
import com.nimbusds.jose.proc.SecurityContext;
import com.nimbusds.jose.util.JSONObjectUtils;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.client.RestClient;

/**
 * The signing keys of one OIDC issuer, held in memory so a token check selects among them
 * without I/O: the JWKS named by the issuer's discovery document
 * ({@code <issuer>/.well-known/openid-configuration}, whose {@code issuer} must equal
 * it), with the endpoints the document names for the browser sign-in
 * ({@link #endpoints()}). A load runs off the caller ({@link #refresh}), one at a time,
 * at most one per {@link OidcIssuers.Timing#minRefreshInterval} unless one is due: every
 * {@link OidcIssuers.Timing#refreshInterval} after a load, every
 * {@code minRefreshInterval} after a failed one. A failed load keeps the keys loaded
 * before. Each document is fetched by the {@link RestClient} within
 * {@link OidcIssuers.Timing#fetchTimeout} and {@link #MAX_DOCUMENT} bytes. Every stage of
 * a load, and a load due, runs on the executor.
 */
final class IssuerKeys implements JWKSource<SecurityContext> {

	private static final Logger log = LoggerFactory.getLogger(IssuerKeys.class);

	/** The largest discovery document or JWKS read. */
	static final int MAX_DOCUMENT = 512 * 1024;

	private final String issuer;

	private final URI discovery;

	private final RestClient http;

	private final Executor executor;

	private final OidcIssuers.Timing timing;

	private final LongSupplier clock;

	private volatile @Nullable JWKSet keys;

	/** The JWKS location once discovered; forgotten when a load fails. */
	private volatile @Nullable URI jwksUri;

	/** The endpoints of the last discovery document read; kept when a load fails. */
	private volatile @Nullable Endpoints endpoints;

	/** The load in flight. With this lock, as all below. */
	private @Nullable CompletableFuture<Void> loading;

	private boolean loadedOnce;

	private long lastLoad;

	private boolean failing;

	private boolean closed;

	/** Counts the loads begun: a load due runs only if none began since it was set. */
	private long generation;

	/**
	 * @param http fetches the documents, bounding each to the fetch timeout
	 * @param executor what every stage of a load runs on
	 * @param clock the time in nanoseconds, for the bound on the loads
	 */
	IssuerKeys(String issuer, RestClient http, Executor executor, OidcIssuers.Timing timing, LongSupplier clock) {
		this.issuer = issuer;
		this.discovery = URI.create((issuer.endsWith("/") ? issuer.substring(0, issuer.length() - 1) : issuer)
				+ "/.well-known/openid-configuration");
		this.http = http;
		this.executor = executor;
		this.timing = timing;
		this.clock = clock;
	}

	String issuer() {
		return this.issuer;
	}

	@Override
	public List<JWK> get(JWKSelector selector, @Nullable SecurityContext context) {
		JWKSet loaded = this.keys;
		return loaded == null ? List.of() : selector.select(loaded);
	}

	/**
	 * The endpoints the discovery document names; null until one was read.
	 */
	@Nullable Endpoints endpoints() {
		return this.endpoints;
	}

	/** Whether keys have loaded. */
	boolean loaded() {
		return this.keys != null;
	}

	/**
	 * Whether the loaded keys hold one of the id; any loaded key for a token naming none.
	 */
	boolean knows(@Nullable String keyId) {
		JWKSet loaded = this.keys;
		return loaded != null && (keyId == null || loaded.getKeyByKeyId(keyId) != null);
	}

	/** Whether {@link #refresh} would load the keys anew: one in flight or allowed. */
	synchronized boolean mayRefresh() {
		return !this.closed && (this.loading != null || !this.loadedOnce
				|| this.clock.getAsLong() - this.lastLoad >= this.timing.minRefreshInterval().toNanos());
	}

	/**
	 * Loads the keys anew off the caller: the load in flight, else a new one unless the
	 * last began within {@link OidcIssuers.Timing#minRefreshInterval}. Completes, never
	 * exceptionally, once the load ended -- at once when none runs.
	 */
	CompletableFuture<Void> refresh() {
		return this.begin(false);
	}

	/**
	 * The load in flight, else a new one when it is due or allowed; completed at once
	 * when none runs.
	 */
	private CompletableFuture<Void> begin(boolean due) {
		CompletableFuture<Void> landed;
		synchronized (this) {
			if (this.loading != null) {
				return this.loading;
			}
			if (this.closed || !due && !mayRefresh()) {
				return CompletableFuture.completedFuture(null);
			}
			landed = new CompletableFuture<>();
			this.loading = landed;
			this.loadedOnce = true;
			this.lastLoad = this.clock.getAsLong();
			this.generation++;
		}
		CompletableFuture<JWKSet> load;
		try {
			load = this.load();
		}
		catch (RuntimeException e) {
			load = CompletableFuture.failedFuture(e);
		}
		load.whenCompleteAsync((loaded, failure) -> {
			try {
				this.landed(loaded, failure);
			}
			finally {
				landed.complete(null);
			}
		}, this.executor);
		return landed;
	}

	/** Stops the loads due; one in flight runs to its end. */
	synchronized void close() {
		this.closed = true;
	}

	private CompletableFuture<JWKSet> load() {
		URI known = this.jwksUri;
		CompletableFuture<URI> located = known != null ? CompletableFuture.completedFuture(known)
				: this.fetch(this.discovery).thenApplyAsync(this::jwksUriOf, this.executor);
		return located.thenComposeAsync(this::fetch, this.executor).thenApplyAsync(IssuerKeys::keysOf, this.executor);
	}

	private void landed(@Nullable JWKSet loaded, @Nullable Throwable failure) {
		boolean first = this.keys == null;
		synchronized (this) {
			this.loading = null;
			if (loaded != null) {
				this.keys = loaded;
			}
			else {
				this.jwksUri = null;
			}
			if (!this.closed) {
				this.schedule(loaded != null ? this.timing.refreshInterval() : this.timing.minRefreshInterval());
			}
			boolean wasFailing = this.failing;
			this.failing = loaded == null;
			if (loaded != null) {
				if (first || wasFailing) {
					log.info("oidc issuer {}: {} keys loaded", this.issuer, loaded.getKeys().size());
				}
				return;
			}
			String cause = failure == null ? "unknown" : rootOf(failure).toString();
			if (wasFailing) {
				log.debug("oidc issuer {}: the keys did not load again: {}", this.issuer, cause);
			}
			else if (first) {
				log.warn("oidc issuer {}: the keys did not load, its tokens are answered 503 until they do: {}",
						this.issuer, cause);
			}
			else {
				log.warn("oidc issuer {}: the keys did not refresh, the ones loaded are kept: {}", this.issuer, cause);
			}
		}
	}

	/**
	 * Sets the next load due after the delay, on the executor; it runs only if no load
	 * began meanwhile and the keys are not closed. With this lock.
	 */
	private void schedule(Duration delay) {
		long setAt = this.generation;
		Executor due = CompletableFuture.delayedExecutor(delay.toNanos(), TimeUnit.NANOSECONDS, this.executor);
		due.execute(() -> {
			synchronized (this) {
				if (this.closed || this.generation != setAt) {
					return;
				}
			}
			this.begin(true);
		});
	}

	/**
	 * The body of a {@code 200} answer to a GET of the URI, within the fetch timeout and
	 * {@link #MAX_DOCUMENT} bytes, fetched on the executor. The client's read timeout
	 * ends the exchange at the same deadline, a body still trickling in among it.
	 */
	private CompletableFuture<String> fetch(URI uri) {
		return CompletableFuture.supplyAsync(
				() -> this.http.get().uri(uri).accept(MediaType.APPLICATION_JSON).exchange((request, response) -> {
					if (!response.getStatusCode().isSameCodeAs(HttpStatus.OK)) {
						throw new IllegalStateException(uri + " answered " + response.getStatusCode().value());
					}
					try (InputStream body = response.getBody()) {
						byte[] read = body.readNBytes(MAX_DOCUMENT + 1);
						if (read.length > MAX_DOCUMENT) {
							throw new IllegalStateException(uri + " answered more than " + MAX_DOCUMENT + " bytes");
						}
						return new String(read, StandardCharsets.UTF_8);
					}
				}), this.executor)
			.orTimeout(this.timing.fetchTimeout().toNanos(), TimeUnit.NANOSECONDS);
	}

	private URI jwksUriOf(String document) {
		Map<String, Object> metadata;
		try {
			metadata = JSONObjectUtils.parse(document);
		}
		catch (ParseException e) {
			throw new IllegalStateException(this.discovery + " is no JSON object: " + e.getMessage(), e);
		}
		Object issuer = metadata.get("issuer");
		if (!this.issuer.equals(issuer)) {
			throw new IllegalStateException(this.discovery + " names the issuer " + issuer);
		}
		URI uri;
		try {
			uri = JSONObjectUtils.getURI(metadata, "jwks_uri");
		}
		catch (ParseException e) {
			throw new IllegalStateException(this.discovery + " holds a malformed jwks_uri", e);
		}
		if (uri == null) {
			throw new IllegalStateException(this.discovery + " names no jwks_uri");
		}
		try {
			List<String> methods = JSONObjectUtils.getStringList(metadata, "token_endpoint_auth_methods_supported");
			this.endpoints = new Endpoints(JSONObjectUtils.getURI(metadata, "authorization_endpoint"),
					JSONObjectUtils.getURI(metadata, "token_endpoint"),
					JSONObjectUtils.getURI(metadata, "end_session_endpoint"), methods == null ? List.of() : methods);
		}
		catch (ParseException e) {
			throw new IllegalStateException(this.discovery + " holds a malformed endpoint: " + e.getMessage(), e);
		}
		this.jwksUri = uri;
		return uri;
	}

	/**
	 * The endpoints of an issuer's discovery document the browser sign-in uses.
	 *
	 * @param authorization the authorization endpoint; null when the document names none
	 * @param token the token endpoint; null when the document names none
	 * @param endSession the end session endpoint (RP-initiated logout); null when the
	 * document names none
	 * @param tokenAuthMethods the client authentication methods the token endpoint
	 * supports; empty when the document names none ({@code client_secret_basic} then)
	 */
	record Endpoints(@Nullable URI authorization, @Nullable URI token, @Nullable URI endSession,
			List<String> tokenAuthMethods) {

		Endpoints {
			tokenAuthMethods = List.copyOf(tokenAuthMethods);
		}

	}

	private static JWKSet keysOf(String document) {
		JWKSet set;
		try {
			set = JWKSet.parse(document);
		}
		catch (ParseException e) {
			throw new IllegalStateException("the JWKS is malformed: " + e.getMessage(), e);
		}
		if (set.getKeys().isEmpty()) {
			throw new IllegalStateException("the JWKS holds no key");
		}
		return set;
	}

	private static Throwable rootOf(Throwable failure) {
		Throwable cause = failure;
		while (cause.getCause() != null && cause.getCause() != cause) {
			cause = cause.getCause();
		}
		return cause;
	}

}
