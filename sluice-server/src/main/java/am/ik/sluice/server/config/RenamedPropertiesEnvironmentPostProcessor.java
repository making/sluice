package am.ik.sluice.server.config;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.logging.Log;

import org.springframework.boot.EnvironmentPostProcessor;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.logging.DeferredLogFactory;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MapPropertySource;

/**
 * Reads deprecated properties under their current names. A former name that is set while
 * its current name is not is carried to the current name (at the lowest precedence) with
 * a deprecation warning; set under both names, the current one wins and the former one is
 * reported as ignored. Names are looked up as the binder binds them, so an environment
 * variable in any relaxed form ({@code SLUICE_PROXYPROTOCOL}) counts.
 */
public class RenamedPropertiesEnvironmentPostProcessor implements EnvironmentPostProcessor {

	/** Former name to current name. */
	static final Map<String, String> RENAMED = Map.of("sluice.proxy-protocol", "sluice.data-proxy-protocol");

	static final String PROPERTY_SOURCE_NAME = "sluiceRenamedProperties";

	private final Log log;

	public RenamedPropertiesEnvironmentPostProcessor(DeferredLogFactory logFactory) {
		this.log = logFactory.getLog(RenamedPropertiesEnvironmentPostProcessor.class);
	}

	@Override
	public void postProcessEnvironment(ConfigurableEnvironment environment, SpringApplication application) {
		Binder binder = Binder.get(environment);
		Map<String, Object> carried = new LinkedHashMap<>();
		RENAMED.forEach((former, current) -> binder.bind(former, String.class).ifBound(value -> {
			if (binder.bind(current, String.class).isBound()) {
				this.log.warn("%s is deprecated and ignored: %s is set".formatted(former, current));
			}
			else {
				this.log.warn("%s is deprecated: use %s".formatted(former, current));
				carried.put(current, value);
			}
		}));
		if (!carried.isEmpty()) {
			environment.getPropertySources().addLast(new MapPropertySource(PROPERTY_SOURCE_NAME, carried));
		}
	}

}
