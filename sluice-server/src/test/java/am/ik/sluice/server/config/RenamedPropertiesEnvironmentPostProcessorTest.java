package am.ik.sluice.server.config;

import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import org.springframework.boot.EnvironmentPostProcessor;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.logging.DeferredLogFactory;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.core.env.SystemEnvironmentPropertySource;
import org.springframework.core.io.support.SpringFactoriesLoader;
import org.springframework.core.io.support.SpringFactoriesLoader.ArgumentResolver;
import org.springframework.core.io.support.SpringFactoriesLoader.FailureHandler;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(OutputCaptureExtension.class)
class RenamedPropertiesEnvironmentPostProcessorTest {

	private static final DeferredLogFactory IMMEDIATE = Supplier::get;

	/**
	 * The former name, as a property or as an environment variable in the binder's
	 * relaxed form, sets the current one and is reported as deprecated.
	 */
	@ParameterizedTest
	@ValueSource(strings = { "sluice.proxy-protocol", "SLUICE_PROXYPROTOCOL" })
	void theFormerNameAloneSetsTheCurrentOne(String name, CapturedOutput output) {
		SluiceServerProperties properties = postProcessed(Map.of(name, "true"));
		assertThat(properties.dataProxyProtocol()).isTrue();
		assertThat(output).contains("sluice.proxy-protocol is deprecated: use sluice.data-proxy-protocol");
	}

	@Test
	void theCurrentNameWinsOverTheFormerOne(CapturedOutput output) {
		SluiceServerProperties properties = postProcessed(
				Map.of("sluice.proxy-protocol", "true", "sluice.data-proxy-protocol", "false"));
		assertThat(properties.dataProxyProtocol()).isFalse();
		assertThat(output)
			.contains("sluice.proxy-protocol is deprecated and ignored: sluice.data-proxy-protocol is set");
	}

	@Test
	void isRegisteredAsAnEnvironmentPostProcessor() {
		List<EnvironmentPostProcessor> processors = SpringFactoriesLoader.forDefaultResourceLocation()
			.load(EnvironmentPostProcessor.class, ArgumentResolver.of(DeferredLogFactory.class, IMMEDIATE),
					FailureHandler.handleMessage((message, failure) -> {
					}));
		assertThat(processors).hasAtLeastOneElementOfType(RenamedPropertiesEnvironmentPostProcessor.class);
	}

	/**
	 * Binds the server properties from an environment holding the given properties, the
	 * upper case ones as environment variables, after the post-processor ran.
	 */
	private static SluiceServerProperties postProcessed(Map<String, Object> properties) {
		StandardEnvironment environment = new StandardEnvironment();
		environment.getPropertySources().remove(StandardEnvironment.SYSTEM_ENVIRONMENT_PROPERTY_SOURCE_NAME);
		environment.getPropertySources().remove(StandardEnvironment.SYSTEM_PROPERTIES_PROPERTY_SOURCE_NAME);
		properties.forEach((name, value) -> environment.getPropertySources()
			.addLast(name.equals(name.toUpperCase())
					// the binder maps relaxed names onto the source of this name only
					? new SystemEnvironmentPropertySource(StandardEnvironment.SYSTEM_ENVIRONMENT_PROPERTY_SOURCE_NAME,
							Map.of(name, value))
					: new MapPropertySource(name, Map.of(name, value))));
		new RenamedPropertiesEnvironmentPostProcessor(IMMEDIATE).postProcessEnvironment(environment,
				new SpringApplication());
		return Binder.get(environment).bind("sluice", Bindable.of(SluiceServerProperties.class)).get();
	}

}
