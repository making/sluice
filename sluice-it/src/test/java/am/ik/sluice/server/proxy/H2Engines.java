package am.ik.sluice.server.proxy;

import java.lang.reflect.AnnotatedElement;
import java.util.Optional;

import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.support.ParameterInfo;
import org.junit.platform.commons.support.AnnotationSupport;

import org.springframework.context.ApplicationContext;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import am.ik.sluice.server.config.SluiceServerProperties;
import am.ik.sluice.server.config.SluiceServerProperties.H2DemuxEngine;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The extension of {@link H2EngineTest}: skips a {@link LegacyOnly} test on the netty
 * engine, switches the data proxy to the invocation's engine before each test and back to
 * the configured one after it, and fails a test that handed an h2 connection to the other
 * engine -- the switch did not take.
 */
public final class H2Engines implements ExecutionCondition, BeforeEachCallback, AfterEachCallback {

	private static final ExtensionContext.Namespace NAMESPACE = ExtensionContext.Namespace.create(H2Engines.class);

	private static final String OTHER_ENGINE_COUNT = "otherEngineCount";

	@Override
	public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
		Optional<H2DemuxEngine> engine = engine(context);
		if (engine.isEmpty() || engine.get() != H2DemuxEngine.NETTY) {
			return ConditionEvaluationResult.enabled("not the netty engine");
		}
		Optional<LegacyOnly> legacyOnly = context.getElement().flatMap(element -> legacyOnly(element, context));
		return legacyOnly.map(annotation -> ConditionEvaluationResult.disabled("netty engine: " + annotation.value()))
			.orElseGet(() -> ConditionEvaluationResult.enabled("the netty engine supports it"));
	}

	@Override
	public void beforeEach(ExtensionContext context) {
		H2DemuxEngine engine = engine(context).orElseThrow(() -> new IllegalStateException("not an @H2EngineTest"));
		DataProxyServer server = server(context);
		server.h2DemuxEngine(engine);
		context.getStore(NAMESPACE).put(OTHER_ENGINE_COUNT, server.h2DemuxMeters().demuxedBy(other(engine)));
	}

	@Override
	public void afterEach(ExtensionContext context) {
		H2DemuxEngine engine = engine(context).orElseThrow(() -> new IllegalStateException("not an @H2EngineTest"));
		DataProxyServer server = server(context);
		ApplicationContext application = SpringExtension.getApplicationContext(context);
		server.h2DemuxEngine(application.getBean(SluiceServerProperties.class).h2DemuxEngine());
		Double before = context.getStore(NAMESPACE).get(OTHER_ENGINE_COUNT, Double.class);
		assertThat(server.h2DemuxMeters().demuxedBy(other(engine)))
			.as("h2 connections the %s engine got during a %s test", other(engine), engine)
			.isEqualTo(before);
	}

	/**
	 * The engine argument of the class invocation: the nearest parameter info may be a
	 * parameterized test method's, so the class's is looked up through the parents.
	 */
	private static Optional<H2DemuxEngine> engine(ExtensionContext context) {
		for (ExtensionContext current = context; current != null; current = current.getParent().orElse(null)) {
			ParameterInfo info = ParameterInfo.get(current);
			if (info == null) {
				return Optional.empty();
			}
			if (info.getDeclarations().getSourceElement() instanceof Class<?>) {
				return Optional.ofNullable(info.getArguments().get(0, H2DemuxEngine.class));
			}
		}
		return Optional.empty();
	}

	private static Optional<LegacyOnly> legacyOnly(AnnotatedElement element, ExtensionContext context) {
		Optional<LegacyOnly> own = AnnotationSupport.findAnnotation(element, LegacyOnly.class);
		if (own.isPresent()) {
			return own;
		}
		return context.getTestClass().flatMap(type -> AnnotationSupport.findAnnotation(type, LegacyOnly.class));
	}

	private static DataProxyServer server(ExtensionContext context) {
		return SpringExtension.getApplicationContext(context).getBean(DataProxyServer.class);
	}

	private static H2DemuxEngine other(H2DemuxEngine engine) {
		return engine == H2DemuxEngine.NETTY ? H2DemuxEngine.LEGACY : H2DemuxEngine.NETTY;
	}

}
