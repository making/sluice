package am.ik.sluice.server.proxy;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.EnumSource;

import am.ik.sluice.server.config.SluiceServerProperties.H2DemuxEngine;

/**
 * Runs the {@code @SpringBootTest} class once per h2 demux engine, both on the class's
 * one application context: before each test the data proxy is switched to the engine
 * ({@link H2Engines}), and after it no h2 connection may have gone to the other engine. A
 * test or class the netty engine does not support yet carries {@link LegacyOnly}.
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@ParameterizedClass(name = "[{index}] engine={0}")
@EnumSource(H2DemuxEngine.class)
@ExtendWith(H2Engines.class)
public @interface H2EngineTest {

}
