package am.ik.sluice.server.proxy;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Skips the test on the legacy engine of an {@link H2EngineTest} class: it pins the netty
 * engine's per-hop behaviour, which the legacy relay does not have.
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface NettyOnly {

	/** Why: the netty behaviour the test pins. */
	String value();

}
