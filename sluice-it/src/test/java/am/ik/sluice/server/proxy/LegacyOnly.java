package am.ik.sluice.server.proxy;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Skips the test, or every test of the class, on the netty engine of an
 * {@link H2EngineTest} class: the netty engine does not have the behaviour it pins yet,
 * or the test does not reach the h2 demux at all.
 */
@Target({ ElementType.TYPE, ElementType.METHOD })
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface LegacyOnly {

	/**
	 * Why: the behaviour the netty engine does not have yet, or why the engine does not
	 * matter.
	 */
	String value();

}
