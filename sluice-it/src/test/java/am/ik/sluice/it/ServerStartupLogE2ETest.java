package am.ik.sluice.it;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import am.ik.sluice.server.SluiceServerApplication;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The server starts without annotation introspection warnings: auto-configurations that
 * reference classes missing from the classpath stay out of the context.
 */
@SpringBootTest(classes = SluiceServerApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ExtendWith(OutputCaptureExtension.class)
class ServerStartupLogE2ETest {

	@DynamicPropertySource
	static void properties(DynamicPropertyRegistry registry) {
		registry.add("spring.grpc.server.port", () -> String.valueOf(TestPorts.freePort()));
		registry.add("sluice.data-port", () -> String.valueOf(TestPorts.freePort()));
		registry.add("sluice.token", () -> "it-token");
	}

	@Test
	void startsWithoutIntrospectionFailures(CapturedOutput output) {
		assertThat(output.getAll()).contains("Started ServerStartupLogE2ETest").doesNotContain("Failed to introspect");
	}

}
