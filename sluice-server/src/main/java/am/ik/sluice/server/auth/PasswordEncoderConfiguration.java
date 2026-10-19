package am.ik.sluice.server.auth;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.crypto.password.PasswordEncoder;

/**
 * The one password encoder of the server ({@link PasswordEncoders#delegating()}): the
 * console's form login matches its user's password with it, the data plane its route
 * credentials.
 */
@Configuration(proxyBeanMethods = false)
class PasswordEncoderConfiguration {

	@Bean
	PasswordEncoder passwordEncoder() {
		return PasswordEncoders.delegating();
	}

}
