package am.ik.sluice.server.console.auth;

import java.util.List;
import java.util.Map;

import am.ik.sluice.server.config.SluiceServerProperties;
import jakarta.servlet.http.HttpServletRequest;

import org.jspecify.annotations.Nullable;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.security.autoconfigure.SecurityProperties;
import org.springframework.boot.security.autoconfigure.actuate.web.servlet.EndpointRequest;
import org.springframework.boot.security.oauth2.client.autoconfigure.OAuth2ClientProperties;
import org.springframework.boot.security.oauth2.client.autoconfigure.OAuth2ClientPropertiesMapper;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.oauth2.client.oidc.web.logout.OidcClientInitiatedLogoutSuccessHandler;
import org.springframework.security.oauth2.client.registration.ClientRegistration;
import org.springframework.security.oauth2.client.registration.ClientRegistrationRepository;
import org.springframework.security.oauth2.client.registration.InMemoryClientRegistrationRepository;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.DelegatingPasswordEncoder;
import org.springframework.security.crypto.password.NoOpPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.provisioning.InMemoryUserDetailsManager;
import org.springframework.security.web.AuthenticationEntryPoint;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.authentication.LoginUrlAuthenticationEntryPoint;
import org.springframework.security.web.authentication.logout.LogoutSuccessHandler;
import org.springframework.security.web.util.matcher.RequestHeaderRequestMatcher;
import org.springframework.security.web.util.matcher.RequestMatcher;

/**
 * Matches any request; used to order the fallback authentication entry point last.
 */
enum AnyRequest implements RequestMatcher {

	INSTANCE;

	@Override
	public boolean matches(HttpServletRequest request) {
		return true;
	}

}

/**
 * Authentication for the management console: the console itself always requires a
 * signed-in user, while the actuator endpoints and the console static assets stay open.
 * The mechanism is selected by {@code sluice.console.auth.type}: {@code simple} form
 * login against {@code spring.security.user.*} or {@code oidc} login against
 * {@code spring.security.oauth2.client.*}.
 */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(OAuth2ClientProperties.class)
class ConsoleSecurityConfiguration {

	private static final String LOGGED_OUT_URL = "/login?logout";

	@Bean
	SecurityFilterChain consoleSecurityFilterChain(HttpSecurity http, SluiceServerProperties properties,
			ClientRegistrationRepository clientRegistrationRepository) throws Exception {
		// @formatter:off
		http
			.authorizeHttpRequests(authz -> authz
				.requestMatchers(EndpointRequest.toAnyEndpoint()).permitAll()
				.requestMatchers("/login", "/error", "/console/css/**", "/console/js/**", "/console/fonts/**",
						"/console/img/**", "/favicon.svg", "/favicon.ico")
					.permitAll()
				.anyRequest().authenticated())
			// htmx polls swap the response into the page; a login redirect would render
			// the login document as a fragment, so the browser is redirected wholesale.
			// Any other request takes the regular login redirect.
			.exceptionHandling(exception -> exception
				.defaultAuthenticationEntryPointFor(htmxAuthenticationEntryPoint(),
						new RequestHeaderRequestMatcher("HX-Request", "true"))
				.defaultAuthenticationEntryPointFor(new LoginUrlAuthenticationEntryPoint("/login"), AnyRequest.INSTANCE))
			.logout(logout -> logout.logoutUrl("/logout").logoutSuccessUrl(LOGGED_OUT_URL).deleteCookies("JSESSIONID"));
		// @formatter:on
		switch (properties.console().auth().type()) {
			case SIMPLE -> http.formLogin(form -> form.loginPage("/login").defaultSuccessUrl("/console", true));
			case OIDC -> http.oauth2Login(oauth2 -> oauth2.loginPage("/login").defaultSuccessUrl("/console", true))
				.logout(logout -> logout.logoutSuccessHandler(oidcLogoutSuccessHandler(clientRegistrationRepository)));
		}
		return http.build();
	}

	/**
	 * RP-initiated logout: signing out of the console also ends the session at the
	 * provider, which then redirects back to the login page. Otherwise the next "Sign in
	 * with" would pass straight through the provider. Providers without an end session
	 * endpoint (e.g. Google) end only the console session.
	 */
	private static LogoutSuccessHandler oidcLogoutSuccessHandler(
			ClientRegistrationRepository clientRegistrationRepository) {
		OidcClientInitiatedLogoutSuccessHandler handler = new OidcClientInitiatedLogoutSuccessHandler(
				clientRegistrationRepository);
		handler.setPostLogoutRedirectUri("{baseUrl}" + LOGGED_OUT_URL);
		handler.setDefaultTargetUrl(LOGGED_OUT_URL);
		return handler;
	}

	/**
	 * Authentication entry point for htmx requests: an {@code HX-Redirect} to the login
	 * page instead of a document body the poll would swap into the live regions.
	 */
	private static AuthenticationEntryPoint htmxAuthenticationEntryPoint() {
		return (request, response, authException) -> {
			response.setStatus(401);
			response.setHeader("HX-Redirect", "/login");
		};
	}

	/**
	 * The user store behind form login. Only registered for the {@code simple} mechanism
	 * so that an OIDC-only deployment does not need {@code spring.security.user.*}.
	 */
	@Bean
	@ConditionalOnProperty(name = "sluice.console.auth.type", havingValue = "simple", matchIfMissing = true)
	UserDetailsService consoleUserDetailsService(SecurityProperties properties) {
		SecurityProperties.User user = properties.getUser();
		UserDetails userDetails = User.withUsername(user.getName()).password(user.getPassword()).build();
		return new InMemoryUserDetailsManager(userDetails);
	}

	/**
	 * Accepts the {@code {noop}} / {@code {bcrypt}} prefixed passwords that
	 * {@code spring.security.user.password} documents, plus bare (noop) passwords.
	 */
	@Bean
	PasswordEncoder passwordEncoder() {
		return new DelegatingPasswordEncoder("bcrypt",
				Map.of("bcrypt", new BCryptPasswordEncoder(), "noop", NoOpPasswordEncoder.getInstance()));
	}

	/**
	 * Client registrations resolved at run time. Boot's own auto-configuration is pruned
	 * from the native image when no registrations exist while building (AOT evaluates its
	 * bean conditions at build time), which would make the {@code oidc} mechanism
	 * unconfigurable at run time; this unconditional bean keeps the decision in the bean
	 * body instead and stays empty until
	 * {@code spring.security.oauth2.client.registration.*} is set.
	 */
	@Bean
	ClientRegistrationRepository clientRegistrationRepository(ObjectProvider<OAuth2ClientProperties> properties) {
		OAuth2ClientProperties oAuth2ClientProperties = properties.getIfAvailable();
		Map<String, ClientRegistration> registrations = oAuth2ClientProperties == null ? Map.of()
				: new OAuth2ClientPropertiesMapper(oAuth2ClientProperties).asClientRegistrations();
		return registrations.isEmpty() ? new UnconfiguredClientRegistrationRepository()
				: new InMemoryClientRegistrationRepository(List.copyOf(registrations.values()));
	}

	/** Stand-in repository for deployments without configured OIDC providers. */
	private static final class UnconfiguredClientRegistrationRepository implements ClientRegistrationRepository {

		@Override
		public @org.jspecify.annotations.Nullable ClientRegistration findByRegistrationId(String registrationId) {
			return null;
		}

	}

}
