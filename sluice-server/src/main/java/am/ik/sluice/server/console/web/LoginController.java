package am.ik.sluice.server.console.web;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import am.ik.sluice.server.config.SluiceServerProperties;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpSession;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.security.oauth2.client.autoconfigure.OAuth2ClientProperties;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.web.WebAttributes;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;

/**
 * Renders the console login page: the username / password form for the {@code simple}
 * mechanism, or one sign-in button per configured OAuth2 client for {@code oidc}.
 */
@Controller
class LoginController {

	private final SluiceServerProperties properties;

	private final ObjectProvider<OAuth2ClientProperties> oAuth2ClientProperties;

	LoginController(SluiceServerProperties properties, ObjectProvider<OAuth2ClientProperties> oAuth2ClientProperties) {
		this.properties = properties;
		this.oAuth2ClientProperties = oAuth2ClientProperties;
	}

	@GetMapping("/login")
	String login(@RequestParam(name = "logout", required = false) String logout,
			@RequestParam(name = "error", required = false) String error, HttpServletRequest request, Model model) {
		boolean simple = this.properties.console().auth().type() == SluiceServerProperties.Console.AuthType.SIMPLE;
		model.addAttribute("simple", simple);
		if (logout != null) {
			model.addAttribute("logout", true);
		}
		if (error != null) {
			model.addAttribute("error", lastExceptionMessage(request));
		}
		if (!simple) {
			model.addAttribute("oidcClients", oidcClients());
		}
		return "console/login";
	}

	/**
	 * The reason for the last failed sign-in. Rejected credentials get a fixed message:
	 * Spring Security localizes its own by the request locale, while the console is in
	 * English.
	 */
	private String lastExceptionMessage(HttpServletRequest request) {
		HttpSession session = request.getSession(false);
		Object exception = (session != null) ? session.getAttribute(WebAttributes.AUTHENTICATION_EXCEPTION) : null;
		if (exception instanceof BadCredentialsException) {
			return "Incorrect username or password.";
		}
		if (exception instanceof Throwable throwable && throwable.getMessage() != null) {
			return throwable.getMessage();
		}
		return "Sign in failed.";
	}

	private List<Map<String, String>> oidcClients() {
		OAuth2ClientProperties properties = this.oAuth2ClientProperties.getIfAvailable();
		if (properties == null) {
			return List.of();
		}
		return properties.getRegistration()
			.entrySet()
			.stream()
			.map(entry -> Map.of("provider", entry.getKey(), "name",
					Objects.requireNonNullElseGet(entry.getValue().getClientName(), entry::getKey)))
			// the registrations are a hash map, so their configured order is lost
			.sorted(Comparator.comparing(client -> client.get("name"), String.CASE_INSENSITIVE_ORDER))
			.toList();
	}

}
