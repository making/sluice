package am.ik.sluice.server.console.web;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import am.ik.sluice.server.config.SluiceServerProperties;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpSession;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.security.oauth2.client.autoconfigure.OAuth2ClientProperties;
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

	private String lastExceptionMessage(HttpServletRequest request) {
		HttpSession session = request.getSession(false);
		if (session == null) {
			return "Sign in failed";
		}
		Throwable exception = (Throwable) session.getAttribute("SPRING_SECURITY_LAST_EXCEPTION");
		return exception == null ? "Sign in failed" : Objects.requireNonNullElse(exception.getMessage(), "");
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
			.toList();
	}

}
