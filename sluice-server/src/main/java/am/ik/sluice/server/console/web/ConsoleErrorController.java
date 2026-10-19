package am.ik.sluice.server.console.web;

import java.util.Map;

import jakarta.servlet.RequestDispatcher;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import org.springframework.boot.webmvc.error.ErrorController;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;

/**
 * The console error page, replacing the whitelabel fallback. It matters most for a stale
 * sign-in: a login posted after a redeploy dropped the session fails CSRF with a bare
 * 403, so the page names the likely cause and offers to sign in again. Requests that do
 * not accept HTML get a minimal JSON body, like Boot's own error controller sends.
 */
@Controller
class ConsoleErrorController implements ErrorController {

	private static final Map<Integer, String> TITLES = Map.of(401, "Not signed in", 403, "Sign-in failed", 404,
			"Page not found", 500, "Something went wrong");

	@RequestMapping("${server.error.path:/error}")
	Object error(HttpServletRequest request, HttpServletResponse response, Model model) {
		int status = status(request);
		String path = String.valueOf(request.getAttribute(RequestDispatcher.ERROR_REQUEST_URI));
		if (!acceptsHtml(request)) {
			response.setStatus(status);
			return ResponseEntity.status(status)
				.contentType(MediaType.APPLICATION_JSON)
				.body(Map.of("status", status, "error", reason(status), "path", path));
		}
		// 401/403 on a POST is almost always a session the last deploy invalidated
		boolean stale = status == 401 || status == 403;
		String detail = stale
				? "Your sign-in session is no longer valid; the server may have been redeployed since this page was loaded."
				: status == 404 ? "Nothing is served at " + path + "." : "";
		model.addAttribute("page", new ErrorPage(status, reason(status),
				TITLES.getOrDefault(status, "Something went wrong"), detail, stale));
		return "console/error";
	}

	private static int status(HttpServletRequest request) {
		Object status = request.getAttribute(RequestDispatcher.ERROR_STATUS_CODE);
		return status instanceof Integer code ? code : HttpServletResponse.SC_INTERNAL_SERVER_ERROR;
	}

	private static String reason(int status) {
		try {
			return HttpStatus.valueOf(status).getReasonPhrase();
		}
		catch (IllegalArgumentException e) {
			return "Error " + status;
		}
	}

	/**
	 * Whether the client asked for a document: browsers do, API clients naming a type get
	 * JSON. An absent header keeps the page, matching the default when nothing is known.
	 */
	private static boolean acceptsHtml(HttpServletRequest request) {
		String accept = request.getHeader("Accept");
		return accept == null || accept.contains("text/html") || accept.contains("*/*");
	}

	/**
	 * What the error page shows. {@code detail} is empty when the status alone says
	 * enough; {@code stale} adds the sign-again call to action.
	 */
	record ErrorPage(int status, String reason, String title, String detail, boolean stale) {

	}

}
