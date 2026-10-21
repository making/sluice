package am.ik.sluice.server.console.web;

import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.List;

import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import am.ik.sluice.server.cert.ClientCertificateIssuer;
import am.ik.sluice.server.cert.ClientCertificateIssuers;
import am.ik.sluice.server.cert.IssuedClientCertificate;
import am.ik.sluice.server.cluster.RouteGatePropagator;
import am.ik.sluice.server.route.GateState;
import am.ik.sluice.server.route.Router;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.security.oauth2.core.oidc.user.OidcUser;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;

/**
 * Management console of this server node: connected clients, the route table and the
 * cluster membership. Served on the servlet port next to the actuator endpoints.
 */
@Controller
class ConsoleController {

	private static final Logger log = LoggerFactory.getLogger(ConsoleController.class);

	private final ConsoleView view;

	private final ActuatorView actuatorView;

	private final ClientCertificateIssuers clientCertificateIssuers;

	private final Router router;

	private final RouteGatePropagator routeGatePropagator;

	ConsoleController(ConsoleView view, ActuatorView actuatorView, ClientCertificateIssuers clientCertificateIssuers,
			Router router, RouteGatePropagator routeGatePropagator) {
		this.view = view;
		this.actuatorView = actuatorView;
		this.clientCertificateIssuers = clientCertificateIssuers;
		this.router = router;
		this.routeGatePropagator = routeGatePropagator;
	}

	@GetMapping("/")
	String root() {
		return "redirect:/console";
	}

	@GetMapping("/console")
	String index(Authentication authentication, Model model) {
		this.addLive(model);
		model.addAttribute("user", displayName(authentication));
		model.addAttribute("settings", this.view.settings());
		ClientCertificateIssuer issuer = this.clientCertificateIssuers.find();
		model.addAttribute("clientCertificate", issuer == null ? null : new ClientCertificate(issuer.caSubject()));
		return "console/index";
	}

	/** The standalone client certificate issuance page. */
	@GetMapping("/console/certificates")
	String certificates(Model model) {
		ClientCertificateIssuer issuer = this.clientCertificateIssuers.find();
		model.addAttribute("clientCertificate", issuer == null ? null : new ClientCertificate(issuer.caSubject()));
		return "console/certificates";
	}

	/**
	 * Issues a client certificate: the response is a zip download with the certificate,
	 * its PKCS#8 key and the CA certificate as separate PEM files, for the operator to
	 * hand to the client.
	 */
	@PostMapping("/console/certificates")
	ResponseEntity<byte[]> issueCertificate(@RequestParam String cn, @RequestParam(defaultValue = "365") int days) {
		ClientCertificateIssuer issuer = this.clientCertificateIssuers.find();
		if (issuer == null) {
			return ResponseEntity.status(HttpStatus.CONFLICT).build();
		}
		String name = ClientCertificateIssuer.sanitize(cn);
		if (name.isEmpty()) {
			return ResponseEntity.badRequest().build();
		}
		IssuedClientCertificate issued = issuer.issue(name, days);
		return ResponseEntity.ok()
			.header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + name + ".zip\"")
			.contentType(MediaType.parseMediaType("application/zip"))
			.body(issued.zip(name, issuer.caCertificatePem()));
	}

	/**
	 * The polled regions as {@code <hx-partial>} elements, each swapped into its own
	 * target.
	 */
	@GetMapping("/console/live")
	String live(Model model) {
		this.addLive(model);
		return "console/live";
	}

	/**
	 * Opens or shuts the gate of one route, in memory on this node and every peer (until
	 * restart: the gate comes back to its configured default).
	 * @param key the route key as shown in the routes table
	 * @param open {@code true} to open the gate, {@code false} to shut it
	 */
	@PostMapping("/console/routes/gate")
	String routeGate(@RequestParam String key, @RequestParam boolean open) {
		GateState state = open ? GateState.OPEN : GateState.SHUT;
		this.router.gate().set(key, state);
		log.debug("gate of route [{}] set {} from the console", key, state.name().toLowerCase(Locale.ROOT));
		this.routeGatePropagator.propagate(key, open);
		return "redirect:/console#routes";
	}

	/** The health of this node as reported by the actuator, indicator by indicator. */
	@GetMapping("/console/health")
	String health(Authentication authentication, Model model) {
		model.addAttribute("user", displayName(authentication));
		model.addAttribute("health", this.actuatorView.health());
		return "console/health";
	}

	/** The polled health region as an {@code <hx-partial>} element. */
	@GetMapping("/console/health/live")
	String healthLive(Model model) {
		model.addAttribute("health", this.actuatorView.health());
		return "console/health-live";
	}

	/** The info document, its top level entries one section each. */
	@GetMapping("/console/info")
	String info(Authentication authentication, Model model) {
		model.addAttribute("user", displayName(authentication));
		model.addAttribute("sections", this.actuatorView.info());
		return "console/info";
	}

	/**
	 * The metrics of this node: the filtered names, plus the tag combinations of the
	 * selected one.
	 */
	@GetMapping("/console/metrics")
	String metrics(@RequestParam(defaultValue = "") String q, @RequestParam(required = false) String name,
			Authentication authentication, Model model) {
		model.addAttribute("user", displayName(authentication));
		this.addMetrics(q, name, model);
		return "console/metrics";
	}

	/** The filtered metric list, swapped in under the filter box. */
	@GetMapping("/console/metrics/list")
	String metricList(@RequestParam(defaultValue = "") String q, @RequestParam(required = false) String name,
			Model model) {
		this.addMetrics(q, name, model);
		return "console/metric-list";
	}

	private void addMetrics(String q, @Nullable String name, Model model) {
		String filter = q == null ? "" : q;
		String selected = name == null ? "" : name.strip();
		List<ActuatorView.MetricGroup> groups = this.actuatorView.metricGroups(filter, selected);
		model.addAttribute("q", filter);
		model.addAttribute("selectedName", selected);
		model.addAttribute("hasSelection", !selected.isEmpty());
		if (!selected.isEmpty()) {
			model.addAttribute("detail", this.actuatorView.metric(selected));
		}
		model.addAttribute("groups", groups);
		model.addAttribute("matched", groups.stream().mapToInt(ActuatorView.MetricGroup::count).sum());
		model.addAttribute("total", this.actuatorView.metricCount());
	}

	@GetMapping("/console/lookup")
	String lookup(@RequestParam(defaultValue = "") String host, Model model) {
		this.view.lookup(host).ifPresent(lookup -> model.addAttribute("lookup", lookup));
		return "console/lookup";
	}

	/**
	 * The signed-in user as shown in the masthead. An OIDC subject is usually an opaque
	 * id, so a readable claim is preferred when the provider sends one.
	 */
	static String displayName(Authentication authentication) {
		if (authentication.getPrincipal() instanceof OidcUser user) {
			for (String name : new String[] { user.getPreferredUsername(), user.getEmail(), user.getFullName() }) {
				if (name != null && !name.isBlank()) {
					return name;
				}
			}
		}
		return authentication.getName();
	}

	private void addLive(Model model) {
		model.addAttribute("status", this.view.status());
		model.addAttribute("flow", this.view.flow());
		model.addAttribute("clients", this.view.clients());
		model.addAttribute("httpRoutes", this.view.httpRoutes());
		model.addAttribute("tcpRoutes", this.view.tcpRoutes());
		model.addAttribute("cluster", this.view.cluster());
	}

	/**
	 * Panel model of the client certificate form; rendered only when a CA is configured.
	 *
	 * @param caSubject the CA the issued certificates are signed by
	 */
	record ClientCertificate(String caSubject) {

	}

}
