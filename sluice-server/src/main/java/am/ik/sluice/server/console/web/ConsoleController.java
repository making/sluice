package am.ik.sluice.server.console.web;

import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;

/**
 * Management console of this server node: connected clients, the route table and the
 * cluster membership. Served on the servlet port next to the actuator endpoints.
 */
@Controller
class ConsoleController {

	private final ConsoleView view;

	ConsoleController(ConsoleView view) {
		this.view = view;
	}

	@GetMapping("/")
	String root() {
		return "redirect:/console";
	}

	@GetMapping("/console")
	String index(Model model) {
		this.addLive(model);
		model.addAttribute("settings", this.view.settings());
		return "console/index";
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

	@GetMapping("/console/lookup")
	String lookup(@RequestParam(defaultValue = "") String host, Model model) {
		this.view.lookup(host).ifPresent(lookup -> model.addAttribute("lookup", lookup));
		return "console/lookup";
	}

	private void addLive(Model model) {
		model.addAttribute("status", this.view.status());
		model.addAttribute("flow", this.view.flow());
		model.addAttribute("clients", this.view.clients());
		model.addAttribute("httpRoutes", this.view.httpRoutes());
		model.addAttribute("tcpRoutes", this.view.tcpRoutes());
		model.addAttribute("cluster", this.view.cluster());
	}

}
