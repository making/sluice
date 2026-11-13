// The Firecracker wicket's console: lists the routes, their VMs and the VMs' budget
// (long-polled, and refreshed for the VM counts), deploys a rootfs image dropped on the page
// or chosen from a file, and replaces, reconfigures or removes the routes deployed here. The
// flow is kit.js's; here is what is this wicket's: the option fields, the image's check, the
// cells, the budget, the refresh. Every string from the wicket is put in the DOM as text, never
// as markup.

import {
  choiceView, consolePage, currentListing, deployedView, envView, field, followMoving, formatMs, h, humanBytes,
  joined, limitView, nextId, plural, refuse, usageView,
} from "/kit.js";

const CLONED = ["per-connection", "reuse", "pool"];
const MODES = ["boot", ...CLONED];
// the modes taking a readiness check: those whose VMs serve connections ahead of them
const POOLED = ["boot", "pool"];
const CHECKS = ["process", "port", "http"];
// a liveness check: port, the watch's (a VM down once its port refuses), or http
const LIVENESS = ["port", "http"];

// what each mode does, after the README's table
const MODE_HINTS = {
  boot: "VMs each booted, none restored, each serving many connections",
  "per-connection": "a clone of a snapshot per connection, killed when it ends",
  reuse: "a clone per connection at a time, kept for the next ones",
  pool: "clones each serving many connections, grown up to instances",
};

/**
 * The numeric options of a route: the `--vm` query's key, the mode(s) taking it, the default
 * (null: none, unbounded), the least value, and where the form puts it; `checked`, a readiness
 * check's (port, http) alone, or a liveness check's (http) alone; `listed`, only where the listing
 * says so (a route's `cpu`: where its VMs' cgroups bound it).
 */
const NUMBERS = [
  { key: "vcpus", group: "vm", label: "vCPUs", unit: "", fallback: 1, min: 1, modes: MODES },
  { key: "memory-mib", group: "vm", label: "Memory", unit: "MiB", fallback: 128, min: 1, modes: MODES },
  { key: "port", group: "vm", label: "Port", unit: "vsock", fallback: 8080, min: 0, modes: MODES },
  { key: "boot-ms", group: "vm", label: "Boot", unit: "ms to listen", fallback: 10000, min: 1, modes: MODES },
  { key: "cpu", group: "vm", label: "CPU", unit: "% of one", fallback: null, min: 1, modes: MODES, listed: "cpu_limit" },
  { key: "warm", group: "clones", label: "Warm", unit: "ahead", fallback: 0, min: 0, modes: CLONED },
  { key: "prime", group: "clones", label: "Prime", unit: "GET /", fallback: 0, min: 0, modes: CLONED },
  { key: "instances", group: "vm", label: "VMs", unit: "at most, boot: kept", fallback: null, min: 1, modes: MODES },
  { key: "wait-ms", group: "clones", label: "Wait", unit: "ms", fallback: 10000, min: 0, modes: CLONED },
  { key: "keep-ms", group: "clones", label: "Keep", unit: "ms", fallback: 60000, min: 1, modes: ["reuse", "pool"] },
  { key: "uses", group: "clones", label: "Uses", unit: "conns", fallback: null, min: 1, modes: ["reuse"] },
  { key: "lifetime-ms", group: "clones", label: "Lifetime", unit: "ms", fallback: null, min: 1, modes: ["reuse"] },
  { key: "concurrency", group: "clones", label: "Concurrency", unit: "conns per VM", fallback: 16, min: 1, modes: ["pool"] },
  { key: "per-client", group: "connections", label: "Per client", unit: "conns", fallback: null, min: 1, modes: MODES },
  { key: "idle-ms", group: "connections", label: "Idle", unit: "ms", fallback: null, min: 1, modes: MODES },
  { key: "readiness-timeout-ms", group: "readiness", label: "A probe within", unit: "ms", fallback: 1000, min: 1, modes: POOLED, checked: "readiness" },
  { key: "readiness-interval-ms", group: "readiness", label: "Ready, every", unit: "ms", fallback: 30000, min: 1, modes: POOLED, checked: "readiness" },
  { key: "liveness-timeout-ms", group: "liveness", label: "A probe within", unit: "ms", fallback: 1000, min: 1, modes: MODES, checked: "liveness" },
  { key: "liveness-interval-ms", group: "liveness", label: "Alive, every", unit: "ms", fallback: 30000, min: 1, modes: POOLED, checked: "liveness" },
];

const GROUPS = { vm: "Each VM", clones: "Clones", connections: "Connections", readiness: "Its probes", liveness: "Its probes" };

// the ext2/3/4 superblock's magic: 0xEF53, little-endian, at byte 1080
const MAGIC_AT = 1080;

// the VM counts move without a new listing: a plain one this often while the page is seen
const REFRESH_MS = 5000;

function sniff(file, magic) {
  if (!(magic.length === 2 && magic[0] === 0x53 && magic[1] === 0xef)) {
    return { error: `${file.name} is not an ext4 image: it has no ext2/3/4 superblock at byte 1024.` };
  }
  return {};
}

/** A route's options as the form holds them: its mode, and each other option's value as text ("" for its default). */
function optionsOf(route) {
  const values = {};
  for (const [key, value] of Object.entries(route?.options ?? {})) {
    if (key !== "mode") {
      values[key] = String(value);
    }
  }
  return { mode: route?.mode ?? "boot", values };
}

const defaultOptions = () => ({ mode: "boot", values: {} });

/** The default of option `number` under `options` (warm: a pool's instances; instances: one VM booted). */
function fallbackOf(number, options) {
  if (number.key === "instances" && options.mode === "boot") {
    return 1;
  }
  if (number.key === "warm" && options.mode === "pool") {
    return options.values.instances === undefined || options.values.instances === "" ? null : Number(options.values.instances);
  }
  return number.fallback;
}

/** The readiness check `options` ask, if their mode takes one: port or http; null else. */
function checkOf(options) {
  const check = options.values.readiness;
  return POOLED.includes(options.mode) && check && check !== "process" ? check : null;
}

/** The liveness check `options` ask that probes: http; null for port, the default. */
function livenessOf(options) {
  return options.values.liveness === "http" ? "http" : null;
}

/** Whether the check of option `number` probes under `options`: true for an option of no check. */
function probing(number, options) {
  if (number.checked === "readiness") {
    return Boolean(checkOf(options));
  }
  return number.checked === "liveness" ? Boolean(livenessOf(options)) : true;
}

/**
 * `options` as query pairs: the mode unless boot, each option its mode takes that is set to
 * other than its default, the readiness and liveness checks, the egress rules. No value needs
 * escaping but the egress (a host name, an address, a range and a port) and the endpoints.
 */
function optionPairs(options) {
  const pairs = [];
  if (options.mode !== "boot") {
    pairs.push(`mode=${options.mode}`);
  }
  const check = checkOf(options);
  for (const number of NUMBERS) {
    const value = options.values[number.key];
    if (!number.modes.includes(options.mode) || value === undefined || value === "" || !probing(number, options)) {
      continue;
    }
    if (Number(value) !== fallbackOf(number, options)) {
      pairs.push(`${number.key}=${Number(value)}`);
    }
  }
  if (check) {
    pairs.push(`readiness=${check}`);
    const endpoint = options.values["readiness-endpoint"];
    if (check === "http" && endpoint && endpoint !== "/") {
      pairs.push(`readiness-endpoint=${encodeURIComponent(endpoint)}`);
    }
  }
  if (livenessOf(options)) {
    pairs.push("liveness=http");
    const endpoint = options.values["liveness-endpoint"];
    if (endpoint && endpoint !== "/") {
      pairs.push(`liveness-endpoint=${encodeURIComponent(endpoint)}`);
    }
  }
  const egress = (options.values.egress ?? "").split(/[\s,]+/).filter(Boolean);
  if (egress.length) {
    pairs.push(`egress=${encodeURIComponent(egress.join(","))}`);
  }
  return pairs;
}

/**
 * The option fields of a route, `initial` filled in: the mode as a choice, then the options
 * the mode takes (the others hidden), the readiness and liveness checks, then the egress.
 * `read()` returns the options, or throws (marking the field) when a value is out of range.
 */
function optionFields(initial, onChange) {
  const id = nextId("options");
  const radios = new Map();
  const modes = h("div", { class: "field__control caps", role: "radiogroup", "aria-labelledby": `${id}-mode` });
  for (const mode of MODES) {
    const radio = h("input", { type: "radio", name: `${id}-mode`, id: `${id}-${mode}`, value: mode });
    radio.checked = initial.mode === mode;
    radios.set(mode, radio);
    modes.append(choiceView(radio, mode, MODE_HINTS[mode]));
  }

  const listing = currentListing();
  const inputs = new Map();
  const limits = new Map();
  for (const number of NUMBERS) {
    if (number.listed && !listing?.[number.listed]) {
      continue;
    }
    const input = h("input", {
      type: "number", id: `${id}-${number.key}`, step: 1, min: number.min, inputmode: "numeric",
      placeholder: number.fallback === null ? "none" : String(number.fallback), "aria-describedby": `${id}-note`,
    });
    input.value = initial.values[number.key] ?? "";
    inputs.set(number.key, input);
    const element = limitView(input, number.label, number.unit, { "data-option": number.key });
    if (!limits.has(number.group)) {
      limits.set(number.group, []);
    }
    limits.get(number.group).push({ number, element });
  }
  const egress = h("input", {
    type: "text", id: `${id}-egress`, autocomplete: "off", autocapitalize: "none", spellcheck: "false",
    placeholder: "none: no network", "aria-describedby": `${id}-egress-note`,
  });
  egress.style.width = "min(100%, 34rem)";
  egress.value = initial.values.egress ?? "";
  const readiness = h("select", { id: `${id}-readiness`, "aria-describedby": `${id}-readiness-note` },
    CHECKS.map((check) => h("option", { value: check }, check)));
  readiness.value = initial.values.readiness ?? "process";
  const endpoint = h("input", {
    type: "text", id: `${id}-readiness-endpoint`, autocomplete: "off", autocapitalize: "none", spellcheck: "false",
    placeholder: "/", "aria-label": "Readiness endpoint",
  });
  endpoint.value = initial.values["readiness-endpoint"] ?? "";
  const readinessField = h("div", { class: "field" },
    h("label", { for: readiness.id }, "Readiness"),
    h("div", { class: "field__control" }, readiness, " ", endpoint,
      h("p", { class: "field__note", id: `${id}-readiness-note` },
        "When a VM takes connections: process, once up; port, once its port accepts; http, once a GET of the path is answered 200. With no VM ready the route leaves the advertisement.")));
  const liveness = h("select", { id: `${id}-liveness`, "aria-describedby": `${id}-liveness-note` },
    LIVENESS.map((check) => h("option", { value: check }, check)));
  liveness.value = initial.values.liveness ?? "port";
  const aliveAt = h("input", {
    type: "text", id: `${id}-liveness-endpoint`, autocomplete: "off", autocapitalize: "none", spellcheck: "false",
    placeholder: "/", "aria-label": "Liveness endpoint",
  });
  aliveAt.value = initial.values["liveness-endpoint"] ?? "";
  const livenessField = h("div", { class: "field" },
    h("label", { for: liveness.id }, "Liveness"),
    h("div", { class: "field__control" }, liveness, " ", aliveAt,
      h("p", { class: "field__note", id: `${id}-liveness-note` },
        "When a VM is up, and while it is: port, once its port accepts, down once it refuses; http, once a GET of the path is answered 200 within Boot, down as one is not. A VM down is started again.")));
  const note = h("p", { class: "sandbox__note", id: `${id}-note` });

  const groupFields = new Map();
  for (const [group, entries] of limits) {
    groupFields.set(group, h("div", { class: "field" },
      h("span", { class: "field__label" }, GROUPS[group]),
      h("div", { class: "field__control" }, h("div", { class: "limits" }, entries.map((entry) => entry.element)))));
  }
  const element = h("fieldset", { class: "sandbox" },
    h("legend", { class: "visually-hidden" }, "Options"),
    h("div", { class: "field" }, h("span", { class: "field__label", id: `${id}-mode` }, "Mode"), modes),
    [...groupFields.values()].filter((field) => field !== groupFields.get("readiness") && field !== groupFields.get("liveness")),
    readinessField,
    groupFields.get("readiness") ?? [],
    livenessField,
    groupFields.get("liveness") ?? [],
    h("div", { class: "field" },
      h("label", { for: egress.id }, "Egress"),
      h("div", { class: "field__control" }, egress,
        h("p", { class: "field__note", id: `${id}-egress-note` },
          "The destinations its guests may connect to through the wicket, comma-separated: host:port, address:port, range:port ([2001:db8::/32]:443)."))),
    h("div", { class: "field" }, h("span", { class: "field__label" }), h("div", { class: "field__control" }, note)));

  const mode = () => [...radios].find(([, radio]) => radio.checked)?.[0] ?? "boot";
  /** Shows the options the mode takes, a check's probing with one, and a pool's warm default. */
  const show = () => {
    const chosen = mode();
    const asked = { mode: chosen, values: { readiness: readiness.value, liveness: liveness.value } };
    const check = checkOf(asked);
    for (const [group, entries] of limits) {
      let shown = 0;
      for (const { number, element: limit } of entries) {
        limit.hidden = !number.modes.includes(chosen) || !probing(number, asked);
        shown += limit.hidden ? 0 : 1;
      }
      groupFields.get(group).hidden = shown === 0;
    }
    readinessField.hidden = !POOLED.includes(chosen);
    endpoint.hidden = check !== "http";
    aliveAt.hidden = !livenessOf(asked);
    const warm = inputs.get("warm");
    warm.placeholder = chosen === "pool" ? "instances" : "0";
  };
  show();
  element.addEventListener("change", (event) => {
    if (event.target.type === "radio" || event.target === readiness || event.target === liveness) {
      show();
    }
    onChange?.();
  });
  element.addEventListener("input", () => onChange?.());

  const fail = (input, message) => refuse(input, note, message);

  return {
    element,
    read() {
      note.textContent = "";
      inputs.forEach((input) => input.removeAttribute("aria-invalid"));
      const options = { mode: mode(), values: { readiness: readiness.value, liveness: liveness.value } };
      const check = checkOf(options);
      const alive = livenessOf(options);
      if (!check) {
        delete options.values.readiness;
      }
      if (!alive) {
        delete options.values.liveness;
      }
      for (const number of NUMBERS) {
        const input = inputs.get(number.key);
        if (!input || !number.modes.includes(options.mode) || !probing(number, options)) {
          continue;
        }
        const text = input.value.trim();
        if (text === "") {
          continue;
        }
        const value = Number(text);
        if (!Number.isInteger(value) || value < number.min || value > 4294967295) {
          fail(input, `${number.label} must be a whole number of at least ${number.min}.`);
        }
        options.values[number.key] = String(value);
      }
      const { values } = options;
      if (options.mode === "pool" && values.instances === undefined) {
        fail(inputs.get("instances"), "A pool needs its VMs at most (instances): its size.");
      }
      if (values.instances !== undefined && values.warm !== undefined && Number(values.instances) < Number(values.warm)) {
        fail(inputs.get("instances"), "VMs must be at least warm: the warm clones are among them.");
      }
      const path = endpoint.value.trim();
      if (check === "http" && path !== "") {
        if (!/^\/[!-~]*$/.test(path) || path.includes("#")) {
          fail(endpoint, `Readiness endpoint ${path}: a path from /, of visible ASCII and no fragment.`);
        }
        values["readiness-endpoint"] = path;
      }
      const alivePath = aliveAt.value.trim();
      if (alive && alivePath !== "") {
        if (!/^\/[!-~]*$/.test(alivePath) || alivePath.includes("#")) {
          fail(aliveAt, `Liveness endpoint ${alivePath}: a path from /, of visible ASCII and no fragment.`);
        }
        values["liveness-endpoint"] = alivePath;
      }
      const rules = egress.value.trim();
      if (rules !== "") {
        const bad = rules.split(/[\s,]+/).filter(Boolean).find((rule) => !/^.+:\d{1,5}$/.test(rule));
        if (bad) {
          fail(egress, `Egress ${bad}: expected <host>:<port>.`);
        }
        values.egress = rules;
      }
      return options;
    },
    set(options) {
      for (const [name, radio] of radios) {
        radio.checked = options.mode === name;
      }
      for (const [key, input] of inputs) {
        input.value = options.values[key] ?? "";
      }
      egress.value = options.values.egress ?? "";
      readiness.value = options.values.readiness ?? "process";
      endpoint.value = options.values["readiness-endpoint"] ?? "";
      liveness.value = options.values.liveness ?? "port";
      aliveAt.value = options.values["liveness-endpoint"] ?? "";
      show();
    },
    focus() {
      element.querySelector("input:checked")?.focus();
    },
  };
}

function rootfsCell(route) {
  const { rootfs } = route;
  return [
    h("span", { class: "route__component mono", title: rootfs.path }, rootfs.name),
    h("span", { class: "route__meta" }, joined([
      rootfs.sha256 ? h("span", { class: "route__digest mono", title: `sha256 ${rootfs.sha256}` }, rootfs.sha256.slice(0, 12)) : null,
      h("span", { class: "num" }, humanBytes(rootfs.bytes)),
    ])),
    deployedView(route),
  ];
}

/** The mode and the options set, then what its VMs are now. */
function modeCell(route) {
  const limit = (text, title = null) => h("span", { class: "route__limit", title }, text);
  const parts = [h("span", { class: "tag", title: MODE_HINTS[route.mode] ?? null }, route.mode)];
  parts.push(limit(`${plural(route.vcpus, "vCPU")}, ${route.memory_mib} MiB`));
  for (const number of NUMBERS) {
    const value = route.options[number.key];
    if (value === undefined || number.key === "vcpus" || number.key === "memory-mib") {
      continue;
    }
    parts.push(limit(number.unit === "ms" ? `${number.key.replace(/-ms$/, "")} ${formatMs(Number(value))}` : `${number.key} ${value}`));
  }
  if (route.options.readiness) {
    const endpoint = route.options.readiness === "http" ? ` ${route.options["readiness-endpoint"] ?? "/"}` : "";
    parts.push(limit(`readiness ${route.options.readiness}${endpoint}`, "when a VM takes connections: its check passed"));
  }
  if (route.options.liveness) {
    const endpoint = ` ${route.options["liveness-endpoint"] ?? "/"}`;
    parts.push(limit(`liveness ${route.options.liveness}${endpoint}`, "when a VM is up: its check passed, started again once it fails"));
  }
  if (route.options.egress) {
    parts.push(h("span", { class: "route__env mono", title: "the destinations its guests may connect to through the wicket" },
      `egress ${route.options.egress.split(",").join(", ")}`));
  }
  parts.push(envView(route.env));
  return [h("div", { class: "route__sandbox" }, parts), vmsView(route)];
}

/** What a route's VMs are: alive, then by mode, its connections, what they use, the template's snapshot. */
function vmsView(route) {
  const { instances: vms, snapshot } = route;
  const counts = [h("span", { class: "num", title: "its VMs alive, as the budget counts them" }, plural(vms.alive, "VM"))];
  if (route.mode === "boot" || route.mode === "pool") {
    counts.push(`${vms.pooled} ${route.mode === "boot" ? "up" : "pooled"}`);
    if (vms.starting) {
      counts.push(`${vms.starting} starting`);
    }
    if (vms.restarts) {
      counts.push(`restarted ${plural(vms.restarts, "time")}`);
    }
    if (route.options.readiness && vms.ready !== null && vms.ready !== undefined) {
      counts.push(`${vms.ready} ready`);
    }
  }
  else {
    counts.push(`${vms.warm} warm`);
    if (route.mode === "reuse") {
      counts.push(`${vms.kept} kept`);
    }
  }
  counts.push(plural(vms.connections, "connection"));
  const lines = [h("span", { class: "route__meta", "data-testid": "instances" }, joined(counts))];
  const used = usageView(route.alive);
  if (used) {
    lines.push(used);
  }
  if (vms.down) {
    lines.push(h("span", { class: "route__meta", title: "its VM starts again by itself" }, vms.down.why));
  }
  if (route.ready === false) {
    lines.push(h("span", { class: "route__meta", title: "every node stream leaves it out until a VM is ready" },
      "not advertised: no VM ready"));
  }
  if (snapshot) {
    lines.push(h("span", { class: "route__meta", title: `snapshot taken ${snapshot.at}` }, joined([
      `snapshot ${humanBytes(snapshot.memory_bytes)}`,
      `booted in ${formatMs(snapshot.booted_ms)}`,
      `taken after ${formatMs(snapshot.taken_ms)}`,
    ])));
  }
  return lines;
}

/** The title block's own fields: the VMs' budget. */
function budgetStatus(listing) {
  const { budget } = listing;
  const vms = field("budget-vms");
  vms.textContent = `${budget.instances} / ${budget.max_instances}`;
  vms.title = "VMs alive over every route (booted, warm, in use, kept, pooled, templates) / --max-instances";
  const memory = field("budget-memory");
  memory.textContent = budget.max_memory_mib === null || budget.max_memory_mib === undefined
    ? `${budget.memory_mib} MiB`
    : `${budget.memory_mib} / ${budget.max_memory_mib} MiB`;
  memory.title = budget.max_memory_mib === null || budget.max_memory_mib === undefined
    ? "what they may take of the host's memory: each its memory-mib, its firecracker's own and its log, a cloned route its snapshot on a tmpfs; no memory budget (--max-memory-mib)"
    : "what they may take of the host's memory: each its memory-mib, its firecracker's own and its log, a cloned route its snapshot on a tmpfs / --max-memory-mib, by default what the host has for them";
  const cpus = field("budget-cpus");
  cpus.textContent = budget.max_cpus === null || budget.max_cpus === undefined
    ? `${budget.cpus}`
    : `${budget.cpus} / ${budget.max_cpus}`;
  cpus.title = budget.max_cpus === null || budget.max_cpus === undefined
    ? "the CPUs they hold: each its vCPUs, or its cpu where that is less; no CPU budget (--max-cpus)"
    : "the CPUs they hold: each its vCPUs, or its cpu where that is less / --max-cpus";
}

consolePage({
  limit: "max_rootfs_bytes",
  head: [MAGIC_AT, MAGIC_AT + 2],
  sniff,
  accept: /\.(ext4|img)$/i,
  extension: ".ext4",
  fallback: "rootfs",
  flags: "--vm",
  settings: {
    noun: "options",
    unchanged: "the options stay as they were.",
    of: optionsOf,
    initial: defaultOptions,
    pairs: optionPairs,
    fields: optionFields,
  },
  cells: [rootfsCell, modeCell],
  status: budgetStatus,
  removal: () => "The route is withdrawn at once, its VMs end once the server let go of it, and its image is deleted unless another console route uses it.",
  renamed: (route, to) => ({ text: `Served at ${to} at once, by the same VMs.`, kind: "" }),
  words: {
    one: "image",
    first: "a rootfs image",
    kind: "an ext4 rootfs image",
    replaces: "Replaces the rootfs and options of this console route: its VMs start anew.",
    onRow: "drop an .ext4 image on a row to replace its rootfs",
    loading: "Booting its VMs and reloading the routes…",
    saving: "Booting its VMs and reloading the routes…",
    keeps: "image, options and VMs",
  },
});

// ----------------------------------------------------------------------- the link

// what moves without a new revision: the VMs' counts, what they use, and the budget
followMoving((listing) => [listing.budget, listing.routes.map((route) => [route.instances, route.ready, route.snapshot, route.alive])], REFRESH_MS);
