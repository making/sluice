// The Firecracker wicket's console: lists the routes, their VMs and the VMs' budget
// (long-polled, and refreshed for the VM counts), deploys a rootfs image dropped on the page
// or chosen from a file, and replaces, reconfigures or removes the routes deployed here. The
// flow is kit.js's; here is what is this wicket's: the option fields, the image's check, the
// cells, the budget, the refresh. Every string from the wicket is put in the DOM as text, never
// as markup.

import {
  choiceView, consolePage, currentListing, deployedView, envView, field, follow, formatMs, h, humanBytes, joined,
  limitView, nextId, parseJson, plural, refuse, showListing,
} from "/kit.js";

const CLONED = ["per-connection", "reuse", "pool"];
const MODES = ["shared", ...CLONED];

// what each mode does, after the README's table
const MODE_HINTS = {
  shared: "one VM serves every connection",
  "per-connection": "a clone of a snapshot per connection, killed when it ends",
  reuse: "a clone per connection at a time, kept for the next ones",
  pool: "clones each serving many connections, grown up to vms",
};

/**
 * The numeric options of a route: the `--vm` query's key, the mode(s) taking it, the default
 * (null: none, unbounded), the least value, and where the form puts it.
 */
const NUMBERS = [
  { key: "vcpus", group: "vm", label: "vCPUs", unit: "", fallback: 1, min: 1, modes: MODES },
  { key: "mem", group: "vm", label: "Memory", unit: "MiB", fallback: 128, min: 1, modes: MODES },
  { key: "port", group: "vm", label: "Port", unit: "vsock", fallback: 8080, min: 0, modes: MODES },
  { key: "boot-ms", group: "vm", label: "Boot", unit: "ms to listen", fallback: 10000, min: 1, modes: MODES },
  { key: "cpu", group: "vm", label: "CPU", unit: "% of one", fallback: null, min: 1, modes: MODES, jailed: true },
  { key: "warm", group: "clones", label: "Warm", unit: "ahead", fallback: 0, min: 0, modes: CLONED },
  { key: "prime", group: "clones", label: "Prime", unit: "GET /", fallback: 0, min: 0, modes: CLONED },
  { key: "vms", group: "clones", label: "VMs", unit: "at most", fallback: null, min: 1, modes: CLONED },
  { key: "wait-ms", group: "clones", label: "Wait", unit: "ms", fallback: 10000, min: 0, modes: CLONED },
  { key: "keep-ms", group: "clones", label: "Keep", unit: "ms", fallback: 60000, min: 1, modes: ["reuse", "pool"] },
  { key: "uses", group: "clones", label: "Uses", unit: "conns", fallback: null, min: 1, modes: ["reuse"] },
  { key: "lifetime-ms", group: "clones", label: "Lifetime", unit: "ms", fallback: null, min: 1, modes: ["reuse"] },
  { key: "conns", group: "clones", label: "Conns", unit: "per VM", fallback: 16, min: 1, modes: ["pool"] },
  { key: "per-client", group: "connections", label: "Per client", unit: "conns", fallback: null, min: 1, modes: MODES },
  { key: "idle-ms", group: "connections", label: "Idle", unit: "ms", fallback: null, min: 1, modes: MODES },
];

const GROUPS = { vm: "Each VM", clones: "Clones", connections: "Connections" };

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
  return { mode: route?.mode ?? "shared", values };
}

const defaultOptions = () => ({ mode: "shared", values: {} });

/** The default of option `number` under `options` (warm: a pool's vms). */
function fallbackOf(number, options) {
  if (number.key === "warm" && options.mode === "pool") {
    return options.values.vms === undefined || options.values.vms === "" ? null : Number(options.values.vms);
  }
  return number.fallback;
}

/**
 * `options` as query pairs: the mode unless shared, each option its mode takes that is set to
 * other than its default, the egress rules. No value needs escaping but the egress (a host
 * name, an address, a range and a port).
 */
function optionPairs(options) {
  const pairs = [];
  if (options.mode !== "shared") {
    pairs.push(`mode=${options.mode}`);
  }
  for (const number of NUMBERS) {
    const value = options.values[number.key];
    if (!number.modes.includes(options.mode) || value === undefined || value === "") {
      continue;
    }
    if (Number(value) !== fallbackOf(number, options)) {
      pairs.push(`${number.key}=${Number(value)}`);
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
 * the mode takes (the others hidden), then the egress. `read()` returns the options, or
 * throws (marking the field) when a value is out of range.
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

  const jailed = Boolean(currentListing()?.jailed);
  const inputs = new Map();
  const limits = new Map();
  for (const number of NUMBERS) {
    if (number.jailed && !jailed) {
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
    [...groupFields.values()],
    h("div", { class: "field" },
      h("label", { for: egress.id }, "Egress"),
      h("div", { class: "field__control" }, egress,
        h("p", { class: "field__note", id: `${id}-egress-note` },
          "The destinations its guests may connect to through the wicket, comma-separated: host:port, address:port, range:port ([2001:db8::/32]:443)."))),
    h("div", { class: "field" }, h("span", { class: "field__label" }), h("div", { class: "field__control" }, note)));

  const mode = () => [...radios].find(([, radio]) => radio.checked)?.[0] ?? "shared";
  /** Shows the options the mode takes, and a pool's warm default. */
  const show = () => {
    const chosen = mode();
    for (const [group, entries] of limits) {
      let shown = 0;
      for (const { number, element: limit } of entries) {
        limit.hidden = !number.modes.includes(chosen);
        shown += limit.hidden ? 0 : 1;
      }
      groupFields.get(group).hidden = shown === 0;
    }
    const warm = inputs.get("warm");
    warm.placeholder = chosen === "pool" ? "vms" : "0";
  };
  show();
  element.addEventListener("change", (event) => {
    if (event.target.type === "radio") {
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
      const options = { mode: mode(), values: {} };
      for (const number of NUMBERS) {
        const input = inputs.get(number.key);
        if (!input || !number.modes.includes(options.mode)) {
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
      if (options.mode === "pool" && values.vms === undefined) {
        fail(inputs.get("vms"), "A pool needs its VMs at most (vms): its size.");
      }
      if (values.vms !== undefined && values.warm !== undefined && Number(values.vms) < Number(values.warm)) {
        fail(inputs.get("vms"), "VMs must be at least warm: the warm clones are among them.");
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
  parts.push(limit(`${plural(route.vcpus, "vCPU")}, ${route.mem_mib} MiB`));
  for (const number of NUMBERS) {
    const value = route.options[number.key];
    if (value === undefined || number.key === "vcpus" || number.key === "mem") {
      continue;
    }
    parts.push(limit(number.unit === "ms" ? `${number.key.replace(/-ms$/, "")} ${formatMs(Number(value))}` : `${number.key} ${value}`));
  }
  if (route.options.egress) {
    parts.push(h("span", { class: "route__env mono", title: "the destinations its guests may connect to through the wicket" },
      `egress ${route.options.egress.split(",").join(", ")}`));
  }
  parts.push(envView(route.env));
  return [h("div", { class: "route__sandbox" }, parts), vmsView(route)];
}

/** What a route's VMs are: alive, then by mode, its connections, the template's snapshot. */
function vmsView(route) {
  const { vms, snapshot } = route;
  const counts = [h("span", { class: "num", title: "its VMs alive, as the budget counts them" }, plural(vms.alive, "VM"))];
  if (route.mode === "shared") {
    counts.push(vms.shared ? "up" : "down");
    if (vms.restarts) {
      counts.push(`restarted ${plural(vms.restarts, "time")}`);
    }
  }
  else if (route.mode === "pool") {
    counts.push(`${vms.pooled} pooled`);
    if (vms.starting) {
      counts.push(`${vms.starting} starting`);
    }
  }
  else {
    counts.push(`${vms.warm} warm`);
    if (route.mode === "reuse") {
      counts.push(`${vms.kept} kept`);
    }
  }
  counts.push(plural(vms.connections, "connection"));
  const lines = [h("span", { class: "route__meta", "data-testid": "vms" }, joined(counts))];
  if (vms.down) {
    lines.push(h("span", { class: "route__meta", title: "its VM starts again by itself" }, vms.down));
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
  vms.textContent = `${budget.vms} / ${budget.max_vms}`;
  vms.title = "VMs alive over every route (shared, warm, in use, kept, pooled, templates) / --max-vms";
  const memory = field("budget-memory");
  memory.textContent = budget.max_memory_mib === null || budget.max_memory_mib === undefined
    ? `${budget.memory_mib} MiB`
    : `${budget.memory_mib} / ${budget.max_memory_mib} MiB`;
  memory.title = budget.max_memory_mib === null || budget.max_memory_mib === undefined
    ? "their guest memory; no memory budget (--max-memory)"
    : "their guest memory / --max-memory";
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

const counts = (listing) => JSON.stringify(listing && [listing.budget, listing.routes.map((route) => [route.vms, route.snapshot])]);

follow((listing, moved) => {
  showListing(listing, moved || counts(currentListing()) !== counts(listing));
});

/**
 * The VMs' counts and the budget move without a new revision: a plain listing every
 * REFRESH_MS while the page is seen, rendered where they moved (a row rebuilt only when its
 * route changed: an open editor stays as it is).
 */
async function refresh() {
  if (document.visibilityState !== "visible" || currentListing() === null) {
    return;
  }
  try {
    const response = await fetch("/api/routes", { cache: "no-store", headers: { Accept: "application/json" } });
    if (!response.ok) {
      return;
    }
    const listing = parseJson(await response.text());
    // a listing older than the one held is no news
    if (!listing || listing.revision < currentListing().revision) {
      return;
    }
    showListing(listing);
  }
  catch {
    // the long poll tells the link is lost
  }
}

setInterval(refresh, REFRESH_MS);
