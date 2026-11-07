// The sluice wicket console: lists the routes (long-polled), deploys a component dropped
// on the page or chosen from a file, and replaces, reconfigures or removes the routes
// deployed here. The flow is kit.js's; here is what is the wasm wicket's: the sandbox fields,
// the component's check, the cells. Every string from the wicket is put in the DOM as text,
// never as markup.

import {
  choiceView, consolePage, currentListing, deployedView, envView, follow, formatMs, h, humanBytes, limitView,
  nextId, refuse, sep, showListing,
} from "/kit.js";

const NETWORK = ["inherit-network", "allow-ip-name-lookup", "tcp", "udp"];

// what each capability grants, after the README's table
const CAP_HINTS = {
  cli: "the full wasi p3 set: sockets, filesystem and more",
  "inherit-network": "sockets may reach every address (implies tcp, udp)",
  "allow-ip-name-lookup": "wasi:sockets/ip-name-lookup",
  tcp: "allows tcp; every address stays denied without inherit-network",
  udp: "allows udp; every address stays denied without inherit-network",
  keyvalue: "wasi:keyvalue on the default store, namespaced by the host",
};

// until the first listing names them
const FALLBACK = {
  capabilities: ["cli", "inherit-network", "allow-ip-name-lookup", "tcp", "udp", "keyvalue"],
  defaults: { "budget-ms": 10000, "memory-mib": 256 },
};

const defaults = () => currentListing()?.defaults ?? FALLBACK.defaults;

/** Its first 8 bytes: the wasm magic, then the version's layer (0: a core module). */
function sniff(file, head) {
  if (!(head[0] === 0x00 && head[1] === 0x61 && head[2] === 0x73 && head[3] === 0x6d)) {
    return { error: `${file.name} is not a WebAssembly binary: it does not begin with \\0asm.` };
  }
  if (head.length === 8 && (head[6] | (head[7] << 8)) === 0) {
    return { error: `${file.name} is a core wasm module, not a component: the wicket runs wasi:http components (wasm32-wasip3).` };
  }
  return {};
}

function sandboxPairs(sandbox) {
  const pairs = [];
  if (sandbox.wasi.length) {
    pairs.push(`wasi=${sandbox.wasi.join(",")}`);
  }
  if (sandbox["budget-ms"] !== defaults()["budget-ms"]) {
    pairs.push(`budget-ms=${sandbox["budget-ms"]}`);
  }
  if (sandbox["memory-mib"] !== defaults()["memory-mib"]) {
    pairs.push(`memory-mib=${sandbox["memory-mib"]}`);
  }
  if (sandbox.warm > 0) {
    pairs.push(`warm=${sandbox.warm}`);
  }
  if (sandbox.instances !== null && sandbox.instances !== undefined) {
    pairs.push(`instances=${sandbox.instances}`);
  }
  if (sandbox["keep-ms"] !== null && sandbox["keep-ms"] !== undefined) {
    pairs.push(`keep-ms=${sandbox["keep-ms"]}`);
  }
  if ((sandbox.concurrency ?? 1) !== 1) {
    pairs.push(`concurrency=${sandbox.concurrency}`);
  }
  return pairs;
}

/**
 * The sandbox fields of a route, `initial` filled in: the capabilities as checkboxes,
 * then the limits. `read()` returns the sandbox, or throws (marking the field) when a
 * value is out of range.
 */
function sandboxFields(initial, onChange) {
  const id = nextId("sandbox");
  const boxes = new Map();
  const caps = h("div", { class: "field__control caps", role: "group", "aria-labelledby": `${id}-caps` });
  for (const name of currentListing()?.capabilities ?? FALLBACK.capabilities) {
    const box = h("input", { type: "checkbox", id: `${id}-${name}`, value: name });
    box.checked = initial.wasi.includes(name);
    boxes.set(name, box);
    caps.append(choiceView(box, name, CAP_HINTS[name] ?? ""));
  }
  // the network capabilities need cli (wasi:sockets is linked only with it)
  caps.addEventListener("change", (event) => {
    const changed = event.target;
    if (NETWORK.includes(changed.value) && changed.checked && boxes.has("cli")) {
      boxes.get("cli").checked = true;
    }
    if (changed.value === "cli" && !changed.checked) {
      for (const name of NETWORK) {
        if (boxes.has(name)) {
          boxes.get(name).checked = false;
        }
      }
    }
  });

  // a refused value's message describes the input the focus is moved to
  const number = (key, label, unit, value, extra = {}) => {
    const input = h("input", {
      type: "number", id: `${id}-${key}`, step: 1, inputmode: "numeric", value: value ?? "", "aria-describedby": `${id}-note`, ...extra,
    });
    return { input, element: limitView(input, label, unit) };
  };
  const budget = number("budget", "Budget", "ms", initial["budget-ms"], { min: 1 });
  const memory = number("memory", "Memory", "MiB", initial["memory-mib"], { min: 1 });
  const warm = number("warm", "Warm", "kept", initial.warm, { min: 0 });
  const instances = number("instances", "Instances", "at most", initial.instances, { min: 1, placeholder: "no cap" });
  const keep = number("keep", "Keep", "ms idle", initial["keep-ms"], { min: 1, placeholder: "none" });
  const concurrency = number("concurrency", "Concurrency", "at once", initial.concurrency ?? 1, { min: 1 });
  const inputs = [budget.input, memory.input, warm.input, instances.input, keep.input, concurrency.input];
  const note = h("p", { class: "sandbox__note", id: `${id}-note` });

  const element = h("fieldset", { class: "sandbox" },
    h("legend", { class: "visually-hidden" }, "Sandbox"),
    h("div", { class: "field" }, h("span", { class: "field__label", id: `${id}-caps` }, "Capabilities"), caps),
    h("div", { class: "field" },
      h("span", { class: "field__label" }, "Limits"),
      h("div", { class: "field__control" },
        h("div", { class: "limits" }, budget.element, memory.element, warm.element, instances.element, keep.element, concurrency.element),
        note)));
  element.addEventListener("input", () => onChange?.());
  element.addEventListener("change", () => onChange?.());

  const fail = (input, message) => refuse(input, note, message);
  const whole = (input, min, what) => {
    const text = input.value.trim();
    const value = Number(text);
    if (text === "" || !Number.isInteger(value) || value < min) {
      fail(input, `${what} must be a whole number of at least ${min}.`);
    }
    return value;
  };

  return {
    element,
    read() {
      note.textContent = "";
      inputs.forEach((input) => input.removeAttribute("aria-invalid"));
      const sandbox = {
        wasi: [...boxes].filter(([, box]) => box.checked).map(([name]) => name),
        "budget-ms": whole(budget.input, 1, "The budget"),
        "memory-mib": whole(memory.input, 1, "The memory"),
        warm: whole(warm.input, 0, "Warm"),
        instances: null,
        "keep-ms": null,
        concurrency: whole(concurrency.input, 1, "The concurrency"),
      };
      if (instances.input.value.trim() !== "") {
        sandbox.instances = whole(instances.input, 1, "Instances");
        if (sandbox.instances < sandbox.warm) {
          fail(instances.input, "Instances must be at least warm: every pooled unit holds one of the route's instance slots.");
        }
      }
      if (keep.input.value.trim() !== "") {
        sandbox["keep-ms"] = whole(keep.input, 1, "Keep");
      }
      return sandbox;
    },
    set(sandbox) {
      for (const [name, box] of boxes) {
        box.checked = sandbox.wasi.includes(name);
      }
      budget.input.value = sandbox["budget-ms"];
      memory.input.value = sandbox["memory-mib"];
      warm.input.value = sandbox.warm;
      instances.input.value = sandbox.instances ?? "";
      keep.input.value = sandbox["keep-ms"] ?? "";
      concurrency.input.value = sandbox.concurrency ?? 1;
    },
    focus() {
      element.querySelector("input")?.focus();
    },
  };
}

const defaultSandbox = () => ({ wasi: [], "budget-ms": defaults()["budget-ms"], "memory-mib": defaults()["memory-mib"], warm: 0, instances: null, "keep-ms": null, concurrency: 1 });

/** The note on moving `route` to the free host `to`. */
function renamed(route, to) {
  if (route.sandbox.wasi.includes("keyvalue")) {
    return { text: `Its keyvalue data stays under ${route.host}: at ${to} it starts with none.`, kind: "replace" };
  }
  return { text: `Served at ${to} at once, its component and sandbox as they are.`, kind: "" };
}

function removal(route) {
  const kept = route.sandbox.wasi.includes("keyvalue") ? " Its keyvalue data stays in the store." : "";
  return `The route is withdrawn at once, and its component file deleted unless another console route uses it.${kept}`;
}

function componentCell(route) {
  const { component } = route;
  return [
    h("span", { class: "route__component mono" }, component.name),
    h("span", { class: "route__meta" },
      h("span", { class: "route__digest mono", title: `sha256 ${component.sha256}` }, component.sha256.slice(0, 12)),
      sep(),
      h("span", { class: "num" }, humanBytes(component.bytes))),
    deployedView(route),
  ];
}

function sandboxCell({ sandbox }) {
  const parts = sandbox.wasi.map((cap) => h("span", { class: "tag", title: CAP_HINTS[cap] ?? null }, cap));
  const limit = (text) => h("span", { class: "route__limit" }, text);
  if (sandbox["budget-ms"] !== defaults()["budget-ms"]) {
    parts.push(limit(`budget ${formatMs(sandbox["budget-ms"])}`));
  }
  if (sandbox["memory-mib"] !== defaults()["memory-mib"]) {
    parts.push(limit(`memory ${sandbox["memory-mib"]} MiB`));
  }
  if (sandbox.warm > 0) {
    parts.push(limit(`warm ${sandbox.warm}`));
  }
  if (sandbox.instances !== null && sandbox.instances !== undefined) {
    parts.push(limit(`≤ ${sandbox.instances} instance${sandbox.instances === 1 ? "" : "s"}`));
  }
  if (sandbox["keep-ms"] !== null && sandbox["keep-ms"] !== undefined) {
    parts.push(limit(`keep ${formatMs(sandbox["keep-ms"])}`));
  }
  if ((sandbox.concurrency ?? 1) !== 1) {
    parts.push(limit(`${sandbox.concurrency} at once`));
  }
  for (const entry of sandbox.keyvalue ?? []) {
    const identifier = entry.identifier === "" ? '""' : entry.identifier;
    parts.push(h("span", { class: "route__kv mono", title: "keyvalue identifier → store/namespace" },
      `${identifier} → ${entry.store}/${entry.namespace}`));
  }
  const env = envView(sandbox.env);
  if (env) {
    parts.push(env);
  }
  if (!parts.length) {
    parts.push(h("span", { class: "route__defaults" }, "defaults"));
  }
  return h("div", { class: "route__sandbox" }, parts);
}

consolePage({
  limit: "max_component_bytes",
  head: [0, 8],
  sniff,
  accept: /\.wasm$/i,
  extension: ".wasm",
  fallback: "component",
  contentType: "application/wasm",
  flags: "--wasm",
  settings: {
    noun: "sandbox",
    unchanged: "the sandbox stays as it was.",
    of: (route) => route.sandbox,
    initial: defaultSandbox,
    pairs: sandboxPairs,
    fields: sandboxFields,
  },
  cells: [componentCell, sandboxCell],
  removal,
  renamed,
  words: {
    one: "component",
    first: "a component",
    kind: "a WebAssembly component",
    replaces: "Replaces the component and sandbox of this console route.",
    onRow: "drop a .wasm on a row to replace its component",
    loading: "Compiling and reloading the routes…",
    saving: "Reloading the routes…",
    keeps: "component and sandbox",
  },
});

follow(showListing);
