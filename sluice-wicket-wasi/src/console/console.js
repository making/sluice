// The sluice wicket console: lists the routes (long-polled), deploys a component dropped
// on the page or chosen from a file, and replaces, reconfigures or removes the routes
// deployed here. Every string from the wicket is put in the DOM as text, never as markup.

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
  defaults: { budget_ms: 10000, memory_mib: 256 },
};

const ORIGINS = {
  console: { title: "Console", note: "drop a .wasm on a row to replace its component" },
  config: { title: "Config file", note: "read-only here: edit the file, the wicket reloads it" },
  flag: { title: "--wasm flags", note: "read-only: set on the command line" },
};

// dot-separated labels of a-z, 0-9 and '-', none starting or ending with '-'
const HOST = /^(?=.{1,253}$)[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)*$/;

const POLL_RETRY_MS = 2000;
// the wicket holds a poll for about 25s; a longer silence is a dead link
const POLL_ABORT_MS = 40000;
const OK_OUTCOME_MS = 10000;

// ------------------------------------------------------------------------- helpers

/** An element with attributes (functions become listeners) and text or element children. */
function h(tag, props = {}, ...children) {
  const node = document.createElement(tag);
  for (const [key, value] of Object.entries(props)) {
    if (value === null || value === undefined || value === false) {
      continue;
    }
    if (typeof value === "function") {
      node.addEventListener(key.replace(/^on/, ""), value);
    }
    else if (key === "class") {
      node.className = value;
    }
    else {
      node.setAttribute(key, value === true ? "" : String(value));
    }
  }
  node.append(...children.flat(Infinity)
    .filter((child) => child !== null && child !== undefined && child !== false)
    .map((child) => (child instanceof Node ? child : String(child))));
  return node;
}

const $ = (selector, root = document) => root.querySelector(selector);
const field = (name) => $(`[data-field="${name}"]`);
const slot = (name) => $(`[data-slot="${name}"]`);
const region = (name) => $(`[data-region="${name}"]`);
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

function humanBytes(bytes) {
  if (bytes < 1024) {
    return `${bytes} B`;
  }
  const units = ["KiB", "MiB", "GiB"];
  let value = bytes / 1024;
  let unit = 0;
  while (value >= 1024 && unit < units.length - 1) {
    value /= 1024;
    unit += 1;
  }
  return `${value >= 100 ? Math.round(value) : value.toFixed(1)} ${units[unit]}`;
}

// a drawn dot: the middle dot of the subset Overpass has no width; read and copied as a comma
const sep = () => h("span", { class: "sep" }, h("span", { class: "visually-hidden" }, ", "));

/** `parts` with a separator between each two. */
const joined = (parts) => parts.filter(Boolean).flatMap((part, index) => (index ? [sep(), part] : [part]));

const formatMs = (ms) => (ms % 1000 === 0 ? `${ms / 1000} s` : `${ms} ms`);

const relativeFormat = new Intl.RelativeTimeFormat("en", { numeric: "auto" });

function relativeTime(iso) {
  const at = Date.parse(iso);
  if (Number.isNaN(at)) {
    return iso;
  }
  const seconds = Math.round((at - Date.now()) / 1000);
  const steps = [[60, "second"], [60, "minute"], [24, "hour"], [30, "day"], [12, "month"]];
  let value = seconds;
  for (const [size, unit] of steps) {
    if (Math.abs(value) < size) {
      return unit === "second" && Math.abs(value) < 45 ? "just now" : relativeFormat.format(value, unit);
    }
    value = Math.round(value / size);
  }
  return relativeFormat.format(value, "year");
}

/** A <time> kept relative ("3 minutes ago") by the refresher, the instant as its title. */
function timeView(iso) {
  const at = new Date(iso);
  const title = Number.isNaN(at.getTime()) ? iso : `${at.toLocaleString()} (${iso})`;
  return h("time", { datetime: iso, title, "data-relative": true }, relativeTime(iso));
}

/** `fileName` as a host: "hello.wasm" -> "hello.localhost", which browsers resolve to loopback. */
function suggestHost(fileName) {
  const label = fileName
    .replace(/\.wasm$/i, "")
    .toLowerCase()
    .replace(/[^a-z0-9-]+/g, "-")
    .replace(/-{2,}/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 63)
    .replace(/-+$/, "");
  return `${label || "component"}.localhost`;
}

const normalizeHost = (value) => value.trim().toLowerCase();

/** `listen` ("127.0.0.1:8080", "[::]:8080") split into a URL host and a port. */
function parseListen(listen) {
  if (!listen) {
    return null;
  }
  const colon = listen.lastIndexOf(":");
  if (colon < 0) {
    return null;
  }
  const host = listen.slice(0, colon).replace(/^\[|\]$/g, "");
  const port = listen.slice(colon + 1);
  const unspecified = host === "0.0.0.0" || host === "::";
  return { host, port, unspecified };
}

const urlHost = (host) => (host.includes(":") && !host.startsWith("[") ? `[${host}]` : host);

const loopbackPage = () => ["localhost", "127.0.0.1", "[::1]", "::1"].includes(location.hostname);

const shellQuote = (text) => `'${text.replaceAll("'", "'\\''")}'`;

function announce(text) {
  const announcer = region("announcer");
  announcer.textContent = "";
  // a new node in the next frame, so the same text is announced again
  requestAnimationFrame(() => {
    announcer.textContent = text;
  });
}

/**
 * The head of `file` checked before it is sent: {error} keeps it from being sent at all.
 * A component begins with `\0asm` and has layer 1 in bytes 6-7; a core module has layer 0.
 */
async function sniff(file) {
  const max = state.listing?.max_component_bytes;
  if (max && file.size > max) {
    return { error: `${file.name} is ${humanBytes(file.size)}, more than the ${humanBytes(max)} the console accepts.` };
  }
  if (file.size === 0) {
    return { error: `${file.name} is empty.` };
  }
  let head;
  try {
    head = new Uint8Array(await file.slice(0, 8).arrayBuffer());
  }
  catch {
    return {};
  }
  if (!(head[0] === 0x00 && head[1] === 0x61 && head[2] === 0x73 && head[3] === 0x6d)) {
    return { error: `${file.name} is not a WebAssembly binary: it does not begin with \\0asm.` };
  }
  if (head.length === 8 && (head[6] | (head[7] << 8)) === 0) {
    return { error: `${file.name} is a core wasm module, not a component: the wicket runs wasi:http components (wasm32-wasip3).` };
  }
  return {};
}

// ---------------------------------------------------------------------------- state

const state = {
  listing: null,
  revision: null,
  /** host -> {phase: "uploading" | "compiling" | "working", progress, total, text} */
  busy: new Map(),
  /** host -> {kind: "ok" | "fault", title, summary, detail} */
  outcomes: new Map(),
  /** host -> {row, fields}: the open sandbox editors */
  editors: new Map(),
  /** {host, until}: the row marked as just deployed */
  fresh: null,
  /** requests carrying a component, which keep the intake's wicket open */
  inFlight: 0,
  /** the file the deploy form is about */
  pending: null,
};

const defaults = () => state.listing?.defaults ?? FALLBACK.defaults;
const routeOf = (host) => state.listing?.routes.find((route) => route.host === host);

// ------------------------------------------------------------------------- requests

class ApiError extends Error {
  constructor(status, message) {
    super(message);
    this.status = status;
  }
}

const TITLES = {
  0: "No answer",
  400: "Refused",
  401: "Not signed in",
  403: "Forbidden",
  404: "Not a console route",
  409: "Host taken",
  413: "Too large",
  422: "Rejected",
  503: "Draining",
};

const titleOf = (error) => TITLES[error.status] ?? `Failed (HTTP ${error.status})`;

function errorMessage(status, answer, text) {
  if (typeof answer?.error === "string" && answer.error) {
    return answer.error;
  }
  if (status === 401) {
    return "The console token was refused or is missing. Reload the page to sign in again.";
  }
  return text ? `HTTP ${status}: ${text.slice(0, 500)}` : `HTTP ${status}`;
}

function parseJson(text) {
  try {
    return JSON.parse(text);
  }
  catch {
    return null;
  }
}

/**
 * The route's sandbox as query pairs, defaults left out (the wicket takes a left-out option
 * as its default). No value needs escaping: capability names are [a-z-], the rest numbers.
 */
function sandboxPairs(sandbox) {
  const pairs = [];
  if (sandbox.wasi.length) {
    pairs.push(`wasi=${sandbox.wasi.join(",")}`);
  }
  if (sandbox.budget_ms !== defaults().budget_ms) {
    pairs.push(`budget-ms=${sandbox.budget_ms}`);
  }
  if (sandbox.memory_mib !== defaults().memory_mib) {
    pairs.push(`memory-mib=${sandbox.memory_mib}`);
  }
  if (sandbox.pool > 0) {
    pairs.push(`pool=${sandbox.pool}`);
  }
  if (sandbox.instances !== null && sandbox.instances !== undefined) {
    pairs.push(`instances=${sandbox.instances}`);
  }
  return pairs;
}

/**
 * `method` on the route of `host` with an XMLHttpRequest (fetch reports no upload
 * progress). `onUpload(fraction, sent)` follows the body; `sent` once all of it left,
 * when the wicket compiles and reloads before it answers.
 */
function send(method, host, pairs, body, onUpload) {
  return new Promise((resolve, reject) => {
    const xhr = new XMLHttpRequest();
    const query = pairs.length ? `?${pairs.join("&")}` : "";
    xhr.open(method, `/api/routes/${encodeURIComponent(host)}${query}`);
    xhr.setRequestHeader("Accept", "application/json");
    if (body) {
      xhr.setRequestHeader("Content-Type", "application/wasm");
      xhr.upload.addEventListener("progress", (event) => {
        onUpload?.(event.lengthComputable ? event.loaded / event.total : 0, false);
      });
      xhr.upload.addEventListener("load", () => onUpload?.(1, true));
    }
    xhr.addEventListener("load", () => {
      const answer = parseJson(xhr.responseText);
      if (xhr.status >= 200 && xhr.status < 300) {
        resolve(answer ?? {});
      }
      else {
        reject(new ApiError(xhr.status, errorMessage(xhr.status, answer, xhr.responseText)));
      }
    });
    xhr.addEventListener("error", () => {
      reject(new ApiError(0, "The wicket did not answer (network error, or it closed the connection)."));
    });
    xhr.send(body ?? null);
  });
}

/** A successful edit: the wicket's account of the change ("added a.localhost", "no change"), then the generation. */
function changed(answer) {
  const message = answer.message || "done";
  return {
    kind: "ok",
    title: `${message.charAt(0).toUpperCase()}${message.slice(1)}.`,
    summary: answer.generation !== undefined ? `Generation ${answer.generation}.` : null,
  };
}

const spoken = (outcome) => [outcome.title, outcome.summary].filter(Boolean).join(" ");

// ------------------------------------------------------------------- shared views

/** The progress of one request: an upload bar, then an indeterminate run. */
function progressView() {
  const bar = h("div", { class: "progress__bar" });
  const track = h("div", { class: "progress__track", role: "progressbar", tabindex: -1, "aria-valuemin": 0, "aria-valuemax": 100 }, bar);
  const label = h("p", { class: "progress__label" });
  const element = h("div", { class: "progress" }, track, label);
  return {
    element,
    /** holds the focus of a control disabled while the request runs */
    focus() {
      track.focus();
    },
    upload(fraction, total) {
      const percent = Math.round(fraction * 100);
      element.classList.remove("progress--indeterminate");
      bar.style.width = `${percent}%`;
      track.setAttribute("aria-valuenow", String(percent));
      track.setAttribute("aria-label", "Upload");
      label.textContent = `Uploading ${percent}% of ${humanBytes(total)}`;
    },
    working(text) {
      element.classList.add("progress--indeterminate");
      bar.style.width = "";
      track.removeAttribute("aria-valuenow");
      track.setAttribute("aria-label", text);
      label.textContent = text;
    },
  };
}

/** An outcome: its title and summary, then `extra` (a node), the error text verbatim, the actions. */
function outcomeView(outcome, ...actions) {
  return h("div", { class: `outcome outcome--${outcome.kind}` },
    h("p", {}, h("span", { class: "outcome__title" }, outcome.title), outcome.summary ? ` ${outcome.summary}` : null),
    outcome.extra ?? null,
    outcome.detail ? h("pre", { class: "outcome__error" }, outcome.detail) : null,
    actions.length ? h("p", { class: "outcome__actions" }, actions) : null);
}

/** How to reach `host` through `--listen`: a link where the browser resolves it, else a curl line. */
function reachView(host) {
  const listen = parseListen(state.listing?.listen);
  if (!listen || !host) {
    return null;
  }
  // the browser resolves *.localhost to the loopback address: a link where --listen takes it
  const loopbackListen = listen.unspecified || ["127.0.0.1", "::1", "localhost"].includes(listen.host);
  if (loopbackPage() && loopbackListen && host.endsWith(".localhost") && HOST.test(host)) {
    const url = `http://${host}:${listen.port}/`;
    return h("span", { class: "route__reach" },
      h("a", { class: "mono route__curl", href: url, target: "_blank", rel: "noopener noreferrer" }, `${url} ↗`));
  }
  const target = listen.unspecified ? urlHost(location.hostname) : urlHost(listen.host);
  const command = `curl -H ${shellQuote(`Host: ${host}`)} http://${target}:${listen.port}/`;
  const code = h("code", { class: "route__curl" }, command);
  const copy = h("button", { type: "button", class: "link-button", "data-action": "copy" }, "Copy");
  copy.addEventListener("click", () => copyText(command, code, copy));
  return h("span", { class: "route__reach" }, code, copy);
}

async function copyText(text, code, button) {
  const label = button.textContent;
  try {
    await navigator.clipboard.writeText(text);
    button.textContent = "Copied";
  }
  catch {
    // no clipboard outside a secure context: select it for the keyboard instead
    const range = document.createRange();
    range.selectNodeContents(code);
    getSelection().removeAllRanges();
    getSelection().addRange(range);
    button.textContent = "Selected: press Ctrl+C";
  }
  setTimeout(() => {
    button.textContent = label;
  }, 1800);
}

let fieldsets = 0;

/**
 * The sandbox fields of a route, `initial` filled in: the capabilities as checkboxes,
 * then the limits. `read()` returns the sandbox, or throws (marking the field) when a
 * value is out of range.
 */
function sandboxFields(initial, onChange) {
  const id = `sandbox-${++fieldsets}`;
  const boxes = new Map();
  const caps = h("div", { class: "field__control caps", role: "group", "aria-labelledby": `${id}-caps` });
  for (const name of state.listing?.capabilities ?? FALLBACK.capabilities) {
    const box = h("input", { type: "checkbox", id: `${id}-${name}`, value: name });
    box.checked = initial.wasi.includes(name);
    boxes.set(name, box);
    caps.append(h("label", { class: "cap", for: box.id },
      box,
      h("span", { class: "cap__name" }, name),
      h("span", { class: "cap__hint" }, CAP_HINTS[name] ?? "")));
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
    const element = h("div", { class: "limit" },
      h("label", { for: input.id }, label),
      h("div", { class: "limit__control" }, input, unit ? h("span", { class: "limit__unit" }, unit) : null));
    return { input, element };
  };
  const budget = number("budget", "Budget", "ms", initial.budget_ms, { min: 1 });
  const memory = number("memory", "Memory", "MiB", initial.memory_mib, { min: 1 });
  const pool = number("pool", "Pool", "warm", initial.pool, { min: 0 });
  const instances = number("instances", "Instances", "at most", initial.instances, { min: 1, placeholder: "no cap" });
  const inputs = [budget.input, memory.input, pool.input, instances.input];
  const note = h("p", { class: "sandbox__note", id: `${id}-note` });

  const element = h("fieldset", { class: "sandbox" },
    h("legend", { class: "visually-hidden" }, "Sandbox"),
    h("div", { class: "field" }, h("span", { class: "field__label", id: `${id}-caps` }, "Capabilities"), caps),
    h("div", { class: "field" },
      h("span", { class: "field__label" }, "Limits"),
      h("div", { class: "field__control" },
        h("div", { class: "limits" }, budget.element, memory.element, pool.element, instances.element),
        note)));
  element.addEventListener("input", () => onChange?.());
  element.addEventListener("change", () => onChange?.());

  const fail = (input, message) => {
    input.setAttribute("aria-invalid", "true");
    note.textContent = message;
    input.focus();
    throw new Error(message);
  };
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
        budget_ms: whole(budget.input, 1, "The budget"),
        memory_mib: whole(memory.input, 1, "The memory"),
        pool: whole(pool.input, 0, "The pool"),
        instances: null,
      };
      if (instances.input.value.trim() !== "") {
        sandbox.instances = whole(instances.input, 1, "Instances");
        if (sandbox.instances < sandbox.pool) {
          fail(instances.input, "Instances must be at least the pool: every pooled unit holds one of the route's instance slots.");
        }
      }
      return sandbox;
    },
    set(sandbox) {
      for (const [name, box] of boxes) {
        box.checked = sandbox.wasi.includes(name);
      }
      budget.input.value = sandbox.budget_ms;
      memory.input.value = sandbox.memory_mib;
      pool.input.value = sandbox.pool;
      instances.input.value = sandbox.instances ?? "";
    },
    focus() {
      element.querySelector("input")?.focus();
    },
  };
}

const defaultSandbox = () => ({ wasi: [], budget_ms: defaults().budget_ms, memory_mib: defaults().memory_mib, pool: 0, instances: null });

// --------------------------------------------------------------------- the intake

const intake = $("[data-testid=drop-zone]");

/** The drawing's wicket: open while a file is dragged over the page or a component is on its way. */
function setIntake(dragging) {
  intake.dataset.intake = dragging ? "over" : state.inFlight > 0 ? "busy" : "idle";
}

let dragEnd = null;
let dragRows = [];

function markRows(target, refusing) {
  for (const row of dragRows) {
    row.classList.remove("is-target", "is-refusing");
  }
  dragRows = [target, refusing].filter(Boolean);
  target?.classList.add("is-target");
  refusing?.classList.add("is-refusing");
}

function endDrag() {
  clearTimeout(dragEnd);
  markRows(null, null);
  setIntake(false);
}

const carriesFiles = (event) => Boolean(event.dataTransfer?.types?.includes("Files"));

/** The console route row under `event`, or the read-only one that refuses the drop. */
function dropRow(event) {
  const row = event.target instanceof Element ? event.target.closest("tr.route") : null;
  if (!row) {
    return { target: null, refusing: null };
  }
  const deployable = row.dataset.origin === "console" && !state.busy.has(row.dataset.host);
  return deployable ? { target: row, refusing: null } : { target: null, refusing: row };
}

// dragover keeps firing while a drag stays over the page; once it stops (left the window,
// or dropped) the timer puts everything back
function onDrag(event) {
  if (!carriesFiles(event)) {
    return;
  }
  event.preventDefault();
  const { target, refusing } = dropRow(event);
  event.dataTransfer.dropEffect = refusing ? "none" : "copy";
  markRows(target, refusing);
  setIntake(true);
  clearTimeout(dragEnd);
  dragEnd = setTimeout(endDrag, 200);
}

document.addEventListener("dragenter", onDrag);
document.addEventListener("dragover", onDrag);
document.addEventListener("drop", (event) => {
  if (!carriesFiles(event)) {
    return;
  }
  // never let the browser open the file instead of the page
  event.preventDefault();
  const { target, refusing } = dropRow(event);
  endDrag();
  const files = [...event.dataTransfer.files];
  if (!files.length) {
    return;
  }
  const file = files.find((candidate) => /\.wasm$/i.test(candidate.name)) ?? files[0];
  const several = files.length > 1 ? `Only one component at a time: ${file.name} was taken.` : null;
  if (target) {
    replace(routeOf(target.dataset.host), file);
  }
  else if (refusing) {
    announce(`${refusing.dataset.host} is read-only here.`);
  }
  else {
    openDeploy(file, several);
  }
});

const intakeFile = region("intake-file");
$("[data-action=choose]").addEventListener("click", () => intakeFile.click());
intakeFile.addEventListener("change", () => {
  const [file] = intakeFile.files;
  intakeFile.value = "";
  if (file) {
    openDeploy(file);
  }
});

// ------------------------------------------------------------------ the deploy form

const deploy = {
  section: region("deploy"),
  form: region("deploy-form"),
  fields: region("deploy-fields"),
  host: $("#deploy-host"),
  note: field("host-note"),
  meta: field("deploy-meta"),
  submit: $("[data-action=deploy]"),
  sandbox: null,
  /** the user changed the sandbox: a host matching a console route no longer refills it */
  touched: false,
  /** the file's check: {error} or {} */
  check: {},
  sending: false,
};

function openDeploy(file, notice = null) {
  if (deploy.sending) {
    announce("A deploy is in progress; drop the next component once it finished.");
    return;
  }
  state.pending = file;
  deploy.check = {};
  deploy.touched = false;
  field("deploy-file").textContent = file.name;
  deploy.host.value = suggestHost(file.name);
  deploy.host.removeAttribute("aria-invalid");
  const existing = routeOf(deploy.host.value);
  deploy.sandbox = sandboxFields(existing?.origin === "console" ? existing.sandbox : defaultSandbox(), () => {
    deploy.touched = true;
  });
  slot("sandbox").replaceChildren(deploy.sandbox.element);
  slot("progress").replaceChildren();
  slot("outcome").replaceChildren();
  deploy.fields.hidden = false;
  deploy.fields.disabled = false;
  deploy.section.hidden = false;
  describeFile(file, notice);
  refreshDeploy();
  deploy.host.focus();
  deploy.host.select();
  deploy.section.scrollIntoView({ block: "nearest", behavior: matchMedia("(prefers-reduced-motion: reduce)").matches ? "auto" : "smooth" });
  sniff(file).then((check) => {
    if (state.pending !== file) {
      return;
    }
    deploy.check = check;
    describeFile(file, notice);
    refreshDeploy();
  });
}

function describeFile(file, notice) {
  const { error } = deploy.check;
  deploy.meta.className = `deploy__meta${error ? " deploy__meta--fault" : notice ? " deploy__meta--marker" : ""}`;
  deploy.meta.replaceChildren(...joined([humanBytes(file.size), error ?? "a WebAssembly component", notice]));
}

function closeDeploy() {
  if (deploy.sending) {
    return;
  }
  state.pending = null;
  deploy.section.hidden = true;
  $("[data-action=choose]").focus();
}

/** The host note and whether the form may be sent, against the routes of the moment. */
function refreshDeploy() {
  if (deploy.section.hidden || deploy.fields.hidden) {
    return;
  }
  const host = normalizeHost(deploy.host.value);
  const route = routeOf(host);
  let note = "";
  let kind = "";
  let blocked = Boolean(deploy.check.error) || deploy.sending;
  if (host === "") {
    note = "The host the route serves.";
    blocked = true;
  }
  else if (!HOST.test(host)) {
    note = "Not a host name: dot-separated labels of a-z, 0-9 and '-', none starting or ending with '-'.";
    kind = "fault";
    blocked = true;
  }
  else if (route?.origin === "console") {
    note = "Replaces the component and sandbox of this console route.";
    kind = "replace";
  }
  else if (route) {
    note = `Routed by the ${route.origin === "config" ? "config file" : "--wasm flags"}: read-only here.`;
    kind = "fault";
    blocked = true;
  }
  else {
    const listen = parseListen(state.listing?.listen);
    note = listen ? `A new route, reached through --listen on port ${listen.port}.` : "A new route.";
  }
  deploy.note.className = `field__note${kind ? ` field__note--${kind}` : ""}`;
  deploy.note.textContent = note;
  if (kind === "fault") {
    deploy.host.setAttribute("aria-invalid", "true");
  }
  else {
    deploy.host.removeAttribute("aria-invalid");
  }
  deploy.submit.disabled = blocked;
}

deploy.host.addEventListener("input", () => {
  // a host of a console route brings its sandbox along, any other host the defaults, unless
  // the user set one already
  const route = routeOf(normalizeHost(deploy.host.value));
  if (!deploy.touched) {
    deploy.sandbox.set(route?.origin === "console" ? route.sandbox : defaultSandbox());
  }
  refreshDeploy();
});

deploy.form.addEventListener("keydown", (event) => {
  if (event.key === "Escape") {
    closeDeploy();
  }
});

$("[data-action=cancel]").addEventListener("click", closeDeploy);

deploy.form.addEventListener("submit", async (event) => {
  event.preventDefault();
  const file = state.pending;
  if (!file || deploy.sending) {
    return;
  }
  refreshDeploy();
  if (deploy.submit.disabled) {
    return;
  }
  const host = normalizeHost(deploy.host.value);
  let sandbox;
  try {
    sandbox = deploy.sandbox.read();
  }
  catch {
    return;
  }
  deploy.sending = true;
  deploy.fields.disabled = true;
  const progress = progressView();
  progress.upload(0, file.size);
  slot("progress").replaceChildren(progress.element);
  slot("outcome").replaceChildren();
  progress.focus();
  state.inFlight += 1;
  setIntake(false);
  let failed = false;
  try {
    const answer = await send("PUT", host, [`name=${encodeURIComponent(file.name)}`, ...sandboxPairs(sandbox)], file,
      (fraction, sent) => (sent ? progress.working("Compiling and reloading the routes…") : progress.upload(fraction, file.size)));
    state.fresh = { host, until: Date.now() + 4000 };
    state.pending = null;
    deploy.fields.hidden = true;
    const close = h("button", { type: "button", class: "button", onclick: closeDeploy }, "Close");
    const another = h("button", { type: "button", class: "link-button", onclick: () => intakeFile.click() }, "deploy another component");
    const reach = reachView(host);
    slot("outcome").replaceChildren(outcomeView({
      ...changed(answer), extra: reach ? h("p", {}, reach) : null,
    }, close, " or ", another));
    announce(spoken(changed(answer)));
    close.focus();
  }
  catch (error) {
    slot("outcome").replaceChildren(outcomeView({ kind: "fault", title: `${titleOf(error)}:`, summary: "the routes stay as they were.", detail: error.message }));
    announce(`Deploy of ${host} failed: ${error.message}`);
    failed = true;
  }
  finally {
    slot("progress").replaceChildren();
    deploy.sending = false;
    deploy.fields.disabled = false;
    state.inFlight -= 1;
    setIntake(false);
    refreshDeploy();
    render();
  }
  if (failed) {
    (deploy.submit.disabled ? deploy.host : deploy.submit).focus();
  }
});

// --------------------------------------------------------------- route operations

const replaceFile = region("replace-file");
let replaceHost = null;

replaceFile.addEventListener("change", () => {
  const [file] = replaceFile.files;
  replaceFile.value = "";
  const route = routeOf(replaceHost);
  if (file && route) {
    replace(route, file);
  }
});

function settle(host, outcome) {
  state.outcomes.set(host, outcome);
  if (outcome.kind === "ok") {
    setTimeout(() => {
      if (state.outcomes.get(host) === outcome) {
        state.outcomes.delete(host);
        render();
      }
    }, OK_OUTCOME_MS);
  }
}

/** Whether the focus is on a control of the rows of `host`. */
const focusIn = (host) => document.activeElement?.closest?.("tr[data-host]")?.dataset.host === host;

const focusAction = (host, action) => $(`tr.route[data-host="${CSS.escape(host)}"] [data-action="${action}"]`)?.focus();

/** Deploys `file` to the console route `route`, keeping its sandbox. */
async function replace(route, file) {
  if (!route || route.origin !== "console" || state.busy.has(route.host)) {
    return;
  }
  const { host } = route;
  // the row's buttons are disabled while it is busy: the focus comes back once it is done
  const refocus = focusIn(host);
  state.outcomes.delete(host);
  const check = await sniff(file);
  if (state.busy.has(host)) {
    return;
  }
  if (check.error) {
    settle(host, { kind: "fault", title: "Not sent:", summary: check.error });
    render();
    return;
  }
  state.busy.set(host, { phase: "uploading", progress: 0, total: file.size });
  state.inFlight += 1;
  setIntake(false);
  render();
  try {
    const answer = await send("PUT", host, [`name=${encodeURIComponent(file.name)}`, ...sandboxPairs(route.sandbox)], file, (fraction, sent) => {
      const busy = state.busy.get(host);
      if (sent) {
        busy.phase = "compiling";
        render();
      }
      else {
        busy.progress = fraction;
        rowStatuses.get(host)?.progress?.upload(fraction, file.size);
      }
    });
    state.fresh = { host, until: Date.now() + 4000 };
    settle(host, changed(answer));
    announce(spoken(changed(answer)));
  }
  catch (error) {
    settle(host, { kind: "fault", title: `${titleOf(error)}:`, summary: `${file.name} was not deployed; ${host} stays as it was.`, detail: error.message });
    announce(`Replacing ${host} failed: ${error.message}`);
  }
  finally {
    state.busy.delete(host);
    state.inFlight -= 1;
    setIntake(false);
    render();
  }
  if (refocus) {
    focusAction(host, "replace");
  }
}

async function removeRoute(route) {
  const { host } = route;
  const kept = route.sandbox.wasi.includes("keyvalue") ? " Its keyvalue data stays in the store." : "";
  if (!confirm(`Remove ${host}?\n\nThe route is withdrawn at once, and its component file deleted unless another console route uses it.${kept}`)) {
    return;
  }
  const refocus = focusIn(host);
  state.outcomes.delete(host);
  state.editors.delete(host);
  state.busy.set(host, { phase: "working", text: "Removing…" });
  render();
  let note = null;
  try {
    const answer = await send("DELETE", host, [], null);
    // marked until the next listing drops the row
    state.busy.set(host, { phase: "working", text: "Removed." });
    note = routesNote(changed(answer));
    announce(spoken(changed(answer)));
  }
  catch (error) {
    state.busy.delete(host);
    settle(host, { kind: "fault", title: `${titleOf(error)}:`, summary: `${host} was not removed.`, detail: error.message });
    announce(`Removing ${host} failed: ${error.message}`);
  }
  finally {
    render();
  }
  if (refocus && note) {
    note.focus();
  }
  else if (refocus) {
    focusAction(host, "remove");
  }
}

/** A note above the table, for an outcome whose row is gone. */
function routesNote(outcome) {
  const note = outcomeView(outcome);
  note.tabIndex = -1;
  slot("routes-note").replaceChildren(note);
  setTimeout(() => {
    if (note.isConnected) {
      note.remove();
    }
  }, OK_OUTCOME_MS);
  return note;
}

function toggleEditor(route) {
  const { host } = route;
  if (state.editors.get(host)?.kind === "sandbox") {
    state.editors.delete(host);
    render();
    focusAction(host, "edit");
    return;
  }
  const fields = sandboxFields(route.sandbox);
  const outcome = h("div");
  const save = h("button", { type: "submit", class: "button button--primary" }, "Save sandbox");
  const cancel = h("button", { type: "button", class: "button", onclick: () => toggleEditor(route) }, "Cancel");
  const form = h("form", { class: "editor", novalidate: true, "aria-label": `Sandbox of ${host}` },
    fields.element, h("div", { class: "editor__actions" }, save, cancel), outcome);
  form.addEventListener("keydown", (event) => {
    if (event.key === "Escape" && !cancel.disabled) {
      toggleEditor(route);
    }
  });
  form.addEventListener("submit", async (event) => {
    event.preventDefault();
    let sandbox;
    try {
      sandbox = fields.read();
    }
    catch {
      return;
    }
    save.disabled = true;
    cancel.disabled = true;
    const progress = progressView();
    progress.working("Reloading the routes…");
    outcome.replaceChildren(progress.element);
    progress.focus();
    try {
      const answer = await send("PATCH", host, sandboxPairs(sandbox), null);
      state.editors.delete(host);
      state.fresh = { host, until: Date.now() + 4000 };
      settle(host, changed(answer));
      announce(spoken(changed(answer)));
      render();
      focusAction(host, "edit");
    }
    catch (error) {
      outcome.replaceChildren(outcomeView({ kind: "fault", title: `${titleOf(error)}:`, summary: "the sandbox stays as it was.", detail: error.message }));
      announce(`Changing the sandbox of ${host} failed: ${error.message}`);
      save.disabled = false;
      cancel.disabled = false;
      save.focus();
    }
  });
  const row = h("tr", { class: "route__editor", "data-host": host }, h("td", { colspan: 4 }, form));
  state.editors.set(host, { kind: "sandbox", row, fields });
  render();
  fields.focus();
}

/** The note on moving `route` to the host `value`, and whether it may be sent. */
function renameCheck(route, value) {
  const to = normalizeHost(value);
  if (to === route.host || to === "") {
    return { text: "The host the route moves to.", kind: "", blocked: true };
  }
  if (!HOST.test(to)) {
    return {
      text: "Not a host name: dot-separated labels of a-z, 0-9 and '-', none starting or ending with '-'.",
      kind: "fault",
      blocked: true,
    };
  }
  const taken = routeOf(to);
  if (taken) {
    const by = taken.origin === "console" ? "a route of the console already" : `routed by the ${taken.origin === "config" ? "config file" : "--wasm flags"}`;
    return { text: `${to} is ${by}.`, kind: "fault", blocked: true };
  }
  if (route.sandbox.wasi.includes("keyvalue")) {
    return { text: `Its keyvalue data stays under ${route.host}: at ${to} it starts with none.`, kind: "replace", blocked: false };
  }
  return { text: `Served at ${to} at once, its component and sandbox as they are.`, kind: "", blocked: false };
}

/** Moves the console route `route` to another host as it is: its component and sandbox. */
function toggleRename(route) {
  const { host } = route;
  if (state.editors.get(host)?.kind === "rename") {
    state.editors.delete(host);
    render();
    focusAction(host, "rename");
    return;
  }
  const id = `rename-${++fieldsets}`;
  const input = h("input", {
    id, type: "text", required: true, autocomplete: "off", autocapitalize: "none", spellcheck: "false",
    inputmode: "url", "aria-describedby": `${id}-note`,
  });
  input.value = host;
  const note = h("p", { id: `${id}-note`, class: "field__note" });
  const outcome = h("div");
  const save = h("button", { type: "submit", class: "button button--primary" }, "Rename");
  const cancel = h("button", { type: "button", class: "button", onclick: () => toggleRename(route) }, "Cancel");
  let sending = false;
  const check = () => {
    const { text, kind, blocked } = renameCheck(routeOf(host) ?? route, input.value);
    note.className = `field__note${kind ? ` field__note--${kind}` : ""}`;
    note.textContent = text;
    if (kind === "fault") {
      input.setAttribute("aria-invalid", "true");
    }
    else {
      input.removeAttribute("aria-invalid");
    }
    save.disabled = blocked || sending;
  };
  input.addEventListener("input", check);
  const form = h("form", { class: "editor", novalidate: true, "aria-label": `Rename ${host}` },
    h("div", { class: "field field--host" },
      h("label", { for: id }, "New host"),
      h("div", { class: "field__control" }, input, note)),
    h("div", { class: "editor__actions" }, save, cancel),
    outcome);
  form.addEventListener("keydown", (event) => {
    if (event.key === "Escape" && !cancel.disabled) {
      toggleRename(route);
    }
  });
  form.addEventListener("submit", async (event) => {
    event.preventDefault();
    check();
    if (save.disabled) {
      return;
    }
    const to = normalizeHost(input.value);
    sending = true;
    save.disabled = true;
    cancel.disabled = true;
    const progress = progressView();
    progress.working("Reloading the routes…");
    outcome.replaceChildren(progress.element);
    progress.focus();
    try {
      const answer = await send("PATCH", host, [`host=${encodeURIComponent(to)}`], null);
      state.editors.delete(host);
      // marked until the next listing drops the row; the route shows at its new host
      state.busy.set(host, { phase: "working", text: `Moved to ${to}.` });
      state.fresh = { host: to, until: Date.now() + 4000 };
      const moved = routesNote(changed(answer));
      announce(spoken(changed(answer)));
      render();
      moved.focus();
    }
    catch (error) {
      outcome.replaceChildren(outcomeView({ kind: "fault", title: `${titleOf(error)}:`, summary: `${host} stays as it was.`, detail: error.message }));
      announce(`Renaming ${host} failed: ${error.message}`);
      sending = false;
      cancel.disabled = false;
      check();
      (save.disabled ? input : save).focus();
    }
  });
  const row = h("tr", { class: "route__editor", "data-host": host }, h("td", { colspan: 4 }, form));
  state.editors.set(host, { kind: "rename", row, refresh: check });
  render();
  input.focus();
  input.select();
}

// --------------------------------------------------------------------- rendering

function renderStatus() {
  const { listing } = state;
  field("build").textContent = listing.build || "unknown";
  field("generation").textContent = String(listing.generation);
  field("route-count").textContent = String(listing.routes.length);
  const listen = field("listen");
  listen.textContent = listing.listen ?? "off";
  listen.title = listing.listen ?? "no --listen";
  const server = field("server");
  server.textContent = listing.server ?? "local mode";
  server.title = listing.server ?? "no --server: local mode";
  field("opening").textContent = `clear opening ≤ ${humanBytes(listing.max_component_bytes)}`;
  field("limit").textContent = humanBytes(listing.max_component_bytes);

  const banner = region("reload-failure");
  const failure = listing.reload_failure;
  banner.hidden = !failure;
  if (failure) {
    field("failure-reason").textContent = `Reload (${failure.reason}) failed`;
    const at = new Date(failure.at);
    const time = field("failure-at");
    time.dateTime = failure.at;
    time.title = failure.at;
    time.textContent = Number.isNaN(at.getTime())
      ? failure.at
      : at.toDateString() === new Date().toDateString() ? `at ${at.toLocaleTimeString()}` : `on ${at.toLocaleString()}`;
    field("failure-error").textContent = failure.error;
  }
}

const groups = new Map(); // origin -> {tbody, key}
const rows = new Map(); // host -> {signature, row}
const rowStatuses = new Map(); // host -> {signature, row, progress}
let emptyBody = null;

function renderRoutes() {
  const table = $("[data-testid=routes]");
  region("routes-loading")?.remove();
  const byOrigin = new Map();
  for (const route of state.listing.routes) {
    if (!byOrigin.has(route.origin)) {
      byOrigin.set(route.origin, []);
    }
    byOrigin.get(route.origin).push(route);
  }

  // keep focus on a button of a row rebuilt under it
  const focused = document.activeElement?.closest?.("tr[data-host]") && document.activeElement.dataset.action
    ? { host: document.activeElement.closest("tr[data-host]").dataset.host, action: document.activeElement.dataset.action }
    : null;

  let anchor = table.tHead;
  if (byOrigin.size === 0) {
    emptyBody ??= h("tbody", {}, h("tr", {}, h("td", { colspan: 4, class: "empty" },
      "No routes yet. Drop a component above to deploy the first one.")));
    anchor.after(emptyBody);
    anchor = emptyBody;
  }
  else {
    emptyBody?.remove();
  }

  const hosts = new Set();
  for (const [origin, routes] of byOrigin) {
    let group = groups.get(origin);
    if (!group) {
      const key = h("tr", { class: "routes__key" }, h("th", { colspan: 4, scope: "rowgroup" }));
      group = { tbody: h("tbody", { "data-origin": origin }, key), key };
      groups.set(origin, group);
    }
    const label = ORIGINS[origin] ?? { title: origin, note: "" };
    group.key.firstElementChild.replaceChildren(label.title, " ",
      h("span", { class: "decision" }, `${routes.length} route${routes.length === 1 ? "" : "s"}`, sep(), label.note));
    const nodes = [group.key];
    for (const route of routes) {
      hosts.add(route.host);
      nodes.push(routeRow(route));
      const status = statusRow(route.host);
      if (status) {
        nodes.push(status);
      }
      const editor = state.editors.get(route.host);
      if (editor && route.origin === "console") {
        nodes.push(editor.row);
      }
    }
    reconcile(group.tbody, nodes);
    if (anchor.nextElementSibling !== group.tbody) {
      anchor.after(group.tbody);
    }
    anchor = group.tbody;
  }
  for (const [origin, group] of groups) {
    if (!byOrigin.has(origin)) {
      group.tbody.remove();
      groups.delete(origin);
    }
  }
  for (const map of [rows, rowStatuses, state.editors, state.outcomes, state.busy]) {
    for (const host of map.keys()) {
      if (!hosts.has(host)) {
        map.delete(host);
      }
    }
  }

  if (focused && (document.activeElement === document.body || !document.activeElement)) {
    focusAction(focused.host, focused.action);
  }
}

/**
 * Puts `nodes` in `parent` in order, moving only the ones out of place: the rest go first, so
 * a row gone above an editor does not move it (a moved node loses the focus).
 */
function reconcile(parent, nodes) {
  const kept = new Set(nodes);
  for (const child of [...parent.children]) {
    if (!kept.has(child)) {
      child.remove();
    }
  }
  nodes.forEach((node, index) => {
    const current = parent.children[index];
    if (current !== node) {
      parent.insertBefore(node, current ?? null);
    }
  });
}

function routeRow(route) {
  const fresh = state.fresh?.host === route.host && state.fresh.until > Date.now();
  const signature = JSON.stringify([route, state.busy.has(route.host), state.editors.get(route.host)?.kind ?? null, fresh, state.listing.listen, state.listing.defaults]);
  const cached = rows.get(route.host);
  if (cached?.signature === signature) {
    return cached.row;
  }
  const classes = ["route", state.busy.has(route.host) ? "is-busy" : "", fresh ? "is-fresh" : ""].filter(Boolean).join(" ");
  const row = h("tr", { class: classes, "data-host": route.host, "data-origin": route.origin, "data-testid": "route" },
    h("td", {}, hostCell(route)),
    h("td", {}, componentCell(route)),
    h("td", {}, sandboxCell(route.sandbox)),
    h("td", {}, actionsCell(route)));
  rows.set(route.host, { signature, row });
  return row;
}

function hostCell(route) {
  const name = route.host === ""
    ? h("span", { class: "route__host catch-all" }, "any host")
    : h("span", { class: "route__host mono" }, route.host);
  return [name, reachView(route.host)];
}

function componentCell(route) {
  const { component } = route;
  return [
    h("span", { class: "route__component mono" }, component.name),
    h("span", { class: "route__meta" },
      h("span", { class: "route__digest mono", title: `sha256 ${component.sha256}` }, component.sha256.slice(0, 12)),
      sep(),
      h("span", { class: "num" }, humanBytes(component.bytes))),
    route.origin === "console" && route.deployed ? h("span", { class: "route__deployed" }, "deployed ", timeView(route.deployed)) : null,
  ];
}

function sandboxCell(sandbox) {
  const parts = sandbox.wasi.map((cap) => h("span", { class: "tag", title: CAP_HINTS[cap] ?? null }, cap));
  const limit = (text) => h("span", { class: "route__limit" }, text);
  if (sandbox.budget_ms !== defaults().budget_ms) {
    parts.push(limit(`budget ${formatMs(sandbox.budget_ms)}`));
  }
  if (sandbox.memory_mib !== defaults().memory_mib) {
    parts.push(limit(`memory ${sandbox.memory_mib} MiB`));
  }
  if (sandbox.pool > 0) {
    parts.push(limit(`pool ${sandbox.pool}`));
  }
  if (sandbox.instances !== null && sandbox.instances !== undefined) {
    parts.push(limit(`≤ ${sandbox.instances} instance${sandbox.instances === 1 ? "" : "s"}`));
  }
  for (const entry of sandbox.keyvalue ?? []) {
    const identifier = entry.identifier === "" ? '""' : entry.identifier;
    parts.push(h("span", { class: "route__kv mono", title: "keyvalue identifier → store/namespace" },
      `${identifier} → ${entry.store}/${entry.namespace}`));
  }
  if (!parts.length) {
    parts.push(h("span", { class: "route__defaults" }, "defaults"));
  }
  return h("div", { class: "route__sandbox" }, parts);
}

function actionsCell(route) {
  if (route.origin !== "console") {
    return null;
  }
  const busy = state.busy.has(route.host);
  const editing = state.editors.get(route.host)?.kind;
  return h("div", { class: "route__actions" },
    h("button", {
      type: "button", class: "button button--small", "data-action": "replace", disabled: busy,
      title: "Deploy another component to this host, keeping its sandbox",
      onclick: () => {
        replaceHost = route.host;
        replaceFile.click();
      },
    }, "Replace…"),
    h("button", {
      type: "button", class: "button button--small", "data-action": "edit", disabled: busy,
      "aria-expanded": String(editing === "sandbox"), onclick: () => toggleEditor(route),
    }, "Edit sandbox"),
    h("button", {
      type: "button", class: "button button--small", "data-action": "rename", disabled: busy,
      title: "Serve this route at another host, keeping its component and sandbox",
      "aria-expanded": String(editing === "rename"), onclick: () => toggleRename(route),
    }, "Rename…"),
    h("button", {
      type: "button", class: "button button--small button--danger", "data-action": "remove", disabled: busy,
      onclick: () => removeRoute(route),
    }, "Remove"));
}

function statusRow(host) {
  const busy = state.busy.get(host);
  const outcome = state.outcomes.get(host);
  if (!busy && !outcome) {
    rowStatuses.delete(host);
    return null;
  }
  const signature = JSON.stringify([busy?.phase, busy?.text, outcome]);
  let cached = rowStatuses.get(host);
  if (cached?.signature !== signature) {
    let content;
    let progress = null;
    if (busy) {
      progress = progressView();
      if (busy.phase === "uploading") {
        progress.upload(busy.progress, busy.total);
      }
      else {
        progress.working(busy.text ?? "Compiling and reloading the routes…");
      }
      content = progress.element;
    }
    else {
      content = outcomeView(outcome, h("button", {
        type: "button", class: "link-button",
        onclick: () => {
          state.outcomes.delete(host);
          render();
        },
      }, "Dismiss"));
    }
    cached = { signature, row: h("tr", { class: "route__status", "data-host": host }, h("td", { colspan: 4 }, content)), progress };
    rowStatuses.set(host, cached);
  }
  return cached.row;
}

function render() {
  if (!state.listing) {
    return;
  }
  renderStatus();
  renderRoutes();
  refreshDeploy();
  // a rename's note follows the listing, as the deploy form's does
  for (const editor of state.editors.values()) {
    editor.refresh?.();
  }
}

// ----------------------------------------------------------------------- the link

function setLink(live, error = null) {
  document.documentElement.dataset.link = live ? "live" : "lost";
  const lost = $(".link__lost");
  lost.title = error ? error.message : "";
}

/** Follows the routes: the wicket answers at once when the listing moved past `after`, else holds the poll. */
async function poll() {
  for (;;) {
    const controller = new AbortController();
    const abort = setTimeout(() => controller.abort(), POLL_ABORT_MS);
    try {
      const url = state.revision === null ? "/api/routes" : `/api/routes?after=${encodeURIComponent(state.revision)}`;
      const response = await fetch(url, { cache: "no-store", signal: controller.signal, headers: { Accept: "application/json" } });
      if (!response.ok) {
        const text = await response.text();
        throw new ApiError(response.status, errorMessage(response.status, parseJson(text), text));
      }
      const listing = await response.json();
      setLink(true);
      const moved = listing.revision !== state.revision || state.listing === null;
      state.listing = listing;
      state.revision = listing.revision;
      if (moved) {
        render();
      }
    }
    catch (error) {
      setLink(false, error.name === "AbortError" ? new Error("The wicket stopped answering.") : error);
      // the next request is answered at once, not held: it tells the link is back
      state.revision = null;
      await sleep(POLL_RETRY_MS);
    }
    finally {
      clearTimeout(abort);
    }
  }
}

// relative times age without a new listing
setInterval(() => {
  for (const time of document.querySelectorAll("time[data-relative]")) {
    time.textContent = relativeTime(time.dateTime);
  }
}, 30000);

poll();
