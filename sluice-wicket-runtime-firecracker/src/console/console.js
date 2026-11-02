// The Firecracker wicket's console: lists the routes, their VMs and the VMs' budget
// (long-polled, and refreshed for the VM counts), deploys a rootfs image dropped on the page
// or chosen from a file, and replaces, reconfigures or removes the routes deployed here.
// Every string from the wicket is put in the DOM as text, never as markup.

import {
  $, HOST, OK_OUTCOME_MS, announce, changed, field, follow, formatMs, h, humanBytes, joined,
  normalizeHost, outcomeView, parseJson, parseListen, progressView, reachView, region, send, sep, slot,
  spoken, suggestHost, timeView, titleOf,
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

const ORIGINS = {
  console: { title: "Console", note: "drop an .ext4 image on a row to replace its rootfs" },
  config: { title: "Config file", note: "read-only here: edit the file, the wicket reloads it" },
  flag: { title: "--vm flags", note: "read-only: set on the command line" },
};

/** What routes `route` instead of this page, as the end of "routed by …". */
const routedBy = (route) => (route.origin === "config" ? "the config file" : "the --vm flags");

// the ext2/3/4 superblock's magic: 0xEF53, little-endian, at byte 1080
const MAGIC_AT = 1080;

// the VM counts move without a new listing: a plain one this often while the page is seen
const REFRESH_MS = 5000;

// ------------------------------------------------------------------------- helpers

/** The head of `file` checked before it is sent: {error} keeps it from being sent at all. */
async function sniff(file) {
  const max = state.listing?.max_rootfs_bytes;
  if (max && file.size > max) {
    return { error: `${file.name} is ${humanBytes(file.size)}, more than the ${humanBytes(max)} the console accepts.` };
  }
  if (file.size === 0) {
    return { error: `${file.name} is empty.` };
  }
  let magic;
  try {
    magic = new Uint8Array(await file.slice(MAGIC_AT, MAGIC_AT + 2).arrayBuffer());
  }
  catch {
    return {};
  }
  if (!(magic.length === 2 && magic[0] === 0x53 && magic[1] === 0xef)) {
    return { error: `${file.name} is not an ext4 image: it has no ext2/3/4 superblock at byte 1024.` };
  }
  return {};
}

const plural = (count, one, many = `${one}s`) => `${count} ${count === 1 ? one : many}`;

const shortDigest = (sha256) => sha256.slice(0, 12);

// ---------------------------------------------------------------------------- state

const state = {
  listing: null,
  /** host -> {phase: "uploading" | "booting" | "working", progress, total, text} */
  busy: new Map(),
  /** host -> {kind: "ok" | "fault", title, summary, detail} */
  outcomes: new Map(),
  /** host -> {kind, row, fields}: the open editors */
  editors: new Map(),
  /** {host, until}: the row marked as just deployed */
  fresh: null,
  /** requests carrying an image, which keep the intake's wicket open */
  inFlight: 0,
  /** the file the deploy form is about */
  pending: null,
};

const routeOf = (host) => state.listing?.routes.find((route) => route.host === host);

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

let fieldsets = 0;

/**
 * The option fields of a route, `initial` filled in: the mode as a choice, then the options
 * the mode takes (the others hidden), then the egress. `read()` returns the options, or
 * throws (marking the field) when a value is out of range.
 */
function optionFields(initial, onChange) {
  const id = `options-${++fieldsets}`;
  const radios = new Map();
  const modes = h("div", { class: "field__control caps", role: "radiogroup", "aria-labelledby": `${id}-mode` });
  for (const mode of MODES) {
    const radio = h("input", { type: "radio", name: `${id}-mode`, id: `${id}-${mode}`, value: mode });
    radio.checked = initial.mode === mode;
    radios.set(mode, radio);
    modes.append(h("label", { class: "cap", for: radio.id },
      radio,
      h("span", { class: "cap__name" }, mode),
      h("span", { class: "cap__hint" }, MODE_HINTS[mode])));
  }

  const jailed = Boolean(state.listing?.jailed);
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
    const element = h("div", { class: "limit", "data-option": number.key },
      h("label", { for: input.id }, number.label),
      h("div", { class: "limit__control" }, input, number.unit ? h("span", { class: "limit__unit" }, number.unit) : null));
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

  const fail = (input, message) => {
    input.setAttribute("aria-invalid", "true");
    note.textContent = message;
    input.focus();
    throw new Error(message);
  };

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

// --------------------------------------------------------------------- the intake

const intake = $("[data-testid=drop-zone]");

/** The drawing's wicket: open while a file is dragged over the page or an image is on its way. */
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
  const file = files.find((candidate) => /\.(ext4|img)$/i.test(candidate.name)) ?? files[0];
  const several = files.length > 1 ? `Only one image at a time: ${file.name} was taken.` : null;
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
  options: null,
  /** the user changed the options: a host matching a console route no longer refills them */
  touched: false,
  /** the file's check: {error} or {} */
  check: {},
  sending: false,
};

function openDeploy(file, notice = null) {
  if (deploy.sending) {
    announce("A deploy is in progress; drop the next image once it finished.");
    return;
  }
  state.pending = file;
  deploy.check = {};
  deploy.touched = false;
  field("deploy-file").textContent = file.name;
  deploy.host.value = suggestHost(file.name, ".ext4", "rootfs");
  deploy.host.removeAttribute("aria-invalid");
  const existing = routeOf(deploy.host.value);
  deploy.options = optionFields(existing?.origin === "console" ? optionsOf(existing) : defaultOptions(), () => {
    deploy.touched = true;
  });
  slot("options").replaceChildren(deploy.options.element);
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
  deploy.meta.replaceChildren(...joined([humanBytes(file.size), error ?? "an ext4 rootfs image", notice]));
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
    note = "Replaces the rootfs and options of this console route: its VMs start anew.";
    kind = "replace";
  }
  else if (route) {
    note = `Routed by ${routedBy(route)}: read-only here.`;
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
  // a host of a console route brings its options along, any other host the defaults, unless
  // the user set some already
  const route = routeOf(normalizeHost(deploy.host.value));
  if (!deploy.touched) {
    deploy.options.set(route?.origin === "console" ? optionsOf(route) : defaultOptions());
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
  let options;
  try {
    options = deploy.options.read();
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
    const answer = await send("PUT", host, [`name=${encodeURIComponent(file.name)}`, ...optionPairs(options)], file,
      (fraction, sent) => (sent ? progress.working("Booting its VMs and reloading the routes…") : progress.upload(fraction, file.size)));
    state.fresh = { host, until: Date.now() + 4000 };
    state.pending = null;
    deploy.fields.hidden = true;
    const close = h("button", { type: "button", class: "button", onclick: closeDeploy }, "Close");
    const another = h("button", { type: "button", class: "link-button", onclick: () => intakeFile.click() }, "deploy another image");
    const reach = reachView(host, state.listing?.listen);
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

/** Deploys `file` to the console route `route`, keeping its options. */
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
    const answer = await send("PUT", host, [`name=${encodeURIComponent(file.name)}`, ...optionPairs(optionsOf(route))], file, (fraction, sent) => {
      const busy = state.busy.get(host);
      if (sent) {
        busy.phase = "booting";
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
  if (!confirm(`Remove ${host}?\n\nThe route is withdrawn at once, its VMs end once the server let go of it, and its image is deleted unless another console route uses it.`)) {
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
  if (state.editors.get(host)?.kind === "options") {
    state.editors.delete(host);
    render();
    focusAction(host, "edit");
    return;
  }
  const fields = optionFields(optionsOf(route));
  const outcome = h("div");
  const save = h("button", { type: "submit", class: "button button--primary" }, "Save options");
  const cancel = h("button", { type: "button", class: "button", onclick: () => toggleEditor(route) }, "Cancel");
  const form = h("form", { class: "editor", novalidate: true, "aria-label": `Options of ${host}` },
    fields.element, h("div", { class: "editor__actions" }, save, cancel), outcome);
  form.addEventListener("keydown", (event) => {
    if (event.key === "Escape" && !cancel.disabled) {
      toggleEditor(route);
    }
  });
  form.addEventListener("submit", async (event) => {
    event.preventDefault();
    let options;
    try {
      options = fields.read();
    }
    catch {
      return;
    }
    save.disabled = true;
    cancel.disabled = true;
    const progress = progressView();
    progress.working("Booting its VMs and reloading the routes…");
    outcome.replaceChildren(progress.element);
    progress.focus();
    try {
      const answer = await send("PATCH", host, optionPairs(options), null);
      state.editors.delete(host);
      state.fresh = { host, until: Date.now() + 4000 };
      settle(host, changed(answer));
      announce(spoken(changed(answer)));
      render();
      focusAction(host, "edit");
    }
    catch (error) {
      outcome.replaceChildren(outcomeView({ kind: "fault", title: `${titleOf(error)}:`, summary: "the options stay as they were.", detail: error.message }));
      announce(`Changing the options of ${host} failed: ${error.message}`);
      save.disabled = false;
      cancel.disabled = false;
      save.focus();
    }
  });
  const row = h("tr", { class: "route__editor", "data-host": host }, h("td", { colspan: 4 }, form));
  state.editors.set(host, { kind: "options", row, fields });
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
    const by = taken.origin === "console" ? "a route of the console already" : `routed by ${routedBy(taken)}`;
    return { text: `${to} is ${by}.`, kind: "fault", blocked: true };
  }
  return { text: `Served at ${to} at once, by the same VMs.`, kind: "", blocked: false };
}

/** Moves the console route `route` to another host as it is: its rootfs, options and VMs. */
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
  // signed in through the OpenID Provider: the masthead names the user
  const session = $(".masthead__user");
  session.hidden = !listing.user;
  field("user").textContent = listing.user ?? "";
  field("generation").textContent = String(listing.generation);
  field("route-count").textContent = String(listing.routes.length);
  const listen = field("listen");
  listen.textContent = listing.listen ?? "off";
  listen.title = listing.listen ?? "no --listen";
  const server = field("server");
  server.textContent = listing.server ?? "local mode";
  server.title = listing.server ?? "no --server: local mode";
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
  field("opening").textContent = `clear opening ≤ ${humanBytes(listing.max_rootfs_bytes)}`;
  field("limit").textContent = humanBytes(listing.max_rootfs_bytes);

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
      "No routes yet. Drop a rootfs image above to deploy the first one.")));
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
      h("span", { class: "decision" }, plural(routes.length, "route"), sep(), label.note));
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
  const signature = JSON.stringify([route, state.busy.has(route.host), state.editors.get(route.host)?.kind ?? null, fresh, state.listing.listen]);
  const cached = rows.get(route.host);
  if (cached?.signature === signature) {
    return cached.row;
  }
  const classes = ["route", state.busy.has(route.host) ? "is-busy" : "", fresh ? "is-fresh" : ""].filter(Boolean).join(" ");
  const row = h("tr", { class: classes, "data-host": route.host, "data-origin": route.origin, "data-testid": "route" },
    h("td", {}, hostCell(route)),
    h("td", {}, rootfsCell(route)),
    h("td", {}, modeCell(route)),
    h("td", {}, actionsCell(route)));
  rows.set(route.host, { signature, row });
  return row;
}

function hostCell(route) {
  return [h("span", { class: "route__host mono" }, route.host), reachView(route.host, state.listing?.listen)];
}

function rootfsCell(route) {
  const { rootfs } = route;
  return [
    h("span", { class: "route__component mono", title: rootfs.path }, rootfs.name),
    h("span", { class: "route__meta" }, joined([
      rootfs.sha256 ? h("span", { class: "route__digest mono", title: `sha256 ${rootfs.sha256}` }, shortDigest(rootfs.sha256)) : null,
      h("span", { class: "num" }, humanBytes(rootfs.bytes)),
    ])),
    route.deployed ? h("span", { class: "route__deployed" }, "deployed ", timeView(route.deployed)) : null,
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
  return [h("div", { class: "route__sandbox" }, parts), vmsView(route)];
}

/** What a route's VMs are: alive, then by mode, its connections, the template's snapshot. */
function vmsView(route) {
  const { vms, snapshot } = route;
  const counts = [h("span", { class: "num", title: "its VMs alive, as the budget counts them" }, plural(vms.alive, "VM"))];
  if (route.mode === "shared") {
    counts.push(vms.shared ? "up" : "down");
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
  if (snapshot) {
    lines.push(h("span", { class: "route__meta", title: `snapshot taken ${snapshot.at}` }, joined([
      `snapshot ${humanBytes(snapshot.memory_bytes)}`,
      `booted in ${formatMs(snapshot.booted_ms)}`,
      `taken after ${formatMs(snapshot.taken_ms)}`,
    ])));
  }
  return lines;
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
      title: "Deploy another image to this host, keeping its options",
      onclick: () => {
        replaceHost = route.host;
        replaceFile.click();
      },
    }, "Replace…"),
    h("button", {
      type: "button", class: "button button--small", "data-action": "edit", disabled: busy,
      "aria-expanded": String(editing === "options"), onclick: () => toggleEditor(route),
    }, "Edit options"),
    h("button", {
      type: "button", class: "button button--small", "data-action": "rename", disabled: busy,
      title: "Serve this route at another host, keeping its image, options and VMs",
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
        progress.working(busy.text ?? "Booting its VMs and reloading the routes…");
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

follow((listing, moved) => {
  const counts = JSON.stringify(state.listing && [state.listing.budget, state.listing.routes.map((route) => [route.vms, route.snapshot])]);
  state.listing = listing;
  if (moved || counts !== JSON.stringify([listing.budget, listing.routes.map((route) => [route.vms, route.snapshot])])) {
    render();
  }
});

/**
 * The VMs' counts and the budget move without a new revision: a plain listing every
 * REFRESH_MS while the page is seen, rendered where they moved (a row rebuilt only when its
 * route changed: an open editor stays as it is).
 */
async function refresh() {
  if (document.visibilityState !== "visible" || state.listing === null) {
    return;
  }
  try {
    const response = await fetch("/api/routes", { cache: "no-store", headers: { Accept: "application/json" } });
    if (!response.ok) {
      return;
    }
    const listing = parseJson(await response.text());
    // a listing older than the one held is no news
    if (!listing || listing.revision < state.listing.revision) {
      return;
    }
    state.listing = listing;
    render();
  }
  catch {
    // the long poll tells the link is lost
  }
}

setInterval(refresh, REFRESH_MS);
