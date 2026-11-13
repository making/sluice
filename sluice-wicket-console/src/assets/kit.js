// What the wickets' consoles share: DOM helpers, the API's requests (an XMLHttpRequest for an
// upload's progress), the progress and outcome views, the long poll following the routes, and
// the page's flow (`consolePage`): the intake and drag and drop, the deploy form, the row
// editors, the route operations and the rendering, a wicket's script supplying its fields, its
// file check, its row cells and its wording.
// Every string from the wicket is put in the DOM as text, never as markup.

// dot-separated labels of a-z, 0-9 and '-', none starting or ending with '-'
export const HOST = /^(?=.{1,253}$)[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)*$/;

const POLL_RETRY_MS = 2000;
// the wicket holds a poll for about 25s; a longer silence is a dead link
const POLL_ABORT_MS = 40000;
export const OK_OUTCOME_MS = 10000;

/** An element with attributes (functions become listeners) and text or element children. */
export function h(tag, props = {}, ...children) {
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

export const $ = (selector, root = document) => root.querySelector(selector);
export const field = (name) => $(`[data-field="${name}"]`);
export const slot = (name) => $(`[data-slot="${name}"]`);
export const region = (name) => $(`[data-region="${name}"]`);
export const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

export function humanBytes(bytes) {
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
export const sep = () => h("span", { class: "sep" }, h("span", { class: "visually-hidden" }, ", "));

/** `parts` with a separator between each two. */
export const joined = (parts) => parts.filter(Boolean).flatMap((part, index) => (index ? [sep(), part] : [part]));

export const plural = (count, one, many = `${one}s`) => `${count} ${count === 1 ? one : many}`;

export const formatMs = (ms) => (ms % 1000 === 0 ? `${ms / 1000} s` : `${ms} ms`);

export const relativeFormat = new Intl.RelativeTimeFormat("en", { numeric: "auto" });

export function relativeTime(iso) {
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
export function timeView(iso) {
  const at = new Date(iso);
  const title = Number.isNaN(at.getTime()) ? iso : `${at.toLocaleString()} (${iso})`;
  return h("time", { datetime: iso, title, "data-relative": true }, relativeTime(iso));
}

/** `fileName` less `extension` as a host ("hello.wasm" -> "hello.localhost", which browsers resolve to loopback); `fallback` for a name of nothing else. */
export function suggestHost(fileName, extension, fallback) {
  const label = fileName
    .slice(0, fileName.toLowerCase().endsWith(extension) ? -extension.length : undefined)
    .toLowerCase()
    .replace(/[^a-z0-9-]+/g, "-")
    .replace(/-{2,}/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 63)
    .replace(/-+$/, "");
  return `${label || fallback}.localhost`;
}

export const normalizeHost = (value) => value.trim().toLowerCase();

/** `listen` ("127.0.0.1:8080", "[::]:8080") split into a URL host and a port. */
export function parseListen(listen) {
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

export const urlHost = (host) => (host.includes(":") && !host.startsWith("[") ? `[${host}]` : host);

export const loopbackPage = () => ["localhost", "127.0.0.1", "[::1]", "::1"].includes(location.hostname);

export const shellQuote = (text) => `'${text.replaceAll("'", "'\\''")}'`;

export function announce(text) {
  const announcer = region("announcer");
  announcer.textContent = "";
  // a new node in the next frame, so the same text is announced again
  requestAnimationFrame(() => {
    announcer.textContent = text;
  });
}

export class ApiError extends Error {
  constructor(status, message) {
    super(message);
    this.status = status;
  }
}

export const TITLES = {
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

export const titleOf = (error) => TITLES[error.status] ?? `Failed (HTTP ${error.status})`;

export function errorMessage(status, answer, text) {
  if (typeof answer?.error === "string" && answer.error) {
    return answer.error;
  }
  if (status === 401) {
    return "Not signed in: the token was refused or is missing, or the session ended. Reload the page to sign in again.";
  }
  return text ? `HTTP ${status}: ${text.slice(0, 500)}` : `HTTP ${status}`;
}

export function parseJson(text) {
  try {
    return JSON.parse(text);
  }
  catch {
    return null;
  }
}

/**
 * `method` on the route of `host` with an XMLHttpRequest (fetch reports no upload
 * progress). `onUpload(fraction, sent)` follows the body, of `type`; `sent` once all of it
 * left, when the wicket loads and reloads before it answers.
 */
export function send(method, host, pairs, body, onUpload, type = "application/octet-stream") {
  return new Promise((resolve, reject) => {
    const xhr = new XMLHttpRequest();
    const query = pairs.length ? `?${pairs.join("&")}` : "";
    xhr.open(method, `/api/routes/${encodeURIComponent(host)}${query}`);
    xhr.setRequestHeader("Accept", "application/json");
    if (body) {
      xhr.setRequestHeader("Content-Type", type);
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
export function changed(answer) {
  const message = answer.message || "done";
  return {
    kind: "ok",
    title: `${message.charAt(0).toUpperCase()}${message.slice(1)}.`,
    summary: answer.generation !== undefined ? `Generation ${answer.generation}.` : null,
  };
}

export const spoken = (outcome) => [outcome.title, outcome.summary].filter(Boolean).join(" ");

/** The progress of one request: an upload bar, then an indeterminate run. */
export function progressView() {
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
export function outcomeView(outcome, ...actions) {
  return h("div", { class: `outcome outcome--${outcome.kind}` },
    h("p", {}, h("span", { class: "outcome__title" }, outcome.title), outcome.summary ? ` ${outcome.summary}` : null),
    outcome.extra ?? null,
    outcome.detail ? h("pre", { class: "outcome__error" }, outcome.detail) : null,
    actions.length ? h("p", { class: "outcome__actions" }, actions) : null);
}

/** How to reach `host` through `address` (`--listen`): a link where the browser resolves it, else a curl line. */
export function reachView(host, address) {
  const listen = parseListen(address);
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

export async function copyText(text, code, button) {
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

export function setLink(live, error = null) {
  document.documentElement.dataset.link = live ? "live" : "lost";
  const lost = $(".link__lost");
  lost.title = error ? error.message : "";
}

/**
 * Follows the routes for good: the wicket answers at once when the listing moved past
 * `after`, else holds the poll. `onListing(listing, moved)` takes each listing, `moved` when
 * its revision is not the last one's.
 */
export async function follow(onListing) {
  let revision = null;
  let first = true;
  for (;;) {
    const controller = new AbortController();
    const abort = setTimeout(() => controller.abort(), POLL_ABORT_MS);
    try {
      const url = revision === null ? "/api/routes" : `/api/routes?after=${encodeURIComponent(revision)}`;
      const response = await fetch(url, { cache: "no-store", signal: controller.signal, headers: { Accept: "application/json" } });
      if (!response.ok) {
        const text = await response.text();
        throw new ApiError(response.status, errorMessage(response.status, parseJson(text), text));
      }
      const listing = await response.json();
      setLink(true);
      const moved = listing.revision !== revision || first;
      first = false;
      revision = listing.revision;
      onListing(listing, moved);
    }
    catch (error) {
      setLink(false, error.name === "AbortError" ? new Error("The wicket stopped answering.") : error);
      // the next request is answered at once, not held: it tells the link is back
      revision = null;
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

// ------------------------------------------------------------- what the fields share

let ids = 0;

/** An id of its own: fields of one form name their parts after it. */
export const nextId = (prefix) => `${prefix}-${++ids}`;

/** A number of the form's limits: its label, its input, its unit. */
export const limitView = (input, label, unit, props = {}) => h("div", { class: "limit", ...props },
  h("label", { for: input.id }, label),
  h("div", { class: "limit__control" }, input, unit ? h("span", { class: "limit__unit" }, unit) : null));

/** A checkbox or radio of the form: its name and what it does. */
export const choiceView = (input, name, hint) => h("label", { class: "cap", for: input.id },
  input,
  h("span", { class: "cap__name" }, name),
  h("span", { class: "cap__hint" }, hint));

/** Refuses a field's value: marks `input`, says why in `note` and throws, out of the fields' `read()`. */
export function refuse(input, note, message) {
  input.setAttribute("aria-invalid", "true");
  note.textContent = message;
  input.focus();
  throw new Error(message);
}

/** The names of a guest's environment variables, or null when it has none. */
export const envView = (names) => (names?.length
  ? h("span", { class: "route__env mono", title: "the guest's environment variables (their values are never shown)" }, `env ${names.join(", ")}`)
  : null);

export const deployedView = (route) => (route.deployed ? h("span", { class: "route__deployed" }, "deployed ", timeView(route.deployed)) : null);

const percent = (cpu) => `${(cpu * 100).toFixed(1)}%`;

/**
 * What a route's instances alive (`alive`, as a listing has them) use of the host: their CPU over
 * the last seconds and their memory, summed, the most one held, and their disks'; each one's in
 * the title. Null before any was measured.
 */
export function usageView(alive) {
  const measured = (alive ?? []).filter((instance) => instance.usage);
  if (!measured.length) {
    return null;
  }
  const disk = (usage) => usage.disk_bytes ?? null;
  const total = (of) => measured.reduce((sum, { usage }) => sum + (of(usage) ?? 0), 0);
  const peak = Math.max(...measured.map(({ usage }) => usage.memory_peak_bytes));
  const each = measured.map(({ index, usage }) => [
    `instance ${index}: ${percent(usage.cpu)} CPU`,
    `${humanBytes(usage.memory_bytes)} of memory, at most ${humanBytes(usage.memory_peak_bytes)}`,
    disk(usage) === null ? null : `${humanBytes(disk(usage))} of its disk`,
  ].filter(Boolean).join(", "));
  return h("span", { class: "route__meta", "data-testid": "usage", title: each.join("\n") }, joined([
    h("span", { class: "num" }, `${percent(total((usage) => usage.cpu))} CPU`),
    h("span", { class: "num" }, `${humanBytes(total((usage) => usage.memory_bytes))} of memory`),
    measured.length > 1 ? `the most one held ${humanBytes(peak)}` : `at most ${humanBytes(peak)}`,
    measured.some(({ usage }) => disk(usage) !== null) ? `${humanBytes(total(disk))} of disk` : null,
  ]));
}

/**
 * Follows the listing as `follow` does, and, as what `moving` returns of one moves without a new
 * revision (the instances, what they use), fetches a plain listing every `ms` while the page is
 * seen, rendered where it moved (a row rebuilt only when its route changed: an open editor stays
 * as it is).
 */
export function followMoving(moving, ms) {
  const moved = (listing) => JSON.stringify(listing && moving(listing));
  follow((listing, changed) => {
    showListing(listing, changed || moved(currentListing()) !== moved(listing));
  });
  setInterval(async () => {
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
      showListing(listing, moved(currentListing()) !== moved(listing));
    }
    catch {
      // the long poll tells the link is lost
    }
  }, ms);
}

// ---------------------------------------------------------------------------- state

/** What the wicket's script supplies: see `consolePage`. */
let page = null;

const state = {
  listing: null,
  /** host -> {phase: "uploading" | "loading" | "working", progress, total, text} */
  busy: new Map(),
  /** host -> {kind: "ok" | "fault", title, summary, detail} */
  outcomes: new Map(),
  /** host -> {kind: "settings" | "rename", row, fields, refresh}: the open editors */
  editors: new Map(),
  /** {host, until}: the row marked as just deployed */
  fresh: null,
  /** requests carrying a file, which keep the intake's wicket open */
  inFlight: 0,
  /** the file the deploy form is about */
  pending: null,
};

/** The listing of the moment; null until the first. */
export const currentListing = () => state.listing;

/** Takes `listing`, rendered when `moved`. */
export function showListing(listing, moved = true) {
  state.listing = listing;
  if (moved) {
    render();
  }
}

const routeOf = (host) => state.listing?.routes.find((route) => route.host === host);

/** The settings a deploy over `route` starts from: a console route's, else the defaults. */
const settingsOver = (route) => (route?.origin === "console" ? page.settings.of(route) : page.settings.initial());

const HOST_RULE = "Not a host name: dot-separated labels of a-z, 0-9 and '-', none starting or ending with '-'.";

/** What routes `route` instead of this page, as the end of "routed by …". */
function routedBy(route) {
  switch (route.origin) {
    case "config":
      return "the config file";
    case "api":
      return `the app '${route.app}' of ${route.owner}`;
    default:
      return `the ${page.flags} flags`;
  }
}

function originLabel(origin) {
  const labels = {
    console: { title: "Console", note: page.words.onRow },
    config: { title: "Config file", note: "read-only here: edit the file, the wicket reloads it" },
    flag: { title: `${page.flags} flags`, note: "read-only: set on the command line" },
    api: { title: "Apps", note: "read-only here: a controller puts them through /api/apps" },
  };
  return labels[origin] ?? { title: origin, note: "" };
}

/** The file's check before it is sent: {error} keeps it from being sent at all. */
async function checkFile(file) {
  const max = state.listing?.[page.limit];
  if (max && file.size > max) {
    return { error: `${file.name} is ${humanBytes(file.size)}, more than the ${humanBytes(max)} the console accepts.` };
  }
  if (file.size === 0) {
    return { error: `${file.name} is empty.` };
  }
  let head;
  try {
    head = new Uint8Array(await file.slice(...page.head).arrayBuffer());
  }
  catch {
    return {};
  }
  return page.sniff(file, head);
}

// --------------------------------------------------------------------- the intake

const intake = $("[data-testid=drop-zone]");

/** The drawing's wicket: open while a file is dragged over the page or a file is on its way. */
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

function onDrop(event) {
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
  const file = files.find((candidate) => page.accept.test(candidate.name)) ?? files[0];
  const several = files.length > 1 ? `Only one ${page.words.one} at a time: ${file.name} was taken.` : null;
  if (target) {
    replace(routeOf(target.dataset.host), file);
  }
  else if (refusing) {
    announce(`${refusing.dataset.host} is read-only here.`);
  }
  else {
    openDeploy(file, several);
  }
}

const intakeFile = region("intake-file");

// ------------------------------------------------------------------ the deploy form

const deploy = {
  section: region("deploy"),
  form: region("deploy-form"),
  fields: region("deploy-fields"),
  host: $("#deploy-host"),
  note: field("host-note"),
  meta: field("deploy-meta"),
  submit: $("[data-action=deploy]"),
  settings: null,
  /** the user changed the settings: a host matching a console route no longer refills them */
  touched: false,
  /** the file's check: {error} or {} */
  check: {},
  sending: false,
};

function openDeploy(file, notice = null) {
  if (deploy.sending) {
    announce(`A deploy is in progress; drop the next ${page.words.one} once it finished.`);
    return;
  }
  state.pending = file;
  deploy.check = {};
  deploy.touched = false;
  field("deploy-file").textContent = file.name;
  deploy.host.value = suggestHost(file.name, page.extension, page.fallback);
  deploy.host.removeAttribute("aria-invalid");
  deploy.settings = page.settings.fields(settingsOver(routeOf(deploy.host.value)), () => {
    deploy.touched = true;
  });
  slot(page.settings.noun).replaceChildren(deploy.settings.element);
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
  checkFile(file).then((check) => {
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
  deploy.meta.replaceChildren(...joined([humanBytes(file.size), error ?? page.words.kind, notice]));
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
    note = HOST_RULE;
    kind = "fault";
    blocked = true;
  }
  else if (route?.origin === "console") {
    note = page.words.replaces;
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

async function submitDeploy(event) {
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
  let settings;
  try {
    settings = deploy.settings.read();
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
    const answer = await send("PUT", host, [`name=${encodeURIComponent(file.name)}`, ...page.settings.pairs(settings)], file,
      (fraction, sent) => (sent ? progress.working(page.words.loading) : progress.upload(fraction, file.size)), page.contentType);
    state.fresh = { host, until: Date.now() + 4000 };
    state.pending = null;
    deploy.fields.hidden = true;
    const close = h("button", { type: "button", class: "button", onclick: closeDeploy }, "Close");
    const another = h("button", { type: "button", class: "link-button", onclick: () => intakeFile.click() }, `deploy another ${page.words.one}`);
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
}

// --------------------------------------------------------------- route operations

const replaceFile = region("replace-file");
let replaceHost = null;

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

/** Deploys `file` to the console route `route`, keeping its settings. */
async function replace(route, file) {
  if (!route || route.origin !== "console" || state.busy.has(route.host)) {
    return;
  }
  const { host } = route;
  // the row's buttons are disabled while it is busy: the focus comes back once it is done
  const refocus = focusIn(host);
  state.outcomes.delete(host);
  const check = await checkFile(file);
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
    const answer = await send("PUT", host, [`name=${encodeURIComponent(file.name)}`, ...page.settings.pairs(page.settings.of(route))], file, (fraction, sent) => {
      const busy = state.busy.get(host);
      if (sent) {
        busy.phase = "loading";
        render();
      }
      else {
        busy.progress = fraction;
        rowStatuses.get(host)?.progress?.upload(fraction, file.size);
      }
    }, page.contentType);
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
  if (!confirm(`Remove ${host}?\n\n${page.removal(route)}`)) {
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

/** The form of a row's editor, sent to `submit`; Escape presses `cancel` while it may be pressed. */
function editorForm(label, cancel, submit, ...children) {
  const form = h("form", { class: "editor", novalidate: true, "aria-label": label }, ...children);
  form.addEventListener("keydown", (event) => {
    if (event.key === "Escape" && !cancel.disabled) {
      cancel.click();
    }
  });
  form.addEventListener("submit", submit);
  return form;
}

const editorRow = (host, form) => h("tr", { class: "route__editor", "data-host": host }, h("td", { colspan: 4 }, form));

function toggleEditor(route) {
  const { host } = route;
  const { noun, unchanged, fields: fieldsOf, of, pairs } = page.settings;
  if (state.editors.get(host)?.kind === "settings") {
    state.editors.delete(host);
    render();
    focusAction(host, "edit");
    return;
  }
  const fields = fieldsOf(of(route));
  const outcome = h("div");
  const save = h("button", { type: "submit", class: "button button--primary" }, `Save ${noun}`);
  const cancel = h("button", { type: "button", class: "button", onclick: () => toggleEditor(route) }, "Cancel");
  const form = editorForm(`${noun.charAt(0).toUpperCase()}${noun.slice(1)} of ${host}`, cancel, async (event) => {
    event.preventDefault();
    let settings;
    try {
      settings = fields.read();
    }
    catch {
      return;
    }
    save.disabled = true;
    cancel.disabled = true;
    const progress = progressView();
    progress.working(page.words.saving);
    outcome.replaceChildren(progress.element);
    progress.focus();
    try {
      const answer = await send("PATCH", host, pairs(settings), null);
      state.editors.delete(host);
      state.fresh = { host, until: Date.now() + 4000 };
      settle(host, changed(answer));
      announce(spoken(changed(answer)));
      render();
      focusAction(host, "edit");
    }
    catch (error) {
      outcome.replaceChildren(outcomeView({ kind: "fault", title: `${titleOf(error)}:`, summary: unchanged, detail: error.message }));
      announce(`Changing the ${noun} of ${host} failed: ${error.message}`);
      save.disabled = false;
      cancel.disabled = false;
      save.focus();
    }
  }, fields.element, h("div", { class: "editor__actions" }, save, cancel), outcome);
  state.editors.set(host, { kind: "settings", row: editorRow(host, form), fields });
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
    return { text: HOST_RULE, kind: "fault", blocked: true };
  }
  const taken = routeOf(to);
  if (taken) {
    const by = taken.origin === "console" ? "a route of the console already" : `routed by ${routedBy(taken)}`;
    return { text: `${to} is ${by}.`, kind: "fault", blocked: true };
  }
  return { ...page.renamed(route, to), blocked: false };
}

/** Moves the console route `route` to another host as it is. */
function toggleRename(route) {
  const { host } = route;
  if (state.editors.get(host)?.kind === "rename") {
    state.editors.delete(host);
    render();
    focusAction(host, "rename");
    return;
  }
  const id = nextId("rename");
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
  const form = editorForm(`Rename ${host}`, cancel, async (event) => {
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
  },
  h("div", { class: "field field--host" },
    h("label", { for: id }, "New host"),
    h("div", { class: "field__control" }, input, note)),
  h("div", { class: "editor__actions" }, save, cancel),
  outcome);
  state.editors.set(host, { kind: "rename", row: editorRow(host, form), refresh: check });
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
  page.status?.(listing);
  field("opening").textContent = `clear opening ≤ ${humanBytes(listing[page.limit])}`;
  field("limit").textContent = humanBytes(listing[page.limit]);

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
      `No routes yet. Drop ${page.words.first} above to deploy the first one.`)));
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
    const label = originLabel(origin);
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
  // the defaults, of a wicket listing them: its cells tell a route's values against them
  const signature = JSON.stringify([route, state.busy.has(route.host), state.editors.get(route.host)?.kind ?? null, fresh, state.listing.listen, state.listing.defaults]);
  const cached = rows.get(route.host);
  if (cached?.signature === signature) {
    return cached.row;
  }
  const classes = ["route", state.busy.has(route.host) ? "is-busy" : "", fresh ? "is-fresh" : ""].filter(Boolean).join(" ");
  const row = h("tr", { class: classes, "data-host": route.host, "data-origin": route.origin, "data-testid": "route" },
    h("td", {}, hostCell(route)),
    page.cells.map((cell) => h("td", {}, cell(route))),
    h("td", {}, actionsCell(route)));
  rows.set(route.host, { signature, row });
  return row;
}

function hostCell(route) {
  const name = route.host === ""
    ? h("span", { class: "route__host catch-all" }, "any host")
    : h("span", { class: "route__host mono" }, route.host);
  const app = route.origin === "api"
    ? h("span", { class: "route__app" }, "app ", h("span", { class: "mono" }, route.app), " of ", route.owner,
      route.held ? ", held until its owner confirms it" : null)
    : null;
  return [name, app, reachView(route.host, state.listing?.listen)];
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
      title: `Deploy another ${page.words.one} to this host, keeping its ${page.settings.noun}`,
      onclick: () => {
        replaceHost = route.host;
        replaceFile.click();
      },
    }, "Replace…"),
    h("button", {
      type: "button", class: "button button--small", "data-action": "edit", disabled: busy,
      "aria-expanded": String(editing === "settings"), onclick: () => toggleEditor(route),
    }, `Edit ${page.settings.noun}`),
    h("button", {
      type: "button", class: "button button--small", "data-action": "rename", disabled: busy,
      title: `Serve this route at another host, keeping its ${page.words.keeps}`,
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
        progress.working(busy.text ?? page.words.loading);
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

/**
 * Runs the page's flow; the wicket's script then feeds it listings (`follow(showListing)`).
 * `wicket` supplies what differs between the wickets:
 * - `limit`: the listing's key of the most bytes the console takes
 * - `head`, `sniff(file, head)`: the range of a file's bytes read, and its check of them
 *   ({error} keeps the file from being sent), after the size's
 * - `accept`, `extension`, `fallback`, `contentType`: the name of a file to take among several,
 *   what a host suggested from a name drops and takes for a name of nothing else, the type sent
 * - `flags`: the command line's flag naming a route
 * - `settings`: what a route is deployed with: `noun`, `unchanged` (a failed edit's "the … stay
 *   as …"), `of(route)`, `initial()`, `pairs(settings)` (as query pairs), and
 *   `fields(initial, onChange)` (`{element, read() (or throws: `refuse`), set(settings), focus()}`);
 *   the deploy form's are in the slot named `noun`
 * - `cells`: a route's cells between its host and its actions
 * - `status(listing)` (optional): the title block's fields of its own
 * - `removal(route)`: what removing tells; `renamed(route, to)`: a free host's note, {text, kind}
 * - `words`: `one` ("component"), `first` ("a component"), `kind` ("a WebAssembly component"),
 *   `replaces`, `onRow` (the console group's note), `loading`, `saving` (the progress of a
 *   deploy once uploaded, of an edit), `keeps` (what a rename keeps)
 */
export function consolePage(wicket) {
  page = wicket;
  document.addEventListener("dragenter", onDrag);
  document.addEventListener("dragover", onDrag);
  document.addEventListener("drop", onDrop);
  $("[data-action=choose]").addEventListener("click", () => intakeFile.click());
  intakeFile.addEventListener("change", () => {
    const [file] = intakeFile.files;
    intakeFile.value = "";
    if (file) {
      openDeploy(file);
    }
  });
  deploy.host.addEventListener("input", () => {
    // a host of a console route brings its settings along, any other host the defaults, unless
    // the user set some already
    if (!deploy.touched) {
      deploy.settings.set(settingsOver(routeOf(normalizeHost(deploy.host.value))));
    }
    refreshDeploy();
  });
  deploy.form.addEventListener("keydown", (event) => {
    if (event.key === "Escape") {
      closeDeploy();
    }
  });
  $("[data-action=cancel]").addEventListener("click", closeDeploy);
  deploy.form.addEventListener("submit", submitDeploy);
  replaceFile.addEventListener("change", () => {
    const [file] = replaceFile.files;
    replaceFile.value = "";
    const route = routeOf(replaceHost);
    if (file && route) {
      replace(route, file);
    }
  });
}
