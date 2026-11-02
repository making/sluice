// What the wickets' consoles share: DOM helpers, the API's requests (an XMLHttpRequest for an
// upload's progress), the progress and outcome views, and the long poll following the routes.
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
