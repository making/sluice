// Drives a console page through its operations in jsdom and prints a transcript: the requests
// made, the prompts, and the page's DOM after each step. Run with --experimental-vm-modules:
// the scripts load as real modules (strict, `/kit.js` linked), so a name a page forgot to
// import fails here as it would in a browser. Timers and the clock are virtual: no step sleeps
// and two runs print the same. COVERAGE=1 lists on stderr the lines no step ran.
process.env.TZ = "UTC";
const fs = require("fs");
const vm = require("vm");
const inspector = require("inspector");
const { JSDOM, VirtualConsole } = require("jsdom");

const START = Date.parse("2026-11-02T02:00:00Z");
const out = (line) => process.stdout.write(`${line}\n`);

const immediate = () => new Promise((resolve) => setImmediate(resolve));

class Clock {
  constructor(window) {
    this.window = window;
    this.now = START;
    this.seq = 0;
    this.timers = new Map();
    window.eval(`(() => {
      const Real = Date;
      globalThis.__now = ${START};
      globalThis.Date = class extends Real {
        constructor(...args) { if (args.length) { super(...args); } else { super(globalThis.__now); } }
        static now() { return globalThis.__now; }
      };
    })()`);
    window.setTimeout = (fn, ms = 0) => this.add(fn, ms);
    window.setInterval = (fn, ms = 0) => this.add(fn, ms, ms);
    window.clearTimeout = (id) => this.timers.delete(id);
    window.clearInterval = (id) => this.timers.delete(id);
    window.requestAnimationFrame = (fn) => this.add(fn, 0);
  }

  add(fn, ms, every = null) {
    this.seq += 1;
    this.timers.set(this.seq, { at: this.now + ms, seq: this.seq, fn, every });
    return this.seq;
  }

  next(limit) {
    let first = null;
    for (const [id, timer] of this.timers) {
      if (timer.at <= limit && (!first || timer.at < first.timer.at || (timer.at === first.timer.at && timer.seq < first.timer.seq))) {
        first = { id, timer };
      }
    }
    return first;
  }

  runOne(limit) {
    const due = this.next(limit);
    if (!due) {
      return false;
    }
    this.now = due.timer.at;
    this.window.__now = this.now;
    if (due.timer.every) {
      due.timer.at += due.timer.every;
    }
    else {
      this.timers.delete(due.id);
    }
    due.timer.fn();
    return true;
  }
}

/** One page load with everything stubbed, and what a step does to it. */
async function open({ index, listing, kit, script, log, holdFirst }) {
  const html = fs.readFileSync(index, "utf8").replace(/<script[^>]*><\/script>/g, "");
  const virtualConsole = new VirtualConsole();
  virtualConsole.on("jsdomError", (error) => log(`! page error: ${error.detail?.stack ?? error.message}`));
  const dom = new JSDOM(html, { url: "http://127.0.0.1:9090/", runScripts: "outside-only", pretendToBeVisual: true, virtualConsole });
  const { window } = dom;
  const { document } = window;
  const clock = new Clock(window);

  // the wicket's listing, its long poll held until it moves
  let current = JSON.parse(JSON.stringify(listing));
  let holds = [];
  const reply = (status, body) => ({ ok: status < 400, status, json: async () => JSON.parse(body), text: async () => body });
  const answer = () => reply(200, JSON.stringify(current));
  // the plain listing answered otherwise, for one request (null: it fails)
  let plainAnswer;
  window.fetch = (url, init = {}) => {
    log(`> fetch ${url}`);
    const after = /[?&]after=([^&]*)/.exec(String(url));
    if (!after && plainAnswer !== undefined) {
      const [status, body] = plainAnswer ?? [];
      const planned = plainAnswer === null ? Promise.reject(new Error("connection reset")) : Promise.resolve(reply(status, body));
      plainAnswer = undefined;
      return planned;
    }
    const held = holdFirst || (after && decodeURIComponent(after[1]) === String(current.revision));
    holdFirst = false;
    if (!held) {
      return Promise.resolve(answer());
    }
    return new Promise((resolve, reject) => {
      const hold = { resolve, reject };
      holds.push(hold);
      init.signal?.addEventListener("abort", () => {
        holds = holds.filter((other) => other !== hold);
        reject(new window.DOMException("aborted", "AbortError"));
      });
    });
  };
  const release = (settle) => {
    const held = holds;
    holds = [];
    held.forEach(settle);
  };

  const pending = [];
  class FakeXHR {
    constructor() {
      this.handlers = {};
      this.uploadHandlers = {};
      this.headers = {};
      this.upload = { addEventListener: (name, fn) => { this.uploadHandlers[name] = fn; } };
    }

    open(method, url) { this.method = method; this.url = url; }

    setRequestHeader(name, value) { this.headers[name] = value; }

    addEventListener(name, fn) { this.handlers[name] = fn; }

    send(body) {
      const what = body ? ` body=File(${body.name}, ${body.size})` : "";
      const headers = Object.entries(this.headers).map(([key, value]) => `${key}=${value}`).join(" ");
      log(`> ${this.method} ${this.url} ${headers}${what}`);
      pending.push(this);
    }
  }
  window.XMLHttpRequest = FakeXHR;

  // jsdom's Blob reads through a FileReader only; `reading` keeps a step from ending before it did
  let reading = 0;
  window.Blob.prototype.arrayBuffer = function arrayBuffer() {
    reading += 1;
    return new Promise((resolve, reject) => {
      const reader = new window.FileReader();
      reader.onload = () => { reading -= 1; resolve(reader.result); };
      reader.onerror = () => { reading -= 1; reject(reader.error); };
      reader.readAsArrayBuffer(this);
    });
  };

  let reducedMotion = false;
  window.matchMedia = () => ({ matches: reducedMotion, addEventListener() {} });
  window.CSS = { escape: (text) => text };
  window.Element.prototype.scrollIntoView = function scrollIntoView() { log(`scrollIntoView ${describe(this)}`); };
  let confirmAnswer = true;
  window.confirm = (message) => {
    log(`confirm(${confirmAnswer}): ${JSON.stringify(message)}`);
    return confirmAnswer;
  };
  // a file input's click opens the browser's dialog
  document.addEventListener("click", (event) => {
    if (event.target.type === "file") {
      log(`file dialog: ${event.target.dataset.region}`);
    }
  }, true);

  const flush = async () => {
    do {
      for (let round = 0; round < 8; round += 1) {
        await immediate();
      }
    } while (reading > 0);
    while (clock.runOne(clock.now)) {
      await immediate();
    }
    for (let round = 0; round < 4; round += 1) {
      await immediate();
    }
  };

  const context = dom.getInternalVMContext();
  const kitModule = new vm.SourceTextModule(fs.readFileSync(kit, "utf8"), { context, identifier: "file:///kit.js" });
  const pageModule = new vm.SourceTextModule(fs.readFileSync(script, "utf8"), { context, identifier: "file:///console.js" });
  await pageModule.link(async (specifier) => {
    if (specifier !== "/kit.js") {
      throw new Error(`import of ${specifier}`);
    }
    return kitModule;
  });
  await pageModule.evaluate();

  const ui = {
    window,
    document,
    log,
    flush,
    $: (selector, root = document) => root.querySelector(selector),
    $$: (selector, root = document) => [...root.querySelectorAll(selector)],
    row: (host) => document.querySelector(`tr.route[data-host="${host}"]`),
    action: (host, action) => document.querySelector(`tr.route[data-host="${host}"] [data-action="${action}"]`),
    editor: () => document.querySelector("tr.route__editor form"),
    async click(element) { log(`click ${describe(element)}`); element.click(); await flush(); },
    /** Types into `input`: the value, then its input event. */
    async type(input, value) {
      log(`type ${describe(input)} ${JSON.stringify(value)}`);
      input.value = value;
      input.dispatchEvent(new window.Event("input", { bubbles: true }));
      await flush();
    },
    async toggle(input, checked = true) {
      log(`toggle ${describe(input)} ${checked}`);
      input.checked = checked;
      input.dispatchEvent(new window.Event("change", { bubbles: true }));
      await flush();
    },
    async submit(form) {
      log(`submit ${describe(form)}`);
      form.dispatchEvent(new window.Event("submit", { bubbles: true, cancelable: true }));
      await flush();
    },
    async escape(element) {
      log(`Escape on ${describe(element)}`);
      element.dispatchEvent(new window.KeyboardEvent("keydown", { key: "Escape", bubbles: true }));
      await flush();
    },
    /** `files` chosen in the browser's dialog of the file input `region`. */
    async choose(region, ...files) {
      ui.chooseAtOnce(region, ...files);
      await flush();
    },
    /** As `choose`, the page's asynchronous check of the file still on its way after it. */
    chooseAtOnce(region, ...files) {
      const input = document.querySelector(`[data-region="${region}"]`);
      log(`choose in ${region}: ${files.map((file) => file.name).join(", ")}`);
      Object.defineProperty(input, "files", { value: files, configurable: true });
      input.dispatchEvent(new window.Event("change", { bubbles: true }));
    },
    /** A drag event on `target`; what the page made of it is logged. */
    async drag(type, target, { files = [], types = ["Files"] } = {}) {
      const event = new window.Event(type, { bubbles: true, cancelable: true });
      event.dataTransfer = { types, files, dropEffect: "unset" };
      log(`${type} on ${describe(target)} of ${types.join("+") || "nothing"}: ${files.map((file) => file.name).join(", ")}`);
      target.dispatchEvent(event);
      log(`  prevented=${event.defaultPrevented} dropEffect=${event.dataTransfer.dropEffect}`);
      await flush();
    },
    async advance(ms) {
      log(`advance ${ms} ms`);
      const end = clock.now + ms;
      await flush();
      while (clock.runOne(end)) {
        await flush();
      }
      clock.now = end;
      window.__now = end;
      await flush();
    },
    confirming(value) { confirmAnswer = value; },
    reduceMotion() { reducedMotion = true; },
    refreshFails(status, body) { plainAnswer = status === null ? null : [status, body]; },
    // the XMLHttpRequest in flight
    pending: () => pending[pending.length - 1],
    async progress(loaded, total, computable = true) {
      log(`upload progress ${loaded}/${total} computable=${computable}`);
      pending[pending.length - 1].uploadHandlers.progress({ lengthComputable: computable, loaded, total });
      await flush();
    },
    async uploaded() {
      log("upload done");
      pending[pending.length - 1].uploadHandlers.load?.();
      await flush();
    },
    async respond(status, body) {
      const xhr = pending.pop();
      if (!xhr) {
        log("! no request to answer");
        return;
      }
      log(`answer ${xhr.method} ${status} ${body}`);
      xhr.status = status;
      xhr.responseText = body;
      xhr.handlers.load();
      await flush();
    },
    async networkError() {
      const xhr = pending.pop();
      log(`network error on ${xhr.method}`);
      xhr.handlers.error();
      await flush();
    },
    /** The next listing: `change` applied, a new revision, the held poll answered. */
    async listing(change) {
      change(current);
      current.revision += 1;
      log(`listing revision ${current.revision}`);
      const body = JSON.stringify(current);
      release((hold) => hold.resolve(reply(200, body)));
      await flush();
    },
    /** `change` applied on the wicket without a new revision (`answer`: the held poll answered too). */
    async quietly(change, answerPoll = false) {
      change(current);
      if (answerPoll) {
        log("the held poll answered, the revision as it was");
        const body = JSON.stringify(current);
        release((hold) => hold.resolve(reply(200, body)));
      }
      await flush();
    },
    async pollFails(status, body) {
      log(`the held poll answered ${status}`);
      release((hold) => hold.resolve(reply(status, body)));
      await flush();
    },
    async pollDies() {
      log("the held poll dies");
      release((hold) => hold.reject(new Error("connection reset")));
      await flush();
    },
  };
  await flush();
  return ui;
}

function describe(element) {
  if (!element) {
    return "(none)";
  }
  if (element.nodeType !== 1) {
    return element.nodeName;
  }
  const attrs = ["id", "data-action", "data-region", "data-field", "data-testid", "data-host"]
    .filter((name) => element.hasAttribute(name))
    .map((name) => `${name}=${element.getAttribute(name)}`);
  return `<${element.localName}${attrs.length ? ` ${attrs.join(" ")}` : ""}>`;
}

/** The DOM as lines (drawings left out), with what no attribute shows: values, checks, the selection. */
function pretty(node, depth = 0) {
  const lines = [];
  const pad = "  ".repeat(depth);
  for (const child of node.childNodes) {
    if (child.nodeType === 3) {
      const text = child.textContent.replace(/\s+/g, " ").trim();
      if (text) {
        lines.push(`${pad}${JSON.stringify(text)}`);
      }
    }
    else if (child.nodeType === 1) {
      const attrs = [...child.attributes].map((attr) => ` ${attr.name}=${JSON.stringify(attr.value)}`).join("");
      if (child.localName === "svg") {
        const live = [...child.querySelectorAll("[data-field]")].map((text) => `${text.dataset.field}=${JSON.stringify(text.textContent)}`).join(" ");
        lines.push(`${pad}<svg …> ${live}`);
        continue;
      }
      let state = "";
      if (child.localName === "input") {
        state = ` {value=${JSON.stringify(child.value)}${child.checked ? " checked" : ""}${child.type === "text" ? ` selection=${child.selectionStart}-${child.selectionEnd}` : ""}}`;
      }
      lines.push(`${pad}<${child.localName}${attrs}>${state}`);
      lines.push(...pretty(child, depth + 1));
    }
  }
  return lines;
}

function snapshot(ui, name, requests) {
  out(`\n=== ${name}`);
  for (const line of requests.splice(0)) {
    out(line);
  }
  const { document } = ui;
  out(`link: ${document.documentElement.dataset.link} focus: ${describe(document.activeElement)}`);
  for (const line of pretty(document.body)) {
    out(line);
  }
}

/** Every line of a script some block of no page load ran. */
function uncovered(results, sources) {
  const counts = new Map();
  for (const script of results) {
    const source = sources[script.url];
    if (!source) {
      continue;
    }
    const chars = new Int32Array(source.length);
    const ranges = script.functions.flatMap((fn) => fn.ranges)
      .sort((a, b) => a.startOffset - b.startOffset || b.endOffset - a.endOffset);
    for (const range of ranges) {
      chars.fill(range.count, range.startOffset, range.endOffset);
    }
    const merged = counts.get(script.url) ?? new Int32Array(source.length);
    chars.forEach((count, at) => { merged[at] = Math.max(merged[at], count); });
    counts.set(script.url, merged);
  }
  for (const [url, merged] of counts) {
    const source = sources[url];
    let line = 1;
    let missed = false;
    let start = 0;
    const report = [];
    for (let at = 0; at <= source.length; at += 1) {
      if (at === source.length || source[at] === "\n") {
        if (missed) {
          report.push(`${line}: ${source.slice(start, at).trim()}`);
        }
        line += 1;
        start = at + 1;
        missed = false;
      }
      else if (merged[at] === 0 && !/[\s{}();]/.test(source[at])) {
        missed = true;
      }
    }
    console.error(`--- ${url}: ${report.length} lines not run`);
    report.forEach((entry) => console.error(entry));
  }
}

// ------------------------------------------------------------------------------- the steps

const FAULTS = [
  ["a refusal with its reason", 422, JSON.stringify({ error: "not a component: <b>bad</b>" })],
  ["too large, plain text", 413, "body too large"],
  ["signed out, no body", 401, ""],
  ["host taken", 409, JSON.stringify({ error: "host taken" })],
  ["draining", 503, JSON.stringify({ error: "the wicket is draining" })],
  ["an unknown status", 500, "x".repeat(600)],
  ["an unknown status, no body", 502, ""],
];

/** Each section starts from a fresh page. `P` is the page's profile: its hosts, files and fields. */
function sections(P) {
  const consoleHost = P.hosts.console;
  const newHost = "fresh.localhost";
  const ok = (message) => JSON.stringify({ generation: 9, message });
  const withRoute = (host, origin = "console") => (listing) => {
    const route = JSON.parse(JSON.stringify(listing.routes.find((other) => other.origin === "console")));
    route.host = host;
    route.origin = origin;
    listing.routes.push(route);
  };
  const withoutRoute = (host) => (listing) => {
    listing.routes = listing.routes.filter((route) => route.host !== host);
  };
  const faultsOf = async (ui, step, name, again) => {
    for (const [title, status, body] of FAULTS) {
      await ui.respond(status, body);
      await step(`${name} fails: ${title}`);
      await again();
    }
    await ui.networkError();
    await step(`${name} fails: no answer`);
  };
  const openForm = async (ui, file = P.good("site")) => {
    await ui.choose("intake-file", file);
  };

  return {
    async rendering(ui, step) {
      await step("the first listing");
      await ui.listing((listing) => { listing.routes = []; });
      await step("no routes");
      await ui.listing((listing) => { listing.routes = P.listing().routes.slice().reverse(); });
      await step("routes back, another order of groups");
      await ui.listing((listing) => { listing.routes = listing.routes.filter((route) => route.origin === "console"); });
      await step("only the console's");
      await ui.listing((listing) => {
        listing.routes = P.listing().routes;
        listing.routes.push({ ...JSON.parse(JSON.stringify(listing.routes[0])), host: "odd.localhost", origin: "elsewhere" });
      });
      await step("an origin the page does not know");
      await ui.listing((listing) => {
        listing.listen = null;
        listing.server = "grpc://node:8001";
        listing.user = "<b>alice</b>";
        listing.build = "";
        listing.reload_failure = null;
        listing.generation = 12;
      });
      await step("no --listen, a server, signed in, no failure");
      await ui.listing((listing) => {
        listing.listen = "[::]:8081";
        listing.user = null;
        listing.reload_failure = { reason: "SIGHUP", error: "<i>today</i>", at: "2026-11-02T01:59:00Z" };
      });
      await step("--listen on every address, a failure of today");
      await ui.listing((listing) => {
        listing.listen = "10.0.0.5:8081";
        listing.reload_failure = { reason: "x", error: "y", at: "not a date" };
      });
      await step("--listen on an address, a failure of no date");
      await ui.listing((listing) => {
        const [first] = listing.routes;
        listing.listen = "[::1]:8081";
        listing.reload_failure = { reason: "x", error: "y", at: "2026-10-30T01:59:00Z" };
        first.deployed = "2026-11-02T01:59:50Z";
      });
      await step("--listen on an IPv6 address, a failure of days ago, deployed just now");
      await ui.listing((listing) => {
        const [first] = listing.routes;
        listing.listen = "no port";
        first.deployed = "2023-01-01T00:00:00Z";
      });
      await step("a --listen of no port, deployed years ago");
      await ui.listing((listing) => {
        listing.routes[0].deployed = "not a date";
      });
      await step("deployed at no date");
      await ui.listing((listing) => {
        listing.routes[0].deployed = "2026-11-02T01:50:00Z";
      });
      await ui.advance(30000);
      await step("the relative times aged");
    },

    async link(ui, step) {
      await ui.pollFails(500, JSON.stringify({ error: "broken <b>" }));
      await step("the poll answered an error");
      await ui.advance(2000);
      await step("the link back");
      await ui.pollFails(401, "");
      await step("the poll refused");
      await ui.advance(2000);
      await ui.pollDies();
      await step("the poll died");
      await ui.advance(2000);
      await ui.advance(40000);
      await step("the poll silent past its limit");
      await ui.advance(2000);
      await step("the link back once more");
    },

    async copy(ui, step) {
      const copy = ui.$(`tr.route[data-host="${P.hosts.curl}"] [data-action="copy"]`);
      await ui.click(copy);
      await step("copy: no clipboard, the command selected");
      await ui.advance(1800);
      await step("the copy button's label back");
      ui.window.navigator.clipboard = { writeText: async () => {} };
      await ui.click(copy);
      await step("copy: the clipboard");
      await ui.advance(1800);
    },

    async intake(ui, step) {
      await ui.click(ui.$("[data-action=choose]"));
      await step("choose: the dialog");
      await ui.choose("intake-file");
      await step("the dialog cancelled");
      await openForm(ui, P.good("My Site"));
      await step("a good file: the deploy form");
      await ui.click(ui.$("[data-action=cancel]"));
      await step("cancelled");
      await openForm(ui, P.good("My Site"));
      await ui.escape(ui.$("#deploy-host"));
      await step("escaped");
      await openForm(ui, P.good(P.names.console));
      await step("a file named as the console route: its settings, a replace");
      await openForm(ui, P.good(P.names.readOnly));
      await step("a file named as a read-only route: refused");
      await openForm(ui, P.good("!!!"));
      await step("a file of no usable name: the fallback host");
      for (const [label, file] of P.refused()) {
        await openForm(ui, file);
        await step(`refused: ${label}`);
        await ui.submit(ui.$("[data-region=deploy-form]"));
        await step(`refused, and not sent: ${label}`);
      }
      // the check of a file given up on must not touch the next one's form
      ui.chooseAtOnce("intake-file", P.refused()[0][1]);
      await ui.choose("intake-file", P.good("second"));
      await step("a second file over the refused one");
      const unreadable = P.good("unreadable");
      unreadable.slice = () => ({ arrayBuffer: () => Promise.reject(new Error("unreadable")) });
      await openForm(ui, unreadable);
      await step("a file that cannot be read here: left to the wicket");
      ui.reduceMotion();
      await openForm(ui, P.good("still"));
      await step("reduced motion: the form scrolled to at once");
    },

    async hosts(ui, step) {
      await openForm(ui);
      const host = ui.$("#deploy-host");
      for (const [label, value] of [
        ["empty", ""],
        ["not a host", "Bad_Host!"],
        ["upper case, spaces", "  Case.LocalHost "],
        ["the console route's", consoleHost],
        ["a config route's", P.hosts.config],
        ["a flag route's", P.hosts.flag],
        ["an app's", P.hosts.api],
        ["a new one", newHost],
      ]) {
        await ui.type(host, value);
        await step(`host: ${label}`);
      }
      await ui.type(host, consoleHost);
      await P.fill(ui, ui.$("[data-region=deploy-form]"));
      await step("settings changed by hand");
      await ui.type(host, newHost);
      await step("touched: another host keeps them");
      await ui.listing(withRoute(newHost));
      await step("the host taken meanwhile: the note follows the listing");
      await ui.listing(withoutRoute(newHost));
      await ui.listing((listing) => { listing.listen = null; });
      await step("the host free again, no --listen");
    },

    async deploy(ui, step) {
      await openForm(ui, P.good("site"));
      const form = ui.$("[data-region=deploy-form]");
      await P.invalid(ui, form);
      await ui.submit(form);
      await step("a value out of range: not sent");
      await P.fill(ui, form);
      await ui.submit(form);
      await step("sent: the PUT, its progress at 0");
      await ui.progress(1024, 4096);
      await step("a quarter up");
      await ui.progress(10, 4096, false);
      await step("progress of no length");
      await ui.submit(form);
      await step("submitted again while sending: nothing");
      await ui.choose("intake-file", P.good("other"));
      await step("another file while sending: announced, not taken");
      await ui.drag("drop", ui.document.body, { files: [P.good("other")] });
      await step("a drop while sending");
      await ui.escape(form);
      await step("escape while sending: stays");
      await ui.uploaded();
      await step("all up: loading");
      await ui.respond(200, ok("added site.localhost"));
      await step("deployed");
      await ui.listing(withRoute("site.localhost"));
      await step("its row marked fresh");
      await ui.advance(4000);
      await ui.listing((listing) => { listing.generation += 1; });
      await step("no longer fresh");
      await ui.click(ui.$('[data-slot="outcome"] .link-button'));
      await step("deploy another: the dialog");
      await ui.click(ui.$('[data-slot="outcome"] .button'));
      await step("closed");
      await openForm(ui, P.good("defaults"));
      await ui.submit(form);
      await step("the defaults: the PUT");
      await ui.respond(200, "not json");
      await step("an answer of no JSON: done, no generation");
      await ui.click(ui.$('[data-slot="outcome"] .button'));
      await ui.listing((listing) => { listing.listen = null; });
      await openForm(ui, P.good("unlistened"));
      await ui.submit(form);
      await ui.respond(200, ok("added unlistened.localhost"));
      await step("deployed with no --listen: no way to reach it told");
    },

    async deployFaults(ui, step) {
      await openForm(ui, P.good("site"));
      const form = ui.$("[data-region=deploy-form]");
      await ui.submit(form);
      await faultsOf(ui, step, "a deploy", () => ui.submit(form));
      await ui.submit(form);
      await ui.respond(200, ok("added site.localhost"));
      await step("sent again, deployed");
      // a deploy refused with the host taken meanwhile: the focus on the host, not the button
      await openForm(ui, P.good("late"));
      await ui.submit(form);
      await ui.quietly(withRoute("late.localhost", "config"));
      await ui.listing((listing) => { listing.generation += 1; });
      await ui.respond(409, JSON.stringify({ error: "host taken" }));
      await step("refused, the host taken meanwhile");
    },

    async replace(ui, step) {
      ui.action(consoleHost, "replace").focus();
      await ui.click(ui.action(consoleHost, "replace"));
      await step("replace: the dialog");
      await ui.choose("replace-file");
      await step("the dialog cancelled");
      await ui.choose("replace-file", P.good("next"));
      await step("sent: the PUT, the row busy");
      await ui.progress(2048, 4096);
      await step("half up");
      await ui.progress(5, 4096, false);
      await ui.choose("replace-file", P.good("again"));
      await step("another file for the busy row: nothing");
      await ui.drag("dragover", ui.row(consoleHost), { files: [P.good("x")] });
      await step("a drag over the busy row: refused");
      await ui.drag("drop", ui.row(consoleHost), { files: [P.good("x")] });
      await step("a drop on the busy row: read-only for now");
      await ui.uploaded();
      await step("all up: loading");
      await ui.respond(200, ok("replaced"));
      await step("replaced: the outcome, the focus back");
      await ui.listing((listing) => { listing.generation += 1; });
      await step("a listing under the outcome");
      await ui.advance(10000);
      await step("the outcome gone by itself");
      await ui.choose("replace-file", P.good("next"));
      await ui.respond(200, ok("replaced"));
      await ui.click(ui.$('tr.route__status [class="link-button"]'));
      await step("an outcome dismissed");
      ui.chooseAtOnce("replace-file", P.good("first"));
      await ui.choose("replace-file", P.good("racing"));
      await step("two files at once: the first sent, the other dropped");
      await ui.respond(200, ok("replaced"));
    },

    async replaceFaults(ui, step) {
      await ui.click(ui.action(consoleHost, "replace"));
      for (const [label, file] of P.refused()) {
        await ui.choose("replace-file", file);
        await step(`replace refused: ${label}`);
      }
      await ui.click(ui.$('tr.route__status [class="link-button"]'));
      await step("the refusal dismissed");
      await ui.choose("replace-file", P.good("next"));
      await faultsOf(ui, step, "a replace", () => ui.choose("replace-file", P.good("next")));
      await ui.choose("replace-file", P.good("next"));
      await ui.listing(withoutRoute(consoleHost));
      await step("the route gone under its replace");
      await ui.networkError();
      await step("its replace failed with the route gone");
    },

    async editor(ui, step) {
      ui.action(consoleHost, "edit").focus();
      await ui.click(ui.action(consoleHost, "edit"));
      await step("the editor");
      await ui.click(ui.action(consoleHost, "edit"));
      await step("toggled closed: the focus back");
      await ui.click(ui.action(consoleHost, "edit"));
      await ui.click(ui.$("button.button:not(.button--primary)", ui.editor()));
      await step("cancelled");
      await ui.click(ui.action(consoleHost, "edit"));
      await ui.escape(ui.editor());
      await step("escaped");
      await ui.click(ui.action(consoleHost, "edit"));
      const kept = ui.editor();
      await P.invalid(ui, kept);
      await ui.submit(kept);
      await step("a value out of range: not sent");
      await ui.listing((listing) => { listing.generation += 1; listing.routes.find((route) => route.host === consoleHost).deployed = "2026-11-01T00:00:00Z"; });
      await step("a listing under the open editor: it stays, as typed");
      await P.fill(ui, kept);
      await ui.submit(kept);
      await step("sent: the PATCH, its progress");
      await ui.escape(kept);
      await step("escape while sending: stays");
      await ui.respond(200, ok("changed"));
      await step("changed: the editor closed, the focus back");
      await ui.click(ui.action(consoleHost, "edit"));
      await ui.submit(ui.editor());
      await ui.respond(200, JSON.stringify({ message: "no change", generation: 9 }));
      await step("nothing changed");
      await ui.click(ui.action(consoleHost, "edit"));
      await ui.listing(withoutRoute(consoleHost));
      await step("the route gone under its open editor");
    },

    async editorFaults(ui, step) {
      await ui.click(ui.action(consoleHost, "edit"));
      const form = ui.editor();
      await P.fill(ui, form);
      await ui.submit(form);
      await faultsOf(ui, step, "an edit", () => ui.submit(form));
      await ui.submit(form);
      await ui.respond(200, ok("changed"));
      await step("sent again, changed");
    },

    async rename(ui, step) {
      ui.action(consoleHost, "rename").focus();
      await ui.click(ui.action(consoleHost, "rename"));
      await step("rename: the form");
      await ui.submit(ui.editor());
      await step("the same host: not sent");
      await ui.click(ui.action(consoleHost, "rename"));
      await step("toggled closed");
      await ui.click(ui.action(consoleHost, "rename"));
      await ui.click(ui.$("button.button:not(.button--primary)", ui.editor()));
      await step("cancelled");
      await ui.click(ui.action(consoleHost, "rename"));
      await ui.escape(ui.editor());
      await step("escaped");
      await ui.listing(withRoute("other.localhost"));
      await ui.click(ui.action(consoleHost, "rename"));
      const input = ui.$("input", ui.editor());
      for (const [label, value] of [
        ["empty", ""],
        ["not a host", "Bad_Host!"],
        ["another console route's", "other.localhost"],
        ["a config route's", P.hosts.config],
        ["a flag route's", P.hosts.flag],
        ["an app's", P.hosts.api],
        ["a new one", ` ${newHost.toUpperCase()} `],
      ]) {
        await ui.type(input, value);
        await step(`rename to: ${label}`);
      }
      await ui.listing(withRoute(newHost, "config"));
      await step("the new host taken meanwhile: the note follows the listing");
      await ui.submit(ui.editor());
      await step("blocked: not sent");
      await ui.listing(withoutRoute(newHost));
      await step("free again");
      await ui.submit(ui.editor());
      await step("sent: the PATCH");
      await ui.escape(ui.editor());
      await step("escape while sending: stays");
      await ui.respond(200, ok(`moved to ${newHost}`));
      await step("moved: the row marked, a note above the table");
      await ui.listing((listing) => {
        withoutRoute(consoleHost)(listing);
        withRoute(newHost)(listing);
      });
      await step("the next listing: the row at its new host, fresh");
      await ui.advance(10000);
      await step("the note gone by itself");
    },

    async renameFaults(ui, step) {
      await ui.click(ui.action(consoleHost, "rename"));
      const form = ui.editor();
      await ui.type(ui.$("input", form), newHost);
      await ui.submit(form);
      await faultsOf(ui, step, "a rename", () => ui.submit(form));
      await ui.listing(withRoute(newHost, "config"));
      await ui.submit(form);
      await step("after the failures, taken: blocked, the focus on the input");
      await ui.listing(withoutRoute(newHost));
      await ui.submit(form);
      await ui.quietly(withRoute(newHost, "config"));
      await ui.listing((listing) => { listing.generation += 1; });
      await ui.respond(409, JSON.stringify({ error: "host taken" }));
      await step("refused, the host taken meanwhile");
    },

    async remove(ui, step) {
      ui.confirming(false);
      await ui.click(ui.action(consoleHost, "remove"));
      await step("remove declined: not sent");
      ui.confirming(true);
      ui.action(consoleHost, "remove").focus();
      await ui.click(ui.action(consoleHost, "edit"));
      ui.action(consoleHost, "remove").focus();
      await ui.click(ui.action(consoleHost, "remove"));
      await step("sent: the DELETE, the editor closed, the row busy");
      await ui.drag("drop", ui.row(consoleHost), { files: [P.good("x")] });
      await step("a drop on the row being removed");
      await ui.respond(200, ok("removed"));
      await step("removed: marked until the listing, the note focused");
      await ui.listing(withoutRoute(consoleHost));
      await step("the listing drops the row");
      await ui.advance(10000);
      await step("the note gone by itself");
    },

    async removeUnfocused(ui, step) {
      await ui.click(ui.action(consoleHost, "remove"));
      await ui.respond(200, JSON.stringify({ message: "removed" }));
      await step("removed with the focus elsewhere: no generation");
    },

    async removeFaults(ui, step) {
      ui.action(consoleHost, "remove").focus();
      await ui.click(ui.action(consoleHost, "remove"));
      await faultsOf(ui, step, "a remove", async () => {
        ui.action(consoleHost, "remove").focus();
        await ui.click(ui.action(consoleHost, "remove"));
      });
      await ui.click(ui.$('tr.route__status [class="link-button"]'));
      await step("the fault dismissed");
    },

    async dragAndDrop(ui, step) {
      const { body } = ui.document;
      const zone = ui.$("[data-testid=drop-zone]");
      const file = P.good("dropped");
      await ui.drag("dragenter", zone, { types: ["text/plain"] });
      await step("a drag of no files: left alone");
      await ui.drag("drop", zone, { types: ["text/plain"] });
      await step("a drop of no files: left alone");
      await ui.drag("dragenter", zone, { files: [file] });
      await step("a file over the page: the wicket open");
      await ui.advance(100);
      await ui.drag("dragover", ui.$(".route__host", ui.row(consoleHost)), { files: [file] });
      await step("over a console row: its target");
      for (const host of P.hosts.readOnly) {
        await ui.drag("dragover", ui.row(host), { files: [file] });
        await step(`over the read-only row ${host}: refused`);
      }
      await ui.drag("dragover", ui.row(consoleHost), { files: [file] });
      await step("back over the console row");
      await ui.advance(200);
      await step("the drag left the window: all back");
      await ui.drag("dragover", ui.document, { files: [file] });
      await step("over the document itself");
      await ui.drag("dragover", body, { files: [file] });
      await ui.drag("drop", body, { types: ["Files"], files: [] });
      await step("a drop naming no file (a folder)");
      await ui.drag("drop", zone, { files: [file] });
      await step("a drop on the page: the deploy form");
      await ui.click(ui.$("[data-action=cancel]"));
      await ui.drag("drop", ui.$("h1"), { files: [P.other("notes.txt"), file, P.good("too")] });
      await step("several dropped on the page: the first of the kind, a notice");
      await ui.click(ui.$("[data-action=cancel]"));
      await ui.drag("drop", ui.$("h1"), { files: [P.other("notes.txt"), P.other("more.txt")] });
      await step("several of no kind: the first, refused");
      await ui.click(ui.$("[data-action=cancel]"));
      for (const host of P.hosts.readOnly) {
        await ui.drag("drop", ui.$("td", ui.row(host)), { files: [file] });
        await step(`dropped on the read-only row ${host}: announced`);
      }
      await ui.drag("drop", ui.row(consoleHost), { files: [P.other("notes.txt")] });
      await step("a refused file dropped on the console row");
      await ui.drag("drop", ui.$("td", ui.row(consoleHost)), { files: [P.other("notes.txt"), file] });
      await step("dropped on the console row: the PUT, the kept settings");
      await ui.drag("dragover", ui.$("h1"), { files: [file] });
      await step("a drag while a replace is on its way");
      await ui.advance(200);
      await step("the drag gone: the wicket busy");
      await ui.respond(200, ok("replaced"));
      await step("replaced");
    },

    async focus(ui, step) {
      ui.action(consoleHost, "edit").focus();
      await ui.listing((listing) => { listing.routes.find((route) => route.host === consoleHost).deployed = "2026-11-01T12:00:00Z"; });
      await step("the row rebuilt under the focused button: the focus follows");
    },

    async quiet(ui, step) {
      await ui.quietly((listing) => { listing.generation = 77; }, true);
      await step("the poll answered, the revision as it was");
      await ui.listing((listing) => { listing.generation = 78; });
      await step("a new revision");
    },

    async beforeFirstListing(ui, step) {
      await step("no listing yet");
      await ui.advance(5000);
      await ui.drag("dragover", ui.$("h1"), { files: [P.good("early")] });
      await openForm(ui, P.good("early"));
      await step("a file before the first listing: the fallback fields");
      await ui.type(ui.$("#deploy-host"), "early.localhost");
      await ui.submit(ui.$("[data-region=deploy-form]"));
      await ui.respond(200, JSON.stringify({ message: "added", generation: 1 }));
      await step("deployed before the first listing");
      await ui.listing(() => {});
      await step("the first listing under the open form");
    },

    ...P.sections({ ok, withRoute, withoutRoute, openForm }),
  };
}

/** Runs the sections of the profile `P`, each on a fresh page, printing the transcript. */
async function run(P, { index, kit, script }) {
  const session = new inspector.Session();
  const post = (method, params) => new Promise((resolve, reject) => {
    session.post(method, params, (error, result) => (error ? reject(error) : resolve(result)));
  });
  const coverage = Boolean(process.env.COVERAGE);
  if (coverage) {
    session.connect();
    await post("Profiler.enable");
    await post("Profiler.startPreciseCoverage", { callCount: true, detailed: true });
  }
  const requests = [];
  const log = (line) => requests.push(line);
  process.on("unhandledRejection", (error) => log(`! unhandled rejection: ${error?.stack ?? error}`));
  const all = sections(P);
  for (const [name, section] of Object.entries(all)) {
    out(`\n################ ${name}`);
    const ui = await open({ index, listing: P.listing(), kit, script, log, holdFirst: name === "beforeFirstListing" });
    P.window = ui.window;
    const step = async (label) => {
      await ui.flush();
      snapshot(ui, label, requests);
    };
    try {
      await section(ui, step);
    }
    catch (error) {
      // a step that cannot go on is a difference to see, not the end of the other sections
      out(`! section stopped: ${error.message}`);
      log(`! ${error.stack}`);
    }
    await ui.flush();
    for (const line of requests.splice(0)) {
      out(line);
    }
    ui.window.close();
  }
  if (coverage) {
    const { result } = await post("Profiler.takePreciseCoverage");
    uncovered(result, {
      "file:///kit.js": fs.readFileSync(kit, "utf8"),
      "file:///console.js": fs.readFileSync(script, "utf8"),
    });
  }
  process.exit(0);
}

module.exports = { run };
