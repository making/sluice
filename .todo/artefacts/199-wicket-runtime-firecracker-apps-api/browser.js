// node browser.js <index.html> <listing.json> <kit.js> <console.js> <assets dir>: the page in a real
// Chromium (npm install --no-save playwright-core; CHROME=<binary> or the Playwright cache), served
// with the wicket's CSP against a fake API: the operations once each, then every page error,
// console error and CSP violation. It proves what jsdom cannot: real modules, the CSP, layout.
const fs = require("fs");
const http = require("http");
const path = require("path");
const os = require("os");
const { chromium } = require("playwright-core");

const [index, listingFile, kit, script, assets] = process.argv.slice(2);
const CSP = "default-src 'none'; script-src 'self'; style-src 'self'; font-src 'self'; img-src 'self' data:; connect-src 'self'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'";
const listing = JSON.parse(fs.readFileSync(listingFile, "utf8"));
const wasm = Boolean(listing.max_component_bytes);
const good = Buffer.alloc(4096);
if (wasm) {
  Buffer.from([0, 0x61, 0x73, 0x6d, 0x0d, 0, 1, 0]).copy(good);
}
else {
  good[1080] = 0x53;
  good[1081] = 0xef;
}
const extension = wasm ? ".wasm" : ".ext4";
const consoleHost = listing.routes.find((route) => route.origin === "console").host;
const readOnly = listing.routes.find((route) => route.origin === "config").host;
const requests = [];
let nextAnswer = null;

const server = http.createServer((request, response) => {
  const { pathname, search } = new URL(request.url, "http://x");
  const send = (status, type, body) => {
    response.writeHead(status, { "Content-Type": type, "Content-Security-Policy": CSP });
    response.end(body);
  };
  const files = {
    "/": ["text/html; charset=utf-8", index],
    "/console.js": ["text/javascript; charset=utf-8", script],
    "/kit.js": ["text/javascript; charset=utf-8", kit],
    "/console.css": ["text/css; charset=utf-8", path.join(assets, "console.css")],
    "/fonts/overpass.woff2": ["font/woff2", path.join(assets, "fonts/overpass.woff2")],
    "/fonts/overpass-mono.woff2": ["font/woff2", path.join(assets, "fonts/overpass-mono.woff2")],
  };
  if (files[pathname]) {
    send(200, files[pathname][0], fs.readFileSync(files[pathname][1]));
  }
  else if (pathname === "/api/routes" && request.method === "GET") {
    if (search.includes("after=")) {
      setTimeout(() => send(200, "application/json", JSON.stringify(listing)), 25000);
    }
    else {
      send(200, "application/json", JSON.stringify(listing));
    }
  }
  else {
    let bytes = 0;
    request.on("data", (chunk) => { bytes += chunk.length; });
    request.on("end", () => {
      requests.push(`${request.method} ${request.url} ${request.headers["content-type"] ?? ""} ${bytes}`);
      const [status, body] = nextAnswer ?? [200, JSON.stringify({ generation: 9, message: "done it" })];
      nextAnswer = null;
      send(status, "application/json", body);
    });
  }
});

function chrome() {
  if (process.env.CHROME) {
    return process.env.CHROME;
  }
  const cache = path.join(os.homedir(), ".cache/ms-playwright");
  const version = fs.readdirSync(cache).find((name) => /^chromium-\d+$/.test(name));
  return path.join(cache, version, "chrome-linux64/chrome");
}

(async () => {
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  const url = `http://127.0.0.1:${server.address().port}/`;
  const browser = await chromium.launch({ executablePath: chrome() });
  const page = await browser.newPage();
  const problems = [];
  page.on("pageerror", (error) => problems.push(`page error: ${error.message}`));
  page.on("console", (message) => {
    if (message.type() === "error" && !message.location().url.includes("/api/")) {
      problems.push(`console error: ${message.text()}`);
    }
  });
  page.on("dialog", (dialog) => { console.log(`dialog: ${JSON.stringify(dialog.message())}`); dialog.accept(); });
  const say = (name, text) => console.log(`${name}: ${String(text).replace(/\s+/g, " ").trim()}`);
  const file = (name, buffer = good) => ({ name, mimeType: "application/octet-stream", buffer });
  const rowAction = (host, action) => page.locator(`tr.route[data-host="${host}"] [data-action="${action}"]`);
  const status = (host) => page.locator(`tr.route__status[data-host="${host}"] .outcome`);

  await page.goto(url);
  await page.locator("tr.route").first().waitFor();
  say("rows", (await page.locator("tr.route").evaluateAll((rows) => rows.map((row) => `${row.dataset.origin}:${row.dataset.host}`))).join(" "));
  const app = page.locator('tr[data-origin="api"]');
  if (await app.count()) {
    say("an app's row", await app.first().innerText());
  }

  // a file chosen, the deploy form, its PUT
  await page.locator('[data-region="intake-file"]').setInputFiles(file(`site${extension}`));
  await page.locator('[data-field="deploy-meta"]').filter({ hasText: wasm ? "WebAssembly" : "ext4" }).waitFor({ timeout: 3000 }).catch(() => problems.push("the deploy form did not describe the file"));
  say("deploy meta", await page.locator('[data-field="deploy-meta"]').innerText());
  say("host note", await page.locator('[data-field="host-note"]').innerText());
  await page.locator("[data-action=deploy]").click();
  await page.locator('[data-slot="outcome"] .outcome').waitFor();
  say("deployed", await page.locator('[data-slot="outcome"]').innerText());
  await page.locator('[data-slot="outcome"] .button').click();

  // a file refused
  await page.locator('[data-region="intake-file"]').setInputFiles(file(`junk${extension}`, Buffer.alloc(4096)));
  await page.locator(".deploy__meta--fault").waitFor({ timeout: 3000 }).catch(() => problems.push("the junk file was not refused"));
  say("refused", await page.locator('[data-field="deploy-meta"]').innerText());
  say("submit disabled", await page.locator("[data-action=deploy]").isDisabled());
  await page.locator("[data-action=cancel]").click();

  // a replace through the file dialog, then failing
  let chooser = page.waitForEvent("filechooser");
  await rowAction(consoleHost, "replace").click();
  await (await chooser).setFiles(file(`next${extension}`));
  await status(consoleHost).waitFor();
  say("replaced", await status(consoleHost).innerText());
  nextAnswer = [422, JSON.stringify({ error: "<b>rejected</b>" })];
  chooser = page.waitForEvent("filechooser");
  await rowAction(consoleHost, "replace").click();
  await (await chooser).setFiles(file(`next${extension}`));
  await page.locator(".outcome--fault").waitFor();
  say("replace failed", await status(consoleHost).innerText());
  await page.locator('tr.route__status [class="link-button"]').click();

  // the settings editor: a failure, then saved
  await rowAction(consoleHost, "edit").click();
  nextAnswer = [503, ""];
  await page.locator("tr.route__editor button[type=submit]").click();
  await page.locator("tr.route__editor .outcome--fault").waitFor();
  say("edit failed", await page.locator("tr.route__editor .outcome--fault").innerText());
  await page.locator("tr.route__editor button[type=submit]").click();
  await status(consoleHost).waitFor();
  say("edited", await status(consoleHost).innerText());

  // a rename: a failure, then moved (the fake listing does not follow: a reload puts it back)
  await rowAction(consoleHost, "rename").click();
  await page.locator("tr.route__editor input").fill("moved.localhost");
  say("rename note", await page.locator("tr.route__editor .field__note").innerText());
  nextAnswer = [409, JSON.stringify({ error: "host taken" })];
  await page.locator("tr.route__editor button[type=submit]").click();
  await page.locator("tr.route__editor .outcome--fault").waitFor();
  say("rename failed", await page.locator("tr.route__editor .outcome--fault").innerText());
  await page.locator("tr.route__editor button[type=submit]").click();
  await page.locator('[data-slot="routes-note"] .outcome').waitFor();
  say("renamed", await page.locator('[data-slot="routes-note"]').innerText());
  await page.reload();
  await page.locator("tr.route").first().waitFor();

  // a real DragEvent with a DataTransfer: on the page, on a console row, on a read-only row
  const drop = (selector, name) => page.evaluate(({ selector, name, bytes }) => {
    const transfer = new DataTransfer();
    transfer.items.add(new File([new Uint8Array(bytes)], name));
    const target = document.querySelector(selector);
    for (const type of ["dragenter", "dragover", "drop"]) {
      target.dispatchEvent(new DragEvent(type, { bubbles: true, cancelable: true, dataTransfer: transfer }));
    }
  }, { selector, name, bytes: [...good] });
  await drop("h1", `dropped${extension}`);
  await page.locator('[data-region="deploy"]:not([hidden])').waitFor();
  say("dropped on the page", await page.locator('[data-field="deploy-file"]').innerText());
  await page.locator("[data-action=cancel]").click();
  await drop(`tr.route[data-host="${readOnly}"]`, `dropped${extension}`);
  await page.waitForFunction(() => document.querySelector('[data-region="announcer"]').textContent.includes("read-only"));
  say("dropped on a read-only row", await page.locator('[data-region="announcer"]').textContent());
  await drop(`tr.route[data-host="${consoleHost}"] td`, `dropped${extension}`);
  await status(consoleHost).waitFor();
  say("dropped on a console row", await status(consoleHost).innerText());

  // a removal
  await rowAction(consoleHost, "remove").click();
  await page.locator('[data-slot="routes-note"] .outcome').waitFor();
  say("removed", await page.locator('[data-slot="routes-note"]').innerText());

  const violations = await page.evaluate(() => document.querySelectorAll("[style]").length);
  say("elements with a style attribute (set through the CSSOM, allowed)", violations);
  requests.forEach((request) => console.log(`> ${request}`));
  console.log(problems.length ? `PROBLEMS:\n${problems.join("\n")}` : "no page error, no console error, no CSP violation");
  await browser.close();
  process.exit(problems.length ? 1 : 0);
})().catch((error) => {
  console.error(error);
  process.exit(2);
});
