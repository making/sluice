// The Firecracker page in jsdom: renders a listing, opens the deploy form for a file carrying
// the ext4 magic, switches modes, and checks the PUT the form sends (XMLHttpRequest stubbed).
const fs = require("fs");
const assert = require("assert");
const { JSDOM } = require("jsdom");
const [index, listingFile, ...scripts] = process.argv.slice(2);
const listing = JSON.parse(fs.readFileSync(listingFile, "utf8"));
const html = fs.readFileSync(index, "utf8").replace(/<script[^>]*><\/script>/g, "");
const dom = new JSDOM(html, { url: "http://127.0.0.1:9090/", runScripts: "outside-only", pretendToBeVisual: true });
const { window } = dom;
const { document } = window;
const fetches = [];
window.fetch = async (url) => {
  fetches.push(url);
  if (String(url).includes("after=")) {
    return new Promise(() => {});
  }
  return { ok: true, status: 200, json: async () => listing, text: async () => JSON.stringify(listing) };
};
window.matchMedia = () => ({ matches: false, addEventListener() {} });
window.CSS = { escape: (text) => text };
window.Element.prototype.scrollIntoView = () => {};
const sent = [];
class FakeXHR {
  constructor() {
    this.upload = { addEventListener() {} };
    this.listeners = {};
    this.headers = {};
  }
  open(method, url) { this.method = method; this.url = url; }
  setRequestHeader(name, value) { this.headers[name] = value; }
  addEventListener(name, fn) { this.listeners[name] = fn; }
  send(body) {
    sent.push({ method: this.method, url: this.url, headers: this.headers, body });
    this.status = 200;
    this.responseText = JSON.stringify({ generation: 4, message: "added a.localhost" });
    setTimeout(() => this.listeners.load(), 0);
  }
}
window.XMLHttpRequest = FakeXHR;
// jsdom's Blob reads through a FileReader only
if (!window.Blob.prototype.arrayBuffer) {
  window.Blob.prototype.arrayBuffer = function arrayBuffer() {
    return new Promise((resolve, reject) => {
      const reader = new window.FileReader();
      reader.onload = () => resolve(reader.result);
      reader.onerror = () => reject(reader.error);
      reader.readAsArrayBuffer(this);
    });
  };
}
let code = scripts.map((file) => fs.readFileSync(file, "utf8")).join("\n");
code = code.replace(/^import \{[^}]*\} from "[^"]+";$/ms, "").replace(/^export /gm, "");
window.eval(code);
const tick = (ms = 50) => new Promise((resolve) => setTimeout(resolve, ms));
const visible = (el) => { for (let n = el; n; n = n.parentElement) { if (n.hidden) return false; } return true; };

(async () => {
  await tick(100);
  // the listing rendered: every row, the budget, the failure as text
  const rows = [...document.querySelectorAll("tr.route")].map((row) => row.dataset.host);
  assert.deepStrictEqual(rows, ["app.localhost", "shared.local", "pool.local"]);
  assert.strictEqual(document.querySelector('[data-field="budget-vms"]').textContent, "5 / 64");
  assert.strictEqual(document.querySelector('[data-field="budget-memory"]').textContent, "640 MiB");
  assert.strictEqual(document.querySelector('[data-field="failure-error"]').textContent, "wicket.yaml: routes[2]: <script>x</script>");
  assert.strictEqual(document.querySelectorAll("script").length, 0, "no markup from the wicket");
  const vms = [...document.querySelectorAll('[data-testid="vms"]')].map((el) => el.textContent.replace(/\s+/g, " "));
  console.error(vms);
  assert.match(vms[0], /2 VMs.*2 warm.*0 connections/);
  assert.match(vms[1], /1 VM.*up.*3 connections/);
  assert.match(vms[2], /2 pooled.*1 starting.*5 connections/);
  assert.ok(document.body.textContent.includes("snapshot 128 MiB"), "the snapshot");
  assert.ok(document.querySelector('tr[data-host="app.localhost"] [data-action="edit"]'), "console rows editable");
  assert.ok(!document.querySelector('tr[data-host="shared.local"] [data-action="edit"]'), "config rows read-only");

  // a file carrying the ext4 magic opens the deploy form
  const bytes = new Uint8Array(4096);
  bytes[1080] = 0x53;
  bytes[1081] = 0xef;
  const file = new window.File([bytes], "My App.ext4");
  const input = document.querySelector('[data-region="intake-file"]');
  Object.defineProperty(input, "files", { value: [file], configurable: true });
  input.dispatchEvent(new window.Event("change"));
  await tick();
  assert.ok(!document.querySelector('[data-region="deploy"]').hidden, "the deploy form open");
  const host = document.querySelector("#deploy-host");
  assert.strictEqual(host.value, "my-app.localhost");
  const meta = document.querySelector('[data-field="deploy-meta"]').textContent;
  assert.ok(meta.includes("an ext4 rootfs image"), meta);
  assert.ok(!document.querySelector('[data-action="deploy"]').disabled, "may be sent");

  // modes: the fields each takes
  const form = document.querySelector('[data-slot="options"]');
  const option = (key) => form.querySelector(`[data-option="${key}"]`);
  const mode = (name) => {
    const radio = form.querySelector(`input[type=radio][value="${name}"]`);
    radio.checked = true;
    radio.dispatchEvent(new window.Event("change", { bubbles: true }));
  };
  assert.ok(visible(option("mem")) && !visible(option("warm")) && !visible(option("conns")), "shared");
  assert.ok(!option("cpu"), "cpu only in jail mode");
  mode("reuse");
  assert.ok(visible(option("warm")) && visible(option("uses")) && visible(option("keep-ms")) && !visible(option("conns")), "reuse");
  mode("pool");
  assert.ok(visible(option("conns")) && !visible(option("uses")), "pool");
  // a pool without vms is refused before anything is sent
  document.querySelector('[data-region="deploy-form"]').dispatchEvent(new window.Event("submit", { cancelable: true }));
  await tick();
  assert.strictEqual(sent.length, 0, "not sent without vms");
  assert.match(form.querySelector(".sandbox__note").textContent, /needs its VMs/);
  form.querySelector('[data-option="vms"] input').value = "4";
  form.querySelector('[data-option="warm"] input').value = "4"; // the default: left out
  form.querySelector('[data-option="mem"] input').value = "256";
  form.querySelector('[data-option="conns"] input').value = "16"; // the default: left out
  form.querySelector('input[type=text]').value = "db.internal:5432, [2001:db8::/32]:443";
  document.querySelector('[data-region="deploy-form"]').dispatchEvent(new window.Event("submit", { cancelable: true }));
  await tick(100);
  assert.strictEqual(sent.length, 1, "sent once");
  const [put] = sent;
  console.error(put.url);
  assert.strictEqual(put.method, "PUT");
  assert.strictEqual(put.url, "/api/routes/my-app.localhost?name=My%20App.ext4&mode=pool&mem=256&vms=4&egress=db.internal%3A5432%2C%5B2001%3Adb8%3A%3A%2F32%5D%3A443");
  assert.strictEqual(put.headers["Content-Type"], "application/octet-stream");
  assert.strictEqual(put.body, file);
  assert.ok(document.querySelector('[data-slot="outcome"]').textContent.includes("Added a.localhost."));

  // the options editor of a console route starts from its options
  document.querySelector('tr[data-host="app.localhost"] [data-action="edit"]').click();
  await tick();
  const editor = document.querySelector("tr.route__editor");
  assert.ok(editor.querySelector('input[type=radio][value="per-connection"]').checked);
  assert.strictEqual(editor.querySelector('[data-option="warm"] input').value, "2");
  assert.strictEqual(editor.querySelector("input[type=text]").value, "db.internal:5432,10.0.0.0/24:6379");
  editor.querySelector("form").dispatchEvent(new window.Event("submit", { cancelable: true }));
  await tick(100);
  console.error(sent[1].method, sent[1].url);
  assert.strictEqual(sent[1].url, "/api/routes/app.localhost?mode=per-connection&warm=2&egress=db.internal%3A5432%2C10.0.0.0%2F24%3A6379");

  // the refresh keeps the counts current
  listing.budget.vms = 7;
  listing.routes[1].vms.connections = 9;
  await tick(5300);
  assert.strictEqual(document.querySelector('[data-field="budget-vms"]').textContent, "7 / 64");
  assert.ok(fetches.filter((url) => url === "/api/routes").length >= 2, "refreshed");

  // a file without the magic is refused before it is sent
  const plain = new window.File([new Uint8Array(4096)], "x.ext4");
  Object.defineProperty(input, "files", { value: [plain], configurable: true });
  input.dispatchEvent(new window.Event("change"));
  await tick();
  const refusedMeta = document.querySelector('[data-field="deploy-meta"]').textContent;
  assert.ok(refusedMeta.includes("not an ext4 image"), refusedMeta);
  assert.ok(document.querySelector('[data-action="deploy"]').disabled);
  console.error("all checks passed");
  process.exit(0);
})().catch((error) => {
  console.error(error);
  process.exit(1);
});
