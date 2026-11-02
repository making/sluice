// node harness.js <index.html> <listing.json> <script>... : the page's body once it rendered
// the listing (the scripts concatenated as one classic script: imports and exports dropped)
const fs = require("fs");
const { JSDOM } = require("jsdom");
const [index, listingFile, ...scripts] = process.argv.slice(2);
const listing = JSON.parse(fs.readFileSync(listingFile, "utf8"));
const html = fs.readFileSync(index, "utf8").replace(/<script[^>]*><\/script>/g, "");
const dom = new JSDOM(html, { url: "http://127.0.0.1:9090/", runScripts: "outside-only", pretendToBeVisual: true });
const { window } = dom;
const requests = [];
window.fetch = async (url, init = {}) => {
  requests.push(["fetch", url]);
  if (String(url).includes("after=")) {
    return new Promise(() => {});
  }
  return { ok: true, status: 200, json: async () => listing, text: async () => JSON.stringify(listing) };
};
window.matchMedia = () => ({ matches: false, addEventListener() {} });
window.__requests = requests;
let code = scripts.map((file) => fs.readFileSync(file, "utf8")).join("\n");
code = code.replace(/^import \{[^}]*\} from "[^"]+";$/ms, "").replace(/^export /gm, "");
window.eval(code);
setTimeout(() => {
  const errors = window.document.querySelectorAll(".outcome--fault");
  if (process.env.DUMP) {
    console.log(window.document.body.innerHTML);
  }
  console.error(JSON.stringify({ requests, faults: errors.length }));
  window.close();
}, 300);
