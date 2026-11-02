// node --experimental-vm-modules harness.js <index.html> <listing.json> <kit.js> <console.js>:
// the wasm page's transcript (transcript.js). DUMP=1: only the page's body once it rendered the
// listing (the scripts concatenated as one classic script: imports and exports dropped).
const fs = require("fs");
const { JSDOM } = require("jsdom");
const [index, listingFile, ...scripts] = process.argv.slice(2);
const listing = () => JSON.parse(fs.readFileSync(listingFile, "utf8"));

if (process.env.DUMP) {
  const fixed = listing();
  const html = fs.readFileSync(index, "utf8").replace(/<script[^>]*><\/script>/g, "");
  const dom = new JSDOM(html, { url: "http://127.0.0.1:9090/", runScripts: "outside-only", pretendToBeVisual: true });
  const { window } = dom;
  const requests = [];
  window.fetch = async (url) => {
    requests.push(["fetch", url]);
    if (String(url).includes("after=")) {
      return new Promise(() => {});
    }
    return { ok: true, status: 200, json: async () => fixed, text: async () => JSON.stringify(fixed) };
  };
  window.matchMedia = () => ({ matches: false, addEventListener() {} });
  let code = scripts.map((file) => fs.readFileSync(file, "utf8")).join("\n");
  code = code.replace(/^import \{[^}]*\} from "[^"]+";$/ms, "").replace(/^export /gm, "");
  window.eval(code);
  setTimeout(() => {
    console.log(window.document.body.innerHTML);
    console.error(JSON.stringify({ requests, faults: window.document.querySelectorAll(".outcome--fault").length }));
    window.close();
  }, 300);
}
else {
  const { run } = require("./transcript");
  const bytes = (...head) => {
    const content = new Uint8Array(64);
    content.set(head);
    return content;
  };
  const COMPONENT = [0x00, 0x61, 0x73, 0x6d, 0x0d, 0x00, 0x01, 0x00];
  const MODULE = [0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00];
  const routes = listing().routes;
  const P = {
    listing,
    window: null,
    good(name) { return new P.window.File([bytes(...COMPONENT)], `${name}.wasm`); },
    other(name) { return new P.window.File([bytes(1, 2, 3, 4, 5, 6, 7, 8)], name); },
    refused() {
      const huge = new P.window.File([bytes(...COMPONENT)], "huge.wasm");
      Object.defineProperty(huge, "size", { value: 134217729 });
      return [
        ["not WebAssembly", new P.window.File([bytes(1, 2, 3, 4, 5, 6, 7, 8)], "text.wasm")],
        ["a core module", new P.window.File([bytes(...MODULE)], "core.wasm")],
        ["empty", new P.window.File([], "empty.wasm")],
        ["short", new P.window.File([new Uint8Array([0, 0x61])], "short.wasm")],
        ["too large", huge],
      ];
    },
    names: { console: "hello", readOnly: "cfg" },
    hosts: {
      console: "hello.localhost",
      config: "cfg.localhost",
      flag: "flag.example",
      api: "app.localhost",
      curl: "flag.example",
      readOnly: routes.filter((route) => route.origin !== "console").map((route) => route.host),
    },
    /** A value of the form under `root` the wicket's check refuses. */
    async invalid(ui, root) {
      await ui.type(ui.$('input[id$="-budget"]', root), "0");
    },
    /** Every field of the form under `root` set to other than its default. */
    async fill(ui, root) {
      await ui.toggle(ui.$('input[value="tcp"]', root));
      await ui.toggle(ui.$('input[value="keyvalue"]', root));
      await ui.type(ui.$('input[id$="-budget"]', root), "500");
      await ui.type(ui.$('input[id$="-memory"]', root), "64");
      await ui.type(ui.$('input[id$="-pool"]', root), "1");
      await ui.type(ui.$('input[id$="-instances"]', root), "2");
    },
    sections: ({ withRoute }) => ({
      async fields(ui, step) {
        await ui.choose("intake-file", P.good("site"));
        const form = ui.$("[data-region=deploy-form]");
        await ui.toggle(ui.$('input[value="udp"]', form));
        await step("a network capability: cli with it");
        await ui.toggle(ui.$('input[value="cli"]', form), false);
        await step("cli off: the network capabilities off");
        for (const [key, value, label] of [
          ["budget", "", "no budget"],
          ["budget", "1.5", "a fraction"],
          ["memory", "0", "no memory"],
          ["pool", "-1", "a negative pool"],
          ["instances", "0", "no instances"],
        ]) {
          await ui.type(ui.$(`input[id$="-${key}"]`, form), value);
          await ui.submit(form);
          await step(`refused: ${label}`);
          await ui.type(ui.$(`input[id$="-${key}"]`, form), key === "instances" ? "" : "10");
        }
        await ui.type(ui.$('input[id$="-pool"]', form), "3");
        await ui.type(ui.$('input[id$="-instances"]', form), "2");
        await ui.submit(form);
        await step("refused: instances under the pool");
      },
      async keyvalue(ui, step) {
        await ui.listing(withRoute("kv.localhost"));
        await ui.listing((listing) => {
          listing.routes.find((route) => route.host === "kv.localhost").sandbox = {
            wasi: ["keyvalue"], budget_ms: 1500, memory_mib: 256, pool: 0, instances: 1, env: ["A", "B"],
            keyvalue: [{ identifier: "", store: "local", namespace: "n" }, { identifier: "x", store: "s", namespace: "m" }],
          };
        });
        await step("a route with keyvalue, env, an instance");
        await ui.click(ui.action("kv.localhost", "rename"));
        await ui.type(ui.$("input", ui.editor()), "moved.localhost");
        await step("renaming a route with keyvalue: its data stays");
        await ui.click(ui.action("kv.localhost", "remove"));
        await ui.respond(200, JSON.stringify({ message: "removed", generation: 9 }));
        await step("removing a route with keyvalue: the prompt says so");
      },
      async unknowns(ui, step) {
        await ui.listing((listing) => {
          listing.capabilities.push("future");
          const [first] = listing.routes;
          first.sandbox.wasi.push("future");
          delete first.sandbox.keyvalue;
        });
        await step("a capability the page has no hint for, no keyvalue listed");
        await ui.click(ui.action("hello.localhost", "edit"));
        await step("its checkbox");
      },
      async defaultsMoved(ui, step) {
        await ui.listing((listing) => { listing.defaults = { budget_ms: 250, memory_mib: 64 }; });
        await step("other defaults: rows rebuilt, their limits told against them");
        await ui.choose("intake-file", P.good("site"));
        await ui.submit(ui.$("[data-region=deploy-form]"));
        await step("the deploy under them");
      },
    }),
  };
  run(P, { index, kit: scripts[0], script: scripts[1] });
}
