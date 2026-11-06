// study/build.js — refresh the offline content snapshot inside index.html.
// Run whenever the .md/.sql files change, then copy the docs/ folder anywhere
// (Android, USB stick, another PC): index.html is self-contained and needs no
// server. Usage:  node study/build.js   (or cd study && node build.js)
const fs = require("fs");
const path = require("path");

const DOC_DIR = __dirname;
const ROOT = path.resolve(__dirname, "..");
const PAGE = path.join(DOC_DIR, "index.html");

// single source of truth: whatever the sidebar links to gets embedded.
// Parse ONLY the NAV manifest block — the embedded JSON blob also lives in
// the page and could contain lookalike strings.
const html = fs.readFileSync(PAGE, "utf8");
const navStart = html.indexOf("const NAV = [");
const navEnd = html.indexOf("const FLAT");
if (navStart < 0 || navEnd < 0) throw new Error("NAV manifest not found in index.html");
const paths = [...html.slice(navStart, navEnd).matchAll(/path: "([^"]+)"/g)].map((m) => m[1]);

const data = {};
for (const p of paths) {
  const file = path.join(ROOT, p);
  if (fs.existsSync(file)) data[p] = fs.readFileSync(file, "utf8");
  else console.log("  skip (not yet created): " + p);
}

// "<" must not appear in the JSON blob: any "</script" inside content would
// terminate the tag early. \u003c is valid JSON and parses back to "<".
const json = JSON.stringify(data).replace(/</g, "\\u003c");
const block =
  "<!--EMBED_START--><script id=\"embedded-data\" type=\"application/json\">" +
  json +
  "</script><!--EMBED_END-->";

const START = "<!--EMBED_START-->";
const END = "<!--EMBED_END-->";
const from = html.indexOf(START);
const to = html.indexOf(END);
if (from < 0 || to < 0) throw new Error("embed markers missing from index.html");

fs.writeFileSync(PAGE, html.slice(0, from) + block + html.slice(to + END.length), "utf8");
console.log(
  "Embedded " + Object.keys(data).length + " files (" +
    Math.round(json.length / 1024) + " KB) into " + path.relative(ROOT, PAGE)
);
