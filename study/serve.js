// study/serve.js — zero-dependency static server for the docs browser.
// Run from anywhere; it always serves the repository root (the folder above
// study/) and redirects "/" to "/docs/" so ../-relative fetches in index.html
// resolve. Usage:
//   cd study && node serve.js        (or: node study/serve.js)
//   then open http://localhost:8080
const http = require("http");
const fs = require("fs");
const path = require("path");

const PORT = Number(process.env.PORT) || 8080;
const ROOT = path.resolve(__dirname, ".."); // repo root, regardless of cwd

const TYPES = {
  ".html": "text/html; charset=utf-8",
  ".md": "text/markdown; charset=utf-8",
  ".sql": "text/plain; charset=utf-8",
  ".txt": "text/plain; charset=utf-8",
  ".sh": "text/plain; charset=utf-8",
  ".py": "text/plain; charset=utf-8",
  ".example": "text/plain; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".js": "text/javascript; charset=utf-8",
  ".json": "application/json; charset=utf-8",
  ".svg": "image/svg+xml",
  ".png": "image/png",
  ".ico": "image/x-icon",
};

http
  .createServer((req, res) => {
    let urlPath = decodeURIComponent(req.url.split("?")[0]);
    if (urlPath === "/") {
      res.writeHead(302, { Location: "/study/" });
      return res.end();
    }
    if (urlPath.endsWith("/")) urlPath += "index.html";
    const file = path.normalize(path.join(ROOT, urlPath));
    if (!file.startsWith(ROOT + path.sep)) {
      // never serve anything outside the repo root
      res.writeHead(403);
      return res.end("forbidden");
    }
    fs.readFile(file, (err, data) => {
      if (err) {
        res.writeHead(404, { "content-type": "text/plain; charset=utf-8" });
        return res.end("404 not found: " + urlPath);
      }
      res.writeHead(200, {
        "content-type": TYPES[path.extname(file).toLowerCase()] || "application/octet-stream",
        "cache-control": "no-store",
      });
      res.end(data);
    });
  })
  .listen(PORT, () => {
    console.log(`Serving ${ROOT}`);
    console.log(`Docs browser: http://localhost:${PORT}/`);
  });
