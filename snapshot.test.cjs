"use strict";
const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const { DatabaseSync } = require("node:sqlite");
const { spawn } = require("node:child_process");
const { createSnapshot } = require("./patrol.cjs");

function fixture(t) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "living snapshot ' space-"));
  t.after(() => fs.rmSync(root, { recursive: true, force: true }));
  const source = path.join(root, "source data.sqlite3");
  const snapshots = path.join(root, "snapshot dir ' space");
  const db = new DatabaseSync(source);
  t.after(() => db.close());
  db.exec("PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0; CREATE TABLE data (value TEXT); INSERT INTO data(rowid, value) VALUES (97, 'WAL committed');");
  return { root, source, snapshots, db };
}
function readRows(file) {
  const db = new DatabaseSync(file, { readOnly: true });
  try {
    assert.equal(db.prepare("PRAGMA integrity_check").get().integrity_check, "ok");
    return db.prepare("SELECT rowid, value FROM data ORDER BY rowid").all().map(row => ({ ...row }));
  } finally { db.close(); }
}

test("WAL snapshot is complete with spaces and quote in paths, no sqlite3 CLI", t => {
  const { root, source, snapshots } = fixture(t);
  const oldPath = process.env.PATH;
  process.env.PATH = path.join(root, "no executables");
  try {
    assert.ok(fs.statSync(source + "-wal").size > 0);
    const dest = createSnapshot(source, snapshots, "20991231", 7);
    assert.deepEqual(readRows(dest), [{ rowid: 97, value: "WAL committed" }]);
    assert.deepEqual(fs.readdirSync(snapshots), ["memory-20991231.sqlite3"]);
  } finally { if (oldPath === undefined) delete process.env.PATH; else process.env.PATH = oldPath; }
});

test("a failing sqlite3 CLI is not used; a completed snapshot replaces same-day output", t => {
  const { root, source, snapshots, db } = fixture(t);
  const bin = path.join(root, "bin"); fs.mkdirSync(bin);
  const marker = path.join(root, "cli-invoked");
  if (process.platform === "win32") fs.writeFileSync(path.join(bin, "sqlite3.cmd"), `@echo invoked > "${marker}"\r\n@exit /b 1\r\n`);
  else { const fake = path.join(bin, "sqlite3"); fs.writeFileSync(fake, `#!/bin/sh\nprintf invoked > ${JSON.stringify(marker)}\nexit 1\n`, { mode: 0o755 }); }
  const oldPath = process.env.PATH; process.env.PATH = bin;
  try {
    const dest = createSnapshot(source, snapshots, "20991231", 7);
    db.exec("INSERT INTO data(rowid, value) VALUES (202, 'second version');");
    assert.equal(createSnapshot(source, snapshots, "20991231", 7), dest);
    assert.deepEqual(readRows(dest), [{ rowid: 97, value: "WAL committed" }, { rowid: 202, value: "second version" }]);
    assert.equal(fs.existsSync(marker), false);
  } finally { if (oldPath === undefined) delete process.env.PATH; else process.env.PATH = oldPath; }
});

test("backup failure propagates and preserves both same-day and expired snapshots", t => {
  const { root, source, snapshots } = fixture(t);
  const dest = createSnapshot(source, snapshots, "20991231", 7);
  const before = fs.readFileSync(dest);
  const old = path.join(snapshots, "memory-20000101.sqlite3");
  fs.writeFileSync(old, "old backup");
  assert.throws(() => createSnapshot(path.join(root, "missing.sqlite3"), snapshots, "20991231", 7));
  assert.deepEqual(fs.readFileSync(dest), before);
  assert.equal(fs.readFileSync(old, "utf8"), "old backup");
  assert.deepEqual(fs.readdirSync(snapshots).sort(), ["memory-20000101.sqlite3", "memory-20991231.sqlite3"]);
  assert.deepEqual(readRows(dest), [{ rowid: 97, value: "WAL committed" }]);
});

test("unsupported backup API is reported without touching existing snapshots", t => {
  const { root, source, snapshots } = fixture(t);
  const dest = createSnapshot(source, snapshots, "20991231", 7);
  const before = fs.readFileSync(dest);
  const shim = path.join(root, "disable-backup.cjs");
  fs.writeFileSync(shim, "require('node:sqlite').backup = undefined;\n");
  const oldOptions = process.env.NODE_OPTIONS;
  process.env.NODE_OPTIONS = `--require=${JSON.stringify(shim)}`;
  try {
    assert.throws(() => createSnapshot(source, snapshots, "20991231", 7), error => {
      assert.match(String(error.stderr), /SQLite online backup is unavailable/);
      return true;
    });
  } finally { if (oldOptions === undefined) delete process.env.NODE_OPTIONS; else process.env.NODE_OPTIONS = oldOptions; }
  assert.deepEqual(fs.readFileSync(dest), before);
  assert.deepEqual(fs.readdirSync(snapshots), ["memory-20991231.sqlite3"]);
});

test("refuses replacing an existing snapshot with WAL sidecars", t => {
  const { source, snapshots } = fixture(t);
  const dest = createSnapshot(source, snapshots, "20991231", 7);
  const before = fs.readFileSync(dest);
  fs.writeFileSync(dest + "-wal", "unrecovered data");
  assert.throws(() => createSnapshot(source, snapshots, "20991231", 7), /WAL sidecars/);
  assert.deepEqual(fs.readFileSync(dest), before);
  assert.equal(fs.readFileSync(dest + "-wal", "utf8"), "unrecovered data");
  assert.deepEqual(fs.readdirSync(snapshots).sort(), ["memory-20991231.sqlite3", "memory-20991231.sqlite3-wal"]);
});

test("concurrent processes publish complete snapshots without sharing temporary files", async t => {
  const { source, snapshots } = fixture(t);
  const script = "require(process.argv[1]).createSnapshot(process.argv[2], process.argv[3], '20991231', 7)";
  const run = () => new Promise((resolve, reject) => {
    const child = spawn(process.execPath, ["-e", script, require.resolve("./patrol.cjs"), source, snapshots], { stdio: ["ignore", "ignore", "pipe"] });
    let errors = ""; child.stderr.on("data", data => { errors += data; });
    child.on("error", reject);
    child.on("close", code => code === 0 ? resolve() : reject(new Error(errors)));
  });
  await Promise.all([run(), run()]);
  assert.deepEqual(readRows(path.join(snapshots, "memory-20991231.sqlite3")), [{ rowid: 97, value: "WAL committed" }]);
  assert.deepEqual(fs.readdirSync(snapshots), ["memory-20991231.sqlite3"]);
});

test("backup child timeout preserves the old snapshot", { timeout: 75000 }, t => {
  const { root, source, snapshots } = fixture(t);
  const dest = createSnapshot(source, snapshots, "20991231", 7);
  const before = fs.readFileSync(dest);
  const shim = path.join(root, "stall-backup.cjs");
  fs.writeFileSync(shim, "require('node:sqlite').backup = () => new Promise(() => setInterval(() => {}, 1000));\n");
  const oldOptions = process.env.NODE_OPTIONS;
  process.env.NODE_OPTIONS = `--require=${JSON.stringify(shim)}`;
  try {
    assert.throws(() => createSnapshot(source, snapshots, "20991231", 7), error => error.code === "ETIMEDOUT");
  } finally { if (oldOptions === undefined) delete process.env.NODE_OPTIONS; else process.env.NODE_OPTIONS = oldOptions; }
  assert.deepEqual(fs.readFileSync(dest), before);
  assert.deepEqual(fs.readdirSync(snapshots), ["memory-20991231.sqlite3"]);
});

test("rotation runs only after a successful snapshot", t => {
  const { source, snapshots } = fixture(t);
  fs.mkdirSync(snapshots);
  fs.writeFileSync(path.join(snapshots, "memory-20000101.sqlite3"), "old backup");
  createSnapshot(source, snapshots, "20991231", 7);
  assert.deepEqual(fs.readdirSync(snapshots), ["memory-20991231.sqlite3"]);
});
