"use strict";
// dsh-living-memory — living memory for DeepSeek Harness.
// Built by 暖暖 (NuanNuan). See README.md for the design tour, and NOTICE for the
// name/persona rights reservation (MIT covers the code, not the persona).
//
// Runtime: node:sqlite (WAL) + session/event observation + automatic extraction +
// memory tools. Mounted through the bundle patch (cordis.patch.yml) in two roles:
// host = read tools + internal pipelines, write = the memory_write tool.
// Credentials are resolved per operation through the harness credential provider
// (DEEPSEEK_MEMORY_KEY for extraction, EMBEDDING_BAILIAN_KEY for vectors/rerank).
// Nothing leaves your machine unless you configure those credentials.

const fs = require("node:fs");
const path = require("node:path");
const os = require("node:os");
const {
	CODEX_KIND, CODEX_SOURCE, FU_NAO_SOURCE, canonicalAnchor, resolveCodexWindow, sha256, sourceMatches,
} = require("./episodic-src.cjs");

const STOP_FLAG = path.join(os.homedir(), ".dsh", "dsh-living-memory", "stop-anchor.flag");
const FRESH_MS = 10 * 60_000;
const FRESH_LIMIT = 500;
const BACKFILL_LIMIT = 200;
const SELECT_ANCHOR = "SELECT * FROM episodic_anchor WHERE mem_id = ?";
const SELECT_MEMORY = "SELECT id, ts, space, source FROM memories WHERE id = ?";
const DDL = 'CREATE TABLE IF NOT EXISTS episodic_anchor (' +
	'mem_id INTEGER PRIMARY KEY, kind TEXT NOT NULL, sid TEXT NOT NULL, msg TEXT, ' +
	'sidechain INTEGER NOT NULL DEFAULT 0, at INTEGER NOT NULL, body TEXT, body_sha256 TEXT, ' +
	'via TEXT NOT NULL, evidence TEXT, created_at TEXT NOT NULL)';

const positiveId = (id) => Number.isSafeInteger(id) && id > 0;
const failure = (reason, why) => ({ ok: false, reason, why });

function sameDatabase(reader, writer) {
	if (reader === writer) return true;
	try {
		const main = (db) => db.prepare("PRAGMA database_list").all().find((row) => row.name === "main")?.file;
		const left = main(reader), right = main(writer);
		return Boolean(left && right && fs.realpathSync(left) === fs.realpathSync(right));
	} catch {
		return false;
	}
}

function encodedAnchor(anchor) {
	if (anchor.kind !== CODEX_KIND) return { sid: anchor.sid, msg: anchor.msg, sidechain: Number(anchor.sidechain), body: null, hash: null };
	const body = JSON.stringify(anchor);
	return { sid: anchor.chatId, msg: null, sidechain: 0, body, hash: sha256(body) };
}

function isSame(existing, kind, encoded) {
	return existing?.kind === kind && existing.sid === encoded.sid &&
		existing.msg === encoded.msg && existing.sidechain === encoded.sidechain &&
		existing.body_sha256 === encoded.hash && existing.body === encoded.body;
}

function createEpisodicAnchorStore({
	nowIso,
	now = Date.now,
	stopFlag = STOP_FLAG,
	resolve = resolveCodexWindow,
}) {
	const fresh = new Map();
	let hostDb;
	let evicted = 0;
	const isOff = () => process.env.LEGION_EPISODIC_ANCHOR_OFF === "1" || fs.existsSync(stopFlag);

	function initHost(db) {
		db.exec(DDL);
		hostDb = new WeakRef(db);
	}

	function prune(time) {
		for (const [id, entry] of fresh) if (time - entry.at >= FRESH_MS) fresh.delete(id);
	}

	function noteFresh(id, at, space, source, db) {
		const match = CODEX_SOURCE.exec(source);
		if (!positiveId(id) || (match ? match[2] !== space : space !== "brain" || !FU_NAO_SOURCE.test(source)) || isOff()) return;
		prune(at);
		fresh.set(id, { at, space, source, db });
		if (fresh.size > FRESH_LIMIT) {
			fresh.delete(fresh.keys().next().value);
			evicted++;
		}
	}

	function save(db, id, row, anchor, encoded, at, via, evidence) {
		if (db.isTransaction) return failure("db-error", "连接已在事务中");
		let started = false;
		try {
			db.exec("BEGIN IMMEDIATE");
			started = db.isTransaction === true;
			if (!started) return failure("db-error", "写入闸未允许开启事务");
			const current = db.prepare(SELECT_MEMORY).get(id);
			if (!current || current.ts !== row.ts || !sourceMatches(current, anchor)) {
				return failure("source-mismatch", "条目归属或时间已变化");
			}
			const existing = db.prepare(SELECT_ANCHOR).get(id);
			if (existing) return isSame(existing, anchor.kind, encoded)
				? { ok: true, memId: id, kind: anchor.kind, via, already: true }
				: failure("has-anchor", "原号已登记另一份锚");
			const inserted = db.prepare(
				"INSERT INTO episodic_anchor (mem_id, kind, sid, msg, sidechain, at, body, body_sha256, via, evidence, created_at) " +
				"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
			).run(id, anchor.kind, encoded.sid, encoded.msg, encoded.sidechain, at, encoded.body, encoded.hash, via, evidence, nowIso());
			if (Number(inserted.changes) !== 1 || !db.isTransaction) return failure("db-error", "锚未写入");
			db.exec("COMMIT");
			if (db.isTransaction || !isSame(db.prepare(SELECT_ANCHOR).get(id), anchor.kind, encoded))
				return failure("db-error", "未确认锚已提交");
			return { ok: true, memId: id, kind: anchor.kind, via };
		} finally {
			if (started && db.isTransaction) db.exec("ROLLBACK");
		}
	}

	function recordEpisodicAnchor(memId, anchor) {
		if (isOff()) return failure("off", "锚登记已关闭");
		if (!positiveId(memId)) return failure("bad-anchor", "记忆编号无效");
		prune(now());
		const entry = fresh.get(memId);
		if (!entry) return failure("not-fresh", "编号不在本进程的新写入记录中");
		const canonical = canonicalAnchor(anchor);
		if (!canonical) return failure("bad-anchor", "锚合同无效");
		if (!sourceMatches(entry, canonical)) return failure("source-mismatch", "来源与锚不匹配");
		try {
			const row = entry.db.prepare(SELECT_MEMORY).get(memId);
			if (!row || row.space !== entry.space || row.source !== entry.source)
				return failure("source-mismatch", "条目归属已变化");
			return save(entry.db, memId, row, canonical, encodedAnchor(canonical), entry.at, "fresh", null);
		} catch {
			return failure("db-error", "锚登记事务未确认");
		}
	}

	function backfillEpisodicAnchors(items, { dryRun = true, db = hostDb?.deref(), withWriteGuard } = {}) {
		const result = (rows) => {
			const counts = { total: rows.length, ok: 0 };
			for (const row of rows) {
				const key = row.ok ? "ok" : row.reason;
				counts[key] = (counts[key] ?? 0) + 1;
			}
			return { dryRun, rows, counts };
		};
		if (typeof dryRun !== "boolean" || !Array.isArray(items) || items.length > BACKFILL_LIMIT)
			return { ...result([]), reason: "bad-anchor", why: "批次须为至多 200 条，dryRun 须为布尔值" };
		const denyBatch = (reason, why) => result(items.map((item) => ({ memId: item?.memId, ...failure(reason, why) })));
		if (isOff()) return denyBatch("off", "锚登记已关闭");
		if (!db || (!dryRun && typeof withWriteGuard !== "function"))
			return denyBatch("db-error", "缺少核对连接或原维护方写入闸");

		const countsById = new Map();
		for (const item of items) countsById.set(item?.memId, (countsById.get(item?.memId) ?? 0) + 1);
		// The resolver may read files. Do this before obtaining the write connection and its lock.
		const accepted = new Map();
		const outcomes = items.map((item) => {
			const memId = item?.memId;
			const deny = (reason, why) => ({ memId, ...failure(reason, why) });
			if (!positiveId(memId)) return deny("bad-anchor", "记忆编号无效");
			if (countsById.get(memId) > 1) return deny("evidence-mismatch", "编号出现在多个补登项");
			const anchor = canonicalAnchor(item.anchor);
			if (!anchor || anchor.kind !== CODEX_KIND) return deny("bad-anchor", "只补登客户端锚");
			try {
				const row = db.prepare(SELECT_MEMORY).get(memId);
				if (!row) return deny("not-found", "原号不存在");
				if (!sourceMatches(row, anchor)) return deny("source-mismatch", "来源与锚不匹配");
				const at = Date.parse(row.ts);
				const times = anchor.messages.map((message) => Date.parse(message.timestamp));
				if (!Number.isFinite(at) || times.some((time) => !Number.isFinite(time) || time > at))
					return deny("evidence-mismatch", "条目时刻早于锚窗末条或时刻不可核");
				const encoded = encodedAnchor(anchor);
				const existing = db.prepare(SELECT_ANCHOR).get(memId);
				if (existing && !isSame(existing, anchor.kind, encoded)) return deny("has-anchor", "原号已登记另一份锚");
				const writeOrigin = item.evidence?.writeOrigin ?? null;
				if (writeOrigin && !["fresh", "fresh-write-response"].includes(writeOrigin))
					return deny("evidence-mismatch", "来路不是已核的原写回");
				if (!dryRun && !existing && !writeOrigin) return deny("evidence-mismatch", "正式补登缺已核的原写回");
				const checked = resolve(anchor);
				if (!checked.ok) return deny("evidence-mismatch", "原件复核未过：" + checked.code);
				const evidence = item.evidence == null ? null : JSON.stringify(item.evidence);
				if (evidence !== null && (typeof evidence !== "string" || Buffer.byteLength(evidence) > 64 * 1024))
					return deny("bad-anchor", "旁证超过 64 KiB 或无法编码");
				accepted.set(memId, { row, anchor, encoded, at, evidence });
				return { memId, ok: true, kind: anchor.kind, via: "backfill", chatId: anchor.chatId,
					root: checked.root, segment: checked.segment, writeOrigin,
					...(existing ? { already: true } : {}) };
			} catch {
				return deny("db-error", "补登核对未完成");
			}
		});
		if (dryRun || !accepted.size) return result(outcomes);

		// Only the maintenance entry point can provide the original lock/flag/window guard.
		return withWriteGuard((writeDb) => {
			const allowed = sameDatabase(db, writeDb);
			return result(outcomes.map((outcome) => {
				if (!outcome.ok) return outcome;
				const id = outcome.memId;
				if (!allowed) return { memId: id, ...failure("db-error", "核对连接与登记连接不属同库") };
				if (isOff()) return { memId: id, ...failure("off", "锚登记已关闭") };
				const item = accepted.get(id);
				try {
					return { memId: id, ...save(writeDb, id, item.row, item.anchor, item.encoded, item.at, "backfill", item.evidence) };
				} catch {
					return { memId: id, ...failure("db-error", "补登事务未确认") };
				}
			}));
		});
	}

	return {
		initHost, isOff, noteFresh, recordEpisodicAnchor, backfillEpisodicAnchors,
		stats: () => { prune(now()); return { fresh: fresh.size, evicted }; },
	};
}

module.exports = { createEpisodicAnchorStore };
