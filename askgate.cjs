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
const crypto = require("node:crypto");
const core = require("./core.cjs");
const { CRIT, nowIso, qTokens, tokenize, queryMatch, organFromPath, sessionOrgan, stripLone } = core;

const envOn = (n) => process.env[n] === "1";
const agOff = () => envOn("LEGION_ASKGATE_OFF");
const actOn = () => envOn("LEGION_ASKGATE_ON") && !agOff(); // 盲审处置（must）：动作缺省关，ON=1 才动作；OFF 压过 ON（每次调用现读）
const denyOff = () => envOn("LEGION_ASKGATE_DENY_OFF");
const degOn = () => envOn("LEGION_ASKGATE_DEGRADE_ON"); // 盲审处置（should）：降级路单设开关、缺省关（每次调用现读）
const G = { ctx: null, stats: null, db: null, on: false, stopEver: false };
const TURN = new Map(); // sid → 本轮态
const SUBS = new Map(); // 子代理 id（subagent/start 的 info.id）→ 记下时刻；盲审处置（nit）：仿 TURN 设上限清理（见 subsSweep）
const TOPIC = new Map(); // 题键 → 上次发闸时刻（24 小时一次）
const DAY = new Map(); // sid → ｛day, n｝每窗每个北京日计数
const DENY = new Map(); // sid → ｛sig, at｝T-B 上次 deny 的问题签名
const SEEN_ITEMS = new Map(); // sid → Set（board_write／todo_write 已判过的项文本）
const STASH = new Map(); // sid → 待 inject 的闸文消息（降级路）
const ASST_CAP = 8000; // 本轮助手正文只留最后 8000 字
const BYTE_CAP = 1200; // 闸文字节帽（规格 ≤1.2KB）
const TOPIC_MS = 24 * 3600e3;
const DENY_MS = 30 * 60e3;
const DAY_CAP = 12;
const ROW_CAP = 3;
const T_C_TOOLS = new Set(["memory_write", "board_write", "todo_write"]);
const QUOTE_PAIRS = [
	["「", "」"],
	["『", "』"],
	["“", "”"],
	['"', '"'],
];
// 已然句：maintainer已提供／给过／发过……——不算索要
const DONE_RE = /您(?:已经?|早已|早就)(?:提供|给|发|补|回|填|上传)|您(?:给过|发过|提供过|补过|回过|上传过)/;

function agErr() {
	if (G.stats) G.stats.askgateErrors = (G.stats.askgateErrors || 0) + 1;
}
// 会话号：宿主 agent.session.header.id（hooks base() 同取法），回落 session.id、agent.id
function sidOf(agent) {
	const s = agent && agent.session;
	return String((s && s.header && s.header.id) || (s && s.id) || (agent && agent.id) || "");
}
const sid8 = (sid) => String(sid || "").replace(/^session-/, "").slice(0, 8);
const bjDay = () => nowIso().slice(0, 10);
function bjHM(ms) {
	const d = new Date(ms + 8 * 3600e3);
	const p = (n) => String(n).padStart(2, "0");
	return `${p(d.getUTCHours())}:${p(d.getUTCMinutes())}`;
}
function bj16(ms) {
	const d = new Date(ms + 8 * 3600e3);
	const p = (n) => String(n).padStart(2, "0");
	return `${d.getUTCFullYear()}-${p(d.getUTCMonth() + 1)}-${p(d.getUTCDate())}T${p(d.getUTCHours())}:${p(d.getUTCMinutes())}`;
}
// 盲审处置（should）：子代理只认 origin:"subagent"、delegationDepth>0（dsh-subagent childSessionMeta 两项同写）与 subagent/start 记下的 id；
//   不认 parentSession——宿主「分叉」（dsh-session fork）造的是主窗，也写 parentSession，但不写 origin（分叉主窗照常闸）
function isSub(agent, sid) {
	const h = (agent && agent.session && agent.session.header) || {};
	return SUBS.has(String(agent && agent.id)) || SUBS.has(String(sid)) || h.origin === "subagent" || Number(h.delegationDepth) > 0;
}
function textOf(c) {
	if (typeof c === "string") return c;
	if (!Array.isArray(c)) return "";
	let t = "";
	for (const b of c) if (b && typeof b.text === "string" && (!b.type || b.type === "text" || b.type === "input_text" || b.type === "output_text")) t += b.text;
	return t;
}
function freshTurn(turn) {
	return { turn: turn === undefined ? null : turn, asst: [], asstLen: 0, qWords: new Set(), at: Date.now(), evalA: false, denied: false, bpost: false, ctxN: 0, fired: false, followed: false };
}
function turnOf(sid) {
	let st = TURN.get(sid);
	if (!st) {
		st = freshTurn();
		TURN.set(sid, st);
	}
	return st;
}
// 进程内表不无界：超 300 个会话时清掉 6 小时没动的；题键表超 2000 时清掉过期的
function sweep() {
	const now = Date.now();
	if (TURN.size > 300) for (const [k, v] of TURN) if (v.at < now - 6 * 3600e3) TURN.delete(k);
	if (TOPIC.size > 2000) for (const [k, at] of TOPIC) if (now - at >= TOPIC_MS) TOPIC.delete(k);
	if (SEEN_ITEMS.size > 300) SEEN_ITEMS.clear();
}
// 盲审处置（nit）：SUBS 不无界——超 500 个时清掉 24 小时前记下的；仍超 2000 个时按插入序删到 1500（子代理会话头另有 origin 判，SUBS 只是兜底）
function subsSweep() {
	if (SUBS.size <= 500) return;
	const now = Date.now();
	for (const [k, at] of SUBS) if (now - at > 24 * 3600e3) SUBS.delete(k);
	if (SUBS.size > 2000) for (const k of SUBS.keys()) {
		if (SUBS.size <= 1500) break;
		SUBS.delete(k);
	}
}
function bump(k, n) {
	try {
		G.db.prepare("INSERT INTO organ_meta(k, v) VALUES(?, ?) ON CONFLICT(k) DO UPDATE SET v = CAST(CAST(v AS INTEGER) + ? AS TEXT)").run(k, String(n || 1), String(n || 1));
	} catch {
		agErr();
	}
}
// 日志环：读改写（多进程并发末写赢、微损可容，同 inject.cjs 被截环注记）；只记 id 与标，不记正文
function ring(route, sid, key, ids, action) {
	try {
		const row = G.db.prepare("SELECT v FROM organ_meta WHERE k = 'askgate_ring'").get();
		let r = [];
		try {
			r = JSON.parse((row && row.v) || "[]");
		} catch {
			r = [];
		}
		if (!Array.isArray(r)) r = [];
		r.push([nowIso().slice(0, 16), sid8(sid), route, String(key || ""), (ids || []).slice(0, 3), action]);
		G.db.prepare("INSERT INTO organ_meta (k, v) VALUES ('askgate_ring', ?) ON CONFLICT(k) DO UPDATE SET v = excluded.v").run(JSON.stringify(r.slice(-300)));
	} catch {
		agErr();
	}
}
function probeMark(kind) {
	try {
		const row = G.db.prepare("SELECT v FROM organ_meta WHERE k = 'askgate_probe'").get();
		let o = {};
		try {
			o = JSON.parse((row && row.v) || "{}") || {};
		} catch {
			o = {};
		}
		if (typeof o !== "object" || Array.isArray(o)) o = {};
		o[kind] = nowIso();
		G.db.prepare("INSERT INTO organ_meta (k, v) VALUES ('askgate_probe', ?) ON CONFLICT(k) DO UPDATE SET v = excluded.v").run(JSON.stringify(o));
	} catch {
		agErr();
	}
}
function hookSeen(kind) {
	const s = G.stats;
	if (!s) return;
	if (!s.askgateHooks || typeof s.askgateHooks !== "object") s.askgateHooks = { stop: 0, pre: 0, post: 0, sub: 0, claim: 0 };
	s.askgateHooks[kind] = (s.askgateHooks[kind] || 0) + 1;
	if (s.askgateHooks[kind] === 1) probeMark(kind);
}

// ── askDetect（纯函数，drill 直调）：从一段文字里找向maintainer索要信息的句子 ──
//   直接索要 askGateAsk、登记式 askGateWait；代码块与行内码剔除；引号内的命中（转述）不算；已然句不算。
//   宾语：命中点之后到分句末（。！？与，；：），最多 30 字；「N 天未回」取其前同一分句 20 字。
//   skip：宾语含 askGateSkip（要的是决定不是信息）——T-A、T-C 跳过，T-B 不用本函数。
function quoteSpans(s) {
	const spans = [];
	for (const [o, c] of QUOTE_PAIRS) {
		let i = 0;
		while ((i = s.indexOf(o, i)) >= 0) {
			const j = s.indexOf(c, i + 1);
			if (j < 0) break;
			spans.push([i, j]);
			i = j + 1;
		}
	}
	return spans;
}
function firstOutside(re, s, spans) {
	const g = new RegExp(re.source, re.flags.replace(/[gy]/g, "") + "g");
	let m;
	while ((m = g.exec(s))) {
		const at = m.index;
		if (!spans.some(([a, b]) => at > a && at < b)) return m;
		if (m[0].length === 0) g.lastIndex++;
	}
	return null;
}
function askDetect(text) {
	const out = [];
	const ask = CRIT.askGateAsk;
	const wait = CRIT.askGateWait;
	const skip = CRIT.askGateSkip;
	if (!(ask instanceof RegExp) && !(wait instanceof RegExp)) return out;
	const s = String(text || "")
		.replace(/```[\s\S]*?```/g, "\n")
		.replace(/`[^`\n]*`/g, " ");
	for (const raw of s.split(/(?<=[。！？!?\n])/)) {
		const sent = raw.trim();
		if (!sent || sent.length < 4) continue;
		if (DONE_RE.test(sent)) continue;
		const spans = quoteSpans(sent);
		let hit = null;
		for (const [kind, re] of [
			["ask", ask],
			["wait", wait],
		]) {
			if (!(re instanceof RegExp)) continue;
			const m = firstOutside(re, sent, spans);
			if (m) {
				hit = { kind, m };
				break;
			}
		}
		if (!hit) continue;
		const mt = hit.m[0];
		let obj;
		if (hit.kind === "wait" && /未回$/.test(mt)) {
			const head = sent.slice(0, hit.m.index);
			const cut = Math.max(head.lastIndexOf("，"), head.lastIndexOf(","), head.lastIndexOf("；"), head.lastIndexOf(";"), head.lastIndexOf("："), head.lastIndexOf(":"));
			obj = head.slice(cut + 1).slice(-20);
		} else obj = sent.slice(hit.m.index + mt.length).split(/[。！？!?\n，,；;]/)[0].slice(0, 30);
		obj = obj.replace(/[「」『』“”"'‘’《》]/g, "").replace(/^[\s:：、]+/, "").trim();
		out.push({ kind: hit.kind, obj, sent: sent.slice(0, 200), skip: !!(skip instanceof RegExp && skip.test(obj)) });
		if (out.length >= 6) break;
	}
	return out;
}
// 闸内小同义表：CRIT.askGateSyn 源串按「;」「=」拆组（装载器只收正则与数值，这是不改装载器的写法）
function synGroups() {
	const re = CRIT.askGateSyn;
	if (!(re instanceof RegExp)) return [];
	return re.source
		.split(";")
		.map((g) => g.split("=").map((w) => w.trim()).filter(Boolean))
		.filter((g) => g.length > 1);
}
// X 词：对「宾语 + 所在句」做 qTokens，去 askGateStop，中文至少 2 字、西文至少 3 字；同义另列
function xWords(det) {
	const base = String(det.obj || "") + " " + String(det.sent || "");
	const stop = CRIT.askGateStop;
	const X = [];
	for (const t0 of qTokens(base)) {
		const t = String(t0).toLowerCase();
		if (stop instanceof RegExp && stop.test(t)) continue;
		if (/^\d+$/.test(t)) continue;
		if (/[一-龥]/.test(t) ? t.length < 2 : t.length < 3) continue;
		if (!X.includes(t)) X.push(t);
	}
	const syn = [];
	for (const grp of synGroups()) if (grp.some((w) => base.includes(w))) for (const w of grp) if (!X.includes(w) && !syn.includes(w)) syn.push(w);
	return { X, syn };
}
function topicKey(sid, X) {
	return crypto
		.createHash("sha1")
		.update(String(sid) + "|" + [...X].sort().slice(0, 4).join(","))
		.digest("hex")
		.slice(0, 8);
}

// ── askSearch（纯函数，drill 直调）：三路检索，返回 ≤3 条 ｛id, title, content, route｝──
//   o：｛X, syn, space（caller 空间，空则不限空间）, sid（本窗）, now（北京 ISO，缺省现在）, asOf（回放：只看 ts ≤ asOf 的条）, exclude（不回的 id，如刚写的那条）｝
//   词路 MATCH 用各词经 tokenize 后的串（与写库同侧分词）；词路只留标题或正文前 200 字含任一 X 词（含同义）的条，
//   指针路另要标题或正文含 askGatePtr 词，本窗自写路要标题或正文前 500 字含 X 词；三路都剔缺口条。
function askSearch(db, o) {
	const words = [...(o.X || []), ...(o.syn || [])].filter(Boolean);
	if (!words.length || !db) return [];
	const toks = [];
	for (const w of words)
		for (const t of tokenize(w)) {
			const lt = String(t).toLowerCase();
			if (lt && !toks.includes(lt)) toks.push(lt);
		}
	const m = queryMatch(toks.slice(0, 32));
	if (!m) return [];
	const now = o.now || o.asOf || nowIso();
	const nowMs = Date.parse(now) || Date.now();
	const asOf16 = o.asOf ? String(o.asOf).slice(0, 16) : null;
	const gapRe = CRIT.askGateGap instanceof RegExp ? CRIT.askGateGap : null;
	const ptrRe = CRIT.askGatePtr instanceof RegExp ? CRIT.askGatePtr : null;
	const lw = words.map((w) => w.toLowerCase());
	const has = (r, n) => {
		const s = (String(r.title || "") + "\n" + String(r.content || "").slice(0, n)).toLowerCase();
		return lw.some((w) => s.includes(w));
	};
	const ex = new Set((o.exclude || []).map(Number));
	const gap = (r) => ex.has(Number(r.id)) || !!(gapRe && gapRe.test(String(r.title || "")));
	const tsCut = asOf16 ? " AND replace(substr(m.ts, 1, 16), ' ', 'T') <= ?" : "";
	const spCut = o.space ? " AND m.space IN (?, 'global')" : "";
	const ftsSql =
		"SELECT m.id, m.title, m.content, m.essential, m.type, bm25(memories_fts) AS rank FROM memories_fts JOIN memories m ON m.id = memories_fts.rowid" +
		" WHERE memories_fts MATCH ? AND m.status = 'active' AND (m.valid_to IS NULL OR m.valid_to > ?)" +
		spCut +
		tsCut;
	const ftsArgs = (mm) => [mm, now, ...(o.space ? [o.space] : []), ...(asOf16 ? [asOf16] : [])];
	// ① 指针路
	//   盲审处置（nit）：源串须是「词|词|…」纯字面才按「|」拆；含分组、转义、量词等元字符（有人改成带分组的正则）时整路跳过、计 stats.askgatePtrBad
	let ptr = [];
	const ptrSrcOk = !!ptrRe && /^[^\\()[\]{}|.*+?^$]+(?:\|[^\\()[\]{}|.*+?^$]+)*$/.test(ptrRe.source);
	if (ptrRe && !ptrSrcOk && G.stats) G.stats.askgatePtrBad = (Number(G.stats.askgatePtrBad) || 0) + 1;
	if (ptrRe && ptrSrcOk) {
		const ptoks = [];
		for (const w of ptrRe.source.split("|"))
			for (const t of tokenize(w)) {
				const lt = String(t).toLowerCase();
				if (lt && !ptoks.includes(lt)) ptoks.push(lt);
			}
		const pm = ptoks.length ? `(${m}) AND (${queryMatch(ptoks)})` : "";
		if (pm)
			ptr = db
				.prepare(ftsSql + " ORDER BY rank LIMIT 10")
				.all(...ftsArgs(pm))
				.filter((r) => !gap(r) && has(r, 1e9) && ptrRe.test(String(r.title || "") + "\n" + String(r.content || "").slice(0, 500)));
	}
	// ② 本窗自写路：source 含本 sid 前 8 位、24 小时内、fact／decision／lesson
	let self = [];
	const s8 = sid8(o.sid);
	if (s8.length === 8) {
		self = db
			.prepare(
				"SELECT m.id, m.title, m.content, m.essential, m.type FROM memories m WHERE m.status = 'active' AND (m.valid_to IS NULL OR m.valid_to > ?) AND m.type IN ('fact', 'decision', 'lesson')" +
					" AND m.source LIKE ? AND replace(substr(m.ts, 1, 16), ' ', 'T') >= ?" +
					tsCut +
					" ORDER BY m.id DESC LIMIT 200",
			)
			.all(now, "%" + s8 + "%", bj16(nowMs - 24 * 3600e3), ...(asOf16 ? [asOf16] : []))
			.filter((r) => !gap(r) && has(r, 500))
			.slice(0, 10);
	}
	// ③ 词路：bm25 取 40 条，essential 优先（同 essential 按 bm25）
	const word = db
		.prepare(ftsSql + " ORDER BY rank LIMIT 40")
		.all(...ftsArgs(m))
		.filter((r) => !gap(r) && has(r, 200))
		.map((r, i) => ({ r, i }))
		.sort((a, b) => (Number(b.r.essential) || 0) - (Number(a.r.essential) || 0) || a.i - b.i)
		.map((x) => x.r);
	const out = [];
	const seen = new Set();
	for (const [route, arr] of [
		["ptr", ptr],
		["self", self],
		["word", word],
	])
		for (const r of arr) {
			if (out.length >= ROW_CAP) break;
			if (seen.has(r.id)) continue;
			seen.add(r.id);
			out.push({ id: r.id, title: r.title, content: r.content, route });
		}
	return out;
}
// 目录扫描（只 T-A、T-C）：cwd 在space目录下时，读 cwd 下深度 ≤3 的文件名和时刻（不读内容；跳过点开头的项与 node_modules；最多看 2000 项）；
//   文件名含 X 词（含同义），或 30 分钟内新放入的，最多 3 个，给相对路径
//   盲审处置（should，Z5／C9）：①只有文件名含 X 词的算命中（byName，按 mtime 倒序、排在前）；②「新放入」改看 birthtimeMs（原看 mtime，抓到的是
//   「近 30 分钟改过」），只作附列、不单独构成发闸理由（gate 判 rows 或 byName），且不列receipt件；拿不到生日的文件系统不算新；
//   ③MEMORY.md、AGENTS.md、CLAUDE.md 与路径含「台账」的一律不列（窗口活跃时这些天天在改，与索要无关）
const DIR_SKIP_BASE = new Set(["memory.md", "agents.md", "claude.md"]);
function dirScan(cwd, words, nowMs) {
	if (!cwd || !organFromPath(cwd)) return [];
	const root = String(cwd);
	const lw = words.map((w) => String(w).toLowerCase()).filter(Boolean);
	const found = [];
	let seen = 0;
	const walk = (dir, depth) => {
		if (depth > 3 || seen >= 2000) return;
		let ents;
		try {
			ents = fs.readdirSync(dir, { withFileTypes: true });
		} catch {
			return;
		}
		for (const e of ents) {
			if (seen >= 2000) return;
			if (e.name.startsWith(".") || e.name === "node_modules") continue;
			seen++;
			const p = path.join(dir, e.name);
			if (e.isDirectory()) {
				walk(p, depth + 1);
				continue;
			}
			if (!e.isFile()) continue;
			const nm = e.name.toLowerCase();
			if (DIR_SKIP_BASE.has(nm)) continue;
			const rel = path.relative(root, p);
			if (rel.includes("台账")) continue;
			let fst;
			try {
				fst = fs.statSync(p);
			} catch {
				continue;
			}
			const bt = Number(fst.birthtimeMs) || 0;
			if (lw.some((w) => nm.includes(w))) found.push({ rel, mt: fst.mtimeMs, byName: true });
			else if (bt > 0 && bt <= nowMs + 60e3 && nowMs - bt <= 30 * 60e3 && !e.name.includes("回执")) found.push({ rel, mt: bt, byName: false });
		}
	};
	walk(root, 1);
	found.sort((a, b) => Number(b.byName) - Number(a.byName) || b.mt - a.mt);
	return found.slice(0, 3);
}

// ── 闸文（≤1.2KB；前缀固定，drill 按前缀认）──
const oneLine = (s) => String(s == null ? "" : s).replace(/\s+/g, " ").trim();
const clip = (s, n) => stripLone(Array.from(oneLine(s)).slice(0, n).join(""));
const HEADS = {
	A: (x) => `〔记忆闸·开口前检索〕你本轮向maintainer索要「${clip(x, 20)}」。开口前替你查了一遍：`,
	B: () => "〔记忆闸〕发问前先核：",
	Bp: (x) => `〔记忆闸·问后检索〕你刚向maintainer发问「${clip(x, 20)}」。替你查了一遍：`,
	C: (x) => `〔记忆闸·登记前检索〕你刚登记了待maintainer提供「${clip(x, 20)}」。替你查了一遍：`,
	Ad: (x) => `〔记忆闸·开口前检索〕你上一轮向maintainer索要「${clip(x, 20)}」。替你查了一遍：`, // 降级路暂存件（下一条真用户消息 claim 时才 inject）
};
const TAILS = {
	A: "先核这几条和文件；确实没有，再向maintainer说明已查过哪些、还缺什么。本提示每题一次。",
	B: "核过仍要问，把问题原样再发一次即放行。",
	Bp: "maintainer已答；对照库里已有的条再核一遍答复。", // 盲审处置（nit）：post-execute 跑时maintainer已答完，原「maintainer回话前先核」与时序不符
	C: "若已有答案，把登记改成已有，并留指针。",
	Ad: "maintainer这条新消息可能已经给了，先对照；再核这几条，确实没有再说明已查过哪些、还缺什么。本提示每题一次。",
};
function compose(route, x, rows, files, nowMs, hk) {
	const k = hk || route; // 闸文抬头与尾句的键（降级路暂存件用 Ad；计数与日志环仍按 route）
	const build = (cn, withFiles) => {
		const ls = [HEADS[k](x)];
		if (rows.length) ls.push("库里已有：" + rows.map((r) => `〔#${r.id}〕${clip(r.title, 30)}${cn > 0 ? "｜" + clip(r.content, cn) : ""}${r.route === "self" ? "（本窗自写）" : ""}`).join("；"));
		if (withFiles && files.length) ls.push("目录里有：" + files.map((f) => `${clip(f.rel, 60)}（${bjHM(f.mt)} ${f.byName ? "改" : "放入"}）`).join("；"));
		ls.push(TAILS[k]);
		return ls.join("\n");
	};
	for (const [cn, wf] of [
		[60, true],
		[40, true],
		[20, true],
		[0, true],
		[0, false],
	]) {
		const t = build(cn, wf);
		if (Buffer.byteLength(t, "utf8") <= BYTE_CAP) return t;
	}
	let t = build(0, false);
	while (Buffer.byteLength(t, "utf8") > BYTE_CAP) t = Array.from(t).slice(0, -10).join("");
	return t;
}
function deepFreeze(o) {
	if (o && typeof o === "object" && !Object.isFrozen(o)) {
		Object.freeze(o);
		for (const v of Object.values(o)) deepFreeze(v);
	}
	return o;
}
// 宿主 createUserMessage 同形：deepFreeze(structuredClone({...input, role:"user", id}))；source 写 legion-askgate
function mkMsg(text) {
	return deepFreeze({ content: [{ type: "text", text }], source: { kind: "legion-askgate" }, role: "user", id: crypto.randomUUID() });
}

// ── 一次判闸（三路共用）：逐个索要句（至多 3 个）判同词放行、题键 24 小时、每窗每日帽，再检索；第一个有命中的发闸 ──
function gate(route, sid, cwd, dets, o) {
	const st = turnOf(sid);
	const now = Date.now();
	let outcome = null;
	for (const d of dets.slice(0, 3)) {
		if (d.skip) continue;
		const { X, syn } = xWords(d);
		if (!X.length && !syn.length) continue;
		const all = [...X, ...syn];
		const key = topicKey(sid, X.length ? X : syn);
		if (all.some((w) => st.qWords.has(String(w).toLowerCase()))) {
			if (!outcome) outcome = { kind: "pass", key };
			continue;
		}
		const tAt = TOPIC.get(key);
		if (tAt !== undefined && now - tAt < TOPIC_MS) {
			if (!outcome || outcome.kind === "nohit") outcome = { kind: "skip", key, why: "topic" };
			continue;
		}
		const day = DAY.get(sid);
		if (day && day.day === bjDay() && day.n >= DAY_CAP) {
			outcome = { kind: "skip", key, why: "day" };
			break;
		}
		let space = null;
		try {
			space = cwd ? organFromPath(cwd) : sessionOrgan(sid); // 同 inject.cjs 召回取法：cwd 直推 > sid > null
		} catch {}
		const rows = askSearch(G.db, { X, syn, space: space || null, sid, asOf: o && o.asOf, exclude: o && o.exclude });
		const files = o && o.dir ? dirScan(cwd, all, now) : [];
		if (!rows.length && !files.some((f) => f.byName)) {
			// 盲审处置（should）：只有「近 30 分钟新放入」的文件、库里无命中时不发闸（新文件只作附列）
			if (!outcome) outcome = { kind: "nohit", key };
			continue;
		}
		TOPIC.set(key, now);
		const dd = DAY.get(sid);
		if (dd && dd.day === bjDay()) dd.n += 1;
		else DAY.set(sid, { day: bjDay(), n: 1 });
		return { kind: "fire", key, det: d, rows, files, ids: rows.map((r) => r.id), text: compose(route, d.obj || X.join(""), rows, files, now, o && o.deferred ? "Ad" : route) };
	}
	return outcome || { kind: "none" };
}
// 非发闸结局计数与记环
function settleQuiet(route, sid, r) {
	if (!r || r.kind === "none" || r.kind === "fire") return;
	bump("askgate_" + r.kind + "_total");
	ring(route, sid, r.key, [], r.kind + (r.why ? ":" + r.why : ""));
}
function fireCount(route, sid, r, action) {
	const st = turnOf(sid);
	st.fired = true;
	bump("askgate_fired_total");
	bump("askgate_" + action + "_total");
	ring(route, sid, r.key, r.ids, action);
}

// T-A：收轮前
function turnText(agent, st) {
	if (st.asst.length) return st.asst.join("\n");
	// 兜底：会话事件没攒到时，从会话快照倒取本轮助手正文（到本轮 turn/start 为止）
	try {
		const s = agent && agent.session;
		const evs = (s && typeof s.snapshotEvents === "function" ? s.snapshotEvents() : s && s.events) || [];
		const parts = [];
		let len = 0;
		for (let i = evs.length - 1; i >= 0 && len < ASST_CAP; i--) {
			const e = evs[i];
			if (!e) continue;
			if (e.type === "turn/start") break;
			if (e.type === "assistant/message") {
				const t = textOf(e.data && e.data.message ? e.data.message.content : e.data && e.data.content);
				if (t) {
					parts.unshift(t);
					len += t.length;
				}
			}
		}
		return parts.join("\n");
	} catch {
		return "";
	}
}
function onTurnStopping(payload) {
	const agent = payload && payload.agent;
	G.stopEver = true;
	// 盲审处置（nit）：已中止的收轮不闸——中止阶段 steer 会改投 next-turn 并唤醒驱动（agent-loop L800-804），闸文单独起一整轮
	if (payload && payload.signal && payload.signal.aborted) return;
	const sid = sidOf(agent);
	if (!sid || isSub(agent, sid)) return;
	const st = turnOf(sid);
	if (st.evalA) return; // 每轮至多判一次（同一轮第二次 turn-stopping 不再闸，否则自己转圈）
	st.evalA = true;
	const dets = askDetect(turnText(agent, st));
	if (!dets.length) return;
	const canSteer = !!agent && typeof agent.steer === "function";
	if (!canSteer && !degOn()) {
		// 盲审处置（should）：agent 上没有 steer、降级路未开——判闸前跳过（不占题键与日帽），记 skip
		bump("askgate_skip_total");
		ring("A", sid, "", [], "skip:nosteer");
		return;
	}
	const cwd = agent && agent.session && agent.session.header ? agent.session.header.cwd : undefined;
	const r = gate("A", sid, cwd, dets, { dir: true, deferred: !canSteer });
	if (r.kind !== "fire") return settleQuiet("A", sid, r);
	const msg = mkMsg(r.text);
	if (canSteer) {
		agent.steer(msg);
		G.stats.askgateMode = "steer";
		fireCount("A", sid, r, "steer");
	} else {
		STASH.set(sid, msg);
		G.stats.askgateMode = "inject";
		fireCount("A", sid, r, "stash");
	}
}
// 降级路一：本进程从没见过 turn-stopping 时，turn/end 判 T-A 并暂存
function onTurnEndDegrade(sid) {
	if (G.stopEver || !degOn()) return; // 盲审处置（should）：降级路缺省关
	if (isSub(null, sid)) return; // 盲审处置（nit）：子代理不闸（此处只有 sid：认 subagent/start 记下的 id）
	const st = turnOf(sid);
	if (st.evalA) return;
	st.evalA = true;
	const dets = askDetect(st.asst.join("\n"));
	if (!dets.length) return;
	const r = gate("A", sid, undefined, dets, { dir: false, deferred: true });
	if (r.kind !== "fire") return settleQuiet("A", sid, r);
	STASH.set(sid, mkMsg(r.text));
	G.stats.askgateMode = "inject";
	fireCount("A", sid, r, "stash");
}
// 降级路二：该窗下一条真用户消息被 claim 时 inject（排到下一次 pre-step）
function onClaimed(payload) {
	const message = (payload && payload.message) || payload;
	if (!message || !message.source || message.source.kind !== "user") return;
	const agent = payload && payload.agent;
	const sid = sidOf(agent);
	const m = sid && STASH.get(sid);
	if (!m) return;
	STASH.delete(sid);
	if (typeof agent.inject === "function") {
		agent.inject(m);
		bump("askgate_inject_total");
		ring("A", sid, "", [], "inject");
	}
}

// T-B：问maintainer前
function questionText(args) {
	const qs = args && Array.isArray(args.questions) ? args.questions : [];
	const parts = [];
	for (const q of qs) {
		if (!q) continue;
		if (q.header) parts.push(String(q.header));
		if (q.question) parts.push(String(q.question));
		if (Array.isArray(q.options)) for (const op of q.options) if (op && op.label) parts.push(String(op.label));
	}
	return parts.join("。");
}
function preGate(exec) {
	if (!exec || exec.name !== "ask_user_question" || exec.parent !== undefined) return null;
	const agent = exec.agent;
	const sid = sidOf(agent);
	if (!sid || isSub(agent, sid)) return null;
	const text = questionText(exec.arguments);
	if (!text) return null;
	const sig = crypto.createHash("sha1").update(JSON.stringify(exec.arguments || {})).digest("hex").slice(0, 12);
	const now = Date.now();
	const d = DENY.get(sid);
	if (d && d.sig === sig && now - d.at <= DENY_MS) {
		DENY.delete(sid);
		bump("askgate_pass_total");
		ring("B", sid, d.key, [], "pass:resend");
		return null;
	}
	const st = turnOf(sid);
	if (st.denied) {
		bump("askgate_skip_total");
		ring("B", sid, "", [], "skip:turn");
		return null;
	}
	const q0 = exec.arguments && Array.isArray(exec.arguments.questions) && exec.arguments.questions[0] ? String(exec.arguments.questions[0].question || "") : text;
	const cwd = agent && agent.session && agent.session.header ? agent.session.header.cwd : undefined;
	const r = gate("B", sid, cwd, [{ kind: "ask", obj: q0.slice(0, 30), sent: text.slice(0, 300), skip: false }], { dir: false });
	if (r.kind !== "fire") {
		settleQuiet("B", sid, r);
		return null;
	}
	st.denied = true;
	DENY.set(sid, { sig, at: now, key: r.key });
	fireCount("B", sid, r, "deny");
	return { kind: "deny", reason: r.text };
}
// T-C（及 DENY_OFF 下的 T-B 问后）：工具返回后追加上下文；返回 ｛msg, commit｝，追加成功才计数
function postGate(exec, result) {
	if (!exec || exec.parent !== undefined) return null;
	const name = String(exec.name || "");
	const isB = name === "ask_user_question" && denyOff();
	if (!isB && !T_C_TOOLS.has(name)) return null;
	if (result && result.isError) return null;
	const agent = exec.agent;
	const sid = sidOf(agent);
	if (!sid || isSub(agent, sid)) return null;
	const st = turnOf(sid);
	const args = exec.arguments || {};
	const cwd = agent && agent.session && agent.session.header ? agent.session.header.cwd : undefined;
	if (isB) {
		if (st.bpost) return null;
		const text = questionText(args);
		if (!text) return null;
		const q0 = Array.isArray(args.questions) && args.questions[0] ? String(args.questions[0].question || "") : text;
		const r = gate("Bp", sid, cwd, [{ kind: "ask", obj: q0.slice(0, 30), sent: text.slice(0, 300), skip: false }], { dir: false });
		if (r.kind !== "fire") {
			settleQuiet("B", sid, r);
			return null;
		}
		return {
			msg: mkMsg(r.text),
			commit: () => {
				st.bpost = true;
				fireCount("B", sid, r, "ctx");
			},
		};
	}
	let texts = [];
	let exclude = [];
	if (name === "memory_write") {
		const v = result && result.value;
		if (v && typeof v === "object" && v.error) return null; // 没写成（工具以 value.error 回报）就不算登记
		if (v && Number.isFinite(Number(v.id))) exclude = [Number(v.id)]; // 刚写的那条不回给窗口
		texts = [String(args.title || "") + "。" + String(args.content || "").slice(0, 300)];
	}
	else {
		// 盲审处置（nit）：status 为 completed／done／cancelled 的项不判；note 并进判项文本
		const items = Array.isArray(args.todos)
			? args.todos
					.filter((x) => x && !/^(?:completed|done|cancell?ed)$/i.test(String(x.status || "")))
					.map((x) => [String(x.content || ""), x.note ? String(x.note) : ""].filter(Boolean).join("。"))
					.filter(Boolean)
			: [];
		let seen = SEEN_ITEMS.get(sid);
		if (!seen) {
			seen = new Set();
			SEEN_ITEMS.set(sid, seen);
		}
		texts = items.filter((t) => !seen.has(t));
		for (const t of texts) if (seen.size < 500) seen.add(t); // 只对新出现的项判（已判过的不再判，免得整表重写时反复计）
	}
	const dets = [];
	for (const t of texts) for (const d of askDetect(t)) dets.push(d);
	if (!dets.length) return null;
	if (st.ctxN >= 2) {
		bump("askgate_skip_total");
		ring("C", sid, "", [], "skip:turn");
		return null;
	}
	// 盲审处置（nit）：判闸时同步预占名额——原「追加成功才加」时同一步并行的几条都能过检；
	//   补笔⑥（10-18）后判闸挪到 await next() 之后，检与占在同一段同步码里完成，并行调用不会同时过检；不发、追加失败时再退（release）
	st.ctxN += 1;
	let r;
	try {
		r = gate("C", sid, cwd, dets, { dir: true, exclude });
	} catch (eG) {
		st.ctxN -= 1;
		throw eG;
	}
	if (r.kind !== "fire") {
		st.ctxN -= 1;
		settleQuiet("C", sid, r);
		return null;
	}
	return {
		msg: mkMsg(r.text),
		commit: () => {
			fireCount("C", sid, r, "ctx");
		},
		release: () => {
			st.ctxN = Math.max(0, st.ctxN - 1);
		},
	};
}

// 会话事件（index.cjs session/event 与 slimOnSessionEvent 并列调用）：攒本轮助手正文与本轮查询词；闸后同轮查库记 follow；降级路 turn/end 判 T-A
function askOnSessionEvent(sid, t, event) {
	try {
		if (!G.on || agOff() || !sid) return;
		if (t === "turn/start") {
			TURN.set(sid, freshTurn(event && event.data ? event.data.turn : undefined));
			sweep();
			return;
		}
		if (t === "assistant/message") {
			const d = (event && event.data) || {};
			const txt = textOf(d.message ? d.message.content : d.content);
			if (!txt) return;
			const st = turnOf(sid);
			st.asst.push(txt);
			st.asstLen += txt.length;
			while (st.asstLen > ASST_CAP && st.asst.length > 1) st.asstLen -= st.asst.shift().length;
			st.at = Date.now();
			return;
		}
		if (t === "tool/call") {
			const d = (event && event.data) || {};
			if (String(d.name || d.toolName || "") !== "memory") return;
			let a = d.arguments !== undefined ? d.arguments : d.input;
			if (typeof a === "string") {
				try {
					a = JSON.parse(a);
				} catch {
					a = {};
				}
			}
			const act = String((a && a.action) || "");
			const st = turnOf(sid);
			if ((act === "search" || act === "timeline") && a.query) for (const w of qTokens(String(a.query))) st.qWords.add(String(w).toLowerCase());
			if (st.fired && !st.followed && (act === "search" || act === "read_episodic")) {
				st.followed = true;
				bump("askgate_follow_total");
			}
			st.at = Date.now();
			return;
		}
		if (t === "turn/end" && actOn()) onTurnEndDegrade(sid); // 探针态（缺省）不判闸
	} catch {
		agErr();
	}
}

function setupAskGate({ ctx, stats, db }) {
	G.ctx = ctx;
	G.stats = stats;
	G.db = db;
	for (const k of ["LEGION_ASKGATE_OFF", "LEGION_ASKGATE_ON", "LEGION_ASKGATE_DENY_OFF", "LEGION_ASKGATE_DEGRADE_ON"]) {
		const v = process.env[k];
		if (v && v !== "1") ctx.logger?.warn?.(`[living-memory] wave ${k}=${String(v).slice(0, 8)} 不是 1，按未设处理`);
	}
	if (agOff()) {
		G.on = false;
		stats.askgateMode = "off";
		return;
	}
	G.on = true;
	// 盲审处置（must）：缺省 probe（只计数）；ON=1 时 armed，真 steer 过才记 steer、真走过降级路才记 inject
	stats.askgateMode = actOn() ? "armed" : "probe";
	stats.askgateHooks = { stop: 0, pre: 0, post: 0, sub: 0, claim: 0 };
	ctx.on("agent/turn-stopping", async (payload) => {
		try {
			hookSeen("stop");
			if (actOn()) onTurnStopping(payload);
		} catch {
			agErr();
		}
	});
	// 补笔⑥（10-18·照侦察 K5 五-4 建议；宿主两法皆合，取保守写法）：先 await next()——排在后面的 pre-execute 监听（如将来挂这一点的插件；在役档没有）照跑；
	//   下游已判 ask／deny／cancel 时原样返回、闸不判（不占题键、日帽与本轮 deny 名额）；只把下游的 allow 改成 deny。next 只调一次，抛错照抛
	ctx.on("tools/pre-execute", async (exec, next) => {
		try {
			hookSeen("pre");
		} catch {
			agErr();
		}
		const d = await next();
		if (!d || d.kind !== "allow") return d;
		let deny = null;
		try {
			if (actOn() && !denyOff()) deny = preGate(exec);
		} catch {
			agErr();
		}
		return deny || d;
	});
	// 补笔⑥（10-18·照侦察 K5 五-6 建议）：先 await next()（下游先定 accept／block），再判闸——下游 block 时原样返回、不判（不占题键、日帽与每轮名额）；
	//   只在下游 additionalContexts 末尾追加一条，content、value 原样不动（宿主不许二者同给）；T-C 跳过 result.isError 在 postGate 开头（原已合规）
	ctx.on("tools/post-execute", async (exec, result, next) => {
		try {
			hookSeen("post");
		} catch {
			agErr();
		}
		const down = await next();
		if (!down || typeof down !== "object" || down.kind === "block") return down;
		let add = null;
		try {
			if (actOn()) add = postGate(exec, result);
		} catch {
			agErr();
		}
		if (!add) return down;
		try {
			const out = { ...down, additionalContexts: [...(Array.isArray(down.additionalContexts) ? down.additionalContexts : []), add.msg] };
			add.commit();
			return out;
		} catch {
			try {
				if (typeof add.release === "function") add.release();
			} catch {}
			agErr();
			return down;
		}
	});
	ctx.on("subagent/start", (info) => {
		try {
			hookSeen("sub");
			if (!agOff() && info && info.id !== undefined) {
				SUBS.set(String(info.id), Date.now());
				subsSweep();
			}
		} catch {
			agErr();
		}
	});
	ctx.on("agent/inbox/claimed", (payload) => {
		try {
			if (!STASH.size || !actOn()) return;
			hookSeen("claim");
			onClaimed(payload);
		} catch {
			agErr();
		}
	});
}

module.exports = { setupAskGate, askOnSessionEvent, askDetect, askSearch, xWords, dirScan, compose, _state: { TURN, SUBS, TOPIC, DAY, DENY, SEEN_ITEMS, STASH, G } };
