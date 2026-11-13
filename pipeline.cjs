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

const core = require("./core.cjs");
const {
	securityCheck,
	SENSITIVE_RE,
	gateTitleTokens,
	gateTokenInter,
	CRIT,
} = core;

// ── 段1：安检闸（原语 securityCheck·载荷参数化）────────────────────────────────────
//    手写路宽面（extra=source+relatedNotes 并入扫描·F0-3 修语义）；提炼/nightly patrol窄面（title+content）。
//    返回统一形态 {pass, kind?, reason?}。
function securityGate(title, content, extra) {
	const sec = securityCheck(String(title), String(content) + (extra ? "\n" + extra : ""));
	return sec.ok ? { pass: true } : { pass: false, kind: "security", reason: sec.reason };
}

// ── 段2：敏感闸（原语·提炼路既有语义）───────────────────────────────────────
function sensitiveGate(title, content) {
	return SENSITIVE_RE.test(String(title)) || SENSITIVE_RE.test(String(content))
		? { pass: false, kind: "sensitive", reason: "sensitive" }
		: { pass: true };
}

// ── 段3：同题轻查（gateTitleTokens 族·同 space active 交叠≥阈）─────────────────────
//    供需要通用同题查重的写入点（现状三源各有语义化实现·本段为统一出口备未来接入；阈键 CRIT.sameTopicOverlapMin）。
function sameTopicGate(db, title, space) {
	const overlapMin = Number(CRIT.sameTopicOverlapMin) || 0;
	if (overlapMin <= 0 || String(title).length < 4) return { pass: true };
	try {
		const tT = gateTitleTokens(String(title));
		if (tT.size < overlapMin) return { pass: true };
		const cands = db
			.prepare("SELECT id, title FROM memories WHERE space = ? AND status = 'active'")
			.all(String(space));
		for (const cr of cands) {
			if (gateTokenInter(tT, gateTitleTokens(cr.title)) >= overlapMin)
				return { pass: false, kind: "same-topic", reason: "overlap " + cr.id, existingId: cr.id };
		}
	} catch {}
	return { pass: true };
}

// ── 组合器：安检→敏感（→同题可选）─────────────────────────────────────────────
//    nightly patrol重接默认形态（安检+敏感全开·同题 flags.sameTopic 开）；闸序照提炼路（sec→sensitive）零变。
function writePipeline({ db, title, content, space, extraScan, sameTopic }) {
	const g1 = securityGate(title, content, extraScan);
	if (!g1.pass) return g1;
	const g2 = sensitiveGate(title, content);
	if (!g2.pass) return g2;
	if (sameTopic) {
		const g3 = sameTopicGate(db, title, space);
		if (!g3.pass) return g3;
	}
	return { pass: true };
}

module.exports = { securityGate, sensitiveGate, sameTopicGate, writePipeline };
