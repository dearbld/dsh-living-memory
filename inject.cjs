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
	CRIT,
	INSTRUCT_QUERY,
	ORD_TODO_RE,
	ORD_WORDS,
	ORD_WORDS_EN,
	PN_RESCUE_SCORE,
	VEC_MODEL,
	blobToF32,
	clipSurrogateTail,
	CUR_SQL, // 车二-9 块一 1-a／1-b（10-25 落码）：注入面共用的现行谓词（core.cjs 定义）
	closureCheck,
	cosine,
	embedOnce,
	exportTaskLedger,
	gateKeywords,
	jieba,
	isSystemRow, // 车二-9 块一 1-b（10-25 落码）：J12 三类系统条判法（core.cjs 定义）
	matchWaitWho,
	normSid,
	nowIso,
	nowIso24hAgo,
	organFromPath,
	porter2Stem,
	projcacheRows,
	promptSafe,
	promptSafeContext,
	qTokens,
	queryMatch,
	recordInjectBytes,
	renderTaskLedger,
	sessionOrgan,
	tokenize,
	vecCredCache,
	vecRecallCore,
} = core;

// ══ 注入瘦身车 W4（10-11·maintainer 10-03 方案 1324 四案＋统筹审核 W4·副脑分支落码·依据 设计/注入瘦身/落码task brief.md §3.1–3.11）══
//    ①两段合一：召回变量占位尾接在状态段条目（legion-memory-recall）上——宿主 interpolate 不二次扫变量值，值照旧 promptSafe；
//    ②按轮冻结：bd 三计数 injTurn／injBrk／injUsr 作状态段键、injTurn 作召回段键——同轮后续步原样返回（宿主判同不重发·前缀缓存友好）；
//    ③同轮字节账：每次装配恰结算一笔（钩子两个 next 调用点之前；状态段入口兜底补结 settleLate）。
//    总闸 LEGION_INJECT_SLIM_OFF=1 ⇒ 注入文本、变量、contexts、injectMeter 旧字段与 1ddf6fa 逐字节相同（§4）；新开关只认 "1"、每次调用现读。
const { envIntCap, injectMeter, recordInjectTurn, stripLone, INJECT_TURN_CAP_BYTES, PROMPT_SAFE_STAT } = core;
const SLIM_VER = "slim-v1"; // 策略版本（本轮记录与冻结键同记·换策略即不认旧冻结）
const envOn = (name) => process.env[name] === "1";
// 1558＋2301 A②，仅约束 PlanFence 提示面，不改 closureCheck/写闸/retrieval gate。
// 写入处 index.cjs 三题名前缀：压缩总结：、上下文压缩锚：、本窗任务账快照：。
const planFenceRepairOn = () => !envOn("LEGION_PLANFENCE_REPAIR_OFF");
function planFenceAllows(db, todo, evidence) {
	if (!planFenceRepairOn() || !evidence) return true;
	const title = String(evidence.title || "");
	if (String(evidence.source || "").startsWith("auto:") && /压缩总结|压缩锚|本窗任务账快照/.test(title)) return false;
	// 终案同族还含“关闭候审48290”“销本轮交付48257”等前缀式；仍只取标题编号。
	if (!/终案|销号|销账|结案|办结|(?:关闭|销)\s*[:：]?\s*(?:(?:本轮|本次|本条)?(?:候审|候回执|待办|任务|原账|账|交付|条目))?\s*[:：]?\s*#?[1-9]\d*/.test(title)) return true;
	const ids = [...title.matchAll(/#([1-9]\d*)\b|(?<![\d.])([1-9]\d*)\s*(?=终案|销号|销账|结案|办结)|(?:终案|销号|销账|结案|办结|关闭|销)\s*[:：]?\s*(?:(?:本轮|本次|本条)?(?:候审|候回执|待办|任务|原账|账|交付|条目))?\s*[:：]?\s*#?([1-9]\d*)\b/g)].map((m) => Number(m[1] || m[2] || m[3]));
	if (!ids.length) return true; // 无标题 id：照旧词面，不从正文臆造关联
	if (ids.includes(Number(todo.id))) return true;
	// 真实关联＝库内一跳有效边（双向），不拿正文 #id 或失效边充血缘。
	const related = db.prepare("SELECT src, dst FROM memories_edges WHERE (src = ? OR dst = ?) AND invalid_at IS NULL").all(todo.id, todo.id);
	return related.some((e) => ids.includes(Number(e.src)) || ids.includes(Number(e.dst)));
}
// wave 车二-3（10-17·落码规格 2.3）：专名救援限席、整词，缺省开；LEGION_PN_LIMIT_OFF=1 回到旧行为（只认 "1"·每次调用现读）
const c2PnLimitOn = () => !envOn("LEGION_PN_LIMIT_OFF");
// 车二-3①：查询原文里的路径子串（(?:~|/)[\w.\-/]+）切词后的集合（小写）——这些词不进专名、不作西文单桥
// 盲审处置（should）：路径只认真路径形态——前一字不是西文词字、点、横、~、/（「Etsy/Shopify」「Airwallex/HSBC」这类 A/B 并列不算）；
//   「~/」「./」「../」开头一段即算；单「/」开头：前一字是行首、空白或标点时至少两段，前一字是中文等别的字时至少三段
//   （「在/srv/<用户名>/…」照认，「淘宝/Shopify/Etsy」不认）；段内可含中文（「~/workspace/<space>/modules」整串算路径，workspace 照剔）。
//   URL（https://…）的「//」不算路径段（协议词、www、com 仍由停用表挡）；无前导「/」的相对路径（a/b/c）不认，与 A/B/C 并列分不开
const C2_PATH_SEG = "[^\\s/，。、；：！？,;:!?（）()「」『』“”‘’\"'`<>【】\\[\\]{}|=*]+";
const C2_PATH_PUN = "\\s\"'`（(「『“‘<>【\\[{=:：，。、；！？,;!?|*";
const C2_PATH_RE = new RegExp(
	"(?<![A-Za-z0-9_.\\-~/])(?:~|\\.{1,2})\\/" + C2_PATH_SEG + "(?:\\/" + C2_PATH_SEG + ")*" +
		"|(?:^|(?<=[" + C2_PATH_PUN + "]))\\/" + C2_PATH_SEG + "(?:\\/" + C2_PATH_SEG + ")+" +
		"|(?<![A-Za-z0-9_.\\-~/" + C2_PATH_PUN + "])\\/" + C2_PATH_SEG + "(?:\\/" + C2_PATH_SEG + "){2,}",
	"g",
);
function c2PathToks(query) {
	const out = new Set();
	try {
		for (const m of String(query || "").matchAll(C2_PATH_RE)) for (const t of tokenize(m[0])) out.add(String(t).toLowerCase());
	} catch {}
	return out;
}
// 车二-3①②③：西文专名／单桥资格——长度 ≥3、非纯数字（批⑰ B5① 照旧）、不在停用表 pnRescueStop（CRIT 外置）、不是路径段
function c2PnOk(t, pathToks) {
	const s = String(t);
	const stop = CRIT.pnRescueStop;
	return /^[\x21-\x7e]{3,}$/.test(s) && !/^\d+$/.test(s) && !(stop && stop.test(s)) && !pathToks.has(s.toLowerCase());
}
// 车二-3④：整词判定（不分大小写；中文等非 [a-z0-9_] 字符算词界）
function c2WholeWord(text, t) {
	const esc = String(t).toLowerCase().replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
	return new RegExp("(^|[^a-z0-9_])" + esc + "($|[^a-z0-9_])").test(String(text).toLowerCase());
}
const slimOn = () => !envOn("LEGION_INJECT_SLIM_OFF");
const slimShow = () => Math.min(4, envIntCap(process.env.LEGION_AUTORECALL_SHOW, 2)); // 保守 C：缺省 2·上限 4（§4）
const c2PtrOn = () => !envOn("LEGION_RECALL_PTR_OFF"); // wave 车二-4（10-17·落码规格 2.4）：被截指针与 cut_search 计数，缺省开；只认 "1"（每次调用现读）
const STATE_BY_CTX = new WeakMap(); // 装配 context（agent-loop 每步新建）→ 本步状态段结果——钩子结算取用（context 非对象时不存·§3.1）
// wave 车二-2（10-17·落码规格 2.2）：催查提示单独回补（A/B 对照试验），缺省关；LEGION_SEARCH_HINT_ON=1 才开（每次调用现读）
const c2HintOn = () => envOn("LEGION_SEARCH_HINT_ON");
const C2_HINT_LINE = "\n（不确定先 search／timeline，换词再查）";
// 车二-2：按会话分臂——sha1(sid) 首字节偶数为 A 臂（加提示行），奇数为 B 臂（对照，不加）；确定、可复算
function hintArm(sid) {
	return require("node:crypto").createHash("sha1").update(String(sid || "")).digest()[0] % 2 === 0 ? "A" : "B";
}
const SLIM_PEND = new Map(); // 待结算标：会话键 → 本步状态段（他插件截断 waterfall／只调 text 不走钩子时，由同会话下一步入口补结）
const slimErr = () => {
	injectMeter.errors = (injectMeter.errors || 0) + 1; // ⑬ 出口（read injectMeterErrors／render「注计错」）
};
const slimSid8 = (sid) => String(sid || "").replace(/^session-/, "").slice(0, 8); // 本轮记录只记 sid 前 8 位（去前缀·不记正文）
// organ_meta 持久计数：单语句 UPSERT（同 bumpNudgeTotal／forelook_shown_total 写法·跨进程无 lost-update 窗）
//   增量与 forelook_shown_total 同绑字符串（盲审 R2·10-12）：node:sqlite 把 JS 数按 REAL 绑定，ON CONFLICT 累加后会存成「3.0」
function slimBump(db, k, n) {
	try {
		db.prepare(
			"INSERT INTO organ_meta(k, v) VALUES(?, ?) ON CONFLICT(k) DO UPDATE SET v = CAST(CAST(v AS INTEGER) + ? AS TEXT)",
		).run(k, String(n), String(n));
	} catch {
		slimErr();
	}
}
// 状态段入口：①同会话上一步状态段算过而钩子未结 ⇒ 先补结（settleLate）；②判冻结——只认真 sid（context.agent.session.id），
//   bd 不在／首个 turn/start 之前／FREEZE_OFF ⇒ 不冻（每步重算＝旧行为，前两种计 freezeUnknown·§3.4）；
//   键＝injTurn:injBrk:injUsr，同版同键命中即冻；同轮键变（白名单、compaction/end、轮中真用户消息）计 freezeBreaks 并复用本轮 nudge。
function slimStateEnter(context, bindMap) {
	const ctl = { isCtx: !!context && typeof context === "object", sid: "", pk: "", bd: null, freezable: false, key: "", frozen: false, raw: "", out: "", vis: [], reuseNudge: false, nudge: "", why: "" };
	try {
		const sid = String((ctl.isCtx && context.agent && context.agent.session && context.agent.session.id) || "");
		ctl.sid = sid;
		ctl.pk = sid; // 无 sid 不挂待结算标（盲审 R6·10-12）：原共用 "_nosid" 键，两个无 sid 装配重叠时后者会把前者误补结、同一装配记两笔
		const p = ctl.pk ? SLIM_PEND.get(ctl.pk) : undefined;
		if (p && p.ctx !== context) {
			SLIM_PEND.delete(ctl.pk);
			injectMeter.settleLate += 1;
			recordInjectTurn(Buffer.byteLength(p.out, "utf8"), { sid: slimSid8(p.sid), turn: p.turn, ver: SLIM_VER, st: 1, why: p.why, late: 1 });
		}
		const bd = sid ? bindMap.get(sid) : null;
		ctl.bd = bd || null;
		if (envOn("LEGION_INJECT_FREEZE_OFF")) {
			ctl.why = "freeze-off";
			return ctl;
		}
		if (!ctl.isCtx || !sid || !bd || !(bd.injTurn > 0)) {
			injectMeter.freezeUnknown += 1;
			ctl.why = "unknown";
			return ctl;
		}
		ctl.freezable = true;
		ctl.key = bd.injTurn + ":" + (bd.injBrk || 0) + ":" + (bd.injUsr || 0);
		const s = bd.injSt;
		if (s && s.ver === SLIM_VER && s.key === ctl.key) {
			ctl.frozen = true;
			ctl.raw = s.raw;
			ctl.out = s.out;
			ctl.vis = s.vis;
			return ctl;
		}
		if (s && s.ver === SLIM_VER && s.turn === bd.injTurn) {
			injectMeter.freezeBreaks += 1;
			ctl.reuseNudge = true;
			ctl.nudge = s.nudge || "";
			ctl.why = bd.injBrkWhy ? "brk:" + bd.injBrkWhy : "break"; // breakFreeze 带的原因记进本轮记录（盲审 TB-01）
		} else ctl.why = "turn";
		if (bd.injBrkWhy) bd.injBrkWhy = ""; // 原因只用一次（新轮首算也清，免串到后一次破冻结）
	} catch {
		slimErr();
	}
	return ctl;
}
// 冻结命中：原样返回本轮文本（经 promptSafeContext 再净化一次＝与首算同字节）·记 ctx 账（不按路比帽）·freezeHits 由钩子结算点加
function slimStateFrozen(context, ctl) {
	try {
		recordInjectBytes("ctx", ctl.out, true);
		STATE_BY_CTX.set(context, { out: ctl.out, recomputed: false, sid: ctl.sid, pk: ctl.pk, turn: ctl.bd.injTurn, why: "frozen", vis: ctl.vis || [] });
	} catch {
		slimErr();
	}
	return ctl.raw;
}
// 状态段出口（案A 两路 L330、L333 与正常路 L975）：stripLone（孤立代理位换 U+FFFD·§3.9）→ 记账 → 存本步结果、挂待结算标、可冻则存本轮。
//   返回 stripLone 后的文本，由外层 promptSafeContext 净化（out＝净化后的实发文本，供结算与合并判定）。
function slimStateOut(context, ctl, text, nudgeText, vis) {
	let raw = String(text == null ? "" : text);
	try {
		raw = stripLone(raw);
		const ps0 = PROMPT_SAFE_STAT ? PROMPT_SAFE_STAT.hits : 0;
		const out = promptSafe(raw);
		if (PROMPT_SAFE_STAT) PROMPT_SAFE_STAT.hits = ps0; // 盲审 R4（10-12）：此处只为算实发字节、不记命中——命中由外层 promptSafeContext 对返回的 raw 记一次（同旧版与冻结步：每步一次）
		recordInjectBytes("ctx", out, true);
		if (ctl && ctl.isCtx) {
			const turn = ctl.bd ? ctl.bd.injTurn || 0 : 0;
			STATE_BY_CTX.set(context, { out, recomputed: true, sid: ctl.sid, pk: ctl.pk, turn, why: ctl.why, vis: vis || [] });
			if (ctl.pk) {
				SLIM_PEND.delete(ctl.pk);
				SLIM_PEND.set(ctl.pk, { ctx: context, out, sid: ctl.sid, turn, why: ctl.why });
				if (SLIM_PEND.size > 256) SLIM_PEND.delete(SLIM_PEND.keys().next().value);
			}
			if (ctl.bd) {
				ctl.bd.injStTurn = turn; // 本轮状态段已算过（轮中 claim 判定用）
				if (ctl.freezable) ctl.bd.injSt = { ver: SLIM_VER, key: ctl.key, turn, raw, out, nudge: nudgeText || "", vis: vis || [] };
			}
		}
	} catch {
		slimErr();
	}
	return raw;
}
// 状态段可见 id（最新记忆 rows、常驻 essRows、现行待办 todos 含directive顶替进席者、PlanFence 指向、任务账三清单）——供被截判定与第 8 笔互斥（§3.6-2）
function slimVisOf(o) {
	const s = new Set();
	try {
		for (const r of [...(o.rows || []), ...(o.essRows || []), ...(o.todos || [])]) if (r && Number.isInteger(Number(r.id))) s.add(Number(r.id));
		for (const r of o.todos || []) {
			const m = /^#(\d+)$/.exec(String((r && r.planFence) || ""));
			if (m) s.add(Number(m[1]));
		}
		if (o.led) for (const k of ["done", "open", "waiting"]) for (const r of (o.led[k] || []).slice(0, 3)) if (r && Number.isInteger(Number(r.id))) s.add(Number(r.id));
	} catch {
		slimErr();
	}
	return [...s];
}
// 钩子取本步状态段可见 id（盲审 TB-02·10-12·task brief §3.1）：先按 context 取 STATE_BY_CTX；取不到（宿主日后不再把同一 context 对象先交状态段、再交
//   waterfall 等）时退回按名找本次装配里的状态条目（legion-memory-recall），对其文本跑 /#(\d+)/g——取 id 的面宽于 slimVisOf（板项open item id 也算），
//   以「最终合并块里出现」为准，偏向少计被截、多剔重
function slimVisFor(h, asm) {
	const st = h && h.ctx && typeof h.ctx === "object" ? STATE_BY_CTX.get(h.ctx) : undefined;
	if (st) return new Set(st.vis || []);
	const s = new Set();
	try {
		const ent = asm && Array.isArray(asm.contexts) ? asm.contexts.find((c) => c && c.name === "legion-memory-recall") : null;
		if (ent && typeof ent.text === "string") for (const m of ent.text.matchAll(/#(\d+)/g)) s.add(Number(m[1]));
	} catch {
		slimErr();
	}
	return s;
}
// 轮中真用户消息（与 L1013 同判 source.kind=user）：bd.injUsr+1；本轮状态段已算过才算「轮中」，另计持久键（白名单③·§3.4）
function slimNoteClaim(payload, bindMap, db) {
	try {
		const sid = payload && payload.agent && payload.agent.session && payload.agent.session.id;
		if (!sid) return;
		const bd = bindMap.get(String(sid));
		if (!bd) return;
		bd.injUsr = (bd.injUsr || 0) + 1;
		if (bd.injTurn > 0 && bd.injStTurn === bd.injTurn) slimBump(db, "inject_mid_turn_user_claims_total", 1);
	} catch {
		slimErr();
	}
}
// 钩子句柄：本次装配的同轮账（结算恰一次）与召回冻结判定；pool／fu*／fuIds 供保守 C 定被截与「附：」行（第 4 笔）
function slimHookEnter(context, bindMap) {
	const h = { ctx: context, settled: false, err: false, frozen: false, rcText: "", rcB: 0, merge: null, bd: null, sid: "", turn: 0, freezable: false, shown: [], cut: [], xd: [], trim: [], pre: undefined, pool: [], fuTop: "", fuStitch: "", fuFore: "", fuIds: [] };
	try {
		const sid = String((context && context.agent && context.agent.session && context.agent.session.id) || "");
		h.sid = sid;
		const bd = sid ? bindMap.get(sid) : null;
		if (bd) {
			h.bd = bd;
			h.turn = bd.injTurn || 0;
		}
		h.freezable = !!bd && h.turn > 0 && !!context && typeof context === "object" && !envOn("LEGION_INJECT_FREEZE_OFF");
	} catch {
		slimErr();
	}
	return h;
}
// 合并出口（§3.11 第 16 条）：变量值 promptSafe(stripLone(召回))；未设 MERGE_OFF 且状态条目在、条目文本无「{{」时尾接占位（含「{{」⇒ 宿主会把
//   其后的占位连读成坏组而整轮抛错，退回原 push）；否则原 push。召回字节记进句柄，同轮账统一在结算点记。
function slimContribute(assembly, text, h) {
	try {
		recordInjectBytes("wf", text, true);
		const base = "legion_auto_recall";
		let name = base,
			n = 2;
		assembly.variables = assembly.variables || {};
		while (Object.hasOwn(assembly.variables, name)) name = base + "_" + n++;
		const val = promptSafe(stripLone(text));
		assembly.variables[name] = val;
		const ent = envOn("LEGION_INJECT_MERGE_OFF") ? null : assembly.contexts.find((c) => c && c.name === "legion-memory-recall");
		if (ent && typeof ent.text === "string" && ent.text.indexOf("{{") < 0) {
			h.merge = { ent, text0: ent.text, name };
			ent.text = ent.text ? ent.text + "\n\n{{" + name + "}}" : "{{" + name + "}}";
		} else {
			h.merge = null;
			assembly.contexts.push({ name, text: "{{" + name + "}}" });
		}
		h.rcText = text;
		h.rcB = Buffer.byteLength(val, "utf8");
	} catch {
		slimErr();
	}
}
// 结算（每次装配恰一次·§3.10）：清本步待结算标；真跑（非冻结、无异常）则存本轮召回；状态段冻结命中只加 freezeHits，
//   否则记一笔＝状态段字节（本步没算到计 0，记 why=no-state）＋合并分隔 2 字节＋召回字节。
function slimSettle(h) {
	if (!h || h.settled) return;
	h.settled = true;
	try {
		const st = h.ctx && typeof h.ctx === "object" ? STATE_BY_CTX.get(h.ctx) : undefined;
		if (st) {
			const p = SLIM_PEND.get(st.pk);
			if (p && p.ctx === h.ctx) SLIM_PEND.delete(st.pk);
		}
		const trB = st && st.recomputed !== false && envOn("LEGION_INJECT_TRIM_ON") ? slimTrim(h, st) : null; // 注入瘦身车 W4 第 9 笔（§3.8，缺省关）：超帽裁剪在记账之前
		if (h.freezable && !h.frozen && !h.err) h.bd.injRc = { ver: SLIM_VER, turn: h.turn, text: h.rcText || "", shown: h.shown.slice(), cut: h.cut.slice(), parts: h.rcParts || null, ...(c2HintOn() ? { arm: hintArm(h.sid) } : {}) }; // wave 车二-2：提示开时本轮召回记录带臂标
		if (st && st.recomputed === false) {
			injectMeter.freezeHits += 1;
			return;
		}
		const stB = st ? Buffer.byteLength(st.out, "utf8") : 0;
		const bytes = trB !== null ? trB : stB + (h.merge && stB > 0 && h.rcB > 0 ? 2 : 0) + (h.rcB || 0);
		recordInjectTurn(bytes, { sid: slimSid8(h.sid), turn: h.turn, ver: SLIM_VER, st: st ? 1 : 0, why: st ? st.why : "no-state", shown: h.shown, cut: h.cut, xd: h.xd, trim: h.trim, pre: h.pre });
	} catch {
		slimErr();
	}
}
// index.cjs session/event 的 bd 段调用（§3.11 第 22–24 条）：turn/start→injTurn；compaction/end→injBrk（不看 COMPACT_BRIDGE_OFF）；
//   本会话 memory_write 带 closeTargetId、或 type=todo、或 essential=true、或 edgeKind 为 solved_by／patches／duplicate_of → injBrk（§3.4 白名单②）
//   第四参是插件 db 连接（盲审 R7·10-12）：漏召观测两键与其余持久键同走 slimBump，写库失败计 injectMeter.errors（注计错）——原借 bumpNudgeTotal，失败记进 nudgePersistErrors
function slimOnSessionEvent(bd, t, event, db) {
	try {
		if (!bd) return;
		if (t === "turn/start") bd.injTurn = (bd.injTurn || 0) + 1;
		else if (t === "compaction/end") bd.injBrk = (bd.injBrk || 0) + 1;
		else if (t === "tool/call") {
			const d = (event && event.data) || {};
			const name = String(d.name || d.toolName || "");
			if (name !== "memory_write" && name !== "memory") return;
			let a = d.arguments || d.args || {};
			if (typeof a === "string") {
				try {
					a = JSON.parse(a);
				} catch {
					a = {};
				}
			}
			if (!a || typeof a !== "object") a = {};
			// 漏召观测（§3.10 末条·§3.11 第 24 条·02 号坑 6 的数据）：memory read_episodic 的 id 落在本轮被截 id → inject_recall_cut_read_total，
			//   落在本轮已显 id（召回条＋「附：」里衔接、前瞻带出的）→ inject_recall_shown_read_total；本轮召回以 bd.injRc（结算点存）为准
			//   wave 复活口·阶段 1（10-18）：get 开时 memory get 的首段（offset 缺省或 0）同计（窗口按 id 取全文是比 search 更直的回读）；续读段不重计。
			//   车二-4 的 inject_recall_cut_search_total 只数 search 选中被截条，get 不进（两路分开数）
			if (name === "memory") {
				const rc = bd.injRc;
				const id = Number(a && a.id);
				if (a && (a.action === "read_episodic" || (a.action === "get" && core.memGetOn() && !(Number(a.offset) > 0))) && Number.isInteger(id) && rc && rc.ver === SLIM_VER && rc.turn === bd.injTurn) {
					if (rc.cut.includes(id)) slimBump(db, "inject_recall_cut_read_total", 1);
					else if (rc.shown.includes(id)) slimBump(db, "inject_recall_shown_read_total", 1);
				}
				return;
			}
			const ek = String((a && a.edgeKind) || "");
			if (
				(a.closeTargetId !== undefined && a.closeTargetId !== null && a.closeTargetId !== "") ||
				a.type === "todo" ||
				a.essential === true ||
				a.essential === "true" ||
				ek === "solved_by" ||
				ek === "patches" ||
				ek === "duplicate_of"
			)
				bd.injBrk = (bd.injBrk || 0) + 1;
		}
	} catch {
		slimErr();
	}
}
// 破冻结入口 breakFreeze(sid, 原因)（盲审 TB-01·10-12·task brief §3.4 末句：统筹稿 L160 的「权限变更、失败、安全告警」现状态段无对应行，留接口、本车不接线）：
//   按真 sid 取会话记录 bd，injBrk+1——下一步状态段同轮重算一次、召回不动（同白名单②）；原因（字母数字、≤12 字）记进本轮记录（why＝brk:原因）。
//   瘦身关（SLIM_OFF）、sid 空、会话记录不在时不动、返回 false。无调用者；日后接线另车。
let slimBindRef = null; // setupInject 每次 apply 记下本插件实例的 bindMap（index.cjs deps 传入·与钩子、状态段同一份）
function breakFreeze(sid, reason) {
	try {
		if (!slimOn() || !slimBindRef || !sid) return false;
		const bd = slimBindRef.get(String(sid));
		if (!bd) return false;
		bd.injBrk = (bd.injBrk || 0) + 1;
		bd.injBrkWhy = String(reason == null ? "" : reason)
			.replace(/[^\p{L}\p{N}_.:-]/gu, "")
			.slice(0, 12);
		return true;
	} catch {
		slimErr();
		return false;
	}
}
// ── 版面 A、B（10-11·1324 四案 A、B·落码task brief §3.3、§3.11 第 5–9 条）──
// 本窗板拆段：要点两行与三桶行就是原码 _boardTxt 的中段（两态共用原码、不另写副本）——精确重建原头行与尾行，核对后取中段；
//   核不上（原码格式日后被改而此处未跟）则不拆，计错并由板态块原样附后（不丢板）。
function slimBoardOf(boardTxt, o) {
	try {
		const s = String(boardTxt || "");
		const head = "\n## 本窗板（" + o.sid18 + "·" + o.upd + (o.stale ? "·⚠陈旧" : "") + "）\n";
		const tail = "（共 " + o.n + " 项·已完成 " + o.c + "·board_read 看全量）\n";
		if (s.length < head.length + tail.length || !s.startsWith(head) || !s.endsWith(tail)) {
			slimErr();
			return { raw: s };
		}
		return { ...o, body: s.slice(head.length, s.length - tail.length) };
	} catch {
		slimErr();
		return null;
	}
}
// ── 三桶显示条件(design note)──
//   SLIM 时板账三桶行只在漂移>0 时留：现码 drift>0||unl>0 即出，与其注释「③桶禁入告警」相反（task brief 1.3 第 13 行；RI-12 实测 287 次全是漂移 0）。
//   三桶计算与 boardLedgerOk／Drift／Unlinked 三键不动（两态共用原码），只从本窗板中段去掉末行；撤本笔即回「漂移或未open item>0 即出」。
const SLIM_LEDGER_LINE_RE = /(^|\n)板账：一致 \d+｜漂移 \d+｜未挂账 (?:\d+|999\+)\n$/;
// 「## 板态」块：space面（本空间 active todo）与本窗（本会话板）并一行计数；只有space面时头写「本窗无板」；两者都无不出块；
//   板陈旧标「·⚠陈旧」照原码 _staleB 位置保留(design note)
//   hint（板机制六缺口车·缺口 5·10-18）：本窗无板时块尾追加的换窗接续提示行（调用方算好；空或不传＝本车前原样）
function slimBoardBlock(org, bo, space, hint) {
	try {
		if (bo && bo.body === undefined && bo.raw)
			return (org ? "\n## 板态（本窗板原样附后）\n📋 space面 " + space + " active todo " + org.n + " 条（候类 " + org.hou + "·最老龄 " + org.days + " 天）\n" : "") + bo.raw;
		const okB = !!bo && bo.body !== undefined;
		if (!org && !okB) return "";
		const segs = [];
		if (org) segs.push("space面 " + space + " active todo " + org.n + " 条（候类 " + org.hou + "·最老龄 " + org.days + " 天）");
		if (okB) segs.push("本窗 " + bo.n + " 项（在办 " + bo.a + "·待办 " + bo.b + "·已完成 " + bo.c + "·board_read 看全量）");
		return (
			(okB ? "\n## 板态（本窗 " + bo.sid18 + "·" + bo.upd + (bo.stale ? "·⚠陈旧" : "") + "）\n" : "\n## 板态（本窗无板）\n") +
			"📋 " +
			segs.join("｜") +
			"\n" +
			(okB ? bo.body : hint || "")
		);
	} catch {
		slimErr();
		return "";
	}
}
// 任务账块尾两行：任务树计数（候裁／待条件／其他·词面分组同原码）与结构边计数（按边型）；账为空而有计数时补块头（§3.3）。
//   「（树形 board_read）」后缀不带：board_read 只列板项、不画树（tools.cjs L446-452），带了即指错路
//   ledTxt 为 null＝账块组装抛错（_eLD）：旧版树在板块里不受账块失败牵连，此处照出两行、头注明失败，不冒称「无账态变动」
function slimTreeLines(ledTxt, tree, edge) {
	try {
		if (!tree && !edge) return "";
		let s = ledTxt ? "" : ledTxt === null ? "\n## 本 space 任务账（账块组装失败·只出计数）\n" : "\n## 本 space 任务账（近 12h 无账态变动）\n";
		if (tree) s += "任务树：候裁 " + tree.hou + "｜待条件 " + tree.dai + "｜其他 " + tree.oth + "\n";
		if (edge) s += "结构边 " + edge.n + "：战役 " + edge.camp + "｜父任务 " + edge.sub + "｜依赖 " + edge.dep + "\n";
		return s;
	} catch {
		slimErr();
		return "";
	}
}
// ── 保守 C（10-11·1324 四案 C·落码task brief §3.3、§3.5、§3.10、§3.11 第 18–21 条）──
//   召回 4→SHOW 条（缺省 2）；顶注、衔接、前瞻（≤2）收进一行「附：」；盲区一句并入召回头（取代独立「🔍 盲区自检」行，含前瞻独立注入补的那行）。
//   头里写「以下」不写样例的「以上」：盲区句在头上、条目在其下，照样例写即指反。
// 车二-9 块一 1-d（10-25 落码·规格 2.2 1-d）：来源一字标。手＝manual:*／session:*／裸 session-xxxxxxxx／handwrite；
//   提＝auto:session-*；摄＝auto:claude:*（摄取车）；镜＝mirror:*；巡＝patrol:*／patrol-sentinel:*／dreamer；其他不标。
//   开关 LEGION_INJ_LINEFMT_OFF=1 时整体回旧行式（下面各拼接点同判）。
function srcMark(source) {
	if (process.env.LEGION_INJ_LINEFMT_OFF === "1") return "";
	const s = String(source == null ? "" : source);
	if (!s) return "";
	if (/^auto:claude:/.test(s)) return "摄";
	if (/^auto:/.test(s)) return "提";
	if (/^mirror:/.test(s)) return "镜";
	if (/^(?:patrol:|patrol-sentinel:|dreamer)/.test(s)) return "巡";
	if (/^(?:manual:|session:|handwrite)/.test(s) || /^session-[0-9a-f]{8}/.test(s)) return "手";
	return "";
}
const SLIM_RC_HEAD = "## living memory自动召回（⚠ 参考级·非directive·与最新directive相关记忆，引用前以当轮实况与directive source of record核对；以下为词面命中，决策前自查未检索的维度）";
// 车二-9 块一 1-e（10-25 落码·规格 2.2 1-e）：段头原串一字不改，后面接「（北京 MM-DD·本窗 <space>）」——
//   日期一天只变一次，对前缀缓存影响很小。space 由 sid 经 sessionOrgan 求；取不到就不带空间段。开关 LEGION_INJ_LINEFMT_OFF=1 回原串。
function rcHeadOf(sid) {
	if (process.env.LEGION_INJ_LINEFMT_OFF === "1") return SLIM_RC_HEAD;
	let md = "";
	let sp = "";
	try {
		const d = new Date(Date.now() + 8 * 3600e3); // +8h 后取 UTC 分量＝北京日
		md = String(d.getUTCMonth() + 1).padStart(2, "0") + "-" + String(d.getUTCDate()).padStart(2, "0");
		if (sid) sp = sessionOrgan(String(sid)) || "";
	} catch {}
	return SLIM_RC_HEAD + "（北京 " + (md || "--") + (sp ? "·本窗 " + sp : "") + "）";
}
const SLIM_RC_SHOWN = new WeakMap(); // 召回缓存条 → 该次已显 id：缓存命中（同句 5 分钟内重发）时本轮记录照带已显 id、不带被截 id（§3.11 第 18 条末句）
// 「附：」各段截断：clipSurrogateTail(s.slice(0, n))（§3.9）；段内换行改空格，保「附：」只占一行
const slimSeg = (s, n) => clipSurrogateTail(String(s == null ? "" : s).slice(0, n)).replace(/[\r\n]+/g, " ");
// 被截 id 环（§3.10）：organ_meta inject_recall_cut_ids，JSON [北京时刻 YYYY-MM-DDTHH:MM, sid 前 8 位, id]，留最近 300 笔；只记 id 与标，不记正文。
//   读改写——多进程并发末写赢、微损可容（同 index.cjs rescue_counts 注记）；条数以 inject_recall_cut_total 单语句 UPSERT 为准，环只供离线复核
//   wave 车二-4（10-17·落码规格 2.4）：环扩到 1000 笔，元素加第 4 项查询哈希 qh（召回查询原文 sha1 前 8 位）＝[时刻, sid8, id, qh]；
//   升级前的 3 元素旧笔原样保留（只 push、slice，读环方按长度兼容）。扩容与 qh 随码走，不受 LEGION_RECALL_PTR_OFF 管（环只是观测）
function slimCutRing(db, h) {
	try {
		const row = db.prepare("SELECT v FROM organ_meta WHERE k = 'inject_recall_cut_ids'").get();
		let ring = [];
		try {
			ring = JSON.parse((row && row.v) || "[]");
		} catch {
			ring = [];
		}
		if (!Array.isArray(ring)) ring = [];
		const at = nowIso().slice(0, 16);
		for (const id of h.cut) ring.push([at, slimSid8(h.sid), id, String(h.qh || "")]);
		db.prepare("INSERT INTO organ_meta (k, v) VALUES ('inject_recall_cut_ids', ?) ON CONFLICT(k) DO UPDATE SET v = excluded.v").run(JSON.stringify(ring.slice(-1000)));
	} catch {
		slimErr();
	}
}
// ── 跨段统一去重(design note)──
//   本步状态段可见 id（STATE_BY_CTX.vis：最新记忆、常驻、现行待办含directive顶替、任务账三清单、PlanFence 指向；案A 取 fb、essRows）与召回条、衔接、前瞻互斥；
//   剔掉的 id 记本轮记录 xd（原因 state-visible），计 injectMeter.recallDedup——在候选层记：召回条按池、衔接按选中条、前瞻按过了词面门的候选（截显示条数之前）。
function slimXdNote(h, id) {
	if (h.xd.includes(id)) return;
	h.xd.push(id);
	injectMeter.recallDedup = (injectMeter.recallDedup || 0) + 1;
}
// 召回条互斥（E-2 之后、截条之前）：自带 try/catch，失败 warn、原样返回，不挡截条；成功即在句柄挂 xdVis，供衔接、前瞻两处并入
//   asm＝本次装配：STATE_BY_CTX 取不到时（宿主给钩子另一 context 对象）可见 id 由其状态条目文本退回取（slimVisFor·盲审 TB-02）
function slimXdRecall(h, picked, logger, asm) {
	try {
		const vis = slimVisFor(h, asm);
		h.xdVis = vis;
		const out = [];
		for (const r of picked) {
			if (vis.has(Number(r.id))) slimXdNote(h, Number(r.id));
			else out.push(r);
		}
		return out;
	} catch (e) {
		slimErr();
		logger?.warn?.("[living-memory] 注入瘦身 XDEDUP 召回互斥失败（召回照常）: " + String((e && e.message) || e).slice(0, 60));
		return picked;
	}
}
// 衔接 pickedIds、前瞻 seenA3 两个既有去重集并入可见 id（两集的建集行不动，只在其后调用）：被并入 id 挡下者（has 命中）记 xd——
//   has 只在选条判定处被问（衔接问选中条一次、前瞻在词面门之后逐条问），挡下即真剔，不另复写词面门
function slimXdSeen(h, seen) {
	try {
		if (!h.xdVis) return;
		const add = new Set([...h.xdVis].filter((id) => !seen.has(id)));
		for (const id of add) seen.add(id);
		const has0 = seen.has.bind(seen);
		seen.has = (id) => {
			const y = has0(id);
			if (y && add.has(Number(id))) slimXdNote(h, Number(id));
			return y;
		};
	} catch {
		slimErr();
	}
}
// 召回定稿（前瞻段之后、缓存写入之前·§3.11 第 18、19 条）：召回头＋≤SHOW 条（正文 clipSurrogateTail 80 字）＋「附：」行（各段有数据才出，
//   全空不出；只有附属没有召回条时照出头加「附：」行）。已显＝召回条＋附带出的 id；被截＝池 − 已显 − 本步状态段可见 id
//   （被截以最终合并块里不出现为准·§3.5）；此函数只在召回链真跑时调用，按被截条数加 inject_recall_cut_total、id 入环。抛错返回 null（调用处留原文）。
//   asm＝本次装配：STATE_BY_CTX 取不到时可见 id 由其状态条目文本退回取（slimVisFor·盲审 TB-02）
//   wave 车二-4（10-17·落码规格 2.4）：先定已显与被截，再拼「附：」行——有被截条且未设 LEGION_RECALL_PTR_OFF=1 时，在「附：」行尾
//   （没有「附：」行就新起一行「附：」）加「✂未显：〔#id〕标题前 20 字；…」，至多 4 条、按池内原序；被截口径不变（指针条仍算 cut、照记
//   inject_recall_cut_total），另记 inject_recall_cut_ptr_total（发出的指针数）。指针不进已显集、不进 fuIds（超帽裁剪退「附：」行时随之退，不动已显集）
function slimRecallFinish(h, picked, db, asm) {
	try {
		const shown = [...new Set([...picked.map((r) => Number(r.id)), ...h.fuIds])];
		const vis = slimVisFor(h, asm); // 盲审 TB-02：取不到 STATE_BY_CTX 时退回状态条目文本
		h.shown = shown;
		h.cut = h.pool.filter((id) => !shown.includes(id) && !vis.has(id));
		const lines = picked.map((r) => {
			// 车二-9 块一 1-d（10-25 落码）：召回行补 #id 与来源一字标；开关 LEGION_INJ_LINEFMT_OFF=1 时逐字回旧式
			const mk = srcMark(r.source);
			const tag = mk ? "#" + r.id + "·" + r.type + "·" + r.space + "·" + mk : "#" + r.id + "·" + r.type + "·" + r.space;
			return process.env.LEGION_INJ_LINEFMT_OFF === "1"
				? "- 🕐" + String(r.eff_ts).slice(5, 10) + (r.vecFallback ? " 🧬[语义召回·" + r.type + "·" + r.space + "] " : " [" + r.type + "·" + r.space + "] ") + r.title + "——" + clipSurrogateTail(String(r.content || "").slice(0, 80))
				: "- 🕐" + String(r.eff_ts).slice(5, 10) + (r.vecFallback ? " 🧬[" + tag + "] " : " [" + tag + "] ") + r.title + "——" + clipSurrogateTail(String(r.content || "").slice(0, 80));
		});
		const fu = [h.fuTop, h.fuStitch, h.fuFore].filter(Boolean);
		const ptrIds = c2PtrOn() && h.cut.length > 0 ? h.cut.slice(0, 4) : []; // wave 车二-4：被截指针（按池内原序·至多 4 条）
		if (ptrIds.length > 0)
			fu.push(
				"✂未显：" +
					ptrIds
						.map((id) => {
							const t = h.poolTitle instanceof Map ? h.poolTitle.get(id) : undefined;
							return "〔#" + id + "〕" + (t ? slimSeg(String(t).replace(/[\r\n{}]/g, " "), 20) : "");
						})
						.join("；"),
			);
		if (fu.length > 0) lines.push("附：" + fu.join("｜"));
		h.rcParts = { items: lines.slice(0, picked.length), fu: fu.length > 0 ? lines[lines.length - 1] : "", ids: picked.map((r) => Number(r.id)), fuIds: h.fuIds.slice() }; // 第 9 笔：召回分件（超帽按件退，不从文本反拆）；ids 与 items 逐条对齐、fuIds 随「附：」行，供退出项移出已显集（盲审 TB-09）
		if (h.cut.length > 0) {
			slimBump(db, "inject_recall_cut_total", h.cut.length);
			slimCutRing(db, h);
		}
		if (ptrIds.length > 0) slimBump(db, "inject_recall_cut_ptr_total", ptrIds.length); // wave 车二-4：发出的指针数
		return lines.length > 0 ? rcHeadOf(h && h.sid) + "\n" + lines.join("\n") : "";
	} catch {
		slimErr();
		return null;
	}
}
// ── 超帽裁剪(design note)──
//   在结算点、记账之前做（不在合并出口：此处才同时有本步状态段字节、合并与否、召回分件三样）；只在本步状态段有重算、未设 MERGE_OFF、召回已并入状态条目
//   （或本无召回）时走——两段分立不裁。可选类按序退出：「附：」行 → 末条召回 → … → 第 1 条（SHOW 缺省 2 即task brief ①附 ②第 2 条 ③第 1 条），
//   每退一项计 trimmed、记原因 over-cap，退到 ≤ 帽即止（最少退出）；召回退尽则撤占位、删变量（条目回原文）。其余各行一律不退。
//   退完仍超：照发，计 overCapProtected，块尾加一行「（记忆块 N 字节，超帽 4200；保护项照发）」（N＝不含此行的块字节；不出现任何「缩小任务」类话），
//   此行同写进本轮状态记录，同轮冻结步照出（块逐字同）；冻结召回被退时同改本轮召回记录。返回最终同轮字节（含尾注）；不裁返回 null。
const SLIM_RC_PARTS = new WeakMap(); // 召回缓存条 → 该次召回分件（缓存命中时照按件退）
function slimTrim(h, st) {
	try {
		if (envOn("LEGION_INJECT_MERGE_OFF") || !h.asm || !Array.isArray(h.asm.contexts)) return null;
		if (!h.merge && (h.rcB || 0) > 0) return null; // 召回走了原 push（状态条目不在或含「{{」）：两段分立，同 MERGE_OFF 不裁
		const ent = h.merge ? h.merge.ent : h.asm.contexts.find((c) => c && c.name === "legion-memory-recall");
		if (!ent || typeof ent.text !== "string") return null;
		const cap = INJECT_TURN_CAP_BYTES;
		const stB = Buffer.byteLength(st.out, "utf8");
		const total = (rcB) => stB + (stB > 0 && rcB > 0 ? 2 : 0) + rcB;
		let rcB = h.rcB || 0;
		const pre = total(rcB);
		if (pre <= cap) return null;
		h.pre = pre;
		const p = h.rcParts;
		if (rcB > 0 && p && Array.isArray(p.items)) {
			const items = p.items.slice();
			let fu = p.fu || "";
			let rc = h.rcText;
			let val = null;
			while (total(rcB) > cap && (fu || items.length > 0)) {
				if (fu) {
					fu = "";
					h.trim.push("附:over-cap");
				} else {
					items.pop();
					h.trim.push("召回" + (items.length + 1) + ":over-cap");
				}
				injectMeter.trimmed = (injectMeter.trimmed || 0) + 1;
				rc = items.length > 0 || fu ? rcHeadOf(h && h.sid) + "\n" + (fu ? [...items, fu] : items).join("\n") : "";
				val = "";
				if (rc) {
					const ps0 = PROMPT_SAFE_STAT ? PROMPT_SAFE_STAT.hits : 0;
					val = promptSafe(stripLone(rc));
					if (PROMPT_SAFE_STAT) PROMPT_SAFE_STAT.hits = ps0; // 盲审 R4 延伸（10-12）：裁后重算变量值不另记命中——本步召回的命中已由合并出口记过一次
				}
				rcB = rc ? Buffer.byteLength(val, "utf8") : 0;
			}
			if (val !== null) {
				// 盲审 TB-09（10-12）：退出项带的 id 同步移出已显集——退「附：」行即退其带出的 id（留下的召回条里有的除外），退第 k 条即退该条 id；
				//   本轮记录 shown 与漏召观测所据的本轮召回（bd.injRc.shown）都以裁后为准：读被退条不记 shown_read，也不记 cut_read（原因 over-cap，不是被截）。
				//   分件缺 ids／fuIds（不应出现）时不动已显集，同改前
				const hasIds = Array.isArray(p.ids) && Array.isArray(p.fuIds);
				const ids = hasIds ? p.ids.slice(0, items.length) : null;
				const fuIds = hasIds && fu ? p.fuIds.slice() : [];
				const keep = new Set(hasIds ? [...ids, ...fuIds] : []);
				const drop = hasIds ? new Set([...p.ids, ...p.fuIds].filter((id) => !keep.has(id))) : new Set();
				if (drop.size > 0) h.shown = h.shown.filter((id) => !drop.has(id));
				if (rc) h.asm.variables[h.merge.name] = val;
				else {
					ent.text = h.merge.text0;
					delete h.asm.variables[h.merge.name];
					h.merge = null;
				}
				h.rcText = rc;
				h.rcB = rcB;
				h.rcParts = rc ? (hasIds ? { items, fu, ids, fuIds } : { items, fu }) : null;
				if (h.frozen && h.bd && h.bd.injRc && h.bd.injRc.turn === h.turn) {
					h.bd.injRc.text = rc;
					h.bd.injRc.parts = h.rcParts;
					if (drop.size > 0 && Array.isArray(h.bd.injRc.shown)) h.bd.injRc.shown = h.bd.injRc.shown.filter((id) => !drop.has(id));
				}
			}
		}
		let fin = total(rcB);
		if (fin > cap && rcB === 0) {
			const note = "\n（记忆块 " + fin + " 字节，超帽 " + cap + "；保护项照发）";
			ent.text += note;
			injectMeter.overCapProtected = (injectMeter.overCapProtected || 0) + 1;
			const s = h.bd && h.bd.injSt;
			if (s && s.ver === SLIM_VER && s.turn === h.turn && s.out === st.out) {
				s.raw += note;
				s.out += note;
			}
			fin += Buffer.byteLength(note, "utf8");
		}
		return fin;
	} catch {
		slimErr();
		return null;
	}
}

// [拆②补丁 v4] db（index.cjs apply 闭包连接·WAL 读面）经 deps 传入——原漏名致 text() 每轮 ReferenceError: db is not defined
// → 兜底 catch 降级空文本 → 四块断供（103 踵后生产 8 次 injectErrors·活体沙箱复现抓出 10-03 新窗首验）。
function setupInject({ ctx, systemPrompt, stats, bindMap, db, getSessionOfLastTurn, currentCompactionN }) {
		// ── 记忆注入：living memory最新记忆进每窗口系统提示（order 300 尾部·低频变化·前缀缓存友好）──
		// Injection-hook vocabulary (completion words such as done/closed are deliberately excluded)
		// AUDIT-FIX-20260912（修复车·审计 C 路第 1 项真漏项）：字面量并入三级链第 10 键 `opsHook`
		//   （源仓词面在 ~/.dsh 数据文件/criteria-rules.default.json·公开面见 CRITERIA_PUBLIC）。
		//   动因：该判据曾被净化链整体英文化 + 加 i 而**未披露**（C 路实测产物＝/in progress|…/i）⇒
		//   字面量留码面＝每次净化都再改一次行为且无人知；外置后 code 面零机改。
		const OPS_HOOK_RE = CRIT.opsHook;
		// 注入瘦身车 W4（10-11·落码task brief §3.10「载入实证」·审核 D8 两证之一）：每次 apply 打一行独有日志（写各开关现值；SLIM_OFF 时写 slim=off）
		try {
			ctx.logger?.info?.(
				"[living-memory] inject-slim v1 loaded (slim=" +
					(slimOn() ? "on" : "off") +
					" merge=" +
					(envOn("LEGION_INJECT_MERGE_OFF") ? "off" : "on") +
					" freeze=" +
					(envOn("LEGION_INJECT_FREEZE_OFF") ? "off" : "on") +
					" xdedup=" +
					(envOn("LEGION_INJECT_XDEDUP_ON") ? "on" : "off") +
					" trim=" +
					(envOn("LEGION_INJECT_TRIM_ON") ? "on" : "off") +
					" show=" +
					slimShow() +
					" cap=" +
					INJECT_TURN_CAP_BYTES +
					")",
			);
			// 盲审 R5（10-12）：新开关只认 "1"（旧开关是非空即真）——设了别的值（true、yes 等）照未设处理，载入时逐个打 warn，免紧急回滚静默不生效
			for (const k of ["LEGION_INJECT_SLIM_OFF", "LEGION_INJECT_MERGE_OFF", "LEGION_INJECT_FREEZE_OFF", "LEGION_INJECT_XDEDUP_ON", "LEGION_INJECT_TRIM_ON"]) {
				const v = process.env[k];
				if (v !== undefined && v !== "1")
					ctx.logger?.warn?.("[living-memory] inject-slim 开关 " + k + "=" + JSON.stringify(String(v).slice(0, 8)) + " 不是 \"1\"，按未设处理（新开关只认 1）");
			}
		} catch {}
		// 板机制六缺口车（10-18）：三开关设了不认的值，载入时 warn（同盲审 R5 惯例：紧急回滚静默不生效最坏）；只打日志，不改行为
		try {
			const _bh = process.env.LEGION_BOARD_HINT_OFF,
				_bs = process.env.LEGION_BOARD_STALE_DAYS,
				_bg = process.env.LEGION_BOARD_GC_OPEN_DAYS;
			if (_bh !== undefined && _bh !== "1")
				ctx.logger?.warn?.("[living-memory] 板机制开关 LEGION_BOARD_HINT_OFF=" + JSON.stringify(String(_bh).slice(0, 8)) + " 不是 \"1\"，按未设处理（新开关只认 1）");
			if (_bs !== undefined && !(Number(_bs) > 0 && Number.isFinite(Number(_bs))))
				ctx.logger?.warn?.("[living-memory] 板机制开关 LEGION_BOARD_STALE_DAYS=" + JSON.stringify(String(_bs).slice(0, 8)) + " 不是正数，按 3 天处理");
			if (_bg !== undefined && core.boardGcOpenDays() === null)
				ctx.logger?.warn?.("[living-memory] 板机制开关 LEGION_BOARD_GC_OPEN_DAYS=" + JSON.stringify(String(_bg).slice(0, 8)) + " 不是大于 30 的数，按未设处理（板 GC 照原样超 30 天整行删）");
		} catch {}
		slimBindRef = bindMap; // 盲审 TB-01：breakFreeze 取本插件实例的会话记录
		if (systemPrompt !== undefined) {
			// 案C(ops note)：section→context 迁移——section 路 scope 层断链（死），
			// context 投影路=每轮 materialize（活·autorecall 同位）；接口同构（types L47-74）·
			// 根治B 三链归属+案A 兜底保留不动。order 300 沿用（context 亦按 order 排序）。
			const stopSection = systemPrompt.context({
				name: "legion-memory-recall",
				order: 300,
				text: (context) => promptSafeContext(() => {
					// 探针还原+env 开关版（2026-08-31 案C 同车·治「注入段无观测面」痛点）：
					// LEGION_INJECT_PROBE=1 时写 /tmp/inject-probe.log（默认零写零开销）——原ops探针 chr(10) 系
					// JS 未定义函数（从不写入）已修为 '\n'
					if (process.env.LEGION_INJECT_PROBE) {
						try {
							require("fs").appendFileSync(
								"/tmp/inject-probe.log",
								new Date().toISOString() +
									" CTX " +
									JSON.stringify({
										cwd: context?.agent?.session?.header?.cwd,
										sid: context?.agent?.session?.id,
									}).slice(0, 160) +
									"\n",
							);
						} catch {}
					}
					// 注入瘦身车 W4（10-11·落码task brief §3.4、§3.11 第 3 条）：冻结外包层——SLIM 时入口先补结、再判冻结，命中即原样返回本轮文本；
					//   SLIM_OFF 时整层绕过，走原构建与原出口（逐字节同 1ddf6fa）
					const SLIM = slimOn();
					const slimCtl = SLIM ? slimStateEnter(context, bindMap) : null;
					if (slimCtl && slimCtl.frozen) return slimStateFrozen(context, slimCtl);
					try {
						// 根治B（design-approved·kickoff 22:46）：归属源改 assemble context 直取——
						// 恢复型会话 constructor seed 不发 firehose（宿主 dsh-session L342-349 注释铁证）致
						// sessionOfLastTurn 闭包恒 ''；assemble 上下文每步必真（agent-loop L1025 实证
						// context.agent.session.id/header.cwd 可用）。三链：cwd 直推 > sid > 闭包兜底。
						const sid = context?.agent?.session?.id || getSessionOfLastTurn();
						const cwd = context?.agent?.session?.header?.cwd;
						const injectCallerSpace = cwd
							? organFromPath(cwd)
							: sid
								? sessionOrgan(sid)
								: null;
						// ── 四闸·injection gate（2026-08-22）：现行待办注入前验真——与近期闭环面冲突的 todo 不注入（防僵尸回流上下文）──
						//    v2.3（8-24 maintainer·降智六项）：+同义查重——与已入选 todo 重叠 ≥3 多字 token 视同重复只注入最新（injection gate只验真不防重是降智源）
						//    v2.4（8-24 maintainer·caller-first）：todo 席同构=本空间1+global1（同义查重保留）
						//    8-31 锈面修（审计 F2-1⑦）：注释承诺的「本空间1+global1」配额长期未实现——原顺序抢占制（本空间 concat global·满 2 即 break）
						//    致本空间 ≥2 条时 global directive型 todo 永不可达被挤掉。改硬配额：本空间 top1 与 global top1 各占一席（fact 席 gPool 补足同构）；
						//    归属失联兜底维持原全库顺序两席行为。
						const todos = [];
						const pickedTk = [];
						// ── 预算化注入·闲时扩容（design-approved「先做闲时多注入几条」·紧时收缩搁置）──
						//    闲=<40% 压力（pressureTokens 假零面用 surfaceTokens 下限·庚批同防）→fact 3→5/todo 2→4；非闲维持原值零变（不收缩·maintainer）
						const factCap = (() => {
							try {
								const cp = projcacheRows().get(String(sid || ""))?.rows
									?.contextPressure?.val;
								if (!cp) return 3;
								const used =
									(cp.pressureTokens || 0) > 0
										? cp.pressureTokens
										: cp.surfaceTokens || 0;
								const win = cp.contextWindow || 1000000;
								return used / win < 0.4 ? 5 : 3;
							} catch {
								return 3;
							}
						})();
						const todoCap = factCap >= 5 ? 4 : 2;
						// ── 批⑳ J8-7（L 路实勘：注入面十处块级裸 catch 零痕迹·违⑬）：块级降级可观测化——
						//    帮助函数：计数 stats.injectBlockErrors + warn 带位标（块级失败=该块静默消失须可见·与 L5742 整段兜底同族补齐）──
						const injectBlkFail = (tag, e) => {
							stats.injectBlockErrors = (stats.injectBlockErrors || 0) + 1;
							try {
								ctx.logger?.warn?.(
									"[living-memory] inject block skipped (" +
										tag +
										" #" +
										stats.injectBlockErrors +
										"): " +
										String(e).slice(0, 60),
								);
							} catch {}
						};

						const pickTodo = (cands, quota) => {
							// 从一池内按序挑 quota 席（closureCheck 验真+同义查重跨池全局生效）
							let n = 0;
							for (const td of cands) {
								if (n >= quota || todos.length >= todoCap) break;
								if (
									closureCheck(db, td.title, td.content, td.id).closed // step：todo 席带 id——solved_by 边前置判定（洞②根治· 型僵尸不再回流）
								)
									continue;
								const tks = gateKeywords(td.title, 10);
								let dup = false;
								for (const prev of pickedTk)
									if (tks.filter((k) => prev.has(k)).length >= 3) {
										dup = true;
										break;
									}
								if (dup) continue;
								todos.push(td);
								pickedTk.push(new Set(tks));
								n++;
							}
						};
						if (injectCallerSpace) {
							pickTodo(
								db
									.prepare(
										"SELECT id, type, title, content, ts, space FROM memories WHERE status='active' AND type='todo' AND space = ? ORDER BY id DESC LIMIT 6",
									)
									.all(injectCallerSpace),
								todoCap >= 4 ? 2 : 1,
							); // 本空间席（审计修正 20:58：残留 callerSpace 致恒空已修；件②池保 DESC·ASC 在 todos 组装后做）
							pickTodo(
								db
									.prepare(
										"SELECT id, type, title, content, ts, space FROM memories WHERE status='active' AND type='todo' AND space='global' ORDER BY id DESC LIMIT 4",
									)
									.all(),
								todoCap >= 4 ? 2 : 1,
							); // global 席（硬配额·directive型不被本空间挤占；件②池保 DESC）
						} else {
							pickTodo(
								db
									.prepare(
										"SELECT id, type, title, content, ts, space FROM memories WHERE status='active' AND type='todo' ORDER BY id DESC LIMIT 12",
									)
									.all(),
								todoCap,
							); // 归属失联兜底：全库顺序两席（件②池保 DESC）
						}
						todos.reverse(); // item append-only：todo 席 ASC 终态（池保 DESC 取最新+去重原语义·选中后旧→新显示：新待办尾部追加不移动旧条目）
						// ── v2.4 caller-first（design-approved·零other spaces噪音原则）：fact 席=本空间最新2（48h鲜度闸）+ global 最新1（META过滤）──
						//    治「注入层噪音泵」：xhs 条目只进 xhs 窗注入面；cross-space动态只经 global 干净管道；低频space陈年账被鲜度闸挡（不足让席 global）
						const past48h = (() => {
							const d = new Date(
								Date.now() + 8 * 3600 * 1000 - 48 * 3600 * 1000,
							);
							const p = (n) => String(n).padStart(2, "0");
							return `${d.getUTCFullYear()}-${p(d.getUTCMonth() + 1)}-${p(d.getUTCDate())}T${p(d.getUTCHours())}:${p(d.getUTCMinutes())}+08:00`;
						})();
						// global 席 META 过滤（例行收官条不占cross-space视野席·审计 20:59：删未用常量·SQL 直写四词）
						const seatVtNow = nowIso(); // 车二-9 块一 1-b（10-25 落码）：三席共用硬排时刻，与自动召回段 vtNow 同口径（一次提值）
						const ownRows = injectCallerSpace
							? db
									.prepare(
										"SELECT id, type, title, space, source, COALESCE(event_at, ts) AS eff_ts FROM memories m WHERE status='active' AND type IN ('fact','lesson','decision') AND space = ? AND COALESCE(event_at, ts) >= ? AND " +
											CUR_SQL + // 车二-9 块一 1-b：最新席接现行谓词——到点／被取代／待复核三类不占席（原来一条不滤）
											" ORDER BY id DESC LIMIT " +
											(factCap - 1 + 2) + // 1-b：池深 +2，供滤掉系统条后由下一条补上
											"",
									)
									.all(injectCallerSpace, past48h, seatVtNow)
									.filter((r) => !isSystemRow(r))
									.slice(0, factCap - 1)
									.reverse()
							: []; // P1 事件钟锚：鲜度按事件时点判（补记的旧事不占 48h 席）
						const essRows = injectCallerSpace
							? (() => {
									try {
										return db
											.prepare(
												"SELECT id, type, title, source FROM memories m WHERE status='active' AND essential=1 AND space IN (?, 'global') AND NOT (title LIKE '锚点：%' AND ts < ?) AND id NOT IN (SELECT dst FROM memories_edges WHERE edge_type IN ('patches','solved_by') AND invalid_at IS NULL) AND " +
													CUR_SQL + // 车二-9 块一 1-b（10-25 落码）：常驻席接现行谓词——到点／被取代／待复核条不再占常驻席（G1-1 的主要来源）；SELECT 补 source 供 isSystemRow 判来源
													" ORDER BY validated_count DESC, id DESC LIMIT 5", // 1-b：取 5（原 3）滤掉系统条后仍上 3 席
											)
											// 史（本车未动，逐字承 09-15 批 C 第 15 案·maintainer）：二级键 id DESC（vc 并列稳定序·新条优先）＋锚点类时效退席（title 前缀「锚点：」∧ ts 超 N 天→不占席·仍保 essential 标与 decay 免疫）＋patches/solved_by 入度排除（09-16 补充交办·与上窗衔接席 c3d64b7 同款——被订正/闭环结论不再占常驻席·#11743 活体）
											.all(
												injectCallerSpace,
												new Date(
													Date.now() +
														8 * 3600e3 -
														Math.max(
															1,
															Number(
																CRIT.essentialAnchorSeatDays,
															) || 3,
														) *
														86400e3,
												)
													.toISOString()
													.slice(0, 16),
												seatVtNow,
											)
											.filter((r) => !isSystemRow(r))
											.slice(0, 3);
									} catch {
										return [];
									}
								})()
							: []; // 常驻核心席（帽 3·按验证度·不占 48h 闸席——essence 常驻意）
						// ── C-1③ 观测键 essentialSeatAgedOut（⑬四位接线①②）：锚点类超期退席计数（「永久占席」治理读数）──
						try {
							const cut = new Date(
								Date.now() +
									8 * 3600e3 -
									Math.max(1, Number(CRIT.essentialAnchorSeatDays) || 3) * 86400e3,
							)
								.toISOString()
								.slice(0, 16);
							const agedN = db
								.prepare(
									"SELECT COUNT(*) c FROM memories WHERE status='active' AND essential=1 AND title LIKE '锚点：%' AND ts < ?",
								)
								.get(cut).c;
							stats.essentialSeatAgedOut = agedN;
						} catch (eB) {
							injectBlkFail("essential-aged", eB); // 批⑳ J8-7①：常驻退席观测段
						}
						const gPool = db
							.prepare(
								"SELECT id, type, title, space, source, COALESCE(event_at, ts) AS eff_ts FROM memories m WHERE status='active' AND type IN ('fact','lesson','decision') AND space='global' AND COALESCE(event_at, ts) >= ? AND NOT (title LIKE '%收官%' OR title LIKE '%全绿%' OR title LIKE '%完成%' OR title LIKE '%闭环%') AND " +
									CUR_SQL + // 车二-9 块一 1-b（10-25 落码）：global 池接现行谓词；SELECT 补 source 供 isSystemRow
									" ORDER BY id DESC LIMIT " +
									(factCap + 1 + 2) + // 1-b：池深再 +2（原 factCap+1），供滤系统条后补位
									"",
							)
							.all(past48h, seatVtNow)
							.filter((r) => !isSystemRow(r)); // P1 事件钟锚（池保 DESC：slice(0,n) 取最新 n 原语义·件② ASC 在组装处做）
						const rows = [
							...ownRows, // 件② ASC：本空间席旧→新（新条目尾部追加）
							...gPool.slice(0, factCap - ownRows.length).reverse(), // 件②：global 池 DESC 取最新 n·再转 ASC 追加在后（批⑰ D14②：硬编 3→factCap——闲时 factCap=5 扩容自落地起名存实亡·J2-1）
						].slice(0, factCap); // 刀B：闲时 5 席·常时 3（池深 factCap+1 保 F2-7 语义）
						// ── 8-31 option：nudge 消费段上提至案A 判定前（原居案A return 后——归属失联态/空库态恒遮蔽·caught in drill）——
						//    案A 兜底路与正常路共享同一消费点：失联恢复窗恰是最需纪律提示的场景。
						let nudgeText = "";
						// 注入瘦身车 W4（10-11·落码task brief §3.4、§3.11 第 4 条）：同轮白名单重算时 nudge 从本轮记录取、不再消费（本轮各步可见、下一轮消失）
						if (slimCtl && slimCtl.reuseNudge) {
							nudgeText = slimCtl.nudge;
						} else {
						try {
							const candKeys = [sid, getSessionOfLastTurn(), "_anon"].filter(
								Boolean,
							); // 8-31 长尾甲档②（F2-0 nudge 串窗·审计 confirmed）：注释承诺三链「精确→sessionOfLastTurn→_anon」原缺精确键——sid（L841 context 真身优先）未入链；多窗并发时 sessionOfLastTurn 被他窗 firehose 覆写→get 落空→兜底乱序取到他窗 pending 串显串清。补齐三链与根治B 同构（context 缺失时 sid===sessionOfLastTurn 天然幂等）
							let nudgeBd = null;
							for (const k of candKeys) {
								const b = bindMap.get(k);
								if (b && b.pendingNudge) {
									nudgeBd = b;
									break;
								}
							}
							if (!nudgeBd) {
								for (const b of bindMap.values())
									if (b.pendingNudge) {
										nudgeBd = b;
										break;
									}
							} // 最后兜底：唯一 pending 者（多窗并发罕见面）
							if (nudgeBd && nudgeBd.pendingNudge) {
								nudgeText =
									nudgeBd.pendingNudge === "B"
										? "\n🔔 纪律提示：本窗已读收件箱但未查记忆库——请立即 memory search/timeline 补查（「读收件箱⇄查记忆库」绑定）"
										: nudgeBd.pendingNudge === "A"
											? "\n🔔 纪律提示：开局两轮未检 memory 调用——请立即执行开局三查（本模块记忆/全局待办/相关经验）"
											: nudgeBd.pendingNudge === "M"
												? "\n🔔 记忆先行：上轮指令实勘/结论前未先查记忆库——请先 memory search（红线级硬性规则·观测期不拦截）"
												: ""; // 审计 I-2 修：else 改显式 === 'M' 判——原 else 分支兜底·未来新增 nudge 类型会误显 M 文案
								if (nudgeBd.pendingNudge === "M")
									nudgeBd.nudgeMShownAt = Date.now(); // option：提示展示戳（10min 内补查记听从）
								nudgeBd.pendingNudge = null; // 提示一次即清（不常驻刷屏）
							}
						} catch (eB) {
							injectBlkFail("nudge", eB); // 批⑳ J8-7②：nudge 展示段
						}
						} // 注入瘦身车 W4：nudge 复用分支wrap-up
						if (rows.length === 0 && todos.length === 0) {
							// 案A止血（design-approved）：归属失联（恢复态/未归属）三席齐空时——全库最新 3 条兜底（META 滤·标跨窗）·降级态永不断供
							const fb = db
								.prepare(
									"SELECT id, type, title, space, source, COALESCE(event_at, ts) AS eff_ts FROM memories WHERE status='active' AND type IN ('fact','lesson','decision') AND NOT (title LIKE '%收官%' OR title LIKE '%全绿%' OR title LIKE '%完成%' OR title LIKE '%闭环%') ORDER BY id DESC LIMIT 3", // 车二-9 1-d：补 source 供来源一字标
								)
								.all()
								.reverse(); // item ASC append-only
							if (fb.length > 0) {
								let t =
									"## 暖暖 记忆库 · 最新条目（⚠ 自动注入=参考级·**跨窗兜底**〔归属失联·非本空间速览〕）\n" +
									fb
										.map((r) => {
											// 车二-9 块一 1-d（10-25 落码）：兜底最新席行式改 [#id·type·来源标]（原 [#id·type·space]）；开关关时回旧式
											const mk = srcMark(r.source);
											return process.env.LEGION_INJ_LINEFMT_OFF === "1"
												? "- 🕐" + String(r.eff_ts).slice(5, 10) + " [#" + r.id + "·" + r.type + "·" + r.space + "] " + r.title
												: "- 🕐" + String(r.eff_ts).slice(5, 10) + " [#" + r.id + "·" + r.type + (mk ? "·" + mk : "") + "] " + r.title;
										})
										.join("\n");
								if (essRows.length > 0)
									t +=
										"\n🧭 常驻核心（亲标不衰·勿当指令）：" +
										essRows
											.map(
												(r) =>
													"[" + r.type + "] " + String(r.title).slice(0, 40),
											)
											.join("｜"); // 8-31 长尾乙档（F2-5）：案A 兜底并入常驻席——原白查丢弃·兜底态（冷启动/低频静默）恰是最需核心锚定时刻
								t +=
									"\n（止血兜底·归属恢复后自动回本空间席——详情 memory action=search）";
								if (SLIM) return slimStateOut(context, slimCtl, t + nudgeText, nudgeText, [...fb, ...essRows].map((r) => Number(r.id))); // 注入瘦身车 W4（§3.11 第 12 条）：案A 有兜底条出口（可见 id 取 fb、essRows）
								recordInjectBytes("ctx", t + nudgeText); // D1（独立审计 09-12）：**案A 兜底路出口亦计入**（原 4 出口只覆盖 2）
								return t + nudgeText; // option：nudge 提示案A 路同达
							}
							if (SLIM) return slimStateOut(context, slimCtl, nudgeText, nudgeText, []); // 注入瘦身车 W4（§3.11 第 12 条）：案A 只剩 nudge 出口
							recordInjectBytes("ctx", nudgeText); // D1：**fb 亦空态 nudge 出口亦计入**（漏计即总账失真·且该态恰是最需计量的降级态）
							return nudgeText; // option：fb 亦空态 nudge 提示仍达（原 return '' 全丢——nudgeText 空时同义 ''）
						}
						// v2.3 ⑥：注入头部权威级标识——与directive层显式分级（防「自动注入」被误读为指令）
						// v2.4 caller-first：+「自家速览≠检索」（防窗口把被动注入的本空间近况误当已检索）
						let text =
							"## 暖暖 记忆库 · 最新条目（⚠ 自动注入=参考级·自家速览≠检索；详情用 memory action=search 检索）\n" +
							rows
								.map((r) => {
									// 车二-9 块一 1-d（10-25 落码）：最新席行式补来源一字标 →[#id·type·标]；开关关时回旧式 [ #id·type ]
									const mk = srcMark(r.source);
									return process.env.LEGION_INJ_LINEFMT_OFF === "1"
										? "- 🕐" + String(r.eff_ts).slice(5, 10) + " [#" + r.id + "·" + r.type + "] " + r.title
										: "- 🕐" + String(r.eff_ts).slice(5, 10) + " [#" + r.id + "·" + r.type + (mk ? "·" + mk : "") + "] " + r.title;
								})
								.join("\n"); // P4 时标+item ENGRAM citation（[#id·type] 可核查引用）；原注：条目头事件时标（MM-DD·≤8字）——注入即带时间观念，directive「时间时效中轴」注入面落点
						if (essRows.length > 0)
							text +=
								"\n🧭 常驻核心（亲标不衰·勿当指令）：" +
								essRows
									.map(
										(r) => "[" + r.type + "] " + String(r.title).slice(0, 40),
									)
									.join("｜"); // 常驻席：独立行不占 3 席（essence 意）
						// ── 回显step（21:43 maintainer·28375d 案治理）：todo 席directive保护——「ops/pending approval/裁决」类directive型待办不被任务型 todo 挤出注入席（在 text 组装前做） ──
						try {
							// directive型词表已提模块级（2026-09-12 审计真缺陷①修复·两路共用单一source of record）
							//   ——见文件顶层 ORD_WORDS／ORD_WORDS_EN／ORD_TODO_RE
							//   （原为本地常量 ⇒ E-2 所在的 assemble 钩子不可达·跨作用域同族「席侧顶替路跨作用域前车」）
							if (
								todos.length >= 1 &&
								!todos.some((x) => ORD_TODO_RE.test(x.title))
							) {
								// ⑥ SQL 参数化（09-13 D 项余项车·二轮审计判据⑥）：词表值走占位符——原为字面拼接进 LIKE，
								//   输入虽受控（模块级常量词表）仍属稳健性缺口：词表一旦含引号类字符即语法崩。
								const ordPats = ORD_WORDS.concat(ORD_WORDS_EN).map(
									(w) => "%" + String(w).replace(/\\b/g, "") + "%",
								);
								const ord = db
									.prepare(
										"SELECT id, title, ts, space, content FROM memories WHERE status='active' AND type='todo'" + // 落码车：SELECT 补 space/content 两列（消费）
										(injectCallerSpace ? " AND (space = ? OR space='global')" : "") + // SQL 侧同条件（参数侧同步·假时保持全库兜底）
										" AND (" +
											ordPats.map(() => "title LIKE ?").join(" OR ") +
											") ORDER BY id DESC LIMIT 1",
									)
										.get(...(injectCallerSpace ? [injectCallerSpace, ...ordPats] : ordPats)); // 参数侧同条件化（space 参首插·与 SQL 侧成对）
								if (
									ord &&
									!todos.some((x) => x.title === ord.title) &&
									!closureCheck(db, ord.title, "", ord.id).closed // 余件item一（09-09·三轮审计 §4.4-3）：directive型顶替补验真——已闭环directive todo 不顶进注入席（僵尸directive回流面·与step/PlanFence 方向对齐）
								)
									todos[0] = {
										// 件② ASC：顶替最旧一席（ASC 头位）
										type: "todo",
										id: ord.id,
										title: ord.title,
										ts: ord.ts,
										space: ord.space, // 落码车：补 space 键（渲染归属前缀用）
										content: ord.content, // 补 content 键（治 closureCheck 收 undefined 静默失效）
									}; // 顶替最旧一席（directive型优先·双席内不扩预算）
							}
						} catch (eOrdTop) {
							// ⑬ 出口（2026-09-12 三轮审计 D2 修复）：本处（席侧「directive型顶替路」）原为空 catch，
							// 与 E-2 侧同族——二轮只枚举了 E-2 侧引用点 ⇒ 席侧成残尾。三轮审计变异实测：
							// E-2 侧连发 5 次 warn 而席侧零信号 ⇒ 顶替路失败时无任何痕迹。补 warn 如实暴露。
							ctx.logger?.warn?.(
								"[living-memory] directive型顶替路失败（directive型 todo 不占注入席·保护失效）: " +
									String((eOrdTop && eOrdTop.message) || eOrdTop).slice(0, 60),
							);
						}
						// ── PlanFence（wave·stale-plan 最小刀·吸收计划·09-07 approved）──
						//    注入面 todo 席升级：Q3 retrieval gate判「已闭环/更晚同题 fact」的 todo → 注入行
						//    前缀 ⚠ 依据已过期——数据面降权（search 路既有）升级为行动面告警（模型看得见才拦
						//    得住·与回显step同哲学·不做全形式化依赖图）。地基=11-A step（todo 席 SELECT 带 id 列）。
						//    判定与 search 路 Q3 同源同参（closureCheck+newer-fact 词命中≥3）·try 静默（注入不因判挂断）。
						for (const t of todos) {
							try {
								const cc = closureCheck(db, t.title, t.content, t.id); // step：边判定前置（todo 席 SELECT 带 id 列=地基）
								if (cc.closed && (!planFenceRepairOn() || planFenceAllows(db, t,
									db.prepare("SELECT title, source FROM memories WHERE id = ?").get(cc.by)))) {
									t.planFence = "#" + cc.by;
									continue;
								}
								const newer = db
									.prepare(
										(planFenceRepairOn() ? "SELECT id, title, content, source" : "SELECT id, title, content") + " FROM memories WHERE status = 'active' AND type IN ('fact','decision','lesson') AND id > ? ORDER BY id DESC LIMIT 40",
									)
									.all(t.id);
								const kws = gateKeywords(
									String(t.title) + " " + String(t.content || ""),
									12,
								);
								for (const f of newer) {
									if (!planFenceAllows(db, t, f)) continue;
									const hay = (
										String(f.title || "") +
										" " +
										String(f.content || "")
									).toLowerCase();
									let hits = 0;
									for (const k of kws)
										if (k.length > 1 && hay.includes(k)) hits += 1; // 余件item一（09-09·三轮审计 §4.4-4）：单字 token 不计 hits——closureCheck 主路 Lβ 同款（CJK 单字命中泛滥→「依据已过期」误标）
									if (hits >= 3) {
										t.planFence = "#" + f.id;
										break;
									}
								}
							} catch (eB) {
								injectBlkFail("planfence", eB); // 批⑳ J8-7③：PlanFence 依据过期标
							}
						}
						// ── ④B「this space板」全貌行（design-approved「同意 按建议」·执行总账 ④B·治「换窗忘·无全貌」）──
						//    定位＝todo 三态（账/板/席）之「席态＝指针位」升级：下方 2 席提醒之外，加 1 行**账态全貌**统计
						//    预算：单行 ≤100 字（帽见 core INJECT_TURN_CAP_BYTES（同轮合并字节）·10-11 RI-09 订正：原注「≤2000 字帽」码中无此帽）·零 active 不输出（零打扰）·try 静默不阻断注入
						let slimOrg = null, // 注入瘦身车 W4（版面 A、B·§3.11 第 5–9 条）：space面三数、本窗板拆段、任务树三数、结构边三数——SLIM 时交板态行与任务账块
							slimBoard = null,
							slimTree = null,
							slimEdge = null;
						try {
							if (injectCallerSpace) {
								const _bs = db
									.prepare(
										"SELECT COUNT(*) n, SUM(CASE WHEN title LIKE '%候%' THEN 1 ELSE 0 END) hou, MIN(ts) oldest FROM memories WHERE type='todo' AND status='active' AND space = ?",
									)
									.get(injectCallerSpace);
								if (_bs && _bs.n > 0) {
									const _bd = _bs.oldest
										? Math.max(
												0,
												Math.floor(
													(Date.now() - Date.parse(_bs.oldest)) /
														86400000,
												),
											)
										: 0;
									if (SLIM) slimOrg = { n: _bs.n, hou: _bs.hou || 0, days: _bd }; // 注入瘦身车 W4（版面 B·§3.11 第 5 条）：不拼「## this space板」，三数交板态计数行
									else
									text +=
										"\n## this space板（全貌行·09-20 起）\n📋 " +
										injectCallerSpace +
										" active todo " +
										_bs.n +
										" 条｜候类 " +
										(_bs.hou || 0) +
										"｜最老龄 " +
										_bd +
										" 天（明细：memory action=search query=待办）\n";
								}
							}
						} catch (_eB) {
							ctx.logger?.warn?.(
								"[living-memory] 板摘要组装失败: " +
									String(_eB).slice(0, 60),
							);
						}
						// ── ⑤-3＋⑥ 任务板块（design-approved「继续 导图同窗」）──
						//    ⑤-3 本窗板回显：读 session_boards（板态·三级回退①层）→ 在办/待办计数＋要点（≤2 条各截 24 字）
						//    ⑥  任务树：账态分组树（候裁／待条件／其他·词面分组零新字段）——首版单层；09-27 命名订正（原称板图）；
						//        结构边（subtask_of／part_of_campaign·闸④ 候重启）数据齐后可升级为多级（本段留扩展位）
						//    预算：单块 ≤400 字（帽见 core INJECT_TURN_CAP_BYTES（同轮合并字节）·10-11 RI-09 订正：原注「≤2000 字帽」码中无此帽）·零数据不输出·try 静默不阻断注入
						try {
							// D 路审计 D-真2 修（09-20 22:4x·⚠中）：原裸 `String(sid)` 未归一 ⇒ 与写入侧（sidOf=normSid）形态
							//   不一致 ⇒ 裸 uuid 会话下本表直查落空、「⑤-3 本窗板回显」静默死——正是 I-5 所治「写得到读不到」
							//   的残留半边（session_boards 四点：写 L3116 ✓／board_read L3201 ✓／⑤-4 L5220 ✓／**本点 ✗**）。
							const _sidB = sid
								? /^session-/.test(String(sid))
									? String(sid)
									: "session-" + String(sid)
								: "";
							let _boardTxt = "";
							let _bRowB = false; // 板机制六缺口车·缺口 5（盲审低-2）：本窗有无板行（含清空的空板）——有行即不出换窗提示
							if (_sidB) {
								const _brow = db
									.prepare(
										"SELECT payload, updated_at FROM session_boards WHERE sid = ?",
									)
									.get(_sidB);
								_bRowB = !!_brow;
								if (_brow) {
									let _bt = [];
									try {
										const _p = JSON.parse(_brow.payload);
										if (_p && Array.isArray(_p.todos)) _bt = _p.todos;
									} catch (eB) {
										injectBlkFail("board-payload", eB); // 批⑳ J8-7④：本窗板 payload 解析
									}
									const _inP = _bt.filter((x) => x.status === "in_progress");
									const _pd = _bt.filter((x) => x.status === "pending");
									const _dn = _bt.filter((x) => x.status === "completed").length;
									// ── 丙（maintainer 09-27「按建议」·治 L4 陈旧）：板陈旧标 ──
									//    板机制六缺口车·乙批·缺口 4①（10-18）：阈值 3 天改走 LEGION_BOARD_STALE_DAYS（正有限数才认·缺省 3＝原值；标记文字不变，slimBoardOf 头行随 stale 位同步）
									const _staleB = Date.now() - Date.parse(_brow.updated_at) > core.boardStaleDays() * 86400000;
									if (_bt.length > 0)
										_boardTxt =
											"\n## 本窗板（" +
											_sidB.slice(0, 18) +
											"·" +
											String(_brow.updated_at).slice(5, 16) + (_staleB ? "·⚠陈旧" : "") +
											"）\n" +
											(_inP.length
												? "■ 在办 " +
													_inP.length +
													"：" +
													_inP
														.slice(0, 2)
														.map((x) => {
															// 2403 板 v2：三要素短注（content 约定句·与导出器 v2 同口径）
															const _f =
																String(x.content).match(/流转[＝=]\s*([^｜|；;\n]{1,16})/) ||
																matchWaitWho(x.content); // 板v2余面修：候谁词表法（与 core WAIT_WHO_RE 逐字同一·经 exports 引用）
															const _fn = _f ? (_f[1] || "").trim() : "";
															const _s = String(x.content).match(/源[＝=]\s*([^｜|；;\n]{1,16})/);
															const _sn = _s ? (_s[1] || "").trim() : ""; // 板v2车余面③：补「源＝」提取（源/流转双带·帽内截断）
															return (
																String(x.content).slice(0, 24) +
																(_sn || _fn
																	? "（" + (_sn ? "源" + _sn.slice(0, 8) : "") + (_sn && _fn ? "·" : "") + _fn.slice(0, 12) + "）"
																	: "")
															);
														})
														.join("；") +
													"\n"
												: "") +
											(_pd.length
												? "☐ 待办 " +
													_pd.length +
													"：" +
													_pd
														.slice(0, 2)
														.map((x) => {
														// 板v2车余面③：待办段同式三要素短注（源/流转提取·帽内截断——原只截 24 字）
														const _f =
															String(x.content).match(/流转[＝=]\s*([^｜|；;\n]{1,16})/) ||
															matchWaitWho(x.content); // 板v2余面修：候谁词表法（与 core WAIT_WHO_RE 逐字同一·经 exports 引用）
														const _fn = _f ? (_f[1] || "").trim() : "";
														const _s = String(x.content).match(/源[＝=]\s*([^｜|；;\n]{1,16})/);
														const _sn = _s ? (_s[1] || "").trim() : "";
														return (
															String(x.content).slice(0, 24) +
															(_sn || _fn
																? "（" + (_sn ? "源" + _sn.slice(0, 8) : "") + (_sn && _fn ? "·" : "") + _fn.slice(0, 12) + "）"
																: "")
														);
													})
														.join("；") +
													"\n"
												: "") +
											(() => {
												// ── L2 板账三桶（design-approved「按建议」·实勘②订正版）──
												//    判据：板项 content 内 #id 全局提取 × 库 type='todo' 精确匹配（幂等·零文本比对）
												//    ②桶只取「板 completed ∧ 账 active」（真漂移）；反向「板未完 ∧ 账 done」系陈旧板天然滞后——归①不告警（实勘②真库实证：字面双向判首日 75% 假阳）
												//    ③桶＝未open item（无 id/非 todo）——**禁入告警**（旧哨 98.7% 假阳生成机制·回退红线）·计数帽 999+
												try {
													const _idsAll = new Set();
													for (const _t of _bt) {
														// 2403 板 v2：id 字段优先（正路）·content 词面退路（老板项兼容）
														if (Number.isInteger(_t.id) && _t.id > 0)
															_idsAll.add(Number(_t.id));
														for (const _m0 of String(_t.content || "").matchAll(/#(\d+)/g))
															_idsAll.add(Number(_m0[1]));
													}
													let ok = 0,
														drift = 0,
														unl = 0;
													// 板机制六缺口车·甲批（10-18）：缺口 2 滞后 lag（板 pending/in_progress ∧ 所挂 todo 账全非 active〔done/merged/aged〕·按板项计、一项只计一次；
													//   盲审建议-1：取「全非」不取「任一」——「待 #A 办完再办 #B」A 毕 B 未毕时该项不算滞后）、
													//   缺口 3 未open item拆 noId（无号）＋badRef（有号而无一中 todo 账）——只进 stats，三桶与三桶行不动；总闸 LEGION_BOARD_HINT_OFF=1 不写三键
													let lag = 0,
														noId = 0,
														badRef = 0;
													const _m2 = new Map();
													if (_idsAll.size > 0) {
														const _in = [..._idsAll].filter((n) => Number.isInteger(n) && n > 0 && n < 1e9);
														if (_in.length > 0) {
															const _rows = db
																.prepare(
																	"SELECT id, status FROM memories WHERE type='todo' AND id IN (" +
																		_in.join(",") +
																		")",
																)
																.all();
															for (const _r of _rows) _m2.set(String(_r.id), String(_r.status));
														}
													}
													for (const _t of _bt) {
														// 2403 板 v2：id 字段优先·词面退路（与 _idsAll 同序）
														const _ids =
															Number.isInteger(_t.id) && _t.id > 0
																? ["#" + _t.id]
																: String(_t.content || "").match(/#(\d+)/g) || [];
														if (!_ids.length) {
															unl++;
															noId++;
															continue;
														}
														let _hit = false;
														let _allClosed = true;
														for (const _id of _ids) {
															const _vs = _m2.get(_id.slice(1));
															if (_vs === undefined) continue;
															_hit = true;
															if (_vs === "active" && _t.status === "completed") drift++;
															else ok++;
															if (_vs === "active") _allClosed = false;
														}
														if (!_hit) {
															unl++;
															badRef++;
														} else if (_allClosed && (_t.status === "pending" || _t.status === "in_progress")) lag++;
													}
													stats.boardLedgerOk = ok;
													stats.boardLedgerDrift = drift;
													stats.boardLedgerUnlinked = unl;
													if (core.boardHintOn()) {
														stats.boardLedgerLag = lag;
														stats.boardLedgerUnlinkedNoId = noId;
														stats.boardLedgerUnlinkedBadRef = badRef;
													}
													return drift > 0 || unl > 0
														? "板账：一致 " +
																ok +
																"｜漂移 " +
																drift +
																"｜未挂账 " +
																(unl > 999 ? "999+" : unl) +
																"\n"
														: "";
												} catch (eLg) {
													stats.boardLedgerErrors = (stats.boardLedgerErrors || 0) + 1; // ⑬出口
													return "";
												}
											})() +
											"（共 " +
											_bt.length +
											" 项·已完成 " +
											_dn +
											"·board_read 看全量）\n";
									// 注入瘦身车 W4（版面 B·§3.11 第 6 条）：要点两行与三桶行即上式中段（两态共用原码）——备好板态头与本窗计数，拆出中段
									if (SLIM && _bt.length > 0)
										slimBoard = slimBoardOf(_boardTxt, {
											sid18: _sidB.slice(0, 18),
											upd: String(_brow.updated_at).slice(5, 16),
											stale: _staleB,
											n: _bt.length,
											a: _inP.length,
											b: _pd.length,
											c: _dn,
										});
									if (slimBoard && typeof slimBoard.body === "string" && !(stats.boardLedgerDrift > 0))
										slimBoard.body = slimBoard.body.replace(SLIM_LEDGER_LINE_RE, "$1"); // 注入瘦身车 W4 第 5 笔（RI-12）：三桶行只在漂移>0 时显（判 stats.boardLedgerDrift，本步刚由上式算出）
								}
							}
							let _mapTxt = "";
							let _edgeTxt = "";
							if (injectCallerSpace) {
								const _rs6 = db
									.prepare(
										"SELECT id, title FROM memories WHERE type='todo' AND status='active' AND space = ? ORDER BY id DESC LIMIT 40",
									)
									.all(injectCallerSpace);
								if (_rs6.length > 0) {
									const _houT = _rs6
										.filter((r) => /候/.test(String(r.title)))
										.map((r) => String(r.title).slice(0, 20));
									const _daiT = _rs6
										.filter(
											(r) =>
												!/候/.test(String(r.title)) &&
												/待|等|须|需/.test(String(r.title)),
										)
										.map((r) => String(r.title).slice(0, 20));
									const _othT = _rs6
										.filter(
											(r) =>
												!/候/.test(String(r.title)) &&
												!/待|等|须|需/.test(String(r.title)),
										)
										.map((r) => String(r.title).slice(0, 20));
									// C3（design-approved「下step」）：末个非空行改用 `└ ` 收尾（原硬编码 `├ ` 三行同符·子代理窗实测观感项）
									const _lineT = (n, a, last) =>
										a.length
											? (last ? "└ " : "├ ") +
												n +
												" " +
												a.length +
												"：" +
												a.slice(0, 2).join("；") +
												(a.length > 2 ? "…" : "") +
												"\n"
											: "";
									_mapTxt =
										"\n## 任务树（账态分组·全量 memory search query=待办·maintainer 09-27 命名订正）\n" +
										_lineT("候裁", _houT, !_daiT.length && !_othT.length) +
										_lineT("待条件", _daiT, !_othT.length) +
										_lineT("其他", _othT, true);
									if (SLIM) slimTree = { hou: _houT.length, dai: _daiT.length, oth: _othT.length }; // 注入瘦身车 W4（版面 A·§3.11 第 7 条）：树缩为任务账块内计数行
									// ── ⑥-2 结构边渲染（09-20 approved）──
									//    边方向约定（与闸④描述一致）：subtask_of＝子→父；part_of_campaign＝任务→战役；depends_on＝任务→前置
									//    有结构边 ⇒ 树形式优先（≤7 行帽）；无边 ⇒ 降级用上方词面分组（_mapTxt 保底）
									try {
										const _ids6 = _rs6.map((r) => r.id);
										const _ph = _ids6.map(() => "?").join(",");
										const _ees = db
											.prepare(
												"SELECT src, dst, edge_type FROM memories_edges WHERE invalid_at IS NULL AND edge_type IN ('subtask_of','part_of_campaign','depends_on') AND src IN (" +
													_ph +
													")",
											)
											.all(..._ids6);
										if (SLIM && _ees.length > 0)
											slimEdge = {
												n: _ees.length,
												camp: _ees.filter((e) => e.edge_type === "part_of_campaign").length,
												sub: _ees.filter((e) => e.edge_type === "subtask_of").length,
												dep: _ees.filter((e) => e.edge_type === "depends_on").length,
											}; // 注入瘦身车 W4（版面 A·§3.11 第 7 条）：结构边树缩为任务账块内一行（按边型计数）
										if (_ees.length > 0) {
											const _ttl = new Map(
												_rs6.map((r) => [r.id, String(r.title).slice(0, 22)]),
											);
											const _kids = new Map();
											const _cmp = new Map();
											const _dep = new Map();
											for (const e of _ees) {
												if (e.edge_type === "subtask_of") {
													if (!_kids.has(e.dst)) _kids.set(e.dst, []);
													_kids.get(e.dst).push(e.src);
												} else if (e.edge_type === "part_of_campaign") {
													if (!_cmp.has(e.dst)) _cmp.set(e.dst, []);
													_cmp.get(e.dst).push(e.src);
												} else {
													if (!_dep.has(e.src)) _dep.set(e.src, []);
													_dep.get(e.src).push(e.dst);
												}
											}
											const _lns = [];
											for (const [p, ks] of _cmp) {
												if (_lns.length >= 5) break;
												_lns.push(
													"├ 战役/主线 #" + p + "（" + ks.length + " 项）",
												);
												for (const k of ks.slice(0, 3))
													_lns.push("│  ├ " + (_ttl.get(k) || "#" + k));
											}
											for (const [p, ks] of _kids) {
												if (_lns.length >= 6) break;
												_lns.push(
													"├ 父任务 #" +
														p +
														" ← 子步 " +
														ks.length +
														"：" +
														ks
															.slice(0, 2)
															.map((k) => _ttl.get(k) || "#" + k)
															.join("；"),
												);
											}
											for (const [s, dsx] of _dep) {
												if (_lns.length >= 7) break;
												_lns.push(
													"└ " +
														(_ttl.get(s) || "#" + s) +
														" ──depends_on──▶ #" +
														dsx.slice(0, 2).join(", #"),
												);
											}
											_edgeTxt =
												"\n## 任务树（结构边树·" +
												_ees.length +
												" 边）\n" +
												_lns.slice(0, 7).join("\n") +
												"\n";
										}
									} catch (eB) {
										injectBlkFail("edge-tree", eB); // 批⑳ J8-7⑤：结构边任务树渲染（09-27 命名订正）
									}
								}
							}
							// ⑤-4 父窗板（子代理会话能见主窗板·DSH `parentSessionId`（origin:'subagent'·api-session-controller types L150）·
							//      防御式多形态读取——取不到即静默不输出·零风险）
							//      ── 审计 D-2 修正（09-20 22:2x·A 路码面审计·高·段 100% 不可达）──
							//      原读 `_sess.parentSessionId || _sess.meta.parentSessionId` 系**幻引用**：真 Session 自有属性仅 13 项
							//      （`Session.create` 纯内存实测：contextFold…header…无 parentSessionId／无 meta）⇒ `_psid` 恒 "" ⇒ 本段永死。
							//      宿主真口径＝`session.header.parentSession`（`childSessionMeta()` 造 header：dsh-subagent L502-513 折进
							//      `dsh-session` L1398；宿主自身取父窗同此写法 dsh-subagent L862/L2073）。改读 header + 同径 sid 归一（I-5）。
							let _parentTxt = "";
							try {
								const _sess = context?.agent?.session;
								const _hdr = _sess && _sess.header;
								const _praw = (_hdr && _hdr.parentSession) || "";
								const _psid = _praw
									? /^session-/.test(String(_praw))
										? String(_praw)
										: "session-" + String(_praw)
									: "";
								if (_psid && String(_psid) !== _sidB) {
									const _prow = db
										.prepare(
											"SELECT payload, updated_at FROM session_boards WHERE sid = ?",
										)
										.get(String(_psid));
									if (_prow) {
										let _pt = [];
										try {
											const _pp = JSON.parse(_prow.payload);
											if (_pp && Array.isArray(_pp.todos)) _pt = _pp.todos;
										} catch (eB) {
											injectBlkFail("parent-payload", eB); // 批⑳ J8-7⑥：父窗板 payload 解析
										}
										const _pin = _pt.filter(
											(x) => x.status === "in_progress",
										);
										if (_pt.length > 0)
											_parentTxt =
												"\n## 父窗板（" +
												String(_psid).slice(0, 18) +
												"·在办 " +
												_pin.length +
												"/" +
												_pt.length +
												"）\n" +
												(_pin
													.slice(0, 2)
													.map(
														(x) =>
															"■ " + String(x.content).slice(0, 24),
													)
													.join("\n") || "（无在办项）") +
												"\n";
									}
								}
							} catch (eB) {
								injectBlkFail("board-map", eB); // 批⑳ J8-7⑦：④B this space板/任务树组（09-27 命名订正）
							}
							// 板机制六缺口车·甲批·缺口 5（10-18）：换窗接续提示——瘦身态本窗无板行、space面非空、非子代理窗（header 无 parentSession）时，
							//   在「## 板态（本窗无板）」块尾提示同space最近一块非空板的 sid 与时刻（不显其内容）：换窗后旧窗板仍在，可 board_read 传 sid 承接；
							//   也可能是并行窗，提示里写明。space归属走 sessionOrgan（projcache／会话目录·各 60 秒缓存）。总闸 LEGION_BOARD_HINT_OFF=1 不算不出。
							//   只取主窗板：板行 sid 经 normSid 归一，子代理窗（会话目录是裸 uuid）与主窗（目录是「session-」原形）同形，故以会话目录有此原形目录判主窗
							//   （10-18 快照实数：260 块板中主窗 93、子代理 131、两形目录都不在 36）。
							//   盲审低-1、低-2：本窗有板行（含清空的空板）即不出；取最近 200 行只取 sid 与时刻，先过主窗、同space两道廉价判，命中者才取正文判非空
							let _handoffTxt = "";
							if (SLIM && !_bRowB && slimOrg && injectCallerSpace && !context?.agent?.session?.header?.parentSession && core.boardHintOn()) {
								try {
									const _hDir = core.sessionsDirMap();
									const _hPay = db.prepare("SELECT payload FROM session_boards WHERE sid = ?");
									for (const _hr of db.prepare("SELECT sid, updated_at FROM session_boards ORDER BY updated_at DESC LIMIT 200").all()) {
										const _hs = String(_hr.sid);
										if (_hs === _sidB || !_hDir.has(_hs) || sessionOrgan(_hs) !== injectCallerSpace) continue;
										let _hn = 0;
										try {
											const _hp = JSON.parse((_hPay.get(_hs) || {}).payload);
											_hn = _hp && Array.isArray(_hp.todos) ? _hp.todos.length : 0;
										} catch {}
										if (!_hn) continue;
										_handoffTxt =
											"同space最近板 " +
											_hs +
											"（" +
											String(_hr.updated_at).slice(5, 16) +
											"）：换窗接续可 board_read 传此 sid 承接；或为并行窗\n";
										break;
									}
								} catch (eH) {
									injectBlkFail("board-handoff", eH);
								}
							}
							if (SLIM) text += slimBoardBlock(slimOrg, slimBoard, injectCallerSpace, _handoffTxt) + _parentTxt; // 注入瘦身车 W4（版面 B·§3.11 第 8 条）：板态＋父窗板（任务树改由任务账块尾接）
							else
							if (_boardTxt || _mapTxt || _parentTxt || _edgeTxt)
								text += _boardTxt + _parentTxt + (_edgeTxt || _mapTxt);
						} catch (_eB3) {
							ctx.logger?.warn?.(
								"[living-memory] 任务板块组装失败: " +
									String(_eB3).slice(0, 60),
							);
							if (SLIM) text += slimBoardBlock(slimOrg, slimBoard, injectCallerSpace); // 注入瘦身车 W4：板块组装中途失败时板态照出（space面在前段已算）
						}
						// ── ⑩D 本窗任务账块（step D·治「接手方付重建成本」）──
						//    账态直导三清单（①本窗已办结 ②未闭环 todo ③候外部依赖）＋记忆 id 出处——
						//    每轮注入组装即出（压缩发不发生都读得到）；零数据不输出·try 出声不吞（⑬）。
						let slimLed = null; // 注入瘦身车 W4（§3.6-2）：任务账三清单可见 id（本轮记录与第 8 笔互斥用）
						try {
							const _ledD = exportTaskLedger(db, {
								space: injectCallerSpace || "",
								sid: String(sid || ""),
								hours: 12,
							});
							const _ledTxtD = renderTaskLedger(_ledD, { limit: 3 });
							if (_ledTxtD) text += _ledTxtD;
							if (_ledTxtD) slimLed = _ledD;
							if (SLIM) text += slimTreeLines(_ledTxtD, slimTree, slimEdge); // 注入瘦身车 W4（版面 A·§3.11 第 9 条）：账后接任务树计数行与结构边行（不改 renderTaskLedger）
						} catch (_eLD) {
							ctx.logger?.warn?.(
								"[living-memory] 任务账块组装失败: " +
									String(_eLD).slice(0, 60),
							);
							if (SLIM) text += slimTreeLines(null, slimTree, slimEdge); // 注入瘦身车 W4：账块抛错时树与边计数照出（旧版二者在板块、不受此处牵连）
						}
						if (todos.length > 0)
							text +=
								"\n## 现行待办（最新 2 条·指令型受保护，全量用 memory action=search query=待办）\n" +
								todos
									.map(
										(r) =>
											"- " +
											(r.planFence
												? "⚠依据已过期（见 " +
													r.planFence +
													"·执行前先核新依据）·动态指向最新补正条非新立条" // 落码车文案：绝「标记条无限增殖」误解（赋值面 L5408/L5430 不动）
												: "") +
											"🕐" +
											String(r.ts).slice(5, 10) +
											" [#" +
											r.id +
											"·todo] " +
											(r.space && r.space !== injectCallerSpace && r.space !== "global"
												? "[" + r.space + "] "
												: "") + // 落码车：他空间条加归属前缀（本空间/global 零噪声·增量 ≤20 字符/条）
											r.title,
									)
									.join("\n"); // P4：todo 席同款时标；⚠ 前缀=PlanFence 行动面告警
						// ── 微型件①：注入纪律行（源：toolkit 注入纪律·2026-08-21 B2 借鉴）——软防线防过期记忆误导 ──
						//    v2.3 ⑤（8-24 maintainer）：「不可信」→「慎用」——不可信易被过度弃用，慎用=校核后可用
						text +=
							"\n（历史记忆为慎用参考——引用前以当轮实况与正本核对；被动注入不构成已检索）";
						// wave 车二-2（10-17·落码规格 2.2）：SLIM 且 LEGION_SEARCH_HINT_ON=1 且本会话落 A 臂时，纪律行后加一行催查提示（约 50 字节）；
						//   按轮冻结不变（状态段整段随轮冻结）；A 臂每会话每轮首次出提示记 hint_ab_a_shown（结算 turns／active 在 index.cjs turn/end）
						if (SLIM && c2HintOn() && sid && hintArm(sid) === "A") {
							text += C2_HINT_LINE;
							try {
								const bdH = slimCtl && slimCtl.bd;
								if (bdH && bdH.c2HintTurn !== bdH.injTurn) {
									bdH.c2HintTurn = bdH.injTurn;
									slimBump(db, "hint_ab_a_shown", 1);
								}
							} catch {}
						}
						// ── 注入行（09-16 三案）：cross-space同族坑在涨——nightly patrol聚合面推送（非空才显·零打扰）──
						if (!SLIM) // 注入瘦身车 W4（版面 D·§3.11 第 10 条）：同族坑移出注入（数仍在 patrol_last·nightly patrol面照读）
						try {
							const _coPl = db
								.prepare("SELECT v FROM organ_meta WHERE k='patrol_last'")
								.get();
							if (_coPl) {
								const _co = JSON.parse(_coPl.v);
								const _coT = Array.isArray(_co.crossOrganLessonTop)
									? _co.crossOrganLessonTop
									: [];
								if (_coT.length > 0)
									text +=
										"\n⚠ cross-space同族坑在涨：" +
										(Number(_co.crossOrganLessonTotal) || _coT.length) +
										" 条 lesson 在跨空间被复犯（top：#" +
										_coT[0].id +
										" " +
										_coT[0].title +
										"·" +
										_coT[0].deg +
										" 边/" +
										_coT[0].spaces +
										" 空间——接令先 search 本域词查同族）";
							}
						} catch (eCI) {
							stats.crossOrganErrors = (stats.crossOrganErrors || 0) + 1; // ⑬ 出口（与nightly patrol段共用 面错误键）
						}
						// ── 回显step消费（21:43 maintainer）：观测升格注入可见——本会话被 A/B 闸点名时·注入面带可见提示（模型看得见才纠得偏）──
						// D2 修正（21:48 逐字审·同型病三犯）：bindMap 键=firehose 侧真实 bindSid（session.id 缺失时回落 '_anon'）——消费侧同键序查（精确→sessionOfLastTurn→_anon），禁单键直查（串键=nudge 永不显示）
						// 8-31 option：消费段上提案A 判定前（此处仅拼接）——M 型（轮内记忆先行）同消费
						// ── 压缩桥刀⑤（09-03 maintainer同车）：压缩态自知——本窗被压过则注入面明示（早期细节离窗·经锚条回流）──
						try {
							const cN = currentCompactionN(sid); // 09-11：持久计数兼取（原进程内 Map 单源 ⇒ 重启后少报）
							if (cN > 0)
								text +=
									"\n🗜 本窗已压缩 " +
									cN +
									" 次：早期细节已离窗——搜「上下文压缩锚」read_episodic 回流原文";
						} catch (eB) {
							injectBlkFail("compact-self", eB); // 批⑳ J8-7⑧：🗜压缩自知块
						}
						// ── P4 轴注记（2026-08-26 stepkickoff）：时龄结构一行——注入即带时间观念，旧态面可见；item移尾：N/M 计数每写必变·压到注入段最尾（前缀稳定·Mastra OM append-only 意）──
						try {
							const fresh48 = db
								.prepare(
									`SELECT COUNT(*) c FROM memories WHERE status='active' AND type IN ('fact','lesson','decision') AND space = ? AND COALESCE(event_at, ts) >= ?`,
								)
								.get(injectCallerSpace || "memory-organ", past48h).c;
							const past30 = (() => {
								const d = new Date(
									Date.now() + 8 * 3600 * 1000 - 30 * 86400000,
								);
								const p = (n) => String(n).padStart(2, "0");
								return `${d.getUTCFullYear()}-${p(d.getUTCMonth() + 1)}-${p(d.getUTCDate())}T${p(d.getUTCHours())}:${p(d.getUTCMinutes())}+08:00`;
							})();
							const aged30 = db
								.prepare(
									`SELECT COUNT(*) c FROM memories WHERE status='active' AND type IN ('fact','lesson','decision') AND space = ? AND COALESCE(event_at, ts) < ?`,
								)
								.get(injectCallerSpace || "memory-organ", past30).c;
							if (SLIM) {
								stats.injAxisFresh48 = fresh48; // 注入瘦身车 W4（版面 D·§3.11 第 11 条）：轴行移出注入，两数归 stats（read 返回 injAxisFresh48／injAxisAged30）
								stats.injAxisAged30 = aged30;
							} else
							text +=
								"\n🕐 轴：近 48h " +
								fresh48 +
								" 条｜超 30d " +
								aged30 +
								" 条（引用旧态条目前先 timeline 核对最新）";
						} catch (eB) {
							injectBlkFail("axis-note", eB); // 批⑳ J8-7⑨：🕐轴注记
						}
						text += nudgeText;
						// ── Injection hook: when an injected entry looks like work-in-progress, append a one-line hint ──
						if (
							OPS_HOOK_RE.test(
								rows
									.concat(todos)
									.map((r) => r.title)
									.join("\n"),
							)
						) {
							text += "\n⚠ work in progress in another window — run timeline before acting";
						}
						// ── 补件 2：完成态钩行（2026-08-22 补件task brief·治审计环 5 完成态盲区·Q2=24h 滚动窗）──
						//    读库计数近 24h 收官类 fact（收官/完成/闭环/全绿）≥3 → 块尾提示「盘账先 timeline」——绕开注入面 5 条标题的窄触发面。
						try {
							const since24h = nowIso24hAgo();
							// option（design-approved·钩行回归异常提醒本位）：排除本窗 space 计数——「他窗」名实相符
							const doneCnt = db
								.prepare(
									"SELECT COUNT(*) AS c FROM memories WHERE status='active' AND type='fact' AND ts >= ? AND space != ? AND (title LIKE '%收官%' OR title LIKE '%完成%' OR title LIKE '%闭环%' OR title LIKE '%全绿%')",
								)
								.get(since24h, injectCallerSpace || "__none__");
							if (Number(doneCnt.c) >= 3)
								text +=
									"\n⚠ other windows wrapped up " +
									Number(doneCnt.c) +
									" item(s) in the last 24h — run timeline before reconciling";
						} catch (eB) {
							injectBlkFail("done-tally", eB); // 批⑳ J8-7⑩：他窗收官钩
						}
						if (SLIM) return slimStateOut(context, slimCtl, text, nudgeText, slimVisOf({ rows, essRows, todos, led: slimLed })); // 注入瘦身车 W4（§3.11 第 12 条）：正常路出口
						recordInjectBytes("ctx", text); // E-1：注入总账（context 10 路整块·实测字节）
						return text;
					} catch (injErr) {
						// 8-31 锈面修（审计 F2-2④）：注入段兜底 catch 吞错无出口——补 logger.warn+stats.injectErrors 计数（⑬：注入静默断供须可见）
						stats.injectErrors = (stats.injectErrors || 0) + 1;
						try {
							ctx.logger?.warn?.(
								"[living-memory] inject failed (#" +
									stats.injectErrors +
									"): " +
									String(injErr).slice(0, 80),
							);
						} catch {}
						recordInjectBytes("ctx", ""); // E-1：降级路亦留账（字节 0·保 samples 连续性）
						return "";
					}
				}),
			});
			ctx.effect(() => stopSection);
		}

}

function setupAutoRecall({ ctx, stats, bindMap, db, runExtract, a25Flush, a25Tombstones, compactionBySid, compactionMetaBySid, pendingBySid }) { // [拆②补丁 v4] db 同 setupInject 补传（段内 18 处裸引用）
		//    机制：agent/inbox/claimed 捕获真用户令 → system-prompt/assemble（宿主契约=dsh-system-prompt waterfall
		//    assembly{sections,contexts,variables}）时以其为 query 跑living memory FTS 检索链 → one-pass 变量注入
		//    （{{var}} 占位+variables 存值——召回文本当数据不当模板源·防提示词注入·graph-memory prompt-data 同法）。
		//    纪律：注入=参考级慎用口径（同 v2.3 ⑤）·高精门 bm25≥0.18 只进强相关·同 query 5min 缓存·
		//    检索失败静默零阻断·env LEGION_AUTORECALL_OFF 一键关。红线：只挂 contexts 不动 sections（人格/directive层零碰）。
		try {
			// wave 车二-3（10-17）：开关只认 "1"——设了别的值启动打一次 warn、按未设处理
			const v3 = process.env.LEGION_PN_LIMIT_OFF;
			if (v3 !== undefined && v3 !== "1") ctx.logger?.warn?.("[living-memory] wave开关 LEGION_PN_LIMIT_OFF=" + JSON.stringify(String(v3).slice(0, 8)) + " 不是 \"1\"，按未设处理");
		} catch {}
		const AUTO_RECALL_MAX = 4;
		const AUTO_RECALL_MIN_SCORE = 0.18;
		try {
			// wave 车二-4（10-17）：开关只认 "1"——设了别的值启动打一次 warn、按未设处理（同 W4 盲审 R5 口径）
			const v4 = process.env.LEGION_RECALL_PTR_OFF;
			if (v4 !== undefined && v4 !== "1") ctx.logger?.warn?.("[living-memory] wave开关 LEGION_RECALL_PTR_OFF=" + JSON.stringify(String(v4).slice(0, 8)) + " 不是 \"1\"，按未设处理");
		} catch {}
		const autoLatestPrompt = new Map(); // agentId -> { text, at }（8-31 修复④：带捕获时点——过期扫按 at 判·治「无 cache 即删」误清 claimed→assemble 窗口新令）
		const autoRecallCache = new Map(); // agentId -> { query, at, text }
		try {
			// wave 车二-2（10-17）：开关只认 "1"——设了别的值启动打一次 warn、按未设处理（提示照旧不开）
			const v2 = process.env.LEGION_SEARCH_HINT_ON;
			if (v2 !== undefined && v2 !== "1") ctx.logger?.warn?.("[living-memory] wave开关 LEGION_SEARCH_HINT_ON=" + JSON.stringify(String(v2).slice(0, 8)) + " 不是 \"1\"，按未设处理");
		} catch {}
		try {
			ctx.on &&
				ctx.on("agent/inbox/claimed", (payload) => {
					try {
						const message = payload?.message ?? payload;
						if (message?.source?.kind !== "user") return;
						if (slimOn()) slimNoteClaim(payload, bindMap, db); // 注入瘦身车 W4（§3.4 白名单③、§3.11 第 15 条）：真用户消息计 injUsr；轮中另计持久键
						const id = String(
							payload?.agent?.id ?? payload?.agent?.session?.id ?? "",
						);
						if (!id) return;
						let text = "";
						const c = message?.data?.content ?? message?.content;
						if (typeof c === "string") text = c;
						else if (Array.isArray(c))
							text = c
								.filter(
									(b) => b && (b.type === "text" || b.type === "input_text"),
								)
								.map((b) => b.text || "")
								.join("");
						if (!text || text.length < 6) return;
						autoLatestPrompt.set(id, {
							text: text.slice(0, 500),
							at: Date.now(),
						});
						autoRecallCache.delete(id);
					} catch {}
				});
		} catch (eHk1) {
			// 批⑲ P1（J3-2·🔴）：注册期异常可观测——原裸 catch ⇒ 捕获路死则 autoLatestPrompt 恒空·整链 100% 死而 stats 全零（假绿不可诊断）
			stats.autorecallHookDead = (stats.autorecallHookDead || 0) + 1;
			ctx.logger?.warn?.(
				`[living-memory] autorecall hook dead (#${stats.autorecallHookDead}·agent/inbox/claimed): ${String(eHk1).slice(0, 80)}`,
			);
		}
		// ── 8-31 移植族修复④：原挂 'session/end' 幻事件（宿主 SessionEventMap 无终结型事件·整段死码从未执行——step清算+2h 过期扫+a25Flush 全灭）——
		//    重构为 sweepExpiredAccounts 挂 turn/end 每轮尾（真事件）；sid 直清段退役（无事件可挂·2h 过期路等价覆盖「未抽段清算+删账+落盘」语义·I5/I6a/I6b 护栏原样保留）；
		//    autoLatestPrompt 过期改按捕获时点 v.at 判（治原「无 cache 即删」误清 claimed→assemble 窗口新令）；bindMap 同扫（bd.at）。
		function sweepExpiredAccounts() {
			try {
				let sweptAny = false; // 件三车病灶2：清账标记——原尾部每轮 force 绕 D15 节流（写放大回潮）·实际清账才强制
				const cutoff = Date.now() - 2 * 3600_000;
				for (const [k, v] of autoLatestPrompt) {
					if ((v && v.at ? v.at : 0) < cutoff) {
						autoLatestPrompt.delete(k);
						autoRecallCache.delete(k);
						sweptAny = true;
					}
				}
				for (const [k, bd2] of bindMap) {
					if (bd2.at && bd2.at < cutoff) {
						bindMap.delete(k);
						sweptAny = true;
					}
				}
				for (const [k, cn] of compactionBySid) {
					// 09-03 压缩桥伴扫：bind/pending 双无=会话已散 → 计数清（防 Map 无界·bindMap 同闸同窗）
					if (!bindMap.has(k) && !pendingBySid.has(k)) {
						compactionBySid.delete(k);
						sweptAny = true;
					}
				}
				for (const k of compactionMetaBySid.keys()) {
					// O3 伴扫（09-30 压缩面优化车·同闸同窗防 B-I3 型「2h 有界变永久」）：会话已散 → summary 元数据清
					if (!bindMap.has(k) && !pendingBySid.has(k)) {
						compactionMetaBySid.delete(k);
						sweptAny = true;
					}
				}
				for (const [k, a] of pendingBySid) {
					if (a.lastTs && Date.now() - a.lastTs > 2 * 3600_000) {
						if (a.msgs.some((m) => m.seq > a.watermark)) {
							// 审计 I6b（20:58）：有未抽段不直删——发起清算（同step同族·丢账暗流失面）·清完由 IIFE 删
							(async () => {
								try {
									let g = 0;
									while (g < 10) {
										const r = await runExtract({ sid: k });
										if (!r || r.skipped) return;
										g++;
									}
									const fin = pendingBySid.get(k);
									if (!fin || fin.msgs.every((m) => m.seq <= fin.watermark)) {
										pendingBySid.delete(k);
										a25Tombstones.add(k); // 批㉒：chase 毕清账记墓碑
										a25Flush(true);
									}
								} catch (e) {
									stats.a25ChaseErrors = (stats.a25ChaseErrors || 0) + 1;
									ctx.logger?.warn?.(
										"[living-memory] a25 chase/flush failed (#" +
											stats.a25ChaseErrors +
											"): " +
											String(e).slice(0, 60),
									);
								}
							})(); // step 脑追办①：I6b 空 catch 补出口（⑬ 立法当日漏一枝·脑认账件）
						} else {
							pendingBySid.delete(k);
							a25Tombstones.add(k); // 批㉒：2h 清账记墓碑
							sweptAny = true;
						}
					}
				} // 2h 无新消息的分账清（防 Map 无界·审计死语句修正版）
				if (sweptAny) a25Flush(true); // 清账后强制落盘（件三车病灶2：实际清账才 force——每轮 force 与 D15 节流矛盾根治）
			} catch (e) {
				stats.sweepErrors = (stats.sweepErrors || 0) + 1;
				ctx.logger?.warn?.(
					"[living-memory] sweepExpiredAccounts failed (#" +
						stats.sweepErrors +
						"): " +
						String(e).slice(0, 60),
				);
			} // ⑬：空 catch 退役
		}
		// [拆②补丁 v4b] sweep 挂载收回本域（原 index.cjs L4142 session/event 监听器尾部裸调 sweepExpiredAccounts——
		//   拆②段B 搬走后该调用悬空：函数只在 setupAutoRecall 作用域内 ⇒ 每轮 turn/end 必 ReferenceError·
		//   且 sweep 功能（2h 过期扫/bindMap 清算/清账落盘）自 103 踵起实断）。治法：挂载点随函数一体归位本域。
		//   事件形态与 index 侧逐字同构（session/event·String(event.type)）；幂等清理无顺序敏感。
		try {
			ctx.on &&
				ctx.on("session/event", (session, event) => {
					try {
						if (String(event?.type) === "turn/end") sweepExpiredAccounts();
					} catch (eSw) {
						stats.sweepCallErrors = (stats.sweepCallErrors || 0) + 1; // ⑬ 出口：调用期异常可见（sweep 体内自 try·此为兜底）
						ctx.logger?.warn?.(
							"[living-memory] sweepExpiredAccounts call failed (#" +
								stats.sweepCallErrors +
								"): " +
								String(eSw).slice(0, 60),
						);
					}
				});
		} catch (eSwHook) {
			stats.sweepHookDead = (stats.sweepHookDead || 0) + 1; // ⑬：注册期异常可观测（同 autorecall hook dead 族）
			ctx.logger?.warn?.(
				"[living-memory] sweep hook dead (#" + stats.sweepHookDead + "): " + String(eSwHook).slice(0, 80),
			);
		}
		function legionContributeAutoRecall(assembly, text, slimH) {
			if (slimH) return slimContribute(assembly, text, slimH); // 注入瘦身车 W4（§3.11 第 16 条）：SLIM 时走合并出口（MERGE_OFF／无状态条目时其内退回原 push）
			try {
				recordInjectBytes("wf", text); // E-1：注入总账（waterfall 4 路变量块·实测字节·本件为唯一写入口）
				const base = "legion_auto_recall";
				let name = base,
					n = 2;
				assembly.variables = assembly.variables || {};
				while (Object.hasOwn(assembly.variables, name)) name = base + "_" + n++;
				assembly.variables[name] = promptSafe(text); // 审计封缝（J-new-1）：wf 路原直赋变量——宿主 interpolate 不递归解析变量值（当前不崩）故原设计留缝，仍并入净化面以杜绝「注入面扩到 content 时」的缝隙
				assembly.contexts.push({ name, text: "{{" + name + "}}" });
			} catch {}
		}
		try {
			ctx.on &&
				ctx.on("system-prompt/assemble", async (assembly, context, next) => {
					// P2 fix（09-03 audit）：双 next 根治——try 内早退支 return await next() 若 reject 被 eAR catch 吞后落尾部再调 next（cordis next 一次性）；
					// 修：早退支全走 runNextAR（打戳+不 await→reject 天然传播宿主 waterfall 不碰 catch）·尾部唯一条件调用点。
					let nextCalledAR = false;
					const slimH = slimOn() ? slimHookEnter(context, bindMap) : null; // 注入瘦身车 W4（§3.10）：本次装配的同轮账句柄
					if (slimH) slimH.asm = assembly; // 注入瘦身车 W4 第 9 笔：结算点超帽裁剪要改本次装配的条目与变量
					const runNextAR = () => {
						if (slimH) slimSettle(slimH); // 注入瘦身车 W4（§3.11 第 17 条）：结算点①——早退支、冻结支都经此（next 之前）
						nextCalledAR = true;
						return next();
					};
					try {
						if (
							process.env.LEGION_AUTORECALL_OFF ||
							!assembly ||
							!Array.isArray(assembly.contexts)
						)
							return runNextAR(); // P2 fix
						// 注入瘦身车 W4（§3.4、§3.11 第 17 条）：召回段键＝injTurn——同轮后续步直接用本轮召回（不看 5 分钟期·AUTORECALL_OFF 照旧在前生效）
						if (slimH && slimH.freezable && slimH.bd.injRc && slimH.bd.injRc.ver === SLIM_VER && slimH.bd.injRc.turn === slimH.turn) {
							slimH.frozen = true;
							slimH.rcParts = slimH.bd.injRc.parts || null; // 第 9 笔：冻结召回带分件（白名单重算步超帽时照按件退）
							if (slimH.bd.injRc.text) slimContribute(assembly, slimH.bd.injRc.text, slimH);
							return runNextAR();
						}
						// 键同源hard rule（审计 D-键族①③·nudge D2 消费侧同型先例）：主键=context.agent.id（与 claimed 捕获键同源）。
						// sessionOfLastTurn 是插件级单例（多窗并发跨污染面）——仅当 autoLatestPrompt 唯一持有时才作 fallback
						// （唯一=无错配可能；多窗并存时宁缺毋滥放弃注入）。sessionOrgan 期望 session id：agent.id 主路直用
						// （DSH 主对话 agent=session 同 id），唯一兜底路不传 callerSpace（防错源退化全库）。
						const agentKey = String(
							context?.agent?.id ?? context?.scope?.agent ?? "",
						);
						let key = agentKey;
						let callerKey = agentKey;
						if (!key) {
							if (autoLatestPrompt.size !== 1) return runNextAR(); // P2 fix
							key = autoLatestPrompt.keys().next().value;
							callerKey = "";
						}
						const query = autoLatestPrompt.get(key)?.text;
						if (!query) return runNextAR(); // P2 fix
						// 高精门 v4 定稿注释（v3 漂移修正·审计改进项）：三层=①长度门（<10 字且无意图词拒——短 query
						// 语义信号不足）②bm25 归一≥0.18 ③token 交叠主门（**无条件**·query×title ≥2 token——治
						// 「常见词强命中」：bm25 0.86 的「无效查询」单 token 命中被此门挡）。中文 2 字核心词天然 1 bigram
						// 交叠会被误挡（召回代价·宁缺毋滥口径·观察一周再议降阈）。
						const intentRe = CRIT.intentGate; // 意图门词表已外置（criteria-rules.default.json·2026-09-12 一车）
						const hasIntent = intentRe.test(query);
						if (query.length < 10 && !hasIntent) {
							autoRecallCache.set(key, { query, at: Date.now(), text: "" });
							return runNextAR(); // P2 fix
						}
						const hit = autoRecallCache.get(key);
						if (
							hit &&
							hit.query === query &&
							Date.now() - hit.at < 5 * 60_000
						) {
							if (slimH) slimH.shown = (SLIM_RC_SHOWN.get(hit) || []).slice(); // 注入瘦身车 W4（§3.11 第 18 条末句）：缓存命中不计被截、照带已显 id
							if (slimH) slimH.rcParts = SLIM_RC_PARTS.get(hit) || null; // 第 9 笔：缓存召回带分件
							if (hit.text) legionContributeAutoRecall(assembly, hit.text, slimH);
							return runNextAR(); // P2 fix
						}
						// 归属源三链（2026-09-12 二轮审计真缺陷③修复）：原仅 `sessionOrgan(agentKey)`，
						// 与席侧（L3436-3442：**cwd 直推 > sid > null**）**不同源** ⇒ 两池不可比
						// （审计实测「本空间最新 6 落在全库最新 12 内」仅 **0-4/6**·creator/video＝0/6）
						// ⇒ callerSpace=null 而席非 null 时池过滤**漏剔**、同条两席仍可达。
						// 改与席侧**逐字同构**（cwd 直推 > sid > null）——口径统一是**正确性要求**，非优化。
						// ⚠ cwd 可用性有实证：根治B（08-30 落注释）记「assemble 上下文每步必真·
						//   context.agent.session.id/header.cwd 可用」（agent-loop 实证）。
						const e2Cwd = context?.agent?.session?.header?.cwd;
						const e2Sid = context?.agent?.session?.id || agentKey;
						const callerSpace = e2Cwd
							? organFromPath(e2Cwd)
							: e2Sid
								? sessionOrgan(e2Sid)
								: null;
						const tokens = qTokens(query); // item：实词优选（jieba 滤虚词+西文专名最前〔option遗产守〕+bigram 兜底）——原「option西文排序+slice(0,8)」与更早纯 slice 两版统一收编本函数
						const match = queryMatch(tokens);
						const c2Pn = c2PnLimitOn(); // wave 车二-3：本次装配一次取值（专名路、主题词补位、前瞻三处同口径）
						const c2Path = c2Pn ? c2PathToks(query) : new Set();
						const ftsBase = `SELECT m.id, m.type, m.title, m.content, m.space, m.source, COALESCE(m.event_at, m.ts) AS eff_ts, bm25(memories_fts) AS rank FROM memories_fts JOIN memories m ON m.id = memories_fts.rowid WHERE memories_fts MATCH ? AND m.status = 'active'`; // option伴修：ftsBase 上提（原 if(match) 块内 const——主题词重查段块外引用 ReferenceError 静默·caught in drill·硬排谓词在各调用点尾追）｜车二-9 1-d：SELECT 补 m.source 供来源一字标
						const vtNow = nowIso(); // 硬排时刻（一次提值·fts/vec 两路同刻·2026-09-24 落码车）
						let picked = [];
						if (match) {
							const rows = callerSpace
								? db
										.prepare(
											ftsBase +
												" AND m.space IN (?, 'global') AND " + CUR_SQL + " ORDER BY rank LIMIT 40", // 车二-9 块一 1-b（10-25 落码）：接现行谓词（superseded_by／stale_state 两段并上）
										)
										.all(match, callerSpace, vtNow) // 落码车硬排：实参尾追（占位符序 MATCH→space→valid_to）
								: db.prepare(ftsBase + " AND " + CUR_SQL + " ORDER BY rank LIMIT 40").all(match, vtNow); // 1-b：同上（占位符序 MATCH→vtNow）
							picked = rows
								.map((r) => {
									const rr = -Number(r.rank || 0);
									return { ...r, score: rr / (1 + rr) };
								})
								.filter((r) => r.score >= AUTO_RECALL_MIN_SCORE)
								.filter((r) => {
									// token 交叠门（高精门③·v4 定稿）：无条件主门——query 与命中条 title ≥2 token
									// 交叠才算可信相关（graph-memory「精确标识必须匹配」哲学）。治「常见词强命中」：bm25 0.86 的
									// 「无效查询」单 token 命中（如「过程查询法」）被此门挡住——自动注入宁缺毋滥。
									// ⚠ 车二修正（09-14·补审 B-1/P2）：tokenize 双侧归一后西文词面=「原词＋词干」双 token ⇒
									//   同一词（running）交叠被双计 [running,run]=2 误过闸（门退化 ≥1）。修＝两侧先**词干归一
									//   去重**再计交叠基数（中文/数字 token 过 stem 原样返回·零影响）——语义恢复 v1 等价，
									//   且形态差桥（books↔book）算 1 交叠（真正同源词·与双侧归一哲学一致·不放松）。
									const stemTk = (w) => (/^[a-z]+$/.test(w) ? porter2Stem(w) : w);
									const tt = new Set(
										[...tokenize(String(r.title))].map(stemTk),
									);
									const hit = new Set();
									for (const tk of tokens) {
										const s = stemTk(tk);
										if (tt.has(s)) hit.add(s);
									}
									return hit.size >= 2;
								})
								.slice(0, AUTO_RECALL_MAX);
						}
						// ── 8-31 option（召回分层settled ·approved）：查询构造升级两段——整句零命中时
						//    ①主题词重查（整句疑问句词面稀释→gateKeywords 提取主题词再造 match——治「问句整丢」）
						//    ②vec 近邻兜底（cos≥0.45 门槛实测标定：换词说法 0.46-0.49/词面强 0.58+/语义远 0.39——治「换词说法」不治语义远——
						//    vec 救不了语义远距离〔 原案 2341 名铁证〕·只兜近邻面）。
						// ── option专名救援路（bm25 泛词灌水免疫）：西文专名（modlens/qwen 等）单路 OR 查——OR 累加使「泛词多中」压过
						//    「专名单中」（ 含 modlens 排 463 名实证），专名路独立查置顶；交叠判 title∪content 前 200 子串直判
						//    （专名在 content 的条目不被 title-only 门误滤）。
						try {
							const properNouns = tokens.filter(
								(t) =>
									/^[\x21-\x7e]{2,}$/.test(t) && !/^\d+$/.test(t) && // 批⑰ B5①（J10 专项）：纯数字 token 不进专名救援路——治 id/日期类查询（如「17689」「0226」）被 0.99 置顶灌水
									(!c2Pn || c2PnOk(t, c2Path)), // wave 车二-3①②③：去路径段、过停用表 pnRescueStop、长度 ≥3（诊 L65：of 中 offline；USD、KYC、本机路径段占两席）
							);
							if (properNouns.length > 0) {
								const pnMatch = properNouns
									.map((t) => '"' + t.replace(/"/g, "") + '"')
									.join(" OR ");
								const pnRows = callerSpace
									? db
											.prepare(
												ftsBase +
													" AND m.space IN (?, 'global') AND " + CUR_SQL + " ORDER BY rank LIMIT 10", // 车二-9 块一 1-b：专名救援接现行谓词
											)
											.all(pnMatch, callerSpace, vtNow) // 落码车硬排（实参尾追）
									: db
											.prepare(ftsBase + " AND " + CUR_SQL + " ORDER BY rank LIMIT 10")
											.all(pnMatch, vtNow);
								const seen0 = new Set(picked.map((r) => r.id));
								// wave 车二-3⑤：SLIM 截显示且 SHOW<4 时专名至多 1 席、插在主路第 1 条之后（[main0, pn0, main1…]，主路空时 [pn0]）；
								//   SHOW≥4 或 SLIM 关时照旧前插至多 2 席（①至④照样生效）
								const c2PnOne = c2Pn && !!slimH && slimShow() < 4;
								const pnPicked = pnRows
									.filter((r) =>
										properNouns.some((t) =>
											c2Pn
												? c2WholeWord(String(r.title) + " " + String(r.content || "").slice(0, 200), t) // wave 车二-3④：子串 includes 改整词
												: (
														String(r.title) +
														" " +
														String(r.content || "").slice(0, 200)
													)
														.toLowerCase()
														.includes(t.toLowerCase()),
										),
									)
									.map((r) => ({ ...r, score: PN_RESCUE_SCORE, pnRescue: true })) // 批⑰ B5③ 常量化
									.filter((r) => !seen0.has(r.id))
									.slice(0, c2PnOne ? 1 : 2);
								if (pnPicked.length > 0) {
									picked = c2PnOne
										? (picked.length > 0 ? [picked[0], ...pnPicked, ...picked.slice(1)] : [...pnPicked]).slice(0, AUTO_RECALL_MAX)
										: [...pnPicked, ...picked].slice(0, AUTO_RECALL_MAX);
									stats.autorecallPnRescue =
										(stats.autorecallPnRescue || 0) + 1;
								}
							}
						} catch {}
						if (picked.length < AUTO_RECALL_MAX) {
							// option：非零命中也补位（主路巧合条占席真相关缺席·caught in drill）——合并去重·主路优先·重查条排后
							try {
								const kwsRaw = gateKeywords(query, 10);
								const kws = [
									...kwsRaw.filter((t) => /^[\x21-\x7e]{2,}$/.test(t)),
									...kwsRaw.filter((t) => !/^[\x21-\x7e]{2,}$/.test(t)),
								]; // 西文专名优先（同主路截断修）
								if (kws.length >= 2) {
									const match2 = queryMatch(kws.slice(0, 8));
									if (match2) {
										const rows2 = callerSpace
											? db
													.prepare(
														ftsBase +
															" AND m.space IN (?, 'global') AND " + CUR_SQL + " ORDER BY rank LIMIT 40", // 车二-9 块一 1-b（10-25 落码）：接现行谓词（superseded_by／stale_state 两段并上）
													)
													.all(match2, callerSpace, vtNow) // 落码车硬排（实参尾追）
											: db
													.prepare(ftsBase + " AND " + CUR_SQL + " ORDER BY rank LIMIT 40")
													.all(match2, vtNow);
										const seen = new Set(picked.map((r) => r.id));
										const more = rows2
											.map((r) => {
												const rr = -Number(r.rank || 0);
												return { ...r, score: rr / (1 + rr) };
											})
											.filter((r) => r.score >= AUTO_RECALL_MIN_SCORE)
											.filter((r) => {
												const tt = new Set(tokenize(String(r.title)));
												const ov = kws.filter((tk) => tt.has(tk));
												return (
													ov.length >= 2 ||
													ov.some(
														(t) =>
															/^[\x21-\x7e]{2,}$/.test(t) &&
															!/^\d+$/.test(t) &&
															(!c2Pn || c2PnOk(t, c2Path)), // wave 车二-3：西文单桥同样排除停用表、路径段，长度 ≥3
													) // 车二件6（治 P10-5·J10-12 多点字面量预言第三次兑现）：≥5 位纯数字不单桥过门——B2（L8333 批⑰）/（L8658 批⑳）同款补丁第三处；gateKeywords 只滤 1-4 位
												);
											}) // option：主题词路交叠门=≥2∨含西文专名——纯 ≥1 被泛词「查询」一词洞穿高精门（autorecall 断言3 实抓）；中文单桥词守宁缺毋滥（v4 既定代价）·专名一单桥即信（modlens 类与专名路belt-and-braces）
											.filter((r) => !seen.has(r.id))
											.slice(0, AUTO_RECALL_MAX - picked.length);
										if (more.length > 0) {
											picked = [...picked, ...more];
											stats.autorecallKwRescue =
												(stats.autorecallKwRescue || 0) + 1;
										}
									}
								}
							} catch {}
						}
						if (picked.length === 0 && !process.env.LEGION_VEC_FALLBACK_OFF) {
							try {
								const vc = await ctx.credentials?.resolve?.(
									"EMBEDDING_BAILIAN_KEY",
								);
								if ((process.env.LEGION_VEC_ON === "1" || process.env.LEGION_VEC_ON === "query") && vc && vc.value) { // option车闸3：默认关断——自动召回路独立查询嵌入（不经 vecRecallCore）同停；query 档小开（09-16）：查询路放行
									if (!core.vecCredCache.v || Date.now() - core.vecCredCache.t > 60000)
										core.vecCredCache = { v: vc.value, t: Date.now() }; // option伴修：embedOnce 凭据前置——vecCredCache 本由 vecRecallCore 填充·直接调 embedOnce 空 Bearer 401 静默（caught in drill·移植接口同构亲验闸同族）
									// item1：查询侧 instruct 同落此兜底（查询身份一致）——此路查 memories_vec
									// 旧表（v4·instruct 未探针）；400 则 catch 静默降级 FTS·留 A/B counter。
									const [qv] = await embedOnce(
										[clipSurrogateTail(String(query).slice(0, 1500))],
										undefined,
										INSTRUCT_QUERY,
									);
									const vsql =
										"SELECT v.id, v.embedding, m.type, m.title, m.content, m.space, m.source, COALESCE(m.event_at, m.ts) AS eff_ts FROM memories_vec v JOIN memories m ON m.id = v.id WHERE v.model_version = ? AND m.status='active' AND " + CUR_SQL + // 车二-9 块一 1-b（10-25 落码）：向量兜底接现行谓词（占位符序 model→vtNow）；1-d：补 m.source
										(callerSpace ? ` AND m.space IN (?, 'global')` : ""); // 落码车硬排：区间B vec 路尾追谓词（占位符序 model→valid_to→space）
									const vrows = callerSpace
										? db.prepare(vsql).all(VEC_MODEL, vtNow, callerSpace)
										: db.prepare(vsql).all(VEC_MODEL, vtNow);
									let best = null;
									for (const r of vrows) {
										const s = cosine(qv, blobToF32(r.embedding));
										if (s >= 0.45 && (!best || s > best.s)) best = { ...r, s };
									} // option门槛 0.45 实测标定：换词说法语义近 0.46-0.49·词面强 0.58+·语义远 0.39（0.55 对中文作战黑话过苛全灭·caught in drill）·误灌面注入 🧬 标可观察
									if (best) {
										picked = [{ ...best, score: best.s, vecFallback: true }];
										stats.autorecallVecFallback =
											(stats.autorecallVecFallback || 0) + 1;
									}
								}
							} catch {}
						}
						// ── E-2 注入席互斥（design-approved·审计 D3 硬证据「同条两席」· 实例）──
						//   治面：同一条 todo 可同时出现在「现行待办席」（systemPrompt.context 路）与本召回区。
						//   两路作用域互不可达（「席侧顶替路跨作用域幻引用前车」：section 回调局部变量在 assemble 不可达）⇒ 不跨作用域
						//   取席的最终集，而按**席的候选池**互斥：池（本空间 LIMIT 6 ＋ global LIMIT 4）⊇ 席最终集
						//   （closureCheck／同义查重只从池内裁除）⇒ 按池过滤＝语义正确的过近似：被裁除者本就不该占
						//   注入席（已闭环／重复），被召回亦属重复。⚠ 席侧零改（零漂移）；池外更旧 todo 不受影响＝
						//   召回独有面保留。callerSpace 与席同源（sessionOrgan 第一源 cwd→organFromPath；L3404 根治B
						//   起口径统一）。
						//   ⚠ **如实订正（2026-09-12 审计真缺陷②）**：原写「若二者取值不同则退化为**不过滤**（安全
						//   方向·宁少剔不误伤）」——**该行为在码中并不存在**（全段无任何同源性比较点）。实况是：
						//   `callerSpace` 为 null 时（`callerKey=''` 的既定降级路·如新建会话首分钟归属未定）池退化
						//   为「全库最新 LIMIT 12」，会剔除**从未入席**的跨空间 todo ⇒ **存在误伤面**（信息面·非安全
						//   面）。本窗处置＝**保持现行为 ＋ 如实改注**（不引入新判据·避免扩大行为面）；误伤面候裁。
						//   ⚠ 另：席侧「directive型顶替路」命中的条**可落在候选池之外**（无 space 限定·不受池 LIMIT 约束）
						//   ⇒ 已由下方 ordSeat 段同构补入 seatIds（2026-09-12 审计真缺陷①修复）。
						try {
							const seatIds = new Set();
							if (callerSpace) {
								for (const r of db
									.prepare(
										"SELECT id FROM memories WHERE status='active' AND type='todo' AND space = ? ORDER BY id DESC LIMIT 6",
									)
									.all(callerSpace))
									seatIds.add(r.id);
								for (const r of db
									.prepare(
										"SELECT id FROM memories WHERE status='active' AND type='todo' AND space='global' ORDER BY id DESC LIMIT 4",
									)
									.all())
									seatIds.add(r.id);
							} else {
								for (const r of db
									.prepare(
										"SELECT id FROM memories WHERE status='active' AND type='todo' ORDER BY id DESC LIMIT 12",
									)
									.all())
									seatIds.add(r.id);
							}
							// ── 席侧「directive型顶替路」补入（2026-09-12 审计真缺陷①修复）──────────────
							//   席侧（section 回调 L3664-3689）在 todos 非空且无directive型时，会用一条**directive型 todo
							//   顶替最旧一席**——该查询**无 space 限定、不受上方候选池 LIMIT 约束** ⇒ 命中的条
							//   可落在候选池之外 ⇒ 仅按池取 id 会漏它（「池 ⊇ 席最终集」不变量不成立），
							//   该路径上「同条两席」仍可达且 autorecallSeatDedup 恒 0（观测假阴）。
							//   ⇒ 此处**照抄席侧同构 SQL**（同一模块级词表 ORD_WORDS/ORD_WORDS_EN ⇒ 零漂移），
							//   取 id 并入 seatIds。⚠ 只取 id 不重放席侧的 closureCheck/去重判定（那是席侧
							//   是否真顶替的条件）；此处取**上界**（可能多剔一条）——与「按池过滤＝过近似」同哲学。
							try {
								// ⑥ SQL 参数化（09-13 D 项余项车·二轮审计判据⑥）：词表值走占位符——原为字面拼接进 LIKE，
								//   输入虽受控（模块级常量词表）仍属稳健性缺口：词表一旦含引号类字符即语法崩。
								const ordSeatPats = ORD_WORDS.concat(ORD_WORDS_EN).map(
									(w) => "%" + String(w).replace(/\\b/g, "") + "%",
								);
								const ordSeat = db
									.prepare(
										"SELECT id FROM memories WHERE status='active' AND type='todo'" +
										" AND (" +
											ordSeatPats.map(() => "title LIKE ?").join(" OR ") +
											") ORDER BY id DESC LIMIT 1",
									)
									.get(...ordSeatPats);
								if (ordSeat) seatIds.add(ordSeat.id);
							} catch (eOrdSeat) {
								// ⑬ 出口（2026-09-12 二轮审计真缺陷②修复）：原为本车新增的**空 catch**，
								// 护的正是本车核心补入路径 ⇒ ordSeat 抛错即静默失效、同条两席静默复发、
								// autorecallSeatDedup 再次假阴（正是本车要治的病）。且原注释「池面已覆盖多数
								// 情形」与上文自我论证（命中条**可落在池外**）直接矛盾。补 warn 如实暴露。
								ctx.logger?.warn?.(
									"[living-memory] E-2 ordSeat 补入失败（该路径剔重失效·同条两席可能复发）: " +
										String((eOrdSeat && eOrdSeat.message) || eOrdSeat).slice(0, 60),
								);
							}
							if (seatIds.size > 0) {
								const beforeSeat = picked.length;
								picked = picked.filter((r) => !seatIds.has(r.id));
								const cutSeat = beforeSeat - picked.length;
								if (cutSeat > 0)
									stats.autorecallSeatDedup =
										(stats.autorecallSeatDedup || 0) + cutSeat;
							}
						} catch (eSeat) {
							// ⑬ 出口（2026-09-12 审计判据①修复）：原为空 catch ⇒ 若 db.prepare 抛错，
							// 剔重会「永久静默失效且零信号」。补 warn（不影响召回注入本身·ceiling 面非正确性面）。
							ctx.logger?.warn?.(
								"[living-memory] E-2 席互斥失败（剔重失效·召回照常）: " +
									String((eSeat && eSeat.message) || eSeat).slice(0, 60),
							);
						}
						if (slimH && envOn("LEGION_INJECT_XDEDUP_ON")) picked = slimXdRecall(slimH, picked, ctx.logger, assembly); // 注入瘦身车 W4 第 8 笔
						// 车二-9 块一 1-b（10-25 落码）：召回各路（主路／专名救援／主题词补位／向量兜底／前瞻）统一滤掉 J12 三类系统条——
						//   上面只给各路 SQL 补了现行谓词（到点／被取代／待复核），系统条不在这三类里，须在 JS 侧判；
						//   放在截显示与瘦身句柄之前，两路（瘦身／非瘦身）共用同一出口，后加的路自动覆盖。
						picked = picked.filter((r) => !isSystemRow(r)); // 上一行 W4 第 8 笔的注脚保留于此：（§3.6-2，缺省关）截条之前与本步状态段可见 id 互斥
						if (slimH) {
							// 注入瘦身车 W4（保守 C·§3.5、§3.11 第 18 条）：E-2 之后、拼头之前截到 SHOW 条（在 E-2 try/catch 之外，E-2 抛错照截）；
							//   池 id 暂存，原第 3、4 条可再由衔接、前瞻带出，被截在前瞻段之后才定（slimRecallFinish）
							slimH.pool = picked.map((r) => Number(r.id));
							slimH.poolTitle = new Map(picked.map((r) => [Number(r.id), String(r.title || "")])); // wave 车二-4：被截指针取标题（不再查库）
							slimH.qh = require("node:crypto").createHash("sha1").update(String(query)).digest("hex").slice(0, 8); // wave 车二-4：被截环第 4 项（查询哈希·不记原文）
							picked = picked.slice(0, slimShow());
						}
						let text = picked.length
							? "## 记忆库自动召回（⚠ 参考级·与最新记忆相关，引用前以当轮实况与正本核对）\n" +
								picked
									.map((r) => {
										// 车二-9 块一 1-d（10-25 落码）：非瘦身路同式补 #id 与来源一字标；开关关时回旧式
										const mk = srcMark(r.source);
										const tag = mk ? "#" + r.id + "·" + r.type + "·" + r.space + "·" + mk : "#" + r.id + "·" + r.type + "·" + r.space;
										return process.env.LEGION_INJ_LINEFMT_OFF === "1"
											? "- 🕐" + String(r.eff_ts).slice(5, 10) + (r.vecFallback ? " 🧬[语义召回·" + r.type + "·" + r.space + "] " : " [" + r.type + "·" + r.space + "] ") + r.title + "——" + String(r.content || "").slice(0, 80)
											: "- 🕐" + String(r.eff_ts).slice(5, 10) + (r.vecFallback ? " 🧬[" + tag + "] " : " [" + tag + "] ") + r.title + "——" + String(r.content || "").slice(0, 80);
									})
									.join("\n")
							: "";
						// ── 开工 lesson 顶注（wave·沉淀闭环「用好」面）：召回命中含 lesson 类时·行为位行送到眼前——
						//    「动笔前过 checklist」从模型自觉变自动推送（治「教训在库里躺着·开工照样踩」）。
						try {
							const lessons = picked.filter((r) => r.type === "lesson");
							if (lessons.length > 0) {
								const lrow = db
									.prepare("SELECT content FROM memories WHERE id = ?")
									.get(lessons[0].id);
								const m =
									lrow &&
									String(lrow.content || "").match(/行为位[:：]\s*(.+)$/m);
								if (slimH) slimH.fuTop = "⚠顶注〔" + slimSeg(lessons[0].title, 40) + "〕" + (m ? slimSeg(m[1], 120) : "见该条正文——引用前先读全文"); // 注入瘦身车 W4（保守 C·§3.11 第 19 条）：顶注收进「附：」段
								else
								text +=
									(text ? "\n" : "") +
									"⚠ 开工教训顶注（" +
									lessons[0].title.slice(0, 40) +
									"）：" +
									(m ? m[1].slice(0, 120) : "见该条正文——引用前先读全文");
							}
						} catch {}
						// ── step：上窗衔接段（神经记忆「上窗口衔接」吸收·08:57 maintainer）——跨窗连续性叙事：
						//    本空间最新一条收官账接续行（不受 48h 鲜度闸·哪怕三天前的窗也衔接）·帽一行·已在召回席则不重挂。
						try {
							const stitchSpace = callerKey ? sessionOrgan(callerKey) : null; // 8-31 移植族修复②：原引用 injectCallerSpace 系 L827 注入段回调局部变量·本 assemble 钩子不可达（幻引用致本段 100% 死+每轮 warn）——本作用域自算·口径同源（sessionOrgan 第一源 identity.cwd→organFromPath·L1459 唯一兜底路 callerKey='' 时 null 防错源）
							if (stitchSpace) {
								const pickedIds = new Set(picked.map((r) => r.id));
								if (slimH) slimXdSeen(slimH, pickedIds); // 注入瘦身车 W4 第 8 笔（§3.6-2）：XDEDUP 开时衔接与状态可见 id 互斥（未开则 xdVis 不在、即返）
								const cut2h = new Date(
									Date.now() - 2 * 3600 * 1000 + 8 * 3600 * 1000,
								); // 审计 I3：2h 闸——排除本窗自产（「上窗」语义保真·原设计=上一窗最后会话）
								const cutIso =
									cut2h.getUTCFullYear() +
									"-" +
									String(cut2h.getUTCMonth() + 1).padStart(2, "0") +
									"-" +
									String(cut2h.getUTCDate()).padStart(2, "0") +
									"T" +
									String(cut2h.getUTCHours()).padStart(2, "0") +
									":" +
									String(cut2h.getUTCMinutes()).padStart(2, "0") +
									"+08:00";
								const last = db
									.prepare(
										// 顶注选条缺陷修（09-16 brain执行令§四-1＋交办§三实证）：排除已被订正/闭环的条
										//   （有入度 patches/solved_by 边 ⇒ 其结论已被后条取代·如 #11349 被 #11427 patches 后仍被推送数月）
										//   ——与批 C「essential 常驻席被过时条占据」同族；排除后选条自然落到订正条（更新·无 patches 入度）
										"SELECT id, title, ts, source FROM memories m WHERE space = ? AND type IN ('fact','decision') AND status = 'active' AND ts <= ? AND id NOT IN (SELECT dst FROM memories_edges WHERE edge_type IN ('patches','solved_by') AND invalid_at IS NULL) AND " +
											CUR_SQL + // 车二-9 块一 1-b（10-25 落码）：衔接段接现行谓词——到点／被取代／待复核条不再进「附：🪡衔接」（原只排 patches、solved_by 入度·G1-4）；SELECT 补 source 供 isSystemRow
											" ORDER BY id DESC LIMIT 1",
									)
									.get(stitchSpace, cutIso, nowIso());
								if (last && !isSystemRow(last) && !pickedIds.has(last.id)) { // 车二-9 块一 1-b：系统条不进「附：🪡衔接」
									if (slimH) {
										// 注入瘦身车 W4（保守 C·§3.11 第 18、19 条）：衔接收进「附：」段；带出的 id 计已显（pickedIds 块内不可达，另记句柄）
										slimH.fuStitch = "🪡衔接〔" + slimSeg(last.title, 60) + "（" + String(last.ts).slice(5, 10) + "）〕";
										slimH.fuIds.push(Number(last.id));
									} else
									text +=
										(text ? "\n" : "## 记忆库自动召回（⚠ 参考级）\n") +
										"🪡 上窗衔接：" +
										String(last.title).slice(0, 60) +
										"（" +
										String(last.ts).slice(5, 10) +
										"）——本空间前一窗收官账，接续前先 timeline 核最新";
								}
							}
						} catch (e2) {
							ctx.logger?.warn?.(
								"[living-memory] 上窗衔接段失败: " +
									String((e2 && e2.message) || e2).slice(0, 60),
							);
						} // 审计 I2：⑬ 回溯——空 catch 当日纪律当日不豁免
						// ── blindspot 盲区提醒（nerve-wake 同款·design-approved「开始吸收」）：召回有命中≠覆盖全——一行提示主动补维度（邻近域/反例/时序边界）·「注入变指针」精神·帽一行不膨胀。
						if (!slimH) // 注入瘦身车 W4（保守 C·§3.3）：盲区一句并入召回头（SLIM_RC_HEAD），不再独立成行
						try {
							if (text)
								text +=
									"\n🔍 盲区自检：以上为词面命中——决策前问自己「与主题相关但未检索的维度」（邻近域/反例/更早先例）·不确定即再发 search 换词";
						} catch {}
						// ── 前瞻召回 Lv2（09-16 三案·maintainer序④·最小版）：任务关键词同族 lesson 定向二查 ──
						//    治四真缺口③「前瞻召回不存在」——「发车前查教训」从自觉变推送；只提示不拦·counter两周（Lv3 拦截级另议）。
						//    与泛召回互补：泛召回 lesson 可能被 fact 挤出席（AUTO_RECALL_MAX 帽）或整路零命中——
						//    本路定向 lesson 面保证同族教训必达眼前（零命中时独立成注入）。
						//    复用主路 match/callerSpace/tokens（零二次分词）·同款 token 交叠门（≥2∨专名单桥·泛词免疫同哲学）。
						//    回退 LEGION_FORELOOK_OFF；观察键 forelookLessons/forelookErrors（两周counter·Lv3 依据）。
						try {
							if (match && !process.env.LEGION_FORELOOK_OFF) {
								const lsnRows = (
									callerSpace
										? db.prepare(
												ftsBase +
													" AND m.space IN (?, 'global') AND m.type = 'lesson' AND " + CUR_SQL + " ORDER BY rank LIMIT 12", // 车二-9 块一 1-b：前瞻 lesson 接现行谓词
											)
										: db.prepare(
												ftsBase +
													" AND m.type = 'lesson' AND " + CUR_SQL + " ORDER BY rank LIMIT 12",
											)
								).all(...(callerSpace ? [match, callerSpace, vtNow] : [match, vtNow])); // 落码车硬排（占位符序随分支·实参尾追）
								const stemA3 = (w) => (/^[a-z]+$/.test(w) ? porter2Stem(w) : w);
								const seenA3 = new Set(picked.map((r) => r.id)); // 泛召回已占席去重（pickedIds 系上窗衔接块内变量不可达——自建同源集·假零族防线）
								if (slimH) slimXdSeen(slimH, seenA3); // 注入瘦身车 W4 第 8 笔（§3.6-2、§3.11 第 20 条）：XDEDUP 开时前瞻互斥集扩大（上行不改·其后追加）
								const lsn = lsnRows
									.filter((r) => {
										const tt = new Set(
											[...tokenize(String(r.title))].map(stemA3),
										);
										let hits = 0,
											pn = false;
										for (const tk of tokens) {
											const s = stemA3(tk);
											if (tt.has(s)) {
												hits++;
												if (
													/^[\x21-\x7e]{2,}$/.test(tk) &&
													!/^\d+$/.test(tk) &&
													(!c2Pn || c2PnOk(tk, c2Path)) // wave 车二-3：前瞻西文单桥同样排除停用表、路径段，长度 ≥3
												)
													pn = true; // 批⑳ L 路残尾：纯数字不单桥过门（与批⑰ B5① 同款·治 ≥5 位 id/日期灌入——gateKeywords 只滤 1-4 位）
											}
										}
										return hits >= 2 || pn;
									})
									.filter((r) => !seenA3.has(r.id))
									.slice(0, 3);
								if (slimH) lsn.splice(2); // 注入瘦身车 W4（保守 C·§3.5、§3.11 第 20 条）：前瞻显示 ≤2 条（选条不动·只截显示）；下方 forelook_shown_total 照按实显条数加
								if (lsn.length > 0) {
									const wasEmptyA3 = !text; // A3-I1（独立审计改进）：独立注入场景记录——注入后补盲区自检行（形态与泛召回路一致）
									if (slimH) {
										// 注入瘦身车 W4（保守 C·§3.11 第 20 条）：前瞻收进「附：」段（形如「⚔前瞻〔#id〕标题30｜〔#id〕标题30」）；带出的 id 计已显
										slimH.fuFore = "⚔前瞻" + lsn.map((r) => "〔#" + r.id + "〕" + slimSeg(String(r.title).replace(/[\r\n{}]/g, " "), 30)).join("｜");
										for (const r of lsn) slimH.fuIds.push(Number(r.id));
									} else {
									text +=
										(text
											? "\n"
											: "## 记忆库自动召回（⚠ 参考级）\n") +
										"⚔ 同族教训前瞻（发车前先查）：" +
										lsn
											.map(
												(r) =>
													"[#" +
													r.id +
													"] " +
													String(r.title)
														.replace(/[\r\n{}]/g, " ")
														.slice(0, 30),
											)
											.join("｜");
									if (wasEmptyA3)
										text +=
											"\n🔍 盲区自检：以上为词面命中——决策前问自己「与主题相关但未检索的维度」（邻近域/反例/更早先例）·不确定即再发 search 换词";
									} // 注入瘦身车 W4：前瞻「附：」分支wrap-up
									stats.forelookLessons =
										(stats.forelookLessons || 0) + lsn.length; // counter（两周后议 Lv3 拦截级）
									try {
										db.prepare(
											"INSERT INTO organ_meta (k, v) VALUES ('forelook_shown_total', ?) ON CONFLICT(k) DO UPDATE SET v = CAST(CAST(v AS INTEGER) + ? AS TEXT)",
										).run(String(lsn.length), String(lsn.length)); // ZD-补1 修（补审三 commit·P3）：原两语句读-改-写纯覆盖——commit 自称「nudge_shown_total 同构」失实（bumpNudgeTotal L5739 实为单语句 UPSERT 免竞态）。改真单语句 UPSERT（跨进程 lost-update 窗闭）。A3-I3 本体（两周counter跨重启累计）不变
									} catch (eFI) {
										stats.forelookErrors =
											(stats.forelookErrors || 0) + 1; // ⑬：持久化失败不破主注入
									}
								}
							}
						} catch (eA3) {
							stats.forelookErrors = (stats.forelookErrors || 0) + 1; // ⑬ 出口
							ctx.logger?.warn?.(
								"[living-memory] forelook failed (#" +
									stats.forelookErrors +
									"): " +
									String((eA3 && eA3.message) || eA3).slice(0, 60),
							); // A3-I2（独立审计改进）：同函数 eSeat/eOrdSeat/e2 均有 warn——同构补齐
						}
						if (slimH) {
							// 注入瘦身车 W4（保守 C·§3.11 第 18、19 条）：前瞻段之后定稿——新头＋≤SHOW 条＋「附：」行，定已显／被截并持久计数；定稿抛错留原文
							const slimTxt = slimRecallFinish(slimH, picked, db, assembly);
							if (slimTxt !== null) text = slimTxt;
						}
						autoRecallCache.set(key, { query, at: Date.now(), text });
						if (slimH) {
							SLIM_RC_SHOWN.set(autoRecallCache.get(key), slimH.shown.slice()); // 缓存命中时本轮记录照带已显 id（§3.11 第 18 条末句）
							SLIM_RC_PARTS.set(autoRecallCache.get(key), slimH.rcParts || null); // 第 9 笔：缓存条挂召回分件
						}
						if (text) legionContributeAutoRecall(assembly, text, slimH);
					} catch (eAR) {
						if (nextCalledAR) throw eAR; // P2 fix：next 已调则此 reject 系下游失败——传播不吞（防尾部双调）
						if (slimH) slimH.err = true; // 注入瘦身车 W4：异常步不存本轮召回（下一步重算·不把一次失败冻结一整轮）
						stats.autorecallErrors = (stats.autorecallErrors || 0) + 1; // P0修（09-03 audit）：召回整链异常计数透出（原静默假绿）
						if (Date.now() - (stats._arWarnAt || 0) > 60000) {
							stats._arWarnAt = Date.now();
							ctx.logger?.warn?.(
								"[living-memory] autorecall chain failed (#" +
									stats.autorecallErrors +
									"): " +
									String(eAR).slice(0, 60),
							);
						}
					}
					if (slimH && !nextCalledAR) slimSettle(slimH); // 注入瘦身车 W4（§3.11 第 17、21 条）：结算点②——正常路、零命中、异常支（尾部唯一 next 之前）
					if (!nextCalledAR) return await next(); // P2 fix：唯一调用点
				});
		} catch (eHk2) {
			// 批⑲ P1（J3-2·🔴·第二钩）：同上——注入路注册期异常可观测
			stats.autorecallHookDead = (stats.autorecallHookDead || 0) + 1;
			ctx.logger?.warn?.(
				`[living-memory] autorecall hook dead (#${stats.autorecallHookDead}·system-prompt/assemble): ${String(eHk2).slice(0, 80)}`,
			);
		}

}

module.exports = { setupInject, setupAutoRecall, slimOnSessionEvent, breakFreeze, hintArm }; // hintArm：wave 车二-2 分臂（index.cjs turn/end 结算与 drill 复算） // slimOnSessionEvent：注入瘦身车 W4（10-11）index.cjs bd 段计数入口；breakFreeze：破冻结接口（盲审 TB-01·无调用者）
