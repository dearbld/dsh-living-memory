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

const { DatabaseSync } = require("node:sqlite");
const path = require("node:path");
const os = require("node:os");
const crypto = require("node:crypto");
const fs2 = require("node:fs"); // nightly patrol读 session_projcache（handover note检测）；快照通道已退役不再写文件
const { execFileSync } = require("node:child_process"); // nightly patrol在线备份（sqlite3 .backup）

// ── step：沙箱开关。MEMORY_DB_PATH 设了→工具面读写指向该路径；未设→正库。
//    全链（含nightly patrol/提炼/注入/面板）都走 DB_PATH——演练可整体重定向（第三轮审计修 L16 旧注释漂移：原称「内部管道永远走正库」不实）；
//    文件写面（快照/G2/镜像/brain inbox）随 DRILL_SANDBOX 总闸改道 /tmp，生产路径零接触。单一解析点，禁散落硬编码。──
const DEFAULT_DB_PATH = path.join(
	os.homedir(), ".dsh", "dsh-living-memory", "memory.sqlite3",
);
const DB_PATH = process.env.MEMORY_DB_PATH || DEFAULT_DB_PATH;
// ── 第三轮审计pending ruling③（design-approved）：DRILL_SANDBOX 总闸——沙箱库演练时文件写面缺省路径统一改道 /tmp ──
//    治 G2 穿孔案（09-08 knife24 两跑覆写生产source of record+归档·9 月真卷 4023 条亡佚）：旋钮逐段手加无总闸=机制生命周期错位族根因。
//    原则：drill 全链可测（写面不 skip 只改道）·生产零污染（沙箱库⇒文件全落 /tmp）；显式 LEGION_*_DIR 旋钮恒优先（drill 自定沙箱路径照走）。
const DRILL_SANDBOX = DB_PATH !== DEFAULT_DB_PATH;
// ── 数据目录自举（2026-09-07 冷启动caught in drill P0·发布阻塞级）──
//    病灶：默认库路径的**父目录在全新机器上不存在** → new DatabaseSync 抛
//    "unable to open database file" → apply 提前 return → memory / memory_write /
//    memory_extract 三个工具全部静默不注册（插件装了等于没装·日志只有一条 warn）。
//    this space内源侧目录恒在位，故该缺陷只在新装用户面上暴露——0.1.0~0.1.3 公开版
//    自 B1 路径改写（迁至 ~/.dsh/dsh-living-memory/）起即带此雷，四版全中。
//    修：解析出库路径后立即 mkdir -p（幂等·失败不阻断，让下游按原路报错）。
try {
	fs2.mkdirSync(path.dirname(DB_PATH), { recursive: true });
} catch {} // 无权限/只读盘 → 交给下面的 DB open 报真因（不在此处吞掉可诊断性）
// ── step：maintenance window闸（文件存在性检查——挂牌即跳过nightly patrol/提炼，摘牌恢复）──
//    演练安全：LEGION_SURGERY_FLAG_PATH / LEGION_SNAPSHOT_DIR 可 env 覆盖（沙箱演练强制nightly patrol时不触正库 flag/真实快照；生产不设=正路径）。
const SURGERY_FLAG =
	process.env.LEGION_SURGERY_FLAG_PATH ||
	path.join(os.homedir(), ".dsh", "dsh-living-memory", "surgery.flag");
// ── step：nightly patrol日快照目录（保留最近 7 天）──
const SNAPSHOT_DIR =
	process.env.LEGION_SNAPSHOT_DIR ||
	(DRILL_SANDBOX
		? path.join(os.tmpdir(), "living-drill-snapshots") // pending ruling③总闸：沙箱库演练缺省改道 /tmp（防沙箱快照同名覆写生产目录+轮转互删）
		: path.join(os.homedir(), ".dsh", "dsh-living-memory", "snapshots"));
const SNAPSHOT_KEEP_DAYS = 7;
const EXTRACT_INTERVAL_MS = 5 * 60 * 1000; // 自动提炼限频：5 分钟
const PATROL_START_HOUR = 2; // nightly patrol窗口 02:00–06:00(design note)
const PATROL_END_HOUR = 6;
const PATROL_CHECK_MS = 30 * 60 * 1000; // 每 30 分钟检查一次是否入窗
// ── 吸收项（wave·design-approved「抽取模型定向 V4-Flash」）──
// 官方 changelog 实证 ID：deepseek-v4-flash（2026-08-13 文本档上线·同端点同 key 零新凭据）。
// 原 deepseek-chat 通用档 → Flash 低成本档；env LEGION_EXTRACT_MODEL 可覆（观察期灵活回退）。
const EXTRACT_MODEL = process.env.LEGION_EXTRACT_MODEL || "deepseek-v4-flash";
// ── issue 修②（design-approved）：单次提炼请求应用级全局超时（ms·env 可调）──
//    防慢速响应长持 extracting 互斥锁（Undici 默认 300s×2 只是兜底·四 attempts 最坏 20 分钟）；
//    默认 120s 下四 attempts 最坏 8 分钟封顶。超时 abort 走既有 fetch fail → skip 路径。
const EXTRACT_TIMEOUT_MS =
	Number(process.env.LEGION_EXTRACT_TIMEOUT_MS) || 120000;
const SPOKEN_MODEL = process.env.LEGION_SPOKEN_MODEL || "deepseek-chat"; // 09-03 裁③（maintainer B 案）：spoken-prefix 模型独立旋钮——修「EXTRACT_MODEL 不传导」契约破洞·缺省现状零行为变·两链独立调优（质量directive按需分配）
// ── AUDN 刀②（09-08 approved·braintask brief 20:1x）：预裁模型独立旋钮——治「枚举外/矛盾」判据质量降级 ──
//    与 EXTRACT_MODEL 脱钩（提炼主链 flash 不动·量大成本敏感；预裁 40 案/巡≈50-100K·谷时 2-4 元可忽略）。
//    缺省=deepseek-v4-pro（maintainer升档方向·A/B 对拍为验证环非前置闸；不达标 env 回退 flash 零码改）。
const PREVERDICT_MODEL =
	process.env.LEGION_PREVERDICT_MODEL || "deepseek-v4-pro";
// ── item1：查询侧 instruct（design-approved「同意」·官方step漏件补件）──
//    探针实证（09-07 /tmp/probe-instruct.cjs 三臂·qwen3.7-text-embedding）：兼容模式 text_type 被
//    忽略（cos=1.000000 向量未变）·instruct 生效（cos=0.951 显著偏移）——故只落 instruct 不落
//    text_type（原生端点才支持·不为此换端点改请求体）。E5「query:」前缀官方等价·qwen3.7 指令
//    遵循较 v4 +16.4%（官方文档）。文档侧（chunks 补嵌/nightly patrol回填/写时）一律不传——查询/文档
//    不对称正形态。A/B：LEGION_INSTRUCT_OFF 回退·LEGION_INSTRUCT_TEXT 指令覆盖（扫描用）。
const INSTRUCT_QUERY =
	process.env.LEGION_INSTRUCT_TEXT ||
	"Given a user's colloquial query in Chinese, retrieve the most relevant long-term memory entries";
const INSTRUCT_OFF = !!process.env.LEGION_INSTRUCT_OFF;
const EXTRACT_API = "https://api.deepseek.com/chat/completions";
// ── Guard rules are data, not code ───────────────────────────────────────────
// Every content-safety rule this plugin enforces lives in a JSON data file, so the
// defense can be tuned without patching code — and so a published package never has
// to ship a site-specific blocklist.
//
// Load order (first readable file wins; every step degrades instead of crashing):
//   1. ~/.dsh/dsh-living-memory/guard-rules.json  — your machine's rules (chmod 600)
//   2. <package>/guard-rules.default.json         — shipped generic default set
//   3. GUARD_FLOOR below                          — built-in last resort, so the gate
//                                                   can never be left wide open
// Override the path with LEGION_GUARD_RULES_PATH (handy for tests and sandboxes).
//
// Merge semantics: whichever file wins is used AS A WHOLE (no per-rule union — a
// half-applied ruleset is much harder to debug than a replaced one). GUARD_FLOOR's
// reject rules are always appended, de-duplicated by (source, flags): a data file
// cannot switch the last-resort gate off.
const GUARD_FLOOR = {
	reject: [
		{
			re: "(?<![A-Za-z0-9])(?:sk|pk|ak|ark)-[A-Za-z0-9._\\-]{16,}",
			flags: "i",
			reason: "credential-like",
		},
		{ re: "[A-Za-z0-9+/=]{80,}", flags: "", reason: "encoded-payload" },
	],
	sensitive: "api[_-]?key|password|secret|token|bearer",
	singleBodyWarn: [],
};
const GUARD_RULES_PATH =
	process.env.LEGION_GUARD_RULES_PATH ||
	path.join(os.homedir(), ".dsh", "dsh-living-memory", "guard-rules.json");
const GUARD_DEFAULTS_PATH = path.join(__dirname, "guard-rules.default.json");
function guardReject(list) {
	const out = [];
	for (const it of Array.isArray(list) ? list : []) {
		const s = typeof it === "string" ? it : it && it.re;
		if (typeof s !== "string" || !s) continue;
		try {
			out.push({
				re: new RegExp(s, (typeof it === "object" && it && it.flags) || ""),
				reason: String(
					(typeof it === "object" && it && it.reason) || "credential-like",
				),
			});
		} catch {} // one invalid regex skips that rule only; the rest still load
	}
	return out;
}
function guardWordList(list) {
	const out = [];
	for (const it of Array.isArray(list) ? list : []) {
		const s = typeof it === "string" ? it : it.re;
		if (typeof s !== "string" || !s) continue;
		try {
			out.push(new RegExp(s, (typeof it === "object" && it && it.flags) || ""));
		} catch {}
	}
	return out;
}
function guardDedupe(list) {
	const seen = new Set();
	return list.filter((p) => {
		const k = p.re.source + "\u0000" + p.re.flags;
		if (seen.has(k)) return false;
		seen.add(k);
		return true;
	});
}
function loadGuardRules() {
	for (const p of [GUARD_RULES_PATH, GUARD_DEFAULTS_PATH]) {
		try {
			const parsed = JSON.parse(fs2.readFileSync(p, "utf8"));
			if (!parsed || typeof parsed !== "object") continue;
			const sSrc =
				typeof parsed.sensitive === "string" && parsed.sensitive.trim()
					? parsed.sensitive
					: GUARD_FLOOR.sensitive;
			let sens;
			try {
				sens = new RegExp(sSrc, "i");
			} catch {
				sens = new RegExp(GUARD_FLOOR.sensitive, "i");
			}
			return {
				reject: guardDedupe(
					guardReject(parsed.reject).concat(guardReject(GUARD_FLOOR.reject)),
				),
				sensitive: sens,
				singleBodyWarn: guardWordList(parsed.singleBodyWarn),
				source: p,
			};
		} catch {} // missing file / corrupt JSON → try the next layer
	}
	return {
		reject: guardReject(GUARD_FLOOR.reject),
		sensitive: new RegExp(GUARD_FLOOR.sensitive, "i"),
		singleBodyWarn: [],
		source: "floor",
	};
}
const GUARD = loadGuardRules();
// ── 行为判据外置（三级链·2026-09-12 一车）：与 GUARD 同法，但**逐键单值语义** ──────
//    加载序（先到先用·逐级兜底·任一环节失败不崩）：
//      ① 用户目录 ~/.dsh/memory-criteria-rules.json —— 本机覆盖件（可省·建议 0600）
//      ② 包内 criteria-rules.default.json —— 随包默认集（公开版＝公开词面／源仓＝内部词面）
//      ③ CRIT_FLOOR 内置保底 —— 只含**最通用形态**·防两级皆缺时该判据完全失效
//    合并语义（**逐键·与 GUARD_FLOOR.reject 的「恒追加」不同**）：命中即**整份取用**该键；
//      floor 仅在该键两级皆缺/损坏时兜底——本组是**行为判据**（非安全判据），floor 保的是
//      「功能可用」而不是「防线不破」，故**不设「关不掉」语义**（同 guard 的 sensitive 键）。
//    未命中任一键时的后果（如实记录·README 同载）：该键退化为 CRIT_FLOOR 通用形态。
//    env 覆盖：LEGION_CRITERIA_RULES_PATH（演练/沙箱指向用·同 LEGION_GUARD_RULES_PATH）。
const CRITERIA_RULES_PATH = process.env.LEGION_CRITERIA_RULES_PATH ||
	path.join(os.homedir(), ".dsh", "dsh-living-memory", "criteria-rules.json");
const CRITERIA_DEFAULTS_PATH = path.join(__dirname, "criteria-rules.default.json");
const CRIT_FLOOR = {
	metaExclusion: {
		re: "\\b(?:audit|checklist|inventory|summary|report|review|retrospective|handover|handoff|proposal|pending)\\b|盘点|清单|汇总|方案|草案|审计|自检",
		flags: "i",
	},
	intentGate: {
		re: "最近|今天|昨天|上周|上次|刚才|进度|状态|怎么|如何|什么|是否|继续|待办|进行|在办|完成|验收|查|找|搜|看|列|对比|差异|审计|检索|写入|召回|\\b(?:yesterday|today|recently|latest|last\\s+(?:week|time)|status|progress|pending|todo|done|audit|review|summary|how|what|where|which|who|why|continue|resume|find|search|look|list|show|check|compare|track|memory|recall|version|backup|snapshot|patrol|nightly|session|panel)\\b",
		flags: "i",
	},
	stashHint: { re: "候|待.{0,2}(批|裁|验|收|复|提)|提醒", flags: "" },
	taskCompleted: { re: "task_completed", flags: "" },
	recallStopwords: { re: "已?(完成|办毕|办结|完结|更新|记录|说明|情况|事项|内容|工作|小结|汇总|总结|备忘|备注|处理|日常|杂项|其他|临时|测试|验证|整理|自查)", flags: "g" },
	inboxPath: { re: "[/\\\\]inbox[/\\\\]|[/\\\\]inbox[/\\\\]", flags: "" },
	exemptShort: { re: "^(继续|同意|好|嗯|行|可以|OK|ok|多谢|谢谢|收到|是|对)\\s*[。.!！~～]?$", flags: "" },
	alertFamily: { re: "(?!)", flags: "" }, // AUDIT-FIX（A-D2）：原 "$^" 实测 test("")===true ⇒ 反向误判空标题
	sysAutoPrefix: { re: "(?!)", flags: "" }, // AUDIT-FIX（A-D2）同上
	// AUDIT-FIX-20260912（修复车·审计 C 路第 1 项普查漏项）：两处**输入匹配型行为判据**原以源码
	//   字面量硬编（OPS_HOOK_RE 注入带钩词表 / CRIT_RE 冲突升级词）——D 链对二者的机改（词表整体
	//   英文化 + flags 加 i）此前**未披露**，且下一次净化仍会被再改。此处并入三级链（实测键序：opsHook 第 10、sentryCritical 第 11·共 11 键），
	//   与其余 9 键同口径：本 FLOOR 只写**通用形态**（英文·零内部词 ⇒ 净化链零命中），
	//   内部词面由 ~/.dsh 数据文件与包内 default 供给。
	opsHook: {
		re: "in progress|in-progress|WIP|pending|handover|follow[- ]?up|watching|blocked",
		flags: "i",
	},
	sentryCritical: {
		re: "policy|charter|constitution|bylaw|directive",
		flags: "i",
	},
};
// AUDIT-FIX-20260912（A-D1/A-I1·独立审计 A 路）：
//   ①**flags 白名单**——`.test()`/`.match()` 面消费的键一律剥掉 `g`/`y`（带 lastIndex 状态 ⇒ 连续调用
//     交替 true/false·实测复现 `true,false,true,false`）；只有**消费面为 `.replace()`** 的键（replace 自行
//     重置 lastIndex）才放行 g。剥掉的 flag **不静默丢弃**：计入 `CRIT_FLAGS_DROPPED`（模块级收集·debug 可读）。
//   ②`re` **不再 trim**：`^ ` 这类以空白为语义的正则会被 trim 放大匹配面；空白仅用于「是否为空」判定。
const CRIT_G_OK = { recallStopwords: 1 }; // flags 含 g 且**消费面安全**的键白名单（逐键声明·可 grep）
const CRIT_FLAGS_DROPPED = []; // 被剥 flag 的键名（审计可见性出口）
function critCompile(entry, key) {
	if (!entry || typeof entry !== "object") return null;
	const src = typeof entry.re === "string" ? entry.re : "";
	if (!src.trim()) return null;
	let fl = typeof entry.flags === "string" ? entry.flags : "";
	if (/[gy]/.test(fl) && !CRIT_G_OK[key]) {
		fl = fl.replace(/[gy]/g, "");
		CRIT_FLAGS_DROPPED.push(key);
	}
	try {
		return new RegExp(src, fl);
	} catch {
		return null; // 单键非法 ⇒ 回退下一级（不整份报废·与 guardDedupe 宽容口径同族）
	}
}
function loadCriteriaRules() {
	// **逐键三级回退**：键 k 取「用户件 → 包内 default → CRIT_FLOOR」中**第一个提供该键的层**；
	//   某层缺某键**不影响其余键**（「逐键整份取用」＝该键取了就不再往下找，而非整层淘汰）。
	//   与 GUARD_FLOOR.reject 的「恒追加」不同：本组是行为判据，floor 保的是功能可用而非防线不破。
	const layers = [];
	const _unknownAll = []; // AUDIT-FIX（A-D5）：数据文件里 CRIT_FLOOR 之外的键（永不被取用·必须可见）
	for (const p of [CRITERIA_RULES_PATH, CRITERIA_DEFAULTS_PATH]) {
		try {
			const j = JSON.parse(fs2.readFileSync(p, "utf8"));
			if (j && typeof j === "object") {
				layers.push([p, j]);
				// AUDIT-FIX-20260912（A-D5）：**未知键可见化**——键集合定义源单一（只遍历 CRIT_FLOOR），
				//   数据文件写了别的键 ⇒ 该键**永不被取用且零信号**（审计实测：加第 10 键无 warn 无断言）。
				try {
					const _unknown = Object.keys(j).filter(
						// A″D6 修（2026-09-12）：原用 `x in CRIT_FLOOR` —— **走原型链** ⇒ `toString`/
						//   `constructor`/`__proto__` 等原型名键被误判为「已在 FLOOR 中」⇒ **零信号**（既不
						//   报未知键也不被取用）。改为 `Object.hasOwn`（只认自身属性）。
						(x) => x !== "_comment" && !Object.hasOwn(CRIT_FLOOR, x),
					);
					if (_unknown.length) {
						_unknownAll.push(..._unknown.map((x) => x + "@" + String(p).split("/").pop()));
					}
				} catch {}
			}
		} catch {} // 文件缺失/JSON 损坏 → 该层整层跳过（不崩·不牵连其余层与其余键）
	}
	const out = {};
	const from = {};
	for (const k of Object.keys(CRIT_FLOOR)) {
		let re = null;
		for (const [p, j] of layers) {
			const r = critCompile(j[k], k);
			if (r) {
				re = r;
				from[k] = p;
				break;
			}
		}
		if (!re) {
			re = critCompile(CRIT_FLOOR[k], k) || new RegExp("(?!)"); // AUDIT-FIX（A-D2）：(?!) 才是真·永不匹配
			from[k] = "floor";
		}
		out[k] = re;
	}
	out.from = from; // 逐键来源（诊断/drill 读）
	out.unknownKeys = _unknownAll; // AUDIT-FIX（A-D5）：数据文件里被忽略的键（空数组=无）
	out.flagsDropped = CRIT_FLAGS_DROPPED; // AUDIT-FIX（A-D1）：被剥掉 g/y 的键
	if (_unknownAll.length) {
		try {
			console.warn(
				"[living-memory] criteria rules: 未知键被忽略（须先加进 CRIT_FLOOR 才生效）: " +
					_unknownAll.join(","),
			);
		} catch {}
	}
	out.source = from.metaExclusion || "floor"; // 兼容旧口径：主来源
	return out;
}
const CRIT = loadCriteriaRules();
// ── Tokenizer dictionary and entity kinds are data, in two layers ───────────────
// jieba's built-in dictionary splits domain-specific compound words, which hurts
// recall (a three-character term becomes two fragments and stops matching). Proper
// nouns are therefore loaded from dictionary files, in two layers:
//   1. <package>/dict-custom.json                  — generic technical terms
//   2. ~/.dsh/dsh-living-memory/dict-extra.json    — your own proper nouns
// Words are de-duplicated (layer 1 wins, its frequency line is kept); `kinds` are
// merged with layer 2 overriding layer 1. Override the second path with
// LEGION_DICT_EXTRA_PATH. A missing or corrupt layer degrades silently.
//
// `kinds` also drives knowledge-graph entity typing (mech / organ / doc): only typed
// entities are eligible for entity-hop recall, so adding kinds for your own
// vocabulary is what makes that path light up.
const DICT_EXTRA_PATH =
	process.env.LEGION_DICT_EXTRA_PATH ||
	path.join(os.homedir(), ".dsh", "dsh-living-memory", "dict-extra.json");
function loadDictData() {
	const out = { lines: [], words: [], kinds: {} };
	const seen = new Set();
	for (const p of [path.join(__dirname, "dict-custom.json"), DICT_EXTRA_PATH]) {
		try {
			const d = JSON.parse(fs2.readFileSync(p, "utf8"));
			if (!d || typeof d !== "object") continue;
			if (typeof d.words === "string")
				for (const line of d.words.split("\n")) {
					const t = line.trim();
					if (!t) continue;
					const w = t.split(/\s+/)[0];
					if (!w || seen.has(w)) continue; // de-dupe across layers (layer 1 wins; its frequency line is kept)
					seen.add(w);
					out.lines.push(t);
					out.words.push(w);
				}
			if (d.kinds && typeof d.kinds === "object")
				for (const [k, v] of Object.entries(d.kinds))
					if (typeof v === "string" && v) out.kinds[k] = v; // layer 2 overrides layer 1
		} catch {} // a missing/corrupt layer degrades to the other one (never throws)
	}
	return out;
}
const DICT = loadDictData();

// ── wave KNOWN_FIXES 修正表（design-approved四车）：包内 known-fixes.json——
//    已知幻觉/截断形态→修正字典·产边 prompt 伴生消费（LLM 产时即避）。失败降级空表。
const KNOWN_FIXES = (() => {
	try {
		const d = JSON.parse(
			fs2.readFileSync(path.join(__dirname, "known-fixes.json"), "utf8"),
		);
		if (Array.isArray(d.fixes))
			return d.fixes.filter(
				(f) => f && typeof f.wrong === "string" && typeof f.right === "string",
			);
	} catch {}
	return [];
})();
const knownFixesHint = () =>
	process.env.LEGION_KNOWN_FIXES_OFF || KNOWN_FIXES.length === 0
		? ""
		: "\n已知修正（产出时照此避错·勿产出左列错误形态）：\n" +
			KNOWN_FIXES.map((f) => "- 「" + f.wrong + "」→「" + f.right + "」").join(
				"\n",
			); // wave：产边 prompt 伴生
// ── Sensitive-topic gate (second layer, downstream of the credential scan) ──────
// Entries matching GUARD.sensitive are kept OUT of auto-extraction and out of the
// auto-recall injection surface: the automatic layer only ever emits reference-grade
// facts, while anything you treat as policy-grade gets written deliberately by hand.
// The word list is data (see above), so the shipped default stays generic and you can
// extend it for your own domain.
const SENSITIVE_RE = GUARD.sensitive;

function sha1(text) {
	return crypto.createHash("sha1").update(text).digest("hex").slice(0, 12);
}

// ── Ingest security scan (shared; runs before anything reaches the database) ─────
// Blocks credential-looking strings, prompt-injection payloads and encoded blobs from
// being persisted — and therefore from being replayed into a later prompt. The rules
// themselves are data (GUARD.reject, see above).
//
// Note the (?<![A-Za-z0-9]) prefix boundary in the shipped credential rules: without
// it, harmless file names such as `…yml.bak-20260827` or `task-…` / `disk-…` get
// caught by the `ak-` / `sk-` substring and the gate starts eating legitimate notes.
// Keep the boundary if you add prefixes of your own.
const REJECT_PATTERNS = GUARD.reject;
// Single-subject wording gate. The word list is data (guard-rules.default.json);
// the public build ships an EMPTY list, so the gate is inert by construction and
// no call site needs patching. Add patterns to your own rules file to enable it.
const SINGLE_BODY_WARN = GUARD.singleBodyWarn;
const ROUTE_MAINT_RE = /launchd|supervisor|dsh-restart|spawn|ops/;
const ROUTE_INHIB_RE =
	/dispose|asset|receive|handover|sync|remind|pending|review/;
function softBodyWarn(title, content) {
	const text = String(title) + "\n" + String(content);
	const hits = [];
	for (const re of SINGLE_BODY_WARN) {
		const m = text.match(re);
		if (m) hits.push(m[0]);
	}
	return hits;
}
function securityCheck(title, content) {
	const text = String(title) + "\n" + String(content);
	for (const p of REJECT_PATTERNS) {
		if (p.re.test(text)) return { ok: false, reason: p.reason };
	}
	return { ok: true, reason: "" };
}
function stripUrls(text) {
	return String(text).replace(/[a-z][a-z0-9+.-]*:\/\/\S+/gi, (m) => {
		const schemeEnd = m.indexOf("://");
		const scheme = m.slice(0, schemeEnd).toLowerCase();
		// ── 2026-09-12 二车（裁 11·判据变已报批）：**保留 URL 短指纹** ──────────────
		//   病根：http/https 一律替换为 `[url]` ⇒ 两条「**仅 URL 不同**」的记忆落库后文本**全同**
		//         ⇒ 同文幂等闸误判同文并硬拦（URL 差异是有意义的实质差异·B 路 D4）。
		//   修法：替换串带 URL 的 md5 前 8 位（**不泄址·不可逆**）⇒ 同文闸可区分「仅 URL 不同」
		//         与「完全同文」；指纹随 content 落库 ⇒ 注入面/回流面可审计「此处曾有外链」。
		//   兼容：旧库存量条目为 `[url]` 形态 ⇒ 与新条目不再判同文（**漏拦不误拦·安全方向**）。
		const fp = crypto.createHash("md5").update(m).digest("hex").slice(0, 8);
		return scheme === "http" || scheme === "https"
			? "[url:" + fp + "]"
			: "[url:" + scheme + "//…:" + fp + "]";
	}); // step B-3：全套 scheme（ftp/ws/wss/file/sftp 等同脱）——非 http 族带 scheme 指纹留痕（可审计「曾有外链」不泄址）
}

function textBlock(value) {
	return [{ type: "text", text: JSON.stringify(value, null, 2) }];
}

// ── 吸收item（design-approved「逐项吸收」·mem0 truncateOutput 设计本地化）：工具输出硬帽 ──
//    200 行 / 50KB，截断附说明行引导模型收窄查询——本窗之前我方无任何输出帽（09-02 01:49 grep 实勘）。
const MAX_RENDER_LINES = 200;
const MAX_RENDER_BYTES = 50 * 1024;
function capRenderText(text) {
	const src = String(text);
	const totalBytes = Buffer.byteLength(src, "utf8");
	const totalLines = src.split("\n").length;
	if (totalBytes <= MAX_RENDER_BYTES && totalLines <= MAX_RENDER_LINES)
		return src;
	let out = src;
	if (totalBytes > MAX_RENDER_BYTES) {
		// P2 fix（09-03 audit）：字节截断回退至 UTF-8 字符边界——原 subarray 硬切可腰斩多字节字符产出 U+FFFD 污染注入面
		const buf = Buffer.from(out, "utf8");
		let cut = Math.min(MAX_RENDER_BYTES, buf.length);
		while (cut > 0 && (buf[cut] & 0xc0) === 0x80) cut--; // 切点是 continuation 字节（10xxxxxx）→ 回退至本序列首字节前
		// subarray(0,cut) 不含 buf[cut]：首字节/ASCII 一律保守弃——产出恒为完整 UTF-8 序列
		out = buf.subarray(0, cut).toString("utf8");
	}
	const ls = out.split("\n");
	if (ls.length > MAX_RENDER_LINES)
		out = ls.slice(0, MAX_RENDER_LINES).join("\n");
	const shownLines = out.split("\n").length;
	return (
		out +
		`\n[输出截断: showing ${shownLines} of ${totalLines} 行（${Buffer.byteLength(out, "utf8")} of ${totalBytes}B）——请收窄 query/limit 重查]`
	);
}

// ── 记忆召回可见化 render（design-approved「同意」·回显step②）：工具卡人类可读——maintainer在流式区直接看见召回情景 ──
function memoryRender(args, value) {
	try {
		const v = value || {};
		const a = args || {};
		const lines = [];
		if (a.action === "search" && Array.isArray(v.hits)) {
			lines.push(
				'🧠 记忆召回 · "' +
					String(a.query || "") +
					'"（' +
					v.total +
					" 候选·命中 " +
					v.hits.length +
					"·" +
					(v.via || "?") +
					(v.recallMode ? "·" + v.recallMode : "") +
					(v.asOf ? "·🕰时点 " + v.asOf : "") + // 时点快照可视化——search 头带时点锚（与 recallMode as-of 并读）
					"）",
			);
			for (const h of v.hits)
				lines.push(
					"  #" +
						h.id +
						" [" +
						h.type +
						"·" +
						String(h.ts || "").slice(5, 10) +
						"·" +
						h.space +
						"·s" +
						h.score +
						(h.ftsBase !== undefined
							? "·词" + h.ftsBase + "/语" + (h.vecPart || 0)
							: "") +
						(h.confidence !== undefined && h.confidence < 1 ? "·⚠外部源" : "") +
						(h.episodic ? "·♻原文可回流" : "") +
						(h.clusterMembers && h.clusterMembers.length ? "·🧩簇直达" : "") +
						(h.supersededBy !== undefined
							? "·↖已被 #" + h.supersededBy + " 取代"
							: "") + // step：被取代标（ render 消费）
						(h.doneMark ? "·" + h.doneMark : "") + // step：办结仅标（不降权）
						"] " +
						h.title +
						(h.content ? "\n      " + String(h.content).slice(0, 120) : ""),
				);
			if (v.axisHint) lines.push("  💡 " + v.axisHint);
			if (v.sagaHint) lines.push("  🧭 " + v.sagaHint); // 件5 面2：Saga 社区叙事脉络行
			if (v.cooccur)
				lines.push(
					"  🔗 共现加成 " +
						v.cooccur.boosted +
						"·伙伴注入 " +
						v.cooccur.injected,
				);
			if (v.entityHop) lines.push("  🔗 实体跳 " + v.entityHop.injected);
		} else if (a.action === "read_evolution") {
			if (v.evolutionContext)
				lines.push(
					"📖 source-document replay #" +
						v.id +
						"「" +
						v.title +
						"」（" +
						v.file +
						"·锚 L" +
						v.anchorLine +
						"·窗 " +
						v.windowRange.join("-") +
						"·" +
						v.entries +
						" 条目）\n" +
						String(v.evolutionContext).slice(0, 2400),
				);
			else lines.push("📖 source-document replay: " + (v.error || "unavailable"));
		} else if (a.action === "timeline" && Array.isArray(v.entries)) {
			lines.push(
				"🧠 时间线（" +
					v.total +
					" 条活跃·最新 " +
					v.entries.length +
					(v.asOf ? "·🕰时点 " + v.asOf : "") + // 时点快照可视化——timeline 头带时点锚（10-D asOf 透出）
					"）",
			);
			for (const e of v.entries)
				lines.push(
					"  #" +
						e.id +
						" [" +
						e.type +
						"·" +
						String(e.ts || "").slice(5, 10) +
						"·" +
						e.space +
						"] " +
						e.title,
				);
		} else if (a.action === "stats") {
			lines.push(
				"🧠 memory self-check: " +
					v.memoryCount +
					" 条（活跃 " +
					v.activeCount +
					"）·FTS " +
					(v.ftsReady ? "✓" : "✗") +
					(v.jieba ? "·jieba" : "") +
					"·nightly patrol史 " +
					v.patrolHistoryTotal +
					"·KG " +
					(v.triplesTotal || 0) +
					"·episodic 读 " +
					(v.episodicReadCount || 0) +
					"/拦 " +
					(v.episodicThrottled || 0),
			);
			if (v.pinboardTop)
				lines.push(
					"  📌 most-repeated lesson: " +
						v.pinboardTop +
						"",
				); // 三轮审计修：字段在数据层但 render 不显示=another space「应答无字段」真因
			if (v.callerIntrospect) {
				const ci = v.callerIntrospect;
				lines.push(
					"  🧭 space attribution: " +
						(!ci.organResolved
							? "⚠ " + (ci.verdict || "未解析")
							: String(ci.verdict || "ok").startsWith("WARN")
								? "⚠ " + ci.verdict
								: "本窗→" + ci.organResolved + " ✓") +
						(ci.organResolved
							? ci.driftCount > 0
								? "·漂移 " +
									ci.driftCount +
									" 条" +
									(ci.driftSample[0]
										? "（如 #" +
											ci.driftSample[0].id +
											"→" +
											ci.driftSample[0].space +
											"）"
										: "")
								: "·近 5 条 auto 零漂移"
							: ""),
				);
			}
			// ── ⑬ 计数透出段(design note)：数据层齐 render 漏=「计数不可见=半合规」自犯根治——非零即显·全零零扰动；drill=a companion script（断言 render 文本含键·只断言返回对象=假绿同防）──
			const _tl = [];
			const _t = (label, n) => {
				if (n) _tl.push(label + " " + n);
			};
			_t("压缩seen", v.compactionSeen);
			_t("桥bridged", v.compactionBridged);
			_t("chase跑", v.compactionChaseRuns);
			_t("chase错", v.compactionChaseErrors);
			_t("bridge错", v.compactionBridgeErrors);
			_t("a25锁", v.a25FlushErrors);
			_t("a25chase错", v.a25ChaseErrors);
			_t("entity错", v.entityExtractErrors);
			_t("g2归档错", v.g2ArchiveErrors);
			_t("mirrorWm错", v.mirrorWmErrors);
			_t("召回错", v.autorecallErrors);
			_t("专名救", v.autorecallPnRescue);
			_t("主题词救", v.autorecallKwRescue);
			_t("vec兜底", v.autorecallVecFallback); // 柱环4/梁4 预备：自动召回三路救援触发数 render 消费补齐（原 KwRescue/VecFallback 有 return 无 render=模型不可见·PnRescue 连 return 都缺·2026-09-11 同车补齐·⑬ 四点接线）
			_t("高压错", v.pressureAlertErrors);
			_t("重抽拦", v.extractDupSkipped);
			_t("同文拦", v.dupWriteBlocked); // 梁1b 同文幂等闸：memory_write 同文重复拒写数（09-11 kimi 21 连发案治本件②·⑬ 四点接线齐）
			_t("元清理", v.organMetaPruned);
			_t("元清理错", v.organMetaPruneErrors); // AUDIT-FIX（A-D3）：错误侧 render（原缺） // 三车：organ_meta
			_t("失效条", v.validToCount); // ⑬ 补（2026-09-11）：事件轴已到点失效条数原「有 return 无 render」＝半合规——本次 21 条污染沉底同车补，载入后 stats 面直接可见处置结果（验收证据位）
			// ── ⑬ option补齐（design-approved「只补错误/拦截类」·共 6 键·逐字核自增点与语义后纳入）──
			//    纳入口径＝「错误/拦截/告警」语义且有真自增点；**观测类不纳**（edgeYieldDeadKey「让位计数」/bothValidLinked/autoEdges/routedCount/spaceGatePassed/signalTriggered 等留盘点面）
			_t("安检拒", v.rejectedCount); // 提炼路 security reject 数（自增 L3773）
			_t("敏感跳", v.sensitiveSkipCount); // 敏感闸跳过数（自增 L3781）
			_t("闸拦", v.spaceGateBlocked); // option空间闸拦截数（spaceGateCounts.blocked）
			_t("双验错", v.bothValidErrors); // 刀A both-valid 处理失败数（自增 L5412）
			_t("术跳总", v.surgerySkipCount); // 挂牌期跳过总数（自增 L3592/L4962——与既有「术跳嵌」「术跳写」同族补齐）
			_t("语义冲", v.semanticConflictsFound); // 语义冲突发现数（L5250 赋值·告警面）
			_t("signal错", v.signalErrors);
			_t("buf恢复错", v.bufferRestoreErrors);
			_t("validated错", v.validatedErrors);
			_t("语义边错", v.semanticEdgeErrors);
			_t("压力读错", v.pressureReadErrors);
			_t("spokenWm错", v.spokenFillWmErrors);
			_t("extractEv错", v.extractEventsErrors);
			_t("闭环闸错", v.closureCheckErrors);
				_t("术跳嵌", v.surgerySkipVecEmbed);
			_t("术跳写", v.surgerySkipWrite);
			_t("周报跑", v.usageWeeklyRuns);
			_t("提示计", v.nudgeTurnShown);
			_t("听从计", v.nudgeTurnFollowed);
			_t("提示累计", v.nudgeTotalShown);
			_t("听从累计", v.nudgeTotalFollowed);
			_t("nudge持久错", v.nudgePersistErrors);
			_t("周报错", v.usageWeeklyErrors);
			_t("遥测错", v.toolUsageErrors);
			_t("注入错", v.injectErrors);
			_t("vec通道错", v.vecChannelErrors);
			_t("rerank错", v.rerankErrors);
			_t("计数闸拦", v.countSkipCount);
			_t("回落窗", v.evoFallbackWindows);
			_t("闸未配", v.spaceGateUnconfigured);
			_t("预裁毕", v.conflictsPreverdicted);
			_t("蒸馏行", v.assistantDistillLines);
			_t("消解并", v.entitiesResolved);
			_t("预裁错", v.conflictsPreclassifErrors);
			_t("社区簇", v.communitiesBuilt);
			_t("Saga摘", v.sagaSummaries);
			_t("Saga错", v.sagaSummaryErrors); // 件5 审计修①：render 透出补三标签（社区簇顺带补）
			// SelRoute：六类分布非零即显（⑬ 精神——观测面 render 可见·全零零扰动）
			if (v.queryClassDist) {
				const qc = Object.entries(v.queryClassDist).filter(
					([, n]) => Number(n) > 0,
				);
				if (qc.length)
					_tl.push("分类 " + qc.map(([k, n]) => k + "×" + n).join("/"));
			}
			if (_tl.length) lines.push("  📊 透出：" + _tl.join("·"));
		} else if (a.action === "read_episodic") {
			if (v.episodicContext)
				lines.push(
					"🧠 原文回流 #" +
						v.id +
						"「" +
						v.title +
						"」（" +
						v.frames +
						" 帧·窗 " +
						(Array.isArray(v.windowRange) ? v.windowRange.join("-") : "") +
						"·" +
						v.elapsedMs +
						"ms）\n" +
						String(v.episodicContext).slice(0, 2400),
				); // step：600→2400 显示帽随降密上调（双截断医下半）
			else lines.push("🧠 原文回流：" + (v.note || "不可回流"));
		} else if (v && v.id !== undefined && v.ts) {
			lines.push(
				"✍️ 已入册 #" +
					v.id +
					" [" +
					(a.type || "?") +
					"·" +
					String(v.ts).slice(5, 10) +
					"·" +
					(a.space || "?") +
					"] " +
					(a.title || ""),
			);
			if (v.softWarn)
				lines.push(
					"⚠ " +
						(Array.isArray(v.softWarn)
							? v.softWarn.join("；")
							: String(v.softWarn)),
				); // render 族第三例修复（09-10·/ 后）：softWarn 数据面在而 render 不画=模型不可见——P1-2/C 档 08-24 起+链回提示 09-10 起同哑·本行激活全族
			if (v.reflectLink)
				lines.push(
					"🪡 写入即反思：链到 #" +
						v.reflectLink.id +
						"「" +
						v.reflectLink.title +
						"」（jaccard " +
						v.reflectLink.j +
						"·auto-link 0.4）——A-MEM dynamic linking",
				); // step
			if (v.stampResult)
				lines.push(
					"⚠ 写时盖戳：旧条 #" +
						v.stampResult.stamped +
						" 标 superseded-auto（jaccard " +
						v.stampResult.j +
						"·否证词+数值变化·已立 conflicts 复核案）——wave",
				);
			if (v.closedTodo)
				lines.push(
					v.closedTodo.ok
						? "✔ 办结闭环：todo #" +
								v.closedTodo.id +
								" → done（closed_at " +
								String(v.closedTodo.closedAt || "").slice(0, 16) +
								(v.closedTodo.alreadyDone ? "·幂等返原态" : "") +
								"）——step closeTargetId"
						: "⚠ 办结闭环未成：" +
								String(v.closedTodo.reason || "") +
								"——step",
				); // step：闭环结果 render 消费
		} else {
			return [
				{ type: "text", text: capRenderText(JSON.stringify(value, null, 2)) },
			];
		}
		return [{ type: "text", text: capRenderText(lines.join("\n")) }];
	} catch {
		return [
			{ type: "text", text: capRenderText(JSON.stringify(value, null, 2)) },
		];
	}
}

// ── step：ts 统一 ISO 8601 Asia/Shanghai（显式 +08:00）──
function nowIso() {
	const d = new Date(Date.now() + 8 * 3600 * 1000); // UTC+8 固定偏移（Asia/Shanghai，无夏令时）
	const p = (n) => String(n).padStart(2, "0");
	return `${d.getUTCFullYear()}-${p(d.getUTCMonth() + 1)}-${p(d.getUTCDate())}T${p(d.getUTCHours())}:${p(d.getUTCMinutes())}+08:00`;
}

function nowIso24hAgo() {
	const d = new Date(Date.now() + 8 * 3600 * 1000 - 24 * 3600 * 1000);
	const p = (n) => String(n).padStart(2, "0");
	return `${d.getUTCFullYear()}-${p(d.getUTCMonth() + 1)}-${p(d.getUTCDate())}T${p(d.getUTCHours())}:${p(d.getUTCMinutes())}+08:00`;
}
// 中文标题 bigram 集合（nightly patrol高相似合并用——JC-1 注释归位 08-32 批）
function bigrams(text) {
	const s = String(text);
	const set = new Set();
	for (let i = 0; i < s.length - 1; i++) set.add(s.slice(i, i + 2));
	return set;
}
function jaccard(a, b) {
	if (a.size === 0 || b.size === 0) return 0;
	let inter = 0;
	for (const g of a) if (b.has(g)) inter += 1;
	return inter / (a.size + b.size - inter);
}

// ══ step：分词器（读写同侧·对齐hard rule）══════════════════════════════════
// 词级（jieba 可选）∪ CJK 二元组 ∪ 西文词，空格连接成 FTS5 tokens 文本。
// 查询侧同一函数转 query——读写同 tokenize，永不对不上。
// ── 刀 3 甲方案（design-approved）：jieba 2.x class 化适配 ──
//    2.x 导出 class Jieba（无函数式 cut）——new Jieba() 空实例自带内置词库（dict 子包非必需·实测优于 npm dict）；
//    loadDict feeds proper nouns from DICT (two layers, see above); on failure it degrades to jieba's built-in dictionary;
//    1.x 函数式已停更(ops note)，本段为 2.x 唯一正路。
let jieba = null; // 2.x=Jieba 实例；加载失败=null→纯二元组兜底（不阻塞开工）
try {
	const J = require("@node-rs/jieba");
	if (J && typeof J.Jieba === "function") {
		jieba = new J.Jieba(); // 空实例自带词库（本机 21:3x 实证：cut('living memory记忆检索测试') 完美切分）
		try {
			if (DICT.lines.length > 0)
				jieba.loadDict(Buffer.from(DICT.lines.join("\n"), "utf8")); // 专有词表：双源合并（①包内基础集 ∪ ②用户目录增量集）
		} catch {} // 词表损坏→降级内置词库（不崩）
	} else if (J && typeof J.cut === "function") {
		// 兼容残留：若环境回退装了 1.x(ops note)，函数式直用
		jieba = { cut: (s, hmm) => J.cut(s, hmm) };
	}
} catch {}
const CJK_RE = /[\u4e00-\u9fa5]/;
function tokenize(text) {
	const s = String(text);
	const out = new Set();
	if (jieba && typeof jieba.cut === "function") {
		try {
			for (const w of jieba.cut(s, true)) {
				const t = w.trim();
				if (t && /[\u4e00-\u9fa5a-zA-Z0-9]/.test(t)) out.add(t);
			}
		} catch {} // 8-31 长尾乙档（F0-4·他窗  佐证）：孤立引号/标点 token 滤除——原计入自动召回 token 交叠门（≥2 过闸）稀释精度；读写同侧同滤天然同步
	}
	for (let i = 0; i < s.length - 1; i++) {
		const g = s.slice(i, i + 2);
		if (CJK_RE.test(g[0]) && CJK_RE.test(g[1])) out.add(g);
	}
	for (const w of s.split(/[^\w]+/))
		if (w.length >= 2) out.add(w.toLowerCase());
	return [...out];
}
function bigramSpace(text) {
	return tokenize(text).join(" ");
}
function queryMatch(tokens) {
	return tokens.length > 0
		? tokens.map((t) => `"${t.replace(/"/g, "")}"`).join(" OR ")
		: "";
}

// ── item（design-approved「同意」）：查询构造实词优选——口语查询虚词 bigram（怎么/才能/为什么）挤占前 8 槽 ──
//    致实词零交叠未上榜（spoken 诊断 20/24 未上榜实锤）。构造：jieba 实词（滤停用词/纯数字）优先+西文专名
//    （option L1607 遗产守）+bigram 兜底补 8 槽。沙箱（eval-layer-qexp.cjs 同构尺）：spoken 12.5%→16.7%·lexical/term 零伤。
const QUERY_STOP = new Set([
	"怎么",
	"为什么",
	"什么",
	"怎么办",
	"会不会",
	"总是",
	"老是",
	"可以",
	"时候",
	"那个",
	"一下",
	"东西",
	"别人",
	"才能",
	"出来",
	"没有",
	"是不是",
	"哪些",
	"哪个",
	"到底",
	"直接",
	"已经",
	"真的",
	"一般",
	"应该",
	"的",
	"了",
	"吗",
	"呢",
	"会",
	"能",
	"要",
	"先",
	"都",
	"就",
	"还",
	"又",
	"也",
	"有",
	"在",
	"是",
	"不",
	"我",
	"你",
	"他",
	"这",
	"那",
	"个",
	"么",
	"谁",
	"哪",
	"多",
	"少",
]);
// ── 件 B 真分词修真(design note)：`cut(false)` 在本环境 @node-rs/jieba 2.x 退化为全单字 ──
//    （实测：「为什么总是…」→全单字·实词优选与 SYN 族从未生效=「参数在位≠生效」族第三例）——改 cut(true)
//    搜索引擎模式（宿主 tk19 在产同形态）。件 B+ SYN 族同义扩撤下：真分词下 SYN 噪音伤 lexical 层 -6.3pp
//    （沙箱实测·先前「零伤」系实词未生效的假相）。真增益=件 B 原形：spoken 16.7%→29.2%（+12.5pp）零伤。
function qTokens(text) {
	const all = tokenize(text);
	const ascii = all.filter((t) => /^[\x21-\x7e]{2,}$/.test(t)); // option：西文专名最强区分度信号恒最前
	const real = [];
	if (jieba) {
		try {
			for (const w of jieba.cut(String(text), true)) {
				const t = w.trim();
				if (
					t.length >= 2 &&
					!QUERY_STOP.has(t) &&
					!/^\d+$/.test(t) &&
					!/^[\x21-\x7e]+$/.test(t) &&
					/[一-龥]/.test(t)
				)
					real.push(t);
			}
		} catch {}
	}
	const slots = [...new Set([...ascii, ...real])];
	if (slots.length < 8)
		for (const t of all) {
			if (slots.length >= 8) break;
			if (!slots.includes(t)) slots.push(t);
		}
	return slots.slice(0, 8);
}

// ── SelRoute 六类路由正则版（wave·design-approved「同意」·观测版零权重变更）──
//    六类：ss-user/ss-assistant（单会话用户/助手侧）·multi-session（跨窗）·knowledge-update（知识
//    新鲜度）·temporal（词表单源复用）·other（兜底）。本期只分类+计数透出（stats.queryClassDist
//    ·观测一周）——权重路由（如 ss-user→FTS 单路权重升·knowledge-update→spoken 加成升）候分布settled
//    再pending approval，**不动排序链**。优先序=特异先泛化后（会话指向词＞知识新鲜度词＞泛时间词）——「上次」归
//    temporal（多会话指向由 multi-session 特异词「他窗/上个会话」承担·重叠面预期内·观测期归因看此）。
const SELROUTE_TEMPORAL_RE =
	/上次|最近|之前|昨天|前天|上周|上个月|今晚|今晨|哪天|什么时候|几点|几号|多久/;
const QUERY_CLASS_RES = [
	{
		k: "ss-user",
		re: /我(?:刚才|之前|前面|先前|早先)?(?:说|提|讲|问|写过?|定过?|定的)/,
	},
	{
		k: "ss-assistant",
		re: /你(?:刚才|之前|前面|先前|早先)?(?:说|提|讲|答|给)/,
	},
	{
		k: "multi-session",
		re: /他窗|别的(?:窗|会话)|另一个(?:会话|窗)|上个(?:会话|窗)|哪个(?:窗|会话)|跨窗|前面(?:那|一)个窗/,
	},
	{
		k: "knowledge-update",
		re: /最[新进]|现在(?:还|是|用|啥)|还(?:在用|是)吗|改(?:了|过)吗|更新(?:后|了)?|升级(?:后|了)|新版|当前版本/,
	},
	{ k: "temporal", re: SELROUTE_TEMPORAL_RE },
];
const queryClassDist = {
	"ss-user": 0,
	"ss-assistant": 0,
	"multi-session": 0,
	"knowledge-update": 0,
	temporal: 0,
	other: 0,
};
function classifyQuery(q) {
	const s = String(q || "");
	for (const c of QUERY_CLASS_RES) if (c.re.test(s)) return c.k;
	return "other";
}

// ── 融合面·前缀命中加成（design-approved·option乙主甲辅·方案稿=模块/创造/a companion script）──
//    治「FTS 榜尾→融合 top5」晋席力不足（前缀入索引后 FTS 单路 spoken 45.8% 而真链 16.7% 纹丝不动——
//    base=max(ftsBase,vecPart) 量纲被 vecPart×10 主导，榜尾 ftsBase≈0.3 对 vecPart 排1=3.33 十倍差救不回）。
//    交叠判定=bigram 字面（前缀设计本义=字面桥）；双锚门：交叠词≥2 且至少 1 个不在高频名单（防「记录/那条」灌水）。
const SPOKEN_BOOST = {
	minAnchors: Number(process.env.SPOKEN_BOOST_MIN_ANCHORS) || 2,
	injectCap: Number(process.env.SPOKEN_BOOST_INJECT_CAP) || 3,
	injectBase: Number(process.env.SPOKEN_BOOST_BASE) || 1.2, // ftsBase 头部级·低于 vecPart 排1（3.33）·高于榜尾 0.3
	seatBonusCap: Number(process.env.SPOKEN_BOOST_SEAT_CAP) || 0.3,
	hiFreq: new Set(["记录", "那条", "东西", "笔记", "找", "改", "写", "那个"]), // 库内高频·不单独构成锚（词表参数化·实测噪音面再扩）
};
function spokenOverlap(prefix, tokens) {
	if (!prefix || !tokens || !tokens.length) return 0;
	const p = new Set();
	const s2 = String(prefix);
	for (let i = 0; i < s2.length - 1; i++) p.add(s2.slice(i, i + 2));
	let n = 0,
		strong = 0;
	for (const t of tokens) {
		const w = String(t);
		if (w.length < 2 || QUERY_STOP.has(w)) continue;
		let hit = false;
		for (let i = 0; i < w.length - 1; i++)
			if (p.has(w.slice(i, i + 2))) {
				hit = true;
				break;
			}
		if (hit) {
			n++;
			if (!SPOKEN_BOOST.hiFreq.has(w)) strong++;
		}
	}
	return n >= SPOKEN_BOOST.minAnchors && strong >= 1 ? n : 0;
}

// ── item 生成器：LLM 口语前缀（DeepSeek 提炼同通道·DEEPSEEK_MEMORY_KEY·生产在产凭据）──
//    产出形态=空格分隔口语短语串（入 spoken_prefix 列→au 触发器重嵌 FTS·bigram 化与查询 token 天然同粒度）。
// ── 吸收item（design-approved「记忆直接做」）：双轮异版并集——v2 寻址腔 ∪ v4d 翻译腔 ──
//    沙箱实证：双轮并集过双锚门 45.8%（11/24）vs 单轮 25~33%（a companion script·三路由settled ）；
//    单轮失败降级用另一轮产出，双败抛错（上层静默=nightly patrol回填兜底）；并集按短语去重后 200 字帽不变。
const SP_PROMPT_V2 = (t, c) =>
	// v2 提示词（三版沙箱对照定稿为 recall 胜者：v1 6 短语 37.5% → v2 10 短语多样 41.7%；v3 few-shot 照抄陷阱/v2.5 专名令挤占变体均不敌）
	"用户想找下面这条记录，会把意思用大白话随口问出来。请写出 10 个不同的口语问法短语，要求：每个短语尽量用不一样的字词说法（同一件事换着字面说），包含最土最直接的问法。只输出短语，空格分隔，不编号不解释。标题：" +
	t +
	" 内容：" +
	c;
const SP_PROMPT_V4D = (t, c) =>
	// v4d 翻译腔（09-02 沙箱定稿）：治「翻译鸿沟」——条目专名表述（唯一正账/注入断供勘正）vs 用户泛化口语（哪个库为准/看不到记录）；
	//    指令=把记录的事翻译成用户日常说法，专名/数字/编号保原字。
	"下面这条记录来自一套内部工作系统。用户将来随口提问时往往不会用记录里的原词（比如记录写「召回质量分层settled」，用户只会问「搜索老是找不到怎么办」；记录写「唯一正账」，用户会问「记东西以哪个库为准」）。请站在用户角度写出 10 个最可能的大白话问法短语，要求：①每个短语是随口问题句式（怎么办/为什么/咋回事/有啥规矩/怎么确认/是哪个）②把记录说的事翻译成用户日常说法，不照抄记录的术语原话③但记录里的关键数字、编号、插件名、人名等专名保留原字。只输出短语，空格分隔，不编号不解释。标题：" +
	t +
	" 内容：" +
	c;
async function llmSpokenPrefixOnce(prompt) {
	const resp = await fetch(EXTRACT_API, {
		method: "POST",
		headers: {
			"Content-Type": "application/json",
			Authorization: "Bearer " + spCredCache.v,
		},
		body: JSON.stringify({
			model: SPOKEN_MODEL, // 裁③：env 分立（原硬编码·audit）
			messages: [{ role: "user", content: prompt }],
			max_tokens: 200,
			temperature: 0.4,
		}),
		signal: AbortSignal.timeout(20000),
	});
	if (!resp.ok) throw new Error("spoken-prefix http " + resp.status);
	const data = await resp.json();
	return String(data?.choices?.[0]?.message?.content || "")
		.replace(/[\n、，,。;；|/]+/g, " ")
		.replace(/\s+/g, " ")
		.trim();
}
async function llmSpokenPrefix(title, content) {
	const t = String(title).slice(0, 120),
		c = String(content).slice(0, 400);
	const [r1, r2] = await Promise.allSettled([
		llmSpokenPrefixOnce(SP_PROMPT_V2(t, c)),
		llmSpokenPrefixOnce(SP_PROMPT_V4D(t, c)),
	]);
	const seen = new Set(),
		merged = [];
	for (const r of [r1, r2]) {
		if (r.status !== "fulfilled" || !r.value) continue;
		for (const p of r.value.split(" ")) {
			const w = p.trim();
			if (w && !seen.has(w)) {
				seen.add(w);
				merged.push(w);
			}
		}
	}
	const txt = merged.join(" ").trim();
	if (!txt)
		throw (
			r1.reason || r2.reason || new Error("spoken-prefix both rounds empty")
		);
	return txt.slice(0, 200); // 200 字帽（FTS 体量闸）
}
let spCredCache = { v: null, t: 0 }; // DEEPSEEK_MEMORY_KEY 60s 缓存（vecCredCache 同手法）

// ══ 检索升级 1a'：向量召回通道（design-approved·DashScope text-embedding-v4 专用 key）════
//    EMBEDDING_BAILIAN_KEY（credentials 官方管道）·1024 维·全库 BLOB float32 JS 暴力 KNN（万条内 <50ms）。
//    memories_vec(id PK, embedding BLOB, model_version)——模型切换=版本戳防混查（同 FTS tokenizer 逻辑）。
//    重嵌入入口：reembed 增量补全（nightly patrol末尾顺带 + search 时惰性补）。
const VEC_MODEL = "text-embedding-v4";
// ── 粒度手术+换底座（design-approved三车排程 ）：chunks 表走 qwen3.7 句级 max-sim ──
//    依据=0.427→0.664 实证+官方 qwen3.7 同价 +20%；双写过渡：旧表 v4 续供 fallback/nightly patrol dedup，nightly patrol刀再统一切。
//    SelRoute §5.5 pinboard：chunk=追加切句索引，原文永不替换。
const VEC_CHUNK_MODEL = "qwen3.7-text-embedding";
const VEC_DIM = 1024;
// 句/段级切分：title 恒 seq=0；content 按句末标点/换行切段，并段 ≤300 字符，帽 8 段（turn 级粒度同哲学）
function chunkMemory(title, content) {
	const out = [];
	const t = String(title || "").trim();
	if (t) out.push(t.slice(0, 300));
	const parts = String(content || "")
		.split(/(?<=[。！？；!?\n])/)
		.map((s) => s.trim())
		.filter(Boolean);
	let buf = "";
	for (const p of parts) {
		if (buf && (buf + p).length > 300) {
			out.push(buf);
			buf = p;
		} else {
			buf += p;
		}
	}
	if (buf) out.push(buf);
	return out.slice(0, 8);
}
let vecCredCache = { v: null, t: 0 }; // credentials 句柄 60s 缓存（避免每查 resolve）
// ── option空间白名单闸计数面（design-approved·braintask brief 22:10「先闸后挂载」）──
//    模块级：write 分支（闸落点）写、host 分支（stats return）读——分支隔离所以模块级共享
const spaceGateCounts = { blocked: 0, passed: 0, unconfigured: 0 };
let spaceGateLastWarnAt = 0; // option闸拦截 warn 节流窗（60s·vecLastWarnAt 同手法）——三轮审计改进②：拦截有轨迹（挂载初期误拦可观测·编码纪律⑬）
// 8-31 锈面修（审计 F0-1③）：vec 静默死遥测——降级归空须计数透出+节流告警（原 catch 静默归空：401/key 失效/网络断全线不可见）
let vecChannelErrors = 0; // 故障累计（stats.vecChannelErrors 透出）
let vecLastWarnAt = 0; // 60s 节流窗（vecCredCache 同级手法）
async function embedOnce(texts, model, instruct) {
	// 第三参 instruct：查询侧专用（查询/文档不对称）——qwen3.7 指令遵循 +16.4%（官方）·文档侧一律不传
	const out = [];
	for (let i = 0; i < texts.length; i += 10) {
		const batch = texts.slice(i, i + 10);
		const resp = await fetch(
			"https://dashscope.aliyuncs.com/compatible-mode/v1/embeddings",
			{
				method: "POST",
				headers: {
					"Content-Type": "application/json",
					Authorization: "Bearer " + vecCredCache.v,
				},
				body: JSON.stringify({
					model: model || VEC_MODEL,
					input: batch,
					dimensions: VEC_DIM,
					encoding_format: "float",
					// item1：instruct 进 body（探针 cos=0.951 实证生效）·OFF 开关族同闸
					...(instruct && !INSTRUCT_OFF ? { instruct } : {}),
				}),
				signal: AbortSignal.timeout(15000),
			},
		);
		if (!resp.ok) throw new Error("embed http " + resp.status);
		const data = await resp.json();
		for (const d of data.data) out.push(new Float32Array(d.embedding));
	}
	return out;
}
function f32ToBlob(f32) {
	return Buffer.from(f32.buffer, f32.byteOffset, f32.byteLength);
}
function blobToF32(buf) {
	return new Float32Array(buf.buffer, buf.byteOffset, buf.byteLength / 4);
}
function cosine(a, b) {
	let dot = 0,
		na = 0,
		nb = 0;
	for (let i = 0; i < a.length; i++) {
		dot += a[i] * b[i];
		na += a[i] * a[i];
		nb += b[i] * b[i];
	}
	return dot / (Math.sqrt(na) * Math.sqrt(nb) || 1);
}
async function vecRecallCore(conn, creds, query, topK) {
	try {
		const now = Date.now();
		if (!vecCredCache.v || now - vecCredCache.t > 60000) {
			const c = await creds.resolve("EMBEDDING_BAILIAN_KEY");
			if (!c || !c.value) return []; // 无 key → 空通道直通（降级不崩）
			vecCredCache = { v: c.value, t: now };
		}
		conn.exec(
			`CREATE TABLE IF NOT EXISTS memories_vec (id INTEGER PRIMARY KEY, embedding BLOB NOT NULL, model_version TEXT NOT NULL)`,
		);
		// ── content_hash 嵌入门（2026-08-24 吸inbox③·graph-memory 设计）：内容没变零嵌入 ──
		try {
			conn.exec(`ALTER TABLE memories_vec ADD COLUMN content_hash TEXT`);
		} catch {}
		// ── 粒度手术（2026-08-31 item2）：句级 chunks 表=max-sim 迟交互载体（0.427→0.664 实证）──
		conn.exec(
			`CREATE TABLE IF NOT EXISTS memories_vec_chunks (id INTEGER NOT NULL, seq INTEGER NOT NULL, chunk_text TEXT, embedding BLOB NOT NULL, model_version TEXT NOT NULL, PRIMARY KEY (id, seq, model_version))`,
		);
		// 惰性补全：缺 chunks 条目切句补嵌——单次帽 6 条（每条 3-8 段×批量 10/请求）防查询延迟
		// P2 fix（09-03 audit）：maintenance window挂牌期检索只读——惰性补嵌写跳过（原缺口=挂牌期 search 仍触库写·摘牌后自愈不致命）
		const surgeryOnVec = fs2.existsSync(SURGERY_FLAG);
		if (surgeryOnVec) surgerySkipVecEmbed += 1;
		const missing = surgeryOnVec
			? []
			: conn
					.prepare(
						`SELECT m.id, m.title, m.content FROM memories m LEFT JOIN memories_vec_chunks c ON c.id = m.id AND c.model_version = ? WHERE m.status='active' AND c.id IS NULL LIMIT 6`,
					)
					.all(VEC_CHUNK_MODEL);
		if (missing.length > 0) {
			const ins = conn.prepare(
				"INSERT OR REPLACE INTO memories_vec_chunks (id, seq, chunk_text, embedding, model_version) VALUES (?, ?, ?, ?, ?)",
			);
			for (const r of missing) {
				const chunks = chunkMemory(r.title, r.content);
				if (!chunks.length) {
					ins.run(r.id, 0, "", Buffer.alloc(0), VEC_CHUNK_MODEL);
					continue;
				} // 二轮审计修：空条目写占位行（与 chunks-backfill.py 对齐）——防永恒重试位挤死真缺嵌条目
				const embs = await embedOnce(chunks, VEC_CHUNK_MODEL); // 09-03 裁①：chunks 句级路真接 qwen3.7（原硬编码 v4 落 qwen3.7 戳=假绿·audit）
				for (let i = 0; i < chunks.length && i < embs.length; i++)
					ins.run(r.id, i, chunks[i], f32ToBlob(embs[i]), VEC_CHUNK_MODEL);
			}
		}
		const [qvec] = await embedOnce(
			[String(query).slice(0, 1500)],
			VEC_CHUNK_MODEL,
			INSTRUCT_QUERY,
		); // 09-03 裁①：query 与 chunks 同空间（跨空间混查防线）·item1：查询侧 instruct（探针实证 cos=0.951 生效·qwen3.7 指令遵循）
		// 句级 max-sim：每条目取其 chunks 与 query 的最大余弦（迟交互同构·条目=多向量文档）
		const allChunks = conn
			.prepare(
				`SELECT c.id, c.embedding FROM memories_vec_chunks c JOIN memories m ON m.id = c.id WHERE c.model_version = ? AND LENGTH(c.embedding) > 0 AND m.status IN ('active','merged','done')`,
			) // step：vec KNN 池同放宽（与 FTS 候选池一致·惰性回填写路保 active 纯净——merged 条 chunks 多在 active 期已建）；LENGTH>0：余件item一（09-09·三轮审计 §4.4-2）——空条目占位行 Buffer.alloc(0) 进 cosine 出 NaN 污染 bestById（父亲验实锤）·SQL 侧滤除
			.all(VEC_CHUNK_MODEL);
		const bestById = new Map();
		for (const r of allChunks) {
			const s = cosine(qvec, blobToF32(r.embedding));
			const cur = bestById.get(r.id);
			if (cur === undefined || s > cur) bestById.set(r.id, s);
		}
		const scored = [...bestById.entries()].map(([id, s]) => ({ id, s }));
		scored.sort((a, b) => b.s - a.s);
		return scored.slice(0, topK).map((x) => x.id);
	} catch (e) {
		// 任何故障 → 空通道（FTS 单路仍可用，降级不崩）
		// 8-31 锈面修（审计 F0-1③）：原静默归空无 warn 无计数——vec 通道死亡检索/stats 面均不可见。补节流 warn（60s 窗）+计数透出（⑬ 出口）
		vecChannelErrors += 1;
		if (Date.now() - vecLastWarnAt > 60000) {
			vecLastWarnAt = Date.now();
			try {
				console.warn(
					"[living-memory] vec channel down (#" +
						vecChannelErrors +
						"): " +
						String(e).slice(0, 80),
				);
			} catch {}
		}
		return [];
	}
}
// ══ option·线上 rerank 精排层（design-approved「按推荐」发车· 件③·论证=模块/memory organ/a companion script）══
//    病灶=语义排序质量（专名域 vec 虚高挤位· 封顶归因）非表征粒度——融合候选帽 RERANK_CAND 按 base 取头 →
//    线上 rerank query-document 真相关度精排 → topN 以 relevance_score 为 base 独尺入加权链（未入列候选弃置=论证稿
//    「精排 top10 入加权链」原案·防 rerank 0~1 与 vecPart×10 新旧量纲混排）。
//    凭据共享 vecCredCache（EMBEDDING_BAILIAN_KEY 同 key 同域·零新凭据·计费=调用次数/Token 分毫级）；
//    故障/无 key/超时 → null → 原链直通（base 不动）+计数透出（vecChannelErrors 同族·F0-1 静默死教训）。
//    回退开关 LEGION_RERANK_OFF（演练开关族·同构）；模型 LEGION_RERANK_MODEL 可扫。
//    默认 gte-rerank-v2（09-02 23:00 定标·沙箱四臂翻转实证：总 84.7/lexical 85.1/term 满分/MRR 0.717 全面最强——
//    专名域零误判正治  病灶；原五裁默认 qwen3.7-text-rerank spoken 鸿沟层 54.2% 专项更强留 env 一键切·09-02 双冒烟 200）。
const RERANK_MODEL = process.env.LEGION_RERANK_MODEL || "gte-rerank-v2";
const RERANK_TOPN = Number(process.env.LEGION_RERANK_TOPN) || 20; // 精排后入加权链条数（沙箱扫 10/20）
const RERANK_CAND = Number(process.env.LEGION_RERANK_CAND) || 50; // 喂 rerank 候选帽（论证稿 top50）
let rerankErrors = 0; // 故障累计（stats.rerankErrors 透出）
let rerankLastWarnAt = 0; // 60s 节流窗（vecLastWarnAt 同手法）
async function rerankDocs(creds, query, docs) {
	// docs=[{id,text}] → Map(id→relevance_score)；任何故障=null（调用方原链直通·不崩不拖）
	try {
		const now = Date.now();
		if (!vecCredCache.v || now - vecCredCache.t > 60000) {
			const c = await creds.resolve("EMBEDDING_BAILIAN_KEY");
			if (!c || !c.value) return null; // 无 key → 原链直通
			vecCredCache = { v: c.value, t: now };
		}
		const resp = await fetch(
			"https://dashscope.aliyuncs.com/api/v1/services/rerank/text-rerank/text-rerank",
			{
				method: "POST",
				headers: {
					"Content-Type": "application/json",
					Authorization: "Bearer " + vecCredCache.v,
				},
				body: JSON.stringify({
					model: RERANK_MODEL,
					input: {
						query: String(query).slice(0, 1000), // query 4000 token 硬限·1000 字保守截
						documents: docs.map((d) => d.text),
					},
					parameters: { top_n: docs.length, return_documents: false }, // 全量回分·截位在调用方
				}),
				signal: AbortSignal.timeout(10000), // embedOnce 15s 同族·精排延迟预算更紧
			},
		);
		if (!resp.ok) throw new Error("rerank http " + resp.status);
		const data = await resp.json();
		const results = data && data.output && data.output.results;
		if (!Array.isArray(results)) throw new Error("rerank bad shape");
		const m = new Map();
		for (const r of results) {
			if (docs[r.index] !== undefined)
				m.set(docs[r.index].id, Number(r.relevance_score) || 0);
		}
		return m;
	} catch (e) {
		rerankErrors += 1;
		if (Date.now() - rerankLastWarnAt > 60000) {
			rerankLastWarnAt = Date.now();
			try {
				console.warn(
					"[living-memory] rerank channel down (#" +
						rerankErrors +
						"): " +
						String(e).slice(0, 80),
				);
			} catch {}
		}
		return null;
	}
}

// ══ 四闸共用核心（2026-08-22 task brief·僵尸待办治本·断裂五环实证 ）══════════
//    闭环面 = 近 72h status=done 的 todo ∪ 近 72h 标题/正文含 销账/收官/闭环 的 fact/decision/lesson。
//    同题判定：关键词（bigram+西文词，去停用词）在闭环面条目文本中重叠 ≥3 → 判已闭环。
//    阈值 3 为保守值（2026-08-22 设计）：「沙箱残留清理」vs 「残留8条escalate」重叠 2 词不误伤；
//    真僵尸「rc.8 升级专项」vs done  重叠 ≥4 必中。宁漏勿误——漏网由盘账兜底，误伤堵死正路。
const GATE_STOP = new Set([
	"任务",
	"务书",
	"task brief",
	"执行",
	"完成",
	"进行",
	"待办",
	"事项",
	"排期",
	"启动",
	"汇总",
	"验收",
	"收官",
	"闭环",
	"销账",
	"专项",
	"修复",
	"清理",
	"field report",
	"晨报",
	"nightly patrol",
	"首跑",
	"汇报",
	"批复",
	"暖暖",
	"的",
	"了",
	"在",
	"是",
	"和",
	"与",
	"生效",
	"落盘",
	"全绿",
	// ── 0.2.0 首发（issue 同车·09-08）：英文停用词追加——纯 ASCII 追加·中文行为零变 ──
	//    治面：英文用户标题/证据全英文时此表空转不滤高频词，同题判定精度伤（§9.9 真功能缺陷）。
	//    功能性高频（对齐中文段语义）+英文结构停用词双段；token 入表前已 toLowerCase。
	"task",
	"todo",
	"done",
	"complete",
	"completed",
	"pending",
	"progress",
	"report",
	"summary",
	"review",
	"plan",
	"execute",
	"start",
	"launch",
	"checklist",
	"audit",
	"update",
	"fix",
	"cleanup",
	"deploy",
	"release",
	"verify",
	"the",
	"a",
	"an",
	"is",
	"are",
	"was",
	"were",
	"be",
	"been",
	"of",
	"for",
	"and",
	"or",
	"to",
	"in",
	"on",
	"at",
	"it",
	"this",
	"that",
	"with",
	"from",
	"by",
	"as",
	"we",
	"you",
]);
function gateKeywords(text, max) {
	const out = [];
	for (const t of tokenize(String(text || ""))) {
		const l = String(t).toLowerCase();
		if (GATE_STOP.has(l)) continue;
		if (/^[a-z0-9]$/i.test(l)) continue;
		if (/^\d{1,4}$/.test(l) || /^\d+[:：.]/.test(l)) continue; // maintainer 22:26 批·brain 22:27 kickoff：纯数字/时间样式 token 不入闸词（治「14/02/00」跨题撞击·B2 单字不计同精神——三连案实测复现）
		out.push(l);
		if (out.length >= (max || 10)) break;
	}
	return out;
}
function closureCheck(conn, title, content, todoId) {
	// step C案：ts 参随②同日排除退役（原仅喂 todoT·P-1 双锚不一致影响面同消）
	// ── step（design-approved·召回状态化）：前置边判定——solved_by 活边指向本 todo ⇒ O(1) 精确判 closed ──
	//    边=显式建边意图非词面巧合（再论证改判②：边路免 META_RE）；idx_edges_dst 部分索引恰在 invalid_at IS NULL 上 O(log n)；
	//    起步只认 solved_by（改判①：patches→todo 语义含混「从待办学到教训」≠办毕·二批再纳）；无边照旧走词面全闸兜底。
	try {
		const tid0 = Number(todoId);
		if (Number.isInteger(tid0) && tid0 > 0) {
			const eRow = conn
				.prepare(
					"SELECT src FROM memories_edges WHERE dst = ? AND edge_type = 'solved_by' AND invalid_at IS NULL LIMIT 1",
				)
				.get(tid0);
			if (eRow) return { closed: true, by: eRow.src, byEdge: "solved_by" }; // byTitle/overlap 不适用（边证非词面证·调用面只消费 by/closed）
		}
		const kw = gateKeywords(
			String(title || "") + " " + String(content || ""),
			12,
		);
		if (kw.length === 0) return { closed: false };
		// ── v2（2026-08-22 P0 批后task brief §一）：治自污染三重复现（6/7 误判）──
		//    ①证据面收紧：fact/decision/lesson 证据须命中闭环词且【不】命中元类模式词——
		//      「关于事实的记忆」（盘点/清单/裁处/审计/复现/汇总类条目）不再被当「事实本体」；
		//    ②SQL 时间窗下推：ts >= 72h 前在 SQL 先过滤（修 LIMIT 150 截断漂移——库增长后老僵尸被挤出窗）；
		//    ③同批互证防线由①元类全排承担（META_RE 72h 窗全排·B1 成果）；同日真 fact 为有效证据，不作时间性排除（C 案终谳·design-approved·step）。
		const META_RE = CRIT.metaExclusion; // 元类排除词表已外置（criteria-rules.default.json·2026-09-12 一车·原字面量→三级链取用）
		const pastIso = (msAgo) => {
			const d = new Date(Date.now() + 8 * 3600 * 1000 - msAgo);
			const p = (n) => String(n).padStart(2, "0");
			return `${d.getUTCFullYear()}-${p(d.getUTCMonth() + 1)}-${p(d.getUTCDate())}T${p(d.getUTCHours())}:${p(d.getUTCMinutes())}+08:00`;
		};
		const since = pastIso(72 * 3600 * 1000);
		const rows = conn
			.prepare(
				"SELECT id, ts, type, title, content FROM memories WHERE ts >= ? AND (" +
					"(type = 'todo' AND status = 'done') OR " +
					"((type = 'fact' OR type = 'decision' OR type = 'lesson') AND (title LIKE '%销账%' OR content LIKE '%销账%' OR title LIKE '%收官%' OR content LIKE '%收官%' OR title LIKE '%闭环%' OR content LIKE '%闭环%'))" +
					") ORDER BY id DESC LIMIT 500", // design-approved）
			)
			.all(since);
		// ── v2.1 同题性锚定（2026-08-22 基线回归发现）：证据 title 须与待办 title ≥1 关键词重叠 ──
		//    病例：done 「注入瘦身方案awaiting approval」content 顺带提「某长尾话题速览下沉」3 词 → 跨主题误判 。
		//    修法：闭环是同题判定，题眼必须在双方 title 层对上；content 顺带词不算数。
		const titleKw = gateKeywords(title, 8);
		for (const r of rows) {
			if (String(title || "").trim() === String(r.title || "").trim()) continue; // 同名条（复述/追问）不算闭环证据
			if (r.type !== "todo") {
				// 证据面收紧：元类条目（关于事实的记忆）不作证据
				if (META_RE.test(String(r.title || "") + String(r.content || "")))
					continue;
				// ②同日排除已删（C 案·step）：同批互证由上方①元类全排承担；同日真 fact 为有效证据
			}
			const rTitle = String(r.title || "").toLowerCase();
			// ── B2 v2.2（design-approved快刀）：锚定门升格合璧案——α(≥2 不同 titleKw 命中) ∨ β(单字 token 双不计) ──
			//    病例：brain 07:50 四连误拦（候/复盘/验收等高频词+单字「候」全库撞击）；单字噪声不计锚定不计 hits。
			const anchorHits = titleKw.filter(
				(k) => k.length > 1 && rTitle.includes(k),
			).length;
			if (anchorHits < 1) continue; // 题眼零命中（多字词口径）→ 非同题
			if (anchorHits < 2) {
				// 单锚弱门：需 content 侧补证（titleKw 多字词在证据 body 至少再现 1 个）
				const bodyHas = titleKw.some(
					(k) =>
						k.length > 1 &&
						String(r.content || "")
							.toLowerCase()
							.includes(k.toLowerCase()),
				);
				if (!bodyHas) continue;
			}
			const hay = (rTitle + " " + String(r.content || "")).toLowerCase();
			let hits = 0;
			for (const k of kw) if (k.length > 1 && hay.includes(k)) hits += 1; // β：单字 token 不计 hits
			if (hits >= 3)
				return { closed: true, by: r.id, byTitle: r.title, overlap: hits };
		}
		return { closed: false };
	} catch (eCC) {
		closureCheckErrors += 1; // P2 fix：四闸核心异常透出（原静默 return false=异常期全放行假绿·stats 可读）
		return { closed: false };
	}
}

// ══ step③：space 规范与space加权 ═══════════════════════════════════════
// ══ 第 1.step：space自注册（design-approved方案 A·配置驱动）════════════════
//    启动扫描 your module directories 目录——SPACES = 内置九值 ∪ 新目录名；ORGAN_DIR_MAP 同源动态扩。
//    目录内可选文件 space（内容=space 值）优先于目录名；扫描失败回退内置九值（降级不崩）。
//    沙箱演练开关：LEGION_MODULE_SCAN_DIR 指向假模块目录（生产不设=正路径）。
const BUILTIN_SPACES = [
	"brain",
	"hand",
	"reflex", // ＝极简space（快神经）空间·ORGAN_DIR_MAP 映射·零条目因该窗少写记忆·非死枚举（09-08 brain亲验纠审计误判·七小件③）
	"creator",
	"fetcher",
	"xhs",
	"maintain",
	"memory-organ",
	"global",
];
// Path-segment → space map, shipped EMPTY on purpose: your own module directories
// are discovered at startup (buildSpacesAndMap) and register themselves under their
// directory name, so no site-specific vocabulary has to ship here. Add entries as
// ["<path segment>", "<space>"] only when a directory should map to a different name.
const BUILTIN_ORGAN_DIR_MAP = [];
const MODULE_SCAN_DIR =
	process.env.LEGION_MODULE_SCAN_DIR ||
	(DRILL_SANDBOX
		? path.join(os.tmpdir(), "living-drill-modules") // pending ruling③总闸：沙箱库演练缺省改道 /tmp（镜像段/①层哨读面同随·生产space目录零接触；空目录→自注册回退内置九值·降级已备）
		: path.join(os.homedir(), ".dsh", "dsh-living-memory", "modules"));
const autoRegistered = []; // [[目录名, space]]——仅映射表外新目录，内置九值不进此列
function buildSpacesAndMap() {
	const spaces = [...BUILTIN_SPACES];
	const map = [...BUILTIN_ORGAN_DIR_MAP];
	try {
		for (const name of fs2.readdirSync(MODULE_SCAN_DIR)) {
			try {
				if (!fs2.statSync(path.join(MODULE_SCAN_DIR, name)).isDirectory())
					continue;
			} catch {
				continue;
			}
			// 驻扎别名目录（内置映射段可命中，如 an alias directory）不加新枚举——保 autoRegisteredSpaces 基线 0
			let alias = null;
			for (const [seg, sp] of BUILTIN_ORGAN_DIR_MAP)
				if (String(name).includes(seg)) {
					alias = sp;
					break;
				}
			if (alias) continue;
			let sp = null;
			try {
				sp = fs2
					.readFileSync(path.join(MODULE_SCAN_DIR, name, "space"), "utf8")
					.trim();
			} catch {}
			if (!sp) sp = name;
			if (spaces.includes(sp)) continue;
			spaces.push(sp);
			map.push([name, sp]);
			autoRegistered.push([name, sp]);
		}
	} catch {
		/* 扫描失败回退内置九值 */
	}
	return { spaces, map };
}
const _built = buildSpacesAndMap();
const SPACES = _built.spaces;
const ORGAN_DIR_MAP = _built.map;
// ── 空间闸「未配置」降级（2026-09-07 冷启动caught in drill P0·发布阻塞级）──
//    闸的判据是「caller 归属空间」，而归属只来自目录映射（BUILTIN_ORGAN_DIR_MAP ∪ 启动时自注册
//    的模块目录）。公开派生版把映射表发成空集（站内专有目录名不随包走），新用户机器上也没有
//    我们的模块目录 → caller 恒不可解析 → 闸拦下 **100%** 的 memory_write：那不是闸，是坏功能
//    （新装用户一条记忆都写不进去，日志只有一行 warn）。
//    修：映射面全空 ＝ 闸未配置 → 降级放行并计数透出；用户一旦配好目录映射（放个模块目录即自注册）
//    闸自动恢复生效。内源侧映射表非空 → 本条件恒假 → 零行为变更。
const SPACE_GATE_CONFIGURED =
	ORGAN_DIR_MAP.length > 0 || autoRegistered.length > 0;
function organFromPath(p) {
	const s = String(p || "");
	for (const [seg, space] of ORGAN_DIR_MAP) if (s.includes(seg)) return space;
	return null;
}
// ── 吸收item（design-approved「记忆直接做」·mem0 captureEvent 设计本地化）：工具使用遥测包装器 ──
//    注入采纳率数据底座（综合推理漏点⑨·AgentRecall「捕获密度>检索技术」落地起点）。
//    hard rule：遥测写库 try/catch 静默+toolUsageErrors 计数透出（stats 可读），绝不拖累主流程——「静默降级透出」立法第一活体。
//    不记查询原文（防敏感堆积）——action 列只记 action/type 参数名面。
let toolUsageErrors = 0;
// ── P2 修（09-03 audit）：模块级计数三键（toolUsageErrors 同族「静默降级透出」立法第一活体同法）──
let closureCheckErrors = 0; // closureCheck 顶层异常累计（四闸核心静默全放行的观测面）
let surgerySkipVecEmbed = 0; // 挂牌期 vecRecallCore 惰性补嵌写跳过数
let surgerySkipWrite = 0; // 挂牌期 memory_write 写路冻结数
// 注：**故意用对象形态而非 `let x = 0`**——lens 写时纠错（prefer-const·局部视野）会在「声明已落、自增未落」的逐次编辑间隙把 let 优化成 const，
// 致后续 `x++` 与 const 冲突（命中即 TypeError·本窗实测踩坑）⇒ 对象属性自增对 const 中立，抗纠错且语义等价。
const dupWriteStat = { blocked: 0 }; // 09-11 梁 1b 同文幂等闸：memory_write 同文重复被拦数（模块级·跨重启清零·四点接线齐＝赋值/return/render/drill）
function callerSpaceOf(exec) {
	// 调用者space探测（遥测专用·与 search 段内联链同源但独立——检索高危面零触碰原则）
	try {
		const cwd =
			exec?.agent?.session?.header?.cwd ||
			exec?.agent?.session?.cwd ||
			exec?.session?.header?.cwd ||
			exec?.cwd;
		return organFromPath(cwd);
	} catch {
		return null;
	}
}
function usageWrap(toolName, getConn, fn) {
	return async (args, exec) => {
		const t0 = Date.now();
		let ok = 1,
			errKind = null,
			rc = -1;
		try {
			const out = await fn(args, exec);
			if (out && Array.isArray(out.hits)) rc = out.hits.length;
			else if (out && Array.isArray(out.entries)) rc = out.entries.length;
			else if (out && typeof out.extracted === "number") rc = out.extracted;
			else if (out && out.id !== undefined) rc = 1;
			if (out && out.error) {
				ok = 0;
				// P2 fix（09-03 audit）：errKind step切 param 污染采纳率归因——按错误文案细分三态
				const es = String(
					typeof out.error === "string"
						? out.error
						: (out.error && out.error.message) || out.error,
				);
				errKind = /需要|参数|必带|非法|invalid|param/i.test(es)
					? "param"
					: /闸|拦截|拒写|冻结|手术|surgery|gate|rejected/i.test(es)
						? "gate"
						: "biz";
			}
			return out;
		} catch (e) {
			ok = 0;
			errKind = "throw";
			throw e;
		} finally {
			try {
				const conn = typeof getConn === "function" ? getConn() : null;
				if (conn)
					conn
						.prepare(
							"INSERT INTO tool_usage (ts, tool, caller_space, action, duration_ms, result_count, success, error_kind) VALUES (?,?,?,?,?,?,?,?)",
						)
						.run(
							nowIso(),
							toolName,
							callerSpaceOf(exec) || null,
							String(args?.action || args?.type || ""),
							Date.now() - t0,
							rc,
							ok,
							errKind,
						);
			} catch {
				toolUsageErrors++;
			}
		}
	};
}
// 加权表：this space 1.0 / global 0.9 / memory organ（跨窗查档）0.4 / 其other spaces 0.05
function spaceWeight(hitSpace, callerSpace) {
	if (!callerSpace) return 1.0;
	if (hitSpace === callerSpace) return 1.0;
	if (hitSpace === "global") return 0.9;
	if (hitSpace === "memory-organ") return 0.4;
	return 0.05;
}
// projcache cwd → sessions 目录名解码 双源映射（自动提炼 space 用；归位脚本同源逻辑）
// P0 修复（design-approved）：②dirMap 缓存 TTL 化 60s（原进程级永固——「重启依赖症」制造者：长稳运行期新建会话永不入缓存·8-26 实证 28375d 八小时 35 条全落 global）
let sessionsDirCache = { at: 0, map: null };
const SESSIONS_DIR_TTL = 60000;
function sessionsDirMap() {
	if (
		sessionsDirCache.map &&
		Date.now() - sessionsDirCache.at < SESSIONS_DIR_TTL
	)
		return sessionsDirCache.map;
	const map = new Map();
	try {
		const base = path.join(os.homedir(), ".dsh", "sessions");
		for (const ws of fs2.readdirSync(base)) {
			const wsDec = ws.replace(/~([0-9A-F]{4})/g, (m, h) =>
				String.fromCharCode(parseInt(h, 16)),
			);
			const organ = organFromPath(wsDec);
			if (!organ) continue;
			const full = path.join(base, ws);
			try {
				for (const sess of fs2.readdirSync(full)) {
					if (
						sess.startsWith("session-") ||
						/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(
							sess,
						)
					)
						map.set(sess, organ); // 第三轮审计修（09-09 pending ruling①）：裸 uuid 会话目录（子代理窗）同收——原 startsWith 单形态过滤丢 47%
				}
			} catch {}
		}
	} catch {}
	sessionsDirCache = { at: Date.now(), map };
	return map;
}
// ── 庚批核心 helper(design note)：projcacheV5 双源读 ──
//    宿主 09-02 改 layout:"per-record"——旧单文件 session_projcache.json 自 09-05 00:12 停更（三读者死指针：token spend端点/归属链/nightly patrol压力哨）。
//    新面：sessions/session-<sid>.json {version,record:{identity,rows}}——本函数归一双源：新目录优先·空则回退旧单文件·返回 Map(sid→rows)
//    第三轮审计修复（2026-09-09·pending ruling①庚批三重死）：①上提模块层（原居 apply host 分支内——模块顶层 sessionOrgan 幻引用恒 ReferenceError 被空 catch 静吞·假绿族第六犯）
//    ②map 键=文件名去 .json 原样（原 slice 剥前缀 vs 查询侧带前缀=键形错位恒不中·双锁死）③放行裸 uuid 文件名（宿主子代理会话 id=裸 uuid·原 startsWith 过滤实测丢 160/338=47% 会话）
function projcacheRows() {
	const out = new Map();
	try {
		const dir = path.join(
			os.homedir(),
			".dsh",
			"storages",
			"session_projcache",
			"sessions",
		);
		for (const f of fs2.readdirSync(dir)) {
			if (!f.endsWith(".json")) continue;
			if (
				!f.startsWith("session-") &&
				!/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\.json$/.test(
					f,
				)
			)
				continue; // 双形态：session-* 主窗 + 裸 uuid 子代理窗同收
			try {
				const j = JSON.parse(fs2.readFileSync(path.join(dir, f), "utf8"));
				const sid = f.slice(0, -".json".length); // 键=原文件名去 .json（前缀保留·与查询侧同形对齐）
				const rows = j?.record?.rows;
				if (rows)
					out.set(sid, {
						rows,
						mtimeMs: fs2.statSync(path.join(dir, f)).mtimeMs,
						identity: j?.record?.identity,
					});
			} catch {}
		}
	} catch {}
	if (out.size > 0) return out;
	try {
		// 回退：旧单文件（≤09-05 数据·保面板不断供）
		const pc = JSON.parse(
			fs2.readFileSync(
				path.join(os.homedir(), ".dsh", "storages", "session_projcache.json"),
				"utf8",
			),
		);
		for (const [sid, tbl] of Object.entries(pc?.tables?.sessions || {}))
			out.set(sid, {
				rows: tbl?.rows || {},
				mtimeMs: 0,
				identity: tbl?.identity,
			});
	} catch {}
	return out;
}
// 审计5 优化（20:59）：projcache 读盘+解析 476KB→加 60s 结果缓存（注入段每轮调用·auto 提炼低频同享）——新建会话首分钟内归属可能滞后，可接受（下轮刷新）
let _pcCache = { at: 0, map: null };
function sessionOrgan(sid) {
	const key = String(sid).startsWith("session-")
		? String(sid)
		: "session-" + String(sid);
	try {
		if (!_pcCache.map || Date.now() - _pcCache.at > 60000) {
			_pcCache = {
				at: Date.now(),
				map: projcacheRows(), // 庚批修②：Map(sid→{rows,identity})——v5 双源（旧 JSON.parse 单文件死指针）
			};
		}
		const tbl =
			_pcCache.map?.get?.(key) ||
			_pcCache.map?.get?.(
				key.startsWith("session-")
					? key.slice("session-".length)
					: "session-" + key,
			); // 第三轮审计修（09-09 pending ruling①）：双形态键兼容——projcache 文件名=session id 原样入键（主窗带前缀·子代理裸 uuid）·两侧形态都试
		// P0 修复（design-approved）：①第一源读 identity.cwd（官方 spec 层·111/111 全在位）——原读 rows.cwd.val 系幻字段（rows 层无此键·两日恒空·二审settled）
		const cwd =
			tbl?.identity?.cwd || tbl?.rows?.cwd?.val || tbl?.rows?.workspace?.val;
		if (cwd) {
			const o = organFromPath(cwd);
			if (o) return o;
		}
	} catch {}
	const fromDir =
		sessionsDirMap().get(key) ||
		sessionsDirMap().get(
			key.startsWith("session-")
				? key.slice("session-".length)
				: "session-" + key,
		); // 第三轮审计自审补（09-09）：兜底路同双形态——收集侧已放行裸 uuid 目录，查询侧单形态则白收（pending ruling①同族残尾）
	if (fromDir) return fromDir;
	return null;
}
const LOW_RELEVANCE_THRESHOLD = 0.1; // 加权后全低于此值 → top1+filtered-low-relevance（防假阴性）

// ══ step：FTS5 外挂索引（双连接共用装配——触发器存于库文件，函数须各连接注册）═══
function ensureFts(dbConn) {
	try {
		// P1 事件钟列（2026-08-26 stepkickoff）：双角色连接启动即备——write 分支 INSERT 与 host 检索 COALESCE 都依赖；
		// 原放nightly patrol段会留「重启后→首nightly patrol前」缺列窗口（INSERT 崩·部署缺陷·沙箱首跑实抓）
		try {
			dbConn.exec("ALTER TABLE memories ADD COLUMN event_at TEXT");
		} catch {}
		// （wave）：write 连接备 vec 表（写时即时嵌入 INSERT 目标——建表兜底·列缺失容错）
		try {
			dbConn.exec(
				`CREATE TABLE IF NOT EXISTS memories_vec (id INTEGER PRIMARY KEY, embedding BLOB NOT NULL, model_version TEXT NOT NULL)`,
			);
		} catch {}
		try {
			dbConn.exec("ALTER TABLE memories_vec ADD COLUMN content_hash TEXT");
		} catch {}
		// 8-31 长尾甲档①（F0-0 引导序破绽·审计settled+他窗  复审成立）：全新库时 edges 表唯建点在nightly patrol段——
		//    首nightly patrol前带边写入全撞 no such table 被吞=显式边/反思链全丢；建表兜底与nightly patrol段同构（含 instruction 列），ALTER 幂等吞。
		try {
			dbConn.exec(`CREATE TABLE IF NOT EXISTS memories_edges (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      src INTEGER NOT NULL, dst INTEGER NOT NULL, edge_type TEXT NOT NULL,
      weight REAL NOT NULL, valid_at TEXT NOT NULL, invalid_at TEXT,
      source_group TEXT, last_seen TEXT NOT NULL, instruction TEXT,
      UNIQUE(src, dst, edge_type))`);
		} catch {}
		// ── 边语义列(design note)：边上承载「为何相关」——
		//    图=衍生层补列：正账仍在条目库·边列只存关系语义（instruction 文本·缺省 NULL 不破存量）。
		try {
			dbConn.exec("ALTER TABLE memories_edges ADD COLUMN instruction TEXT");
		} catch {}
		// P2 三态硬标记（2026-08-26 step wave）：stale_state 空=active/review=待复核/retired=退役——过时≠删除·可见流转；stale_at=标记时点；stale_by=patrol-auto|manual:<space>
		try {
			dbConn.exec("ALTER TABLE memories ADD COLUMN stale_state TEXT");
		} catch {}
		// ── (design note)⑭启动即备：closed_at 闭环时刻列（墓碑时间线·merged/aged 写点同步记）+confidence 信任列（external 分级·默认 1.0）
		try {
			dbConn.exec("ALTER TABLE memories ADD COLUMN closed_at TEXT");
		} catch {}
		try {
			dbConn.exec("ALTER TABLE memories ADD COLUMN valid_to TEXT");
		} catch {} // 双时态（step·2026-08-30）：事件轴失效戳——closed_at=事务轴·valid_to=事件轴（五轴齐）·写路 格式闸同款
		try {
			dbConn.exec(
				"ALTER TABLE memories ADD COLUMN essential INTEGER DEFAULT 0",
			);
		} catch {} // essential 常驻分级（step·essence/CORE 旗标意）：核心记忆不衰+恒注入
		// ── item（design-approved）：spoken_prefix 口语前缀列——LLM 生成「用户随口问法」词组入 FTS 索引侧 ──
		//    治 spoken 层 12.5%（词汇鸿沟：口语查询 vs 书面条目零交叠·沙箱效上限 95.8% 实测）；
		//    SelRoute asymmetry 守门：前缀只进 FTS 不进 embedding/chunks（存储侧词表扩展伤向量路——嵌入面全文不碰）。
		try {
			dbConn.exec("ALTER TABLE memories ADD COLUMN spoken_prefix TEXT");
		} catch {}
		try {
			dbConn.exec(
				"ALTER TABLE memories ADD COLUMN confidence REAL DEFAULT 1.0",
			);
		} catch {}
		try {
			dbConn.exec("ALTER TABLE memories ADD COLUMN stale_at TEXT");
		} catch {}
		try {
			dbConn.exec("ALTER TABLE memories ADD COLUMN stale_by TEXT");
		} catch {}
		// （step·07:30 maintainer）：validated_count ALTER+回填挪至启动即建——原唯在nightly patrol段·重启后→首nightly patrol前消费面（PPR）缺列=边权从未生效
		try {
			dbConn.exec(
				"ALTER TABLE memories ADD COLUMN validated_count INTEGER DEFAULT 1",
			);
		} catch {}
		try {
			dbConn.exec(
				"UPDATE memories SET validated_count = 1 WHERE status='active' AND validated_count IS NULL",
			);
		} catch {} // 回填（NULL→1·幂等·原nightly patrol残段挪此）
		// ── 吸收item 建表（design-approved「记忆直接做」·mem0 captureEvent 本地化）：工具使用遥测 ──
		//    注入采纳率数据底座（综合推理漏点⑨）——ts/tool/caller_space/action/duration_ms/result_count/success/error_kind；
		//    hard rule：遥测写库 try/catch 静默+计数透出（toolUsageErrors），绝不拖累主流程（静默降级透出立法第一活体）。
		try {
			dbConn.exec(
				`CREATE TABLE IF NOT EXISTS tool_usage (id INTEGER PRIMARY KEY AUTOINCREMENT, ts TEXT NOT NULL, tool TEXT NOT NULL, caller_space TEXT, action TEXT, duration_ms INTEGER, result_count INTEGER, success INTEGER, error_kind TEXT)`,
			);
		} catch {}
		// step VEC_MODEL 哨（B级P1·wave建议）：启动比对模型戳——不符 warn 提示重嵌（防跨批漂移·8-28 vec-eval 33.3% 案的同族预防）
		try {
			const vStamp = dbConn
				.prepare("SELECT v FROM organ_meta WHERE k='vec_model_stamp'")
				.get();
			if (!vStamp)
				dbConn
					.prepare(
						"INSERT OR REPLACE INTO organ_meta (k, v) VALUES ('vec_model_stamp', ?)",
					)
					.run(VEC_MODEL);
			else if (String(vStamp.v) !== VEC_MODEL)
				console.warn?.(
					`[living-memory] VEC_MODEL 切换告警：库=${vStamp.v} 运行时=${VEC_MODEL}——旧向量不参与 vec 路·请跑 reembed 全库重嵌`,
				);
		} catch {}
		dbConn.function("legion_bigram", bigramSpace); // 触发器在两角色连接上都可能触发
		dbConn.exec(
			`CREATE VIRTUAL TABLE IF NOT EXISTS memories_fts USING fts5(tokens)`,
		);
		// ── 微型件②：分词器版本戳（源：layered-memory FTS 版本戳·2026-08-21 B2 借鉴）──
		//    token 形态变更时防新旧索引混查——不匹配仅 warn 提示重建，不自动重建（maintainer措辞）。
		const TOKENIZER_VERSION = jieba ? "jieba2-cjk-v1" : "cjk-bigram-v1"; // 刀 3：jieba2=2.x class 实例（词表版）——与旧 bigram 索引不兼容，mismatch warn 后按手册重建
		dbConn.exec(
			`CREATE TABLE IF NOT EXISTS organ_meta (k TEXT PRIMARY KEY, v TEXT NOT NULL)`,
		);
		try {
			const cur = dbConn
				.prepare("SELECT v FROM organ_meta WHERE k = ?")
				.get("tokenizer_version");
			if (!cur)
				dbConn
					.prepare("INSERT OR REPLACE INTO organ_meta (k, v) VALUES (?, ?)")
					.run("tokenizer_version", TOKENIZER_VERSION);
			else if (cur.v !== TOKENIZER_VERSION) {
				console.warn?.(
					`[living-memory] tokenizer version mismatch: index=${cur.v} runtime=${TOKENIZER_VERSION}——请按手册重建 FTS（DELETE FROM memories_fts; INSERT INTO memories_fts(rowid,tokens) SELECT id,legion_bigram(title||' '||content||' '||COALESCE(spoken_prefix,'')) FROM memories）`,
				);
			}
		} catch {}
		// ── item 触发器迁移：DROP+CREATE（IF NOT EXISTS 不更新既有触发器体）——tokens 拼 spoken_prefix； ──
		//    au 监听列加 spoken_prefix：写时/nightly patrol前缀生成后 UPDATE 该列即自动重嵌 FTS 行（零额外刷索引码）。
		dbConn.exec("DROP TRIGGER IF EXISTS memories_fts_ai");
		dbConn.exec("DROP TRIGGER IF EXISTS memories_fts_au");
		dbConn.exec("DROP TRIGGER IF EXISTS memories_fts_ad");
		dbConn.exec(`CREATE TRIGGER memories_fts_ai AFTER INSERT ON memories BEGIN
      INSERT INTO memories_fts(rowid, tokens) VALUES (new.id, legion_bigram(new.title || ' ' || new.content || ' ' || COALESCE(new.spoken_prefix, ''))); END`);
		dbConn.exec(`CREATE TRIGGER memories_fts_au AFTER UPDATE OF title, content, status, spoken_prefix ON memories BEGIN
      DELETE FROM memories_fts WHERE rowid = old.id;
      INSERT INTO memories_fts(rowid, tokens) VALUES (new.id, legion_bigram(new.title || ' ' || new.content || ' ' || COALESCE(new.spoken_prefix, ''))); END`);
		dbConn.exec(`CREATE TRIGGER memories_fts_ad AFTER DELETE ON memories BEGIN
      DELETE FROM memories_fts WHERE rowid = old.id; END`);
	} catch (error) {
		return String(error).slice(0, 100);
	}
	return null;
}

// ── step：memory_write 工具注册器（write 角色/preset 层挂载专用）──
function registerMemoryWrite(tools, dbConn, getSessionId, credResolve, logger) {
	// 审计 D3 修正（14:42）：+logger 参数——模块级函数无 ctx，原两处 ctx.logger 引用系作用域幻引用
	tools.register({
		name: "memory_write",
		description:
			"Write one memory entry. type: fact | decision | todo | lesson. space is required (see stats.autoRegisteredSpaces for the live list). A per-space write gate applies: a window may write to its own space, privileged spaces may write anywhere, and cross-space writes are rejected with an explanation. Reading and searching go through the `memory` tool.",
		// P1-2/C 档软提示（2026-08-24）：open item型 todo 与缺行为位 lesson——softWarn 返回值提示（不阻断·模型可见）
		// cGateHint 为每次 execute 的局部变量（在 type 判定段赋值）
		parameters: {
			type: "object",
			required: ["type", "title", "content", "space"],
			properties: {
				type: {
					type: "string",
					enum: ["fact", "decision", "todo", "lesson"],
					description:
						"记忆类型（fact 事实/decision 决策/todo 待办/lesson 教训）",
				},
				title: { type: "string", description: "一句话标题" },
				content: {
					type: "string",
					description: "正文 1-3 句（含关键数字/路径/依据）",
				},
				space: {
					type: "string",
					enum: SPACES,
					description:
						"Owning space (required): one of the built-in spaces, plus any module directory auto-registered at startup (see stats.autoRegisteredSpaces)", // reflex=极简space空间（BUILTIN_SPACES 同名行注释）·三处出现点之一·删则极简窗写记忆无参数可选（七小件③）
				},
				source: { type: "string", description: "来源会话/文件指针（可选）" },
				event_at: {
					type: "string",
					description:
						"事件时间 YYYY-MM-DD[THH:mm]（可选·P1 事件钟）：条目描述的事件真实发生时点——补记/复盘/未来事件必带，缺省=入册时点",
				},
				valid_to: {
					type: "string",
					description:
						"可选·双时态事件轴失效戳 YYYY-MM-DD[THH:mm]：条目所述状态/事实的失效时点（如待办已毕/机制已废）——到点检索沉底×0.2·与 closed_at（事务轴）互补；缺省=不失效",
				},
				ttl_days: {
					type: "number",
					description:
						"可选·在库 TTL 寿命天数（1-365）：声明状态事实的存活期——写入时换算 valid_to=入册+ttl 并打 ttl 标，**到点nightly patrol自动转 aged 退出检索**（set_state_expiry 意·硬退出）；与 valid_to 显式补记（软沉底 ⏦）区分",
				},
				essential: {
					type: "boolean",
					description:
						"可选·常驻核心标（ORIGIN/CORE 旗标意）：亲判核心知识——decay 恒 1.0 不衰+注入面常驻席（帽 3·按验证度排序）·directive级/根机制类条目适用",
				},
				relatedIds: {
					type: "array",
					items: { type: "number" },
					description:
						"可选·显式关联边（graph-memory relatedSkill 建边意融入）：关联既有条目 id 数组——人知因果/从属关系显式建边（kind=explicit），区别于nightly patrol jaccard 派生边；单写≤5 条",
				},
				relatedNotes: {
					type: "string",
					description:
						"可选·边语义（wave·graph-memory 边带 instruction 意）：relatedIds 的关系说明（为何相关）——写入边 instruction 列",
				},
				edgeKind: {
					type: "string",
					enum: [
						"explicit",
						"solved_by",
						"requires",
						"used_skill",
						"patches",
						"conflicts_with",
					],
					description:
						"可选·语义边型（wave·graph-memory 五语义边意）：显式边关系类型·缺省 explicit",
				},
				closeTargetId: {
					type: "number",
					description:
						"可选·step（09-08 maintainer·召回状态化）：todo 办结闭环参数——携带时双发合一：本条正常入库（title 作销句·建议 type=fact）+ 目标 todo 立即 status='done'+closed_at（L1849 闭环正路工程化）。限本 space 的 todo（option空间闸同构）·幂等（已 done 返原态不报错）。与 edgeKind=solved_by+relatedIds=[目标id] 同发即完整闭环（边供检索判定·状态位供注入过滤）",
				},
			},
		},
		output: {
			schema: { type: "object", additionalProperties: true },
			render: (args, value) => memoryRender(args, value),
		},
		execute: usageWrap(
			"memory_write",
			() => dbConn,
			async (args, exec) => {
				// option：+exec 形参（空间闸 caller 主路·与 host 工具调用约定同源——多传不碍）
				let cGateHint = null; // P1-2/C 档软提示载体（softWarn 返回）
				if (!args.type || !args.title || !args.content) {
					return { error: "memory_write 需要 type + title + content 三个参数" };
				}
				if (!SPACES.includes(args.space)) {
					return {
						error:
							"memory_write 需要 space ∈ [" +
							SPACES.join("/") +
							"]（step：space 必带，缺省拒绝）",
					};
				}
				// ── option空间白名单闸（design-approved·braintask brief 22:10）：写权从按窗收编改按空间收编 ──
				//    Per-space write gate: caller == target space is allowed (own space); the two
//    privileged spaces (brain / memory-organ) may write anywhere; anything else is
//    rejected, and an unresolved caller is rejected too (a window of unknown
//    ownership must not be able to pollute the store).
//    Caller resolution: exec header cwd (same source as the 🧭 attribution line) →
//    getSessionId → sessionOrgan fallback.
				const spaceGateCaller = (() => {
					if (process.env.LEGION_SPACEGATE_OFF) return args.space; // 演练通道：drill 框架无 caller 归属头（非生产面）——4gates/autorecall 回归惯例开关（LEGION_A14_OFF 同族）；spacegate-drill 不设此开关真测闸
					try {
						const cwd =
							exec?.agent?.session?.header?.cwd ||
							exec?.agent?.session?.cwd ||
							exec?.session?.header?.cwd ||
							exec?.cwd ||
							"";
						const byCwd = organFromPath(cwd);
						if (byCwd) return byCwd;
						const sid = getSessionId ? getSessionId() : "";
						return sid ? sessionOrgan(sid) : null;
					} catch {
						return null;
					}
				})();
				const spaceGatePriv =
					spaceGateCaller === "brain" || spaceGateCaller === "memory-organ";
				if (!SPACE_GATE_CONFIGURED) spaceGateCounts.unconfigured += 1; // 降级放行面可观测（⑬：计数不可见＝半合规）
				if (
					!(
						!SPACE_GATE_CONFIGURED || // 闸未配置（无任何目录映射）→ 放行·见上「空间闸未配置降级」段
						(spaceGateCaller &&
							(spaceGatePriv || spaceGateCaller === args.space))
					)
				) {
					spaceGateCounts.blocked += 1;
					if (Date.now() - spaceGateLastWarnAt > 60000) {
						spaceGateLastWarnAt = Date.now();
						try {
							logger?.warn?.(
								"[living-memory] 空间闸拦截：caller=" +
									spaceGateCaller +
									" space=" +
									args.space +
									"（累计 " +
									spaceGateCounts.blocked +
									"）",
							);
						} catch {}
					}
					if (!spaceGateCaller)
						return {
							error:
								"space gate: caller space unresolved — a window of unknown ownership cannot write (tests may set LEGION_SPACEGATE_OFF)",
						};
					return {
						error:
							"space gate: this window belongs to 「" +
							spaceGateCaller +
							"」 but the target space is 「" +
							args.space +
							"」 — cross-space writes are not allowed from this window",
					};
				}
				if (!process.env.LEGION_SPACEGATE_OFF) spaceGateCounts.passed += 1; // 三轮审计改进①：演练 OFF 通道不计数（防观测面虚增）
				// ── P2 fix（09-03 audit）：maintenance window闸补写路覆盖——挂牌期 memory_write 冻结（extract L2622/patrol L3861 两闸同族；原缺口=写路不查旗标）──
				if (fs2.existsSync(SURGERY_FLAG)) {
					surgerySkipWrite += 1;
					return {
						error: "maintenance window挂牌中——写入冻结（摘牌恢复·SURGERY_FLAG 在位）",
						surgery: true,
					};
				}
				const sec = securityCheck(
					stripUrls(String(args.title)),
					stripUrls(String(args.content)) +
						"\n" +
						String(args.source || "") +
						"\n" +
						String(args.relatedNotes || ""),
				); // 8-31 长尾乙档（F0-3）：source/relatedNotes 并入扫描面——原两列模型可控输入裸奔（凭据/指令载荷可经 source/边语义落盘并回流注入面）；件三车病灶3（09-09）：title/content 改检 stripUrls 后形态（与落库面一致——原检原文致长 URL query 含 sk- 族形态误伤·提炼路先 strip 后检同序对齐）·source/relatedNotes 保持原样（独立落库列无 strip·真载荷面）
				if (!sec.ok)
					return {
						error:
							"rejected: " + sec.reason + "（记忆安检：凭据/指令载荷不入库）",
					};
				// ── 四闸·closure gate（2026-08-22）：todo 写入前验真——主题与近期闭环面条目重叠≥3 → 拒绝入库 ──
				//    先 UPDATE status=done 再写销账 fact（闭环正路）；「重开：」前缀=显式重开专项，越过闸门。
				if (String(args.type) === "todo") {
					const t0 = String(args.title || "");
					if (!t0.startsWith("重开：")) {
						const cc = closureCheck(dbConn, args.title, args.content); // step C案：ts 参退役（②同日排除已删·原补传目的随之消）
						if (cc.closed) {
							return {
								error:
									"closure gate拦截：主题与近期闭环条目 #" +
									cc.by +
									"「" +
									cc.byTitle +
									"」重叠 " +
									cc.overlap +
									" 词——已闭环事项不再生成 todo。若确需重开请用「重开：」前缀（如「重开：rc.8 升级专项」）",
							};
						}
						// ── C 档open item词表软提示（2026-08-24 P1-2 细则核准·v2 收窄无「发」族）：open item型建议 fact 装（返回值提示·模型可见）──
						if (CRIT.stashHint.test(t0)) { // C 档open item词表已外置（criteria-rules.default.json·2026-09-12 一车）
							cGateHint =
								"C档提示：todo「" +
								t0.slice(0, 30) +
								"」疑似open item型——建议 fact 装带时点（P1-2 细则·A 档口径）";
						}
					}
				}
				// ── P1-2 行为位软提示（design-approved细则）：lesson 无「行为位：」提示补位（返回值提示·不阻断）──
				//    审计修正 19:03：todo+lesson 双命中时叠加（原互斥覆盖丢一提示）；非 todo/lesson 型不覆盖
				if (
					String(args.type) === "lesson" &&
					!/行为位[:：]/.test(String(args.content || ""))
				) {
					const p12 =
						"P1-2 提示：lesson「" +
						String(args.title || "").slice(0, 30) +
						"」缺行为位——无行为位不算教训（checklist/闸位/口径居一），建议补「行为位：XXX」";
					cGateHint = cGateHint ? cGateHint + "；" + p12 : p12;
				}
				// ── 复犯链回软提示（design-approved「同意」·审计漏③治法）：lesson 无 relatedIds 提醒链回 ──
				//    零阻断（首犯合法无边照常入库）·与 P1-2/C 档同构；治 51.3% 无边面的「忘了链」部分（8-31 hard rule机械闸化）
				if (
					String(args.type) === "lesson" &&
					!(Array.isArray(args.relatedIds) && args.relatedIds.length > 0)
				) {
					const relHint =
						"链回提示：lesson 无 relatedIds——若为复犯必链回原条（relatedIds 参数·复犯链回hard rule 8-31·边入度=真复犯计数）";
					cGateHint = cGateHint ? cGateHint + "；" + relHint : relHint;
				}
				// ── 梁 1 写入面·可召回性闸（design-approved「dispatch」·软提示版）──
				//    缺口背景：五梁唯一整项空白（第四轮审计报告 §二/§五：码面 grep「可召回性/titleQuality/recallGate」零命中）
				//    判据双面：①title 过短（去空白 <8 字）或泛词主导（去泛词/标点后 <8 实义字）——日后召回无抓手
				//              ②content 首句过短（<12 字）——摘要不自含，命中后无上下文可判
				//    阈值标定＝真库 150 条样本实测（title 最短 8 字／首句最短 15 字 ⇒ 双侧零误伤·2026-09-11 本窗标定）
				//    零阻断（「度量不代裁」精神·提示而非拦截）·守写路快径（纯字符串判断·零 DB 查询·不做 tokenize）
				if (args.title || args.content) {
					const _recallTitleRaw = String(args.title || "").trim();
					const _recallTitleTight = _recallTitleRaw.replace(/\s/g, "");
					const _recallCore = _recallTitleTight
						.replace(/[\p{P}\p{S}]/gu, "")
						.replace(CRIT.recallStopwords, ""); // 泛词表已外置（2026-09-12 一车）
					const _recallFirst = String(args.content || "")
						.split(/[。！？!?；;\n]/)[0]
						.replace(/\s/g, "");
					if (_recallTitleTight.length < 8 || _recallCore.length < 8) {
						const rHint =
							"可召回性提示：title「" +
							_recallTitleRaw.slice(0, 24) +
							"」缺检索抓手（过短或泛词主导）——建议补主题词/专名/编号，否则日后搜不回（梁 1 闸·提示不阻断）";
						cGateHint = cGateHint ? cGateHint + "；" + rHint : rHint;
					}
					if (
						String(args.content || "").trim().length > 0 &&
						_recallFirst.length < 12
					) {
						const crHint =
							"可召回性提示：content 首句过短（" +
							_recallFirst.length +
							" 字）——建议首句自含摘要（结论/编号/出处），命中后才有上下文（梁 1 闸·提示不阻断）";
						cGateHint = cGateHint ? cGateHint + "；" + crHint : crHint;
					}
				}
				const ts = nowIso();
				const title = stripUrls(String(args.title));
				const content = stripUrls(String(args.content));
				// ── 梁 1b 写入面·同文幂等闸（design-approved「同意」·创造dispatch）──
				//    案情：session-2bf4aec4 同参 memory_write 21 连发·21 条同题全入库（/5950/5952-5970·jaccard 1.0）
				//    机制缝隙（**2026-09-11 独立审计 D1 订正**）：原记述「掺 ts ⇒ 每发哈希皆异·21 条 checksum 各不相同」**被真库实测否证**——
				//      实测 distinct checksum = **10/21**（5954/5955 同值·5961/5962/5963 同值·5964/5965/5966 同值）；真因＝①`nowIso` **分钟精度**（无秒
				//      ⇒ 同分钟同文哈希相同·跨分钟必异）②**checksum 列无 UNIQUE 约束＋memories 无二级索引**（sqlite_master 索引表为空）⇒ 内容级判据缺位。
				//      本闸（完全一致口径硬拦）**不受此订正影响**·对真案有效性已由独立审计复刻 SQL 实测（命中  ⇒ 应拦）
				//    判据（保守·完全一致口径）：同 space + status='active' + 24h 窗内存在 title+content 去空白后全同条目 → 硬拦拒写
				//    **不采 jaccard≥0.98 近同**：task brief需求③「显式修订（title/content 有变体）不拦」与近同判据天然互斥——0.98 阈值必误伤微改修订
				//    错位互补：宿主熔断闸（loop-breaker 插件）治「同参 10min 窗秒级连发」·本闸治「跨窗/微变参同文」（判据不重叠·叠加零误伤）
				//    逃生口：内容任何变化即成变体放行·换 space 放行·超 24h 窗放行（宁漏勿误——**兜底口径订正（审计 N2）**：nightly patrol①去重 key=`type+title` 且**不自动合并**·只挂 conflicts pending 候maintainer（人工执行）⇒ 实为「候裁兜底」非「自动去重兜底」）
				// ── 【前置搬移·2026-09-11 独立审计 D1 修复】本段原居本条 INSERT 之后的下方（即闸 return 之下）——
				//    审计实测（子代理专项 drill 实锤）：同文闸命中即 return ⇒ 闭环副作用被**静默吞**（目标 todo 仍 active/closed_at=null·receipt只字不提）
				//    ——恰为 09-10 directive 「办结=改状态位·非写叙述」的反面复犯。修法（审计建议 a）：整段上移至同文闸**之前**。
				//    安全性依据：本段只操作**目标**条（不依赖本条 id）·且此点 ts/dbConn/args/logger 已齐备 ⇒ 上移零依赖风险。
				//    语义：本条同文被拦时，目标 todo 的闭环**照常执行**，receipt同时透出「同文拦 + 闭环结果」两条信息。
				// ── step（design-approved·召回状态化）：closeTargetId 办结闭环——L1849 注释承诺「先 UPDATE status=done 再写销账 fact」的工程化 ──
				//    再论证改判④：独立可选参数（不动 type 枚举——type=条目语义 vs close=动作·LangMem/TodoWrite 同业皆独立动作参数）；
				//    携带时双发合一：本条正常入库（title 作销句）+ UPDATE 目标 todo → done+closed_at（照 L5127/L5458 merged/aged 写点同步 closed_at 先例）。
				//    权限=option同构：目标 space 必须等于本次写入 space（写权闸已在前把守·WHERE 三重收窄）；幂等：已 done 返原态不报错。
				let closedTodo = null;
				{
					const closeTarget = Number(args.closeTargetId);
					if (
						args.closeTargetId !== undefined &&
						(!Number.isInteger(closeTarget) || closeTarget <= 0)
					) {
						closedTodo = {
							ok: false,
							reason:
								"closeTargetId 非法（正整数）: " +
								String(args.closeTargetId).slice(0, 20),
						};
					} else if (Number.isInteger(closeTarget) && closeTarget > 0) {
						try {
							const t3 = dbConn
								.prepare(
									"SELECT id, status, closed_at, space, type FROM memories WHERE id = ?",
								)
								.get(closeTarget);
							if (
								!t3 ||
								t3.type !== "todo" ||
								t3.space !== String(args.space)
							) {
								closedTodo = {
									ok: false,
									reason:
										"目标 #" +
										closeTarget +
										" 非 space「" +
										String(args.space) +
										" — close only accepts todos in the same space as this window",
								};
							} else if (t3.status === "done") {
								closedTodo = {
									ok: true,
									id: t3.id,
									alreadyDone: true,
									closedAt: t3.closed_at,
								}; // 幂等返原态（不重戳 closed_at）
							} else if (t3.status !== "active") {
								// 三案修补案1（09-09 approved·三轮审计 P2）：merged（正账在保留方·被合≠办结）/
								// aged（TTL 硬退出·done 可回查=复活）不可 close——复活面根治
								closedTodo = {
									ok: false,
									reason:
										"目标 #" +
										closeTarget +
										" status=" +
										t3.status +
										"——非可闭环态（close 仅限 active todo·merged/aged 不可复活）",
								};
							} else {
								dbConn
									.prepare(
										"UPDATE memories SET status = 'done', closed_at = ? WHERE id = ? AND type = 'todo' AND space = ? AND status = 'active'",
									)
									.run(ts, closeTarget, String(args.space)); // WHERE belt-and-braces收窄 status='active'（防 SELECT→UPDATE 间竞态·三案修补案1）
								closedTodo = {
									ok: true,
									id: t3.id,
									closedAt: ts,
									prior: t3.status,
								};
							}
						} catch (e3c) {
							try {
								logger?.warn?.(
									"[living-memory] close fail: " + String(e3c).slice(0, 60),
								);
							} catch {}
							closedTodo = {
								ok: false,
								reason: "close 执行异常（主条已入库·目标未闭）",
							};
						}
					}
				}
				if (args.title || args.content) {
					const _dupSpace = String(args.space || "memory-organ");
					const _dupNorm = (s) => String(s || "").replace(/\s+/g, "");
					const _dupT = _dupNorm(title);
					const _dupC = _dupNorm(content);
					try {
						const _dupRows = dbConn
							.prepare(
								"SELECT id, ts, title, content FROM memories WHERE space = ? AND status = 'active' AND ts >= ? ORDER BY id DESC LIMIT 500",
							)
							.all(_dupSpace, nowIso24hAgo()); // 取同 space 24h 窗最新 500 条（超 500 只致漏拦不致误拦——安全方向）
						const _dupHit = _dupRows.find(
							(r) =>
								_dupNorm(r.title) === _dupT && _dupNorm(r.content) === _dupC,
						);
						if (_dupHit) {
							dupWriteStat.blocked++;
							// 审计 I2 修（2026-09-11）：非法 ts ⇒ Date.parse→NaN ⇒ _dupMin 曾渲染「NaN 分钟前入库」；加 Number.isFinite 守卫兜底 0
							const _dupMs = Date.now() - Date.parse(String(_dupHit.ts));
							const _dupMin = Number.isFinite(_dupMs)
								? Math.max(0, Math.round(_dupMs / 60000))
								: 0;
							// 审计 D1 修复配套（2026-09-11）：close 段已前置执行 ⇒ receipt须同时透出闭环结果（原闸命中即 return ⇒ 闭环信息被静默吞）
							const _closeNote = closedTodo
								? closedTodo.ok
									? "｜✔ 目标 todo #" +
										closedTodo.id +
										" 已闭环（" +
										String(closedTodo.closedAt || "").slice(0, 16) +
										(closedTodo.alreadyDone ? "·幂等返原态" : "") +
										"）"
									: "｜⚠ 目标 todo 未闭环：" + String(closedTodo.reason || "")
								: "";
							return {
								error:
									"⏭ 同文幂等拦：#" +
									_dupHit.id +
									" 已存在同文条目（space=" +
									_dupSpace +
									"·" +
									_dupMin +
									" 分钟前入库·title「" +
									title.slice(0, 24) +
									"」）——去空白后 title+content 全同，重复写入已拒绝。若确为更新：请写有实质变体的新条目并以 relatedIds 链回 #" +
									_dupHit.id +
									"（同文闸 09-11 kimi 21 连发案·同文重发=注入面污染源）" +
									_closeNote,
							};
						}
					} catch (eDup) {
						// ⑬ 纪律：闸自身故障必有出口——warn（放行不阻断：闸坏不得锁死写路）
						logger?.warn?.(
							`[living-memory] 同文闸查询失败（放行不阻断·⑬ 出口）: ${String(eDup && eDup.message).slice(0, 120)}`,
						);
					}
				}
				// （2026-08-26 修法包·wave 审计）：event_at 格式校验——非法字符串回落 null（防 P2 超龄线 COALESCE 字符串比较语义漂移）
				// E2（design-approved同车）：补月日范围校验——2026-13-99 类越界值不再过闸
				let eventAt = String(args.event_at || "").trim() || null;
				if (
					eventAt !== null &&
					!/^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])(T(0[0-9]|1\d|2[0-3]):[0-5]\d)?$/.test(
						eventAt,
					)
				) {
					logger?.warn?.(
						`[living-memory] event_at 格式非法回落: "${String(args.event_at).slice(0, 30)}"`,
					);
					eventAt = null;
				}
				let validTo = String(args.valid_to || "").trim() || null; // 双时态：同款格式闸
				if (
					validTo !== null &&
					!/^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])(T(0[0-9]|1\d|2[0-3]):[0-5]\d)?$/.test(
						validTo,
					)
				) {
					logger?.warn?.(
						`[living-memory] valid_to 格式非法回落: "${String(args.valid_to).slice(0, 30)}"`,
					);
					validTo = null;
				}
				// 在库 TTL：声明寿命→valid_to 换算+ttl 标（nightly patrol硬退出 aged·与 显式补记软沉底区分）
				const ttlDays = Number(args.ttl_days);
				let ttlMark = false;
				if (Number.isInteger(ttlDays) && ttlDays >= 1 && ttlDays <= 365) {
					const exp = new Date(
						Date.now() + 8 * 3600 * 1000 + ttlDays * 86400000,
					); // 8-31 时区修复（审计 P0-7）：+8h 后取 UTC 分量=上海本地日——原裸 toISOString=UTC 日·本地 0-8 点写入到期日早一天硬退出（全库 +08:00 轴·对照 时态列口径）
					const iso = exp.toISOString().slice(0, 10);
					if (validTo === null) validTo = iso; // 显式 valid_to 优先·TTL 仅补
					ttlMark = true;
				} else if (args.ttl_days !== undefined) {
					logger?.warn?.(
						`[living-memory] ttl_days 非法忽略（1-365 整数）: "${String(args.ttl_days).slice(0, 20)}"`,
					);
				}
				// ── P1② 写闸 source 自动带（design-approved·契约审计 C4/C12 治单）：手写路缺 source 时自动补会话锚，回链不断、写权可审计 ──
				const sidAnchor =
					(typeof getSessionId === "function" && getSessionId()) || "";
				const finalSource =
					String(args.source || "") ||
					(sidAnchor ? "session:" + sidAnchor : "handwrite");
				const info = ttlMark
					? dbConn
							.prepare(
								"INSERT INTO memories (ts, type, title, content, space, source, checksum, event_at, valid_to, stale_state) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
							)
							.run(
								ts,
								String(args.type),
								title,
								content,
								String(args.space || "memory-organ"),
								finalSource,
								sha1(ts + title + content),
								eventAt,
								validTo,
								"ttl",
							)
					: dbConn
							.prepare(
								"INSERT INTO memories (ts, type, title, content, space, source, checksum, event_at, valid_to) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
							)
							.run(
								ts,
								String(args.type),
								title,
								content,
								String(args.space || "memory-organ"),
								finalSource,
								sha1(ts + title + content),
								eventAt,
								validTo,
							); // TTL 声明条预写 stale_state='ttl'（nightly patrol硬退出依据·与 软沉底区分）——run 返回值链式（lastInsertRowid 在 run 结果上·statement 本体无）
				const row = dbConn.prepare("SELECT COUNT(*) AS c FROM memories").get();
				if (args.essential === true) {
					try {
						dbConn
							.prepare("UPDATE memories SET essential = 1 WHERE id = ?")
							.run(Number(info.lastInsertRowid));
					} catch (e22) {
						try {
							logger?.warn?.(
								"[living-memory] essential mark fail: " +
									String(e22).slice(0, 60),
							);
						} catch {}
					}
				} // 常驻核心标（后置 UPDATE·免 INSERT 分支——statement/run 混淆教训）；8-31 长尾乙档（F1-6）：空 catch 补 warn——常驻标静默落空（decay 恒 1.0+注入常驻席失效）自此可见
				// ── 显式关联边（13:49 approved·graph-memory relatedSkill 建边意融入·wave）：人知关联>事后 jaccard
				//    派生——写入时显式声明 relatedTo 既有条目。幂等：同对已存在则刷 last_seen（UPSERT·不重插）。单写≤5。
				//    wave 扩（15:54）：+instruction 边语义列（relatedNotes「为何相关」）+语义边型
				//    （edgeKind 五型·graph-memory 五语义边意）——统一版。
				let relatedLinked = 0;
				try {
					const relIds = Array.isArray(args.relatedIds)
						? Array.from(
								new Set(
									args.relatedIds
										.map(Number)
										.filter(
											(n) =>
												Number.isInteger(n) &&
												n > 0 &&
												n !== Number(info.lastInsertRowid),
										),
								),
							).slice(0, 5)
						: []; // 8-31 长尾乙档（F1-3）：Set 去重——原重复入参 [3,3,3] 同键三次 UPSERT 致 weight 虚增+relatedLinked 虚报
					const EDGE_KINDS = [
						"explicit",
						"solved_by",
						"requires",
						"used_skill",
						"patches",
						"conflicts_with",
					];
					const eKind = EDGE_KINDS.includes(String(args.edgeKind))
						? String(args.edgeKind)
						: "explicit";
					const eNote = String(args.relatedNotes || "").slice(0, 300) || null; // 边语义（为何相关）·300 帽
					if (relIds.length > 0) {
						const chk = dbConn.prepare("SELECT id FROM memories WHERE id = ?");
						const insEdge =
							dbConn.prepare(`INSERT INTO memories_edges (src, dst, edge_type, weight, valid_at, source_group, last_seen, instruction)
            VALUES (?, ?, ?, 1.0, ?, 'manual:' || ?, ?, ?)
            ON CONFLICT(src, dst, edge_type) DO UPDATE SET last_seen = excluded.last_seen, weight = weight + 0.1, instruction = COALESCE(excluded.instruction, memories_edges.instruction)`); // D4 修正沿用：ts 单源；instruction 覆写非空时更新（COALESCE 空不抹旧）
						for (const rid of relIds) {
							if (!chk.get(rid)) continue; // 目标不存在静默跳过（不炸写入主路）
							insEdge.run(
								Number(info.lastInsertRowid),
								rid,
								eKind,
								ts,
								finalSource,
								ts,
								eNote,
							);
							relatedLinked += 1;
						}
					}
				} catch (e2) {
					try {
						logger?.warn?.(
							"[living-memory] edge fail: " + String(e2).slice(0, 60),
						);
					} catch {}
				}
				// ── A-MEM 写入即反思（step·2026-08-30 goal 五连刀·A-MEM 论文 dynamic linking 意）──
				//    新条写入后即时扫同 space 同 type 活跃条·题 token 交叠 jaccard 双门（j≥0.20 且 inter≥2·真库标定）→ 建 'auto-link' 边
				//    （0.4 低档·与 提炼时 'auto-llm' 互补——本刀覆盖手写路即时链）。零 LLM 零嵌入（纯内存扫描·
				//    千条毫秒级）。幂等 UPSERT 同 。write 分支与 host stats 隔离（P1② 教训）——计数走应答透出。
				let reflectLink = null;
				let best19 = null,
					bestJ19 = 0; // 作用域上提（盖戳复用——原 try 块内 let 致平级引用 ReferenceError 被 catch 静吞）
				try {
					const tk19 = (s) => {
						const out = new Set();
						const str = String(s);
						try {
							if (jieba)
								for (const w of jieba.cut(str, true))
									if (w.trim().length >= 2) out.add(w.trim());
						} catch {}
						if (out.size === 0)
							for (const w of str.split(/[^\u2E80-\u9FFF\w]+/))
								if (w.length >= 2) out.add(w);
						return out;
					}; // jieba 分词（regex 分割对 CJK 长串无效——「nightly patrol」≠「nightly patrol九件套终账」·caught in drill）·加载失败 regex 兜底
					const nt19 = tk19(title);
					if (nt19.size >= 2) {
						const cands19 = dbConn
							.prepare(
								"SELECT id, title FROM memories WHERE status='active' AND space = ? AND type = ? AND id != ? LIMIT 2000",
							)
							.all(
								String(args.space),
								String(args.type),
								Number(info.lastInsertRowid),
							);
						best19 = null;
						bestJ19 = 0;
						let bestI19 = 0;
						for (const c of cands19) {
							const ct19 = tk19(c.title);
							let inter19 = 0;
							for (const w of nt19) if (ct19.has(w)) inter19++;
							const j19 = inter19 / (nt19.size + ct19.size - inter19 || 1);
							if (j19 > bestJ19) {
								bestJ19 = j19;
								best19 = c;
								bestI19 = inter19;
							}
						}
						if (best19 && bestJ19 >= 0.2 && bestI19 >= 2) {
							// 双门（真库标定 20:28：j≥0.20 拦单 token 巧合 0.167·inter≥2 拦单词交叠——0.400/4·0.250/2·0.222/2 三正例过）
							dbConn
								.prepare(`INSERT INTO memories_edges (src, dst, edge_type, weight, valid_at, source_group, last_seen, instruction)
              VALUES (?, ?, 'auto-link', 0.4, ?, 'reflect', ?, ?)
              ON CONFLICT(src, dst, edge_type) DO UPDATE SET last_seen = excluded.last_seen`)
								.run(
									Number(info.lastInsertRowid),
									best19.id,
									ts,
									ts,
									"写入即反思: 题交叠 jaccard=" + bestJ19.toFixed(2),
								);
							reflectLink = {
								id: best19.id,
								title: String(best19.title).slice(0, 40),
								j: Number(bestJ19.toFixed(2)),
							};
						}
					}
				} catch (e19) {
					try {
						logger?.warn?.(
							"[living-memory] reflect fail: " + String(e19).slice(0, 60),
						);
					} catch {}
				}

				// ── wave Graphiti 写时矛盾盖戳（design-approved四车·病灶⑥「半天件」论证在案·复用 best19 扫描零重算）──
				//    保守面：否证词在场 AND 数值/版本面有变化 才自动——j≥0.6 且旧条近 72h 同 space 同 type。
				//    动作：旧条 stale_state='superseded-auto'+valid_to=今日（软失效不删）+立 conflicts 复核案(design note)。
				//    语义矛盾不自动（走nightly patrol conflicts 人工）。幂等：stale_state IS NULL 才盖·同对 pending 不重立。LEGION_STAMP_OFF 回退。
				let stampResult = null;
				try {
					if (!process.env.LEGION_STAMP_OFF && best19 && bestJ19 >= 0.6) {
						const oldRow2 = dbConn
							.prepare("SELECT ts, title, content FROM memories WHERE id = ?")
							.get(best19.id);
						const tsOld2 = Date.parse(String(oldRow2 && oldRow2.ts)) || 0;
						if (tsOld2 && Date.now() - tsOld2 <= 72 * 3600 * 1000) {
							const NEG_RE2 =
								/已废|已失效|已被.{0,6}取代|不再|改为|更正|推翻|否证|过期|作废|已撤/;
							const numsOf2 = (t, c) => {
								const out2 = new Map();
								const re2 = /\d+(?:\.\d+)?/g;
								const src2 = String(t) + " " + String(c);
								let m2;
								while ((m2 = re2.exec(src2)))
									if (m2[0].length >= 2)
										out2.set(m2[0], (out2.get(m2[0]) || 0) + 1);
								return out2;
							};
							const negHit2 = NEG_RE2.test(String(title) + String(content));
							let numChanged2 = false;
							if (negHit2) {
								const nn2 = numsOf2(title, content);
								const on2 = numsOf2(oldRow2.title, oldRow2.content);
								for (const k2 of nn2.keys())
									if (!on2.has(k2)) numChanged2 = true;
							}
							if (negHit2 && numChanged2) {
								try {
									dbConn.exec(`CREATE TABLE IF NOT EXISTS conflicts (
                    conflict_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    new_id INTEGER NOT NULL, old_id INTEGER NOT NULL,
                    basis TEXT NOT NULL, evidence TEXT,
                    status TEXT NOT NULL DEFAULT 'pending',
                    decided_at TEXT, decided_by TEXT, created_at TEXT NOT NULL,
                    pre_verdict TEXT, pre_reason TEXT, pre_at TEXT)`);
								} catch {} // F0-0 教训：write 分支 conflicts 建表兜底（含预裁三列）
								const dupStamp = dbConn
									.prepare(
										"SELECT COUNT(*) c FROM conflicts WHERE new_id = ? AND old_id = ? AND status = 'pending'",
									)
									.get(Number(info.lastInsertRowid), best19.id).c;
								if (dupStamp === 0) {
									dbConn
										.prepare(
											"UPDATE memories SET stale_state='superseded-auto', valid_to=? WHERE id=? AND stale_state IS NULL",
										)
										.run(ts.slice(0, 10), best19.id);
									dbConn
										.prepare(
											"INSERT INTO conflicts (new_id, old_id, basis, evidence, status, created_at) VALUES (?, ?, 'stamp-auto', ?, 'pending', ?)",
										)
										.run(
											Number(info.lastInsertRowid),
											best19.id,
											"写时盖戳: j=" + bestJ19.toFixed(2) + " 否证词+数值变化",
											ts.slice(0, 10),
										);
									stampResult = {
										stamped: best19.id,
										j: Number(bestJ19.toFixed(2)),
									};
								}
							}
						}
					}
				} catch (eStamp) {
					try {
						// 七小件①（09-08）：幻引用修正——registerMemoryWrite（L1600-2246）无第一参上下文对象，
						// 原幻引用在双层 catch 内静吞 ReferenceError（warn 从未真出）。签名已有 logger（D3），改调 logger。
						logger?.warn?.(
							"[living-memory] stamp fail: " + String(eStamp).slice(0, 60),
						);
					} catch {}
				}
				// ── 写时即时嵌入（13:49 approved·graph-memory syncEmbed 意融入·wave）：fire-and-forget 异步
				//    不阻塞返回（graph-memory void recaller.syncEmbed 同法）；治「新directive条目要等nightly patrol才有向量」——写后即刻
				//    可被向量路召回。失败静默（nightly patrol惰性补嵌仍兜底）。凭据 60s 缓存共享 vecCredCache（同源单点）。
				try {
					if (credResolve && !process.env.LEGION_A14_OFF) {
						void (async () => {
							try {
								if (!vecCredCache.v || Date.now() - vecCredCache.t > 60000) {
									const c = await credResolve("EMBEDDING_BAILIAN_KEY");
									if (!c || !c.value) return;
									vecCredCache = { v: c.value, t: Date.now() };
								}
								const [emb] = await embedOnce([
									(title + " " + content).slice(0, 1500),
								]);
								if (emb) {
									dbConn
										.prepare(
											"INSERT OR REPLACE INTO memories_vec (id, embedding, model_version, content_hash) VALUES (?, ?, ?, ?)",
										)
										.run(
											Number(info.lastInsertRowid),
											f32ToBlob(emb),
											VEC_MODEL,
											crypto
												.createHash("md5")
												.update(String(title + " " + content).slice(0, 1500))
												.digest("hex"),
										); // 8-31 移植族修复⑥：content_hash 与nightly patrol门同源（md5·title+' '+content 前1500·L2407 同式）——原 sha1(content) 双不一致（算法+基准）致 条当夜必冗余重嵌
									// ── 粒度手术双写过渡（2026-08-31 item2）：chunks 句级嵌入同步嵌（旧表 v4 保留给 fallback/dedup）──
									try {
										dbConn.exec(
											`CREATE TABLE IF NOT EXISTS memories_vec_chunks (id INTEGER NOT NULL, seq INTEGER NOT NULL, chunk_text TEXT, embedding BLOB NOT NULL, model_version TEXT NOT NULL, PRIMARY KEY (id, seq, model_version))`,
										);
										const chunks = chunkMemory(title, content);
										if (chunks.length) {
											const cembs = await embedOnce(chunks, VEC_CHUNK_MODEL); // 09-03 裁①：写时 chunks 嵌入同接 qwen3.7
											const cins = dbConn.prepare(
												"INSERT OR REPLACE INTO memories_vec_chunks (id, seq, chunk_text, embedding, model_version) VALUES (?, ?, ?, ?, ?)",
											);
											for (
												let ci = 0;
												ci < chunks.length && ci < cembs.length;
												ci++
											)
												cins.run(
													Number(info.lastInsertRowid),
													ci,
													chunks[ci],
													f32ToBlob(cembs[ci]),
													VEC_CHUNK_MODEL,
												);
										}
									} catch {}
								}
							} catch {}
						})();
					}
				} catch {}
				// ── item 写时前缀生成（design-approved）：fire-and-forget 异步（同法·不阻塞返回）──
				//    生成口语前缀 UPDATE spoken_prefix——au 触发器自动重嵌 FTS（监听列已含）。失败静默=nightly patrol回填段兜底。
				//    双特权/8 窗挂载全走此路（写权空间闸在前已判）。env LEGION_SPOKEN_OFF 回退。
				try {
					if (credResolve && !process.env.LEGION_SPOKEN_OFF) {
						void (async () => {
							try {
								if (!spCredCache.v || Date.now() - spCredCache.t > 60000) {
									const c = await credResolve("DEEPSEEK_MEMORY_KEY");
									if (!c || !c.value) return;
									spCredCache = { v: c.value, t: Date.now() };
								}
								const sp = await llmSpokenPrefix(title, content);
								if (sp)
									dbConn
										.prepare(
											"UPDATE memories SET spoken_prefix = ? WHERE id = ?",
										)
										.run(sp, Number(info.lastInsertRowid));
							} catch {}
						})();
					}
				} catch {}
				// Single-subject wording gate: warn-level tagging (the entry is still written)
				const bodyHits = softBodyWarn(title, content);
				return {
					id: Number(info.lastInsertRowid),
					ts,
					count: Number(row.c),
					...(relatedLinked > 0 ? { relatedLinked } : {}),
					...(reflectLink ? { reflectLink } : {}),
					...(stampResult ? { stampResult } : {}), // wave 盖戳透出
					...(closedTodo ? { closedTodo } : {}), // step：办结闭环结果透出（ok/alreadyDone/closedAt·模型可见闭环证据）
					...(bodyHits.length > 0
						? {
								softWarn:
									"wording gate hit: 「" +
									bodyHits.join("、") +
									"」 — rewrite using single-subject wording",
							}
						: {}),
					...(cGateHint
						? {
								softWarn: [
									bodyHits.length > 0
										? "wording gate hit: 「" + bodyHits.join("、") + "」"
										: null,
									cGateHint,
								]
									.filter(Boolean)
									.join("；"),
							}
						: {}),
				};
			},
		),
	});
}

module.exports = {
	name: "dsh-living-memory",
	inject: ["tools", "credentials", "systemPrompt"],
	// ── step：写分权双角色。role='write'（preset 层挂载行专用）只注册 memory_write 工具；
	//    role='host'（默认，host 层挂载行）注册只读 memory 工具 + 内部管道（nightly patrol/提炼/注入/面板）。
	//    挂载矩阵：memory-zh / standard-zh 组合文件挂 write 行；其余space只继承 host 行=只读。──
	apply(ctx, config) {
		const role = (config && config.role) || "host";
		const tools = ctx.tools;
		if (role === "write") {
			// ── write 角色：仅 memory_write 工具（preset 层挂载，只对该 preset 的会话可见）──
			let writeSessionId = ""; // P1②：write 分支自持会话锚（sessionOfLastTurn 在 host 分支·此处不可达——作用域陷阱 07:19 实抓）
			try {
				ctx.on &&
					ctx.on("session/event", (session) => {
						try {
							if (session && typeof session.id === "string")
								writeSessionId = session.id;
						} catch {}
					});
			} catch {}
			let dbW;
			try {
				dbW = new DatabaseSync(DB_PATH);
				dbW.exec("PRAGMA journal_mode = WAL");
				dbW.exec("PRAGMA busy_timeout = 5000"); // 2026-08-24 并发保险：写撞写排队5s（默认0ms抛SQLITE_BUSY·多窗并发写炸弹）
			} catch (error) {
				ctx.logger?.warn?.(
					`[living-memory/write] db open failed: ${String(error)}`,
				);
				return;
			}
			ctx.effect(() => () => {
				try {
					dbW.close();
				} catch {}
			});
			const ftsErrW = ensureFts(dbW); // write 连接的 INSERT 同样触发触发器→须注册函数
			if (ftsErrW)
				ctx.logger?.warn?.(`[living-memory/write] fts init failed: ${ftsErrW}`);
			registerMemoryWrite(
				tools,
				dbW,
				() => writeSessionId,
				ctx.credentials?.resolve?.bind(ctx.credentials),
				ctx.logger,
			);
			return;
		}
		const credentials = ctx.credentials;
		const systemPrompt = ctx.systemPrompt;

		// ── 数据库（WAL：读不阻塞写，写串行——多窗口并发安全）──
		let db;
		try {
			db = new DatabaseSync(DB_PATH);
			db.exec("PRAGMA journal_mode = WAL");
			db.exec("PRAGMA busy_timeout = 5000"); // 2026-08-24 并发保险（读连接同配）
			db.exec(`CREATE TABLE IF NOT EXISTS memories (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts TEXT NOT NULL,
        type TEXT NOT NULL,
        title TEXT NOT NULL,
        content TEXT NOT NULL,
        space TEXT NOT NULL DEFAULT 'memory-organ',
        source TEXT,
        status TEXT NOT NULL DEFAULT 'active',
        checksum TEXT NOT NULL
      )`);
			// ── step：图谱共现组表（新增表·memories 主表零变更；回滚=DROP 表）──
			db.exec(`CREATE TABLE IF NOT EXISTS memories_cooccur (
        id_a INTEGER NOT NULL,
        id_b INTEGER NOT NULL,
        score REAL NOT NULL,
        PRIMARY KEY (id_a, id_b)
      )`);
			// ── 第 2.step：哨基线持久化（重启不丢；哨自身状态不占 memories 表）──
			db.exec(
				`CREATE TABLE IF NOT EXISTS organ_meta (k TEXT PRIMARY KEY, v TEXT NOT NULL)`,
			);
			// ── 第 1.step：自注册发现日志（映射表外新目录）──
			for (const [name, sp] of autoRegistered)
				ctx.logger?.warn?.(
					`[living-memory] organ auto-registered: ${name} → ${sp}`,
				);
		} catch (error) {
			ctx.logger?.warn?.(`[living-memory] db open failed: ${String(error)}`);
			return;
		}
		ctx.effect(() => () => {
			try {
				db.close();
			} catch {}
		});

		// ── step：FTS5 装配 + 存量回填（幂等：FTS 空则回填未入索引行）──
		const ftsErr = ensureFts(db);
		if (ftsErr)
			ctx.logger?.warn?.(`[living-memory] fts init failed: ${ftsErr}`);
		else {
			try {
				const ftsCount = db
					.prepare("SELECT COUNT(*) AS c FROM memories_fts")
					.get();
				if (Number(ftsCount.c) === 0) {
					const n = db
						.prepare(
							"INSERT INTO memories_fts(rowid, tokens) SELECT id, legion_bigram(title || ' ' || content || ' ' || COALESCE(spoken_prefix, '')) FROM memories WHERE status != 'deleted'", // 09-03 P1 修（audit）：回填补 spoken_prefix——与触发器 L1301/1304 同源（重建即丢 spoken 词面根治）,
						)
						.run();
					ctx.logger?.warn?.(
						`[living-memory] fts backfill: ${Number(n.changes)} rows`,
					);
				}
			} catch (error) {
				ctx.logger?.warn?.(
					`[living-memory] fts backfill failed: ${String(error).slice(0, 80)}`,
				);
			}
		}

		// ── HTTP 快照端点：面板的数据通道（现查库·重启不丢）──
		const webServer = ctx.get("webServer");
		if (webServer !== undefined) {
			const stopRoute = webServer.register({
				kind: "exact",
				path: "/living-memory/memory-snapshot",
				handler: (req, res) => {
					try {
						const rows = db
							.prepare(
								"SELECT id, ts, type, title, space FROM memories WHERE status = 'active' ORDER BY id DESC LIMIT 30",
							)
							.all();
						const total = db
							.prepare(
								"SELECT COUNT(*) AS c FROM memories WHERE status = 'active'",
							)
							.get();
						res.writeHead(200, {
							"Content-Type": "application/json; charset=utf-8",
						});
						res.end(JSON.stringify({ entries: rows, total: Number(total.c) }));
					} catch {
						res.writeHead(500, { "Content-Type": "application/json" });
						res.end('{"error":"db"}');
					}
				},
			});
			ctx.effect(() => stopRoute);
		}

		// ── HTTP token spend端点：token 消耗仪表（2026-08-19 C 线·读 session_projcache 聚合）──
		if (webServer !== undefined) {
			const stopTokenRoute = webServer.register({
				kind: "exact",
				path: "/living-memory/token-snapshot",
				handler: (req, res) => {
					try {
						// 庚批修①（2026-09-08）：projcacheV5 双源（新目录优先——旧单文件 09-05 停更=面板冻结 9-4 根因）
						const sessions = projcacheRows();
						const grand = {
							uncachedIn: 0,
							cacheRead: 0,
							cacheWrite: 0,
							out: 0,
							total: 0,
						};
						const rows = [];
						for (const [sid, ent] of sessions.entries()) {
							const r = ent.rows || {};
							const tu = r.tokenUsage?.val?.totals;
							if (!tu) continue;
							const cp = r.contextPressure?.val || {};
							const row = {
								sid: sid.slice(0, 16),
								lastAt: ent.mtimeMs || 0, // v5 面：文件 mtime 代活跃时（旧 sessionListMetadata 已无）
								title: String((r.title && r.title.val) || "").slice(0, 28),
								turns: r.tokenUsage?.val?.last?.turn || 0, // v5 面：末轮号代 sessionStats.turns
								uncachedIn: tu.uncachedInputTokens || 0,
								cacheRead: tu.cacheReadTokens || 0,
								cacheWrite: tu.cacheWriteTokens || 0,
								out: tu.outputTokens || 0,
								total:
									(tu.uncachedInputTokens || 0) +
									(tu.cacheReadTokens || 0) +
									(tu.cacheWriteTokens || 0) +
									(tu.outputTokens || 0),
								pressurePct: cp.contextWindow // 压力假零兜底（09-08 brain报障·失败请求 pressureTokens 被写 0）：pressure=0||<surface 时以 surfaceTokens÷contextWindow 作实测下限+标注
									? (cp.pressureTokens || 0) === 0 ||
										(cp.pressureTokens || 0) < (cp.surfaceTokens || 0)
										? Math.round(
												((cp.surfaceTokens || 0) / cp.contextWindow) * 100,
											)
										: Math.round(
												((cp.pressureTokens || 0) / cp.contextWindow) * 100,
											)
									: null,
								legacyWindow:
									cp.contextWindow && cp.contextWindow < 500000
										? true
										: undefined, // 审计修②：面板老分母标注（262144 型与现役 1M 不可比·与哨同构）
								pressureFloor:
									cp.contextWindow &&
									((cp.pressureTokens || 0) === 0 ||
										(cp.pressureTokens || 0) < (cp.surfaceTokens || 0))
										? "下限·pressure 缺失（上次请求失败）"
										: undefined, // 信号位：tokenUsage.last 全 0 同源判据（brain  实证）
							};
							grand.uncachedIn += row.uncachedIn;
							grand.cacheRead += row.cacheRead;
							grand.cacheWrite += row.cacheWrite;
							grand.out += row.out;
							grand.total += row.total;
							rows.push(row);
						}
						rows.sort((a, b) => (b.lastAt || 0) - (a.lastAt || 0)); // 最近活跃优先：lastPromptAt 为真活跃时间戳（seq 是每会话各自计数，非全局，不可用作排序）
						grand.sessions = rows.length;
						res.writeHead(200, {
							"Content-Type": "application/json; charset=utf-8",
						});
						res.end(JSON.stringify({ grand, top: rows.slice(0, 10) }));
					} catch {
						res.writeHead(500, { "Content-Type": "application/json" });
						res.end('{"error":"projcache"}');
					}
				},
			});
			ctx.effect(() => stopTokenRoute);
		}

		// ── HTTP sentry endpoint: newest markdown report in LEGION_SENTRY_DIR (panel data channel) ──
		if (webServer !== undefined) {
			const stopSentryRoute = webServer.register({
				kind: "exact",
				path: "/living-memory/sentry-snapshot",
				handler: (req, res) => {
					try {
						const dir =
							process.env.LEGION_SENTRY_DIR ||
							path.join(os.homedir(), ".dsh", "dsh-living-memory", "sentry");
						const files = fs2
							.readdirSync(dir)
							.filter((f) => f.endsWith(".md"))
							.sort();
						if (files.length === 0) {
							res.writeHead(200, {
								"Content-Type": "application/json; charset=utf-8",
							});
							res.end(JSON.stringify({ date: null, content: null }));
							return;
						}
						const latest = files[files.length - 1];
						const content = fs2.readFileSync(path.join(dir, latest), "utf8");
						res.writeHead(200, {
							"Content-Type": "application/json; charset=utf-8",
						});
						res.end(JSON.stringify({ date: latest.slice(5, 15), content }));
					} catch {
						res.writeHead(500, { "Content-Type": "application/json" });
						res.end('{"error":"sentry"}');
					}
				},
			});
			ctx.effect(() => stopSentryRoute);
		}

		// ── 记忆注入：living memory最新记忆进每窗口系统提示（order 300 尾部·低频变化·前缀缓存友好）──
		// Injection-hook vocabulary (completion words such as done/closed are deliberately excluded)
		// AUDIT-FIX-20260912（修复车·审计 C 路第 1 项真漏项）：字面量并入三级链第 10 键 `opsHook`
		//   （源仓词面在 ~/.dsh 数据文件/criteria-rules.default.json·公开面见 CRITERIA_PUBLIC）。
		//   动因：该判据曾被净化链整体英文化 + 加 i 而**未披露**（C 路实测产物＝/in progress|…/i）⇒
		//   字面量留码面＝每次净化都再改一次行为且无人知；外置后 code 面零机改。
		const OPS_HOOK_RE = CRIT.opsHook;
		if (systemPrompt !== undefined) {
			// 案C(ops note)：section→context 迁移——section 路 scope 层断链（死），
			// context 投影路=每轮 materialize（活·autorecall 同位）；接口同构（types L47-74）·
			// 根治B 三链归属+案A 兜底保留不动。order 300 沿用（context 亦按 order 排序）。
			const stopSection = systemPrompt.context({
				name: "legion-memory-recall",
				order: 300,
				text: (context) => {
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
					try {
						// 根治B（design-approved·kickoff 22:46）：归属源改 assemble context 直取——
						// 恢复型会话 constructor seed 不发 firehose（宿主 dsh-session L342-349 注释铁证）致
						// sessionOfLastTurn 闭包恒 ''；assemble 上下文每步必真（agent-loop L1025 实证
						// context.agent.session.id/header.cwd 可用）。三链：cwd 直推 > sid > 闭包兜底。
						const sid = context?.agent?.session?.id || sessionOfLastTurn;
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
										"SELECT id, type, title, content, ts FROM memories WHERE status='active' AND type='todo' AND space = ? ORDER BY id DESC LIMIT 6",
									)
									.all(injectCallerSpace),
								todoCap >= 4 ? 2 : 1,
							); // 本空间席（审计修正 20:58：残留 callerSpace 致恒空已修；件②池保 DESC·ASC 在 todos 组装后做）
							pickTodo(
								db
									.prepare(
										"SELECT id, type, title, content, ts FROM memories WHERE status='active' AND type='todo' AND space='global' ORDER BY id DESC LIMIT 4",
									)
									.all(),
								todoCap >= 4 ? 2 : 1,
							); // global 席（硬配额·directive型不被本空间挤占；件②池保 DESC）
						} else {
							pickTodo(
								db
									.prepare(
										"SELECT id, type, title, content, ts FROM memories WHERE status='active' AND type='todo' ORDER BY id DESC LIMIT 12",
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
						const ownRows = injectCallerSpace
							? db
									.prepare(
										"SELECT id, type, title, space, COALESCE(event_at, ts) AS eff_ts FROM memories WHERE status='active' AND type IN ('fact','lesson','decision') AND space = ? AND COALESCE(event_at, ts) >= ? ORDER BY id DESC LIMIT " +
											(factCap - 1) +
											"",
									)
									.all(injectCallerSpace, past48h)
									.reverse()
							: []; // P1 事件钟锚：鲜度按事件时点判（补记的旧事不占 48h 席）
						const essRows = injectCallerSpace
							? (() => {
									try {
										return db
											.prepare(
												"SELECT id, type, title FROM memories WHERE status='active' AND essential=1 AND space IN (?, 'global') ORDER BY validated_count DESC LIMIT 3",
											)
											.all(injectCallerSpace);
									} catch {
										return [];
									}
								})()
							: []; // 常驻核心席（帽 3·按验证度·不占 48h 闸席——essence 常驻意）
						const gPool = db
							.prepare(
								"SELECT id, type, title, space, COALESCE(event_at, ts) AS eff_ts FROM memories WHERE status='active' AND type IN ('fact','lesson','decision') AND space='global' AND COALESCE(event_at, ts) >= ? AND NOT (title LIKE '%收官%' OR title LIKE '%全绿%' OR title LIKE '%完成%' OR title LIKE '%闭环%') ORDER BY id DESC LIMIT " +
									(factCap + 1) +
									"",
							)
							.all(past48h); // P1 事件钟锚（池保 DESC：slice(0,n) 取最新 n 原语义·件② ASC 在组装处做）：同 ownRows 口径；8-31 长尾乙档（F2-7）：LIMIT 2→3——原池深 2 与 slice 上限 3 打架致 ownRows=0 时第三席恒空（本空间静默窗口 global 视野少 1 条）
						const rows = [
							...ownRows, // 件② ASC：本空间席旧→新（新条目尾部追加）
							...gPool.slice(0, 3 - ownRows.length).reverse(), // 件②：global 池 DESC 取最新 n·再转 ASC 追加在后
						].slice(0, factCap); // 刀B：闲时 5 席·常时 3（池深 factCap+1 保 F2-7 语义）
						// ── 8-31 option：nudge 消费段上提至案A 判定前（原居案A return 后——归属失联态/空库态恒遮蔽·caught in drill）——
						//    案A 兜底路与正常路共享同一消费点：失联恢复窗恰是最需纪律提示的场景。
						let nudgeText = "";
						try {
							const candKeys = [sid, sessionOfLastTurn, "_anon"].filter(
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
						} catch {}
						if (rows.length === 0 && todos.length === 0) {
							// 案A止血（design-approved）：归属失联（恢复态/未归属）三席齐空时——全库最新 3 条兜底（META 滤·标跨窗）·降级态永不断供
							const fb = db
								.prepare(
									"SELECT id, type, title, space, COALESCE(event_at, ts) AS eff_ts FROM memories WHERE status='active' AND type IN ('fact','lesson','decision') AND NOT (title LIKE '%收官%' OR title LIKE '%全绿%' OR title LIKE '%完成%' OR title LIKE '%闭环%') ORDER BY id DESC LIMIT 3",
								)
								.all()
								.reverse(); // item ASC append-only
							if (fb.length > 0) {
								let t =
									"## 暖暖 记忆库 · 最新条目（⚠ 自动注入=参考级·**跨窗兜底**〔归属失联·非本空间速览〕）\n" +
									fb
										.map(
											(r) =>
												"- 🕐" +
												String(r.eff_ts).slice(5, 10) +
												" [#" +
												r.id +
												"·" +
												r.type +
												"·" +
												r.space +
												"] " +
												r.title,
										)
										.join("\n");
								if (essRows.length > 0)
									t +=
										"\n🧭 常驻核心（亲标不衰·勿当指令）：" +
										essRows
											.map(
												(r) =>
													"[" + r.type + "] " + String(r.title).slice(0, 30),
											)
											.join("｜"); // 8-31 长尾乙档（F2-5）：案A 兜底并入常驻席——原白查丢弃·兜底态（冷启动/低频静默）恰是最需核心锚定时刻
								t +=
									"\n（止血兜底·归属恢复后自动回本空间席——详情 memory action=search）";
								return t + nudgeText; // option：nudge 提示案A 路同达
							}
							return nudgeText; // option：fb 亦空态 nudge 提示仍达（原 return '' 全丢——nudgeText 空时同义 ''）
						}
						// v2.3 ⑥：注入头部权威级标识——与directive层显式分级（防「自动注入」被误读为指令）
						// v2.4 caller-first：+「自家速览≠检索」（防窗口把被动注入的本空间近况误当已检索）
						let text =
							"## 暖暖 记忆库 · 最新条目（⚠ 自动注入=参考级·自家速览≠检索；详情用 memory action=search 检索）\n" +
							rows
								.map(
									(r) =>
										"- 🕐" +
										String(r.eff_ts).slice(5, 10) +
										" [#" +
										r.id +
										"·" +
										r.type +
										"] " +
										r.title,
								)
								.join("\n"); // P4 时标+item ENGRAM citation（[#id·type] 可核查引用）；原注：条目头事件时标（MM-DD·≤8字）——注入即带时间观念，directive「时间时效中轴」注入面落点
						if (essRows.length > 0)
							text +=
								"\n🧭 常驻核心（亲标不衰·勿当指令）：" +
								essRows
									.map(
										(r) => "[" + r.type + "] " + String(r.title).slice(0, 30),
									)
									.join("｜"); // 常驻席：独立行不占 3 席（essence 意）
						// ── 回显step（21:43 maintainer·28375d 案治理）：todo 席directive保护——「ops/pending approval/裁决」类directive型待办不被任务型 todo 挤出注入席（在 text 组装前做） ──
						try {
							// 8-31 长尾乙档（F2-4）：RE 与 LIKE 提同一词源——原 RE 7 词/LIKE 5 词缺「申请件/裁决」致该两词directive todo 顶替通道失效；>=2 放宽 >=1（单席态替换不扩预算）
							const ORD_WORDS = [
								"申请件",
								"pending approval",
								"pending ruling",
								"awaiting approval",
								"awaiting approval",
								"裁决",
							]; // 0.2.0 余件 ORD 泛化（09-10 approved）：英文会话directive型 todo 顶替通道——英文分支短语级（防 request 泛词误伤）·与三词表英文化同型
							const ORD_WORDS_EN = [
								"\\bawaiting approval\\b",
								"\\bpending approval\\b",
								"\\bapproval request\\b",
								"\\bfor approval\\b",
								"\\bsubmitted for review\\b",
							];
							const ORD_TODO_RE = new RegExp(
								ORD_WORDS.concat(ORD_WORDS_EN).join("|"),
								"i",
							); // i flag：英文短语大小写不敏感（中文词不受影响）·SQLite LIKE 对 ASCII 天然不敏感=SQL 侧免 COLLATE
							if (
								todos.length >= 1 &&
								!todos.some((x) => ORD_TODO_RE.test(x.title))
							) {
								const ord = db
									.prepare(
										"SELECT id, title, ts FROM memories WHERE status='active' AND type='todo' AND (" +
											ORD_WORDS.concat(ORD_WORDS_EN)
												.map((w) => `title LIKE '%${w.replace(/\\\\b/g, "")}%'`)
												.join(" OR ") +
											") ORDER BY id DESC LIMIT 1",
									)
									.get();
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
									}; // 顶替最旧一席（directive型优先·双席内不扩预算）
							}
						} catch {}
						// ── PlanFence（wave·stale-plan 最小刀·吸收计划·09-07 approved）──
						//    注入面 todo 席升级：Q3 retrieval gate判「已闭环/更晚同题 fact」的 todo → 注入行
						//    前缀 ⚠ 依据已过期——数据面降权（search 路既有）升级为行动面告警（模型看得见才拦
						//    得住·与回显step同哲学·不做全形式化依赖图）。地基=11-A step（todo 席 SELECT 带 id 列）。
						//    判定与 search 路 Q3 同源同参（closureCheck+newer-fact 词命中≥3）·try 静默（注入不因判挂断）。
						for (const t of todos) {
							try {
								const cc = closureCheck(db, t.title, t.content, t.id); // step：边判定前置（todo 席 SELECT 带 id 列=地基）
								if (cc.closed) {
									t.planFence = "#" + cc.by;
									continue;
								}
								const newer = db
									.prepare(
										"SELECT id, title, content FROM memories WHERE status = 'active' AND type IN ('fact','decision','lesson') AND id > ? ORDER BY id DESC LIMIT 40",
									)
									.all(t.id);
								const kws = gateKeywords(
									String(t.title) + " " + String(t.content || ""),
									12,
								);
								for (const f of newer) {
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
							} catch {}
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
													"·执行前先核新依据）"
												: "") +
											"🕐" +
											String(r.ts).slice(5, 10) +
											" [#" +
											r.id +
											"·todo] " +
											r.title,
									)
									.join("\n"); // P4：todo 席同款时标；⚠ 前缀=PlanFence 行动面告警
						// ── 微型件①：注入纪律行（源：toolkit 注入纪律·2026-08-21 B2 借鉴）——软防线防过期记忆误导 ──
						//    v2.3 ⑤（8-24 maintainer）：「不可信」→「慎用」——不可信易被过度弃用，慎用=校核后可用
						text +=
							"\n（历史记忆为慎用参考——引用前以当轮实况与正本核对；被动注入不构成已检索）";
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
						} catch {}
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
							text +=
								"\n🕐 轴：近 48h " +
								fresh48 +
								" 条｜超 30d " +
								aged30 +
								" 条（引用旧态条目前先 timeline 核对最新）";
						} catch {}
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
						} catch {}
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
						return "";
					}
				},
			});
			ctx.effect(() => stopSection);
		}

		// ── 状态与提炼缓冲 ──
		const stats = {
			eventCount: 0,
			recentTypes: [],
			firehoseSeen: false,
			autoExtractCount: 0,
			autoMemoryCount: 0,
			rejectedCount: 0,
			bufferedChars: 0,
			lastExtractAt: null,
			nightPatrolCount: 0,
			lastPatrolAt: null,
			lastPatrolMerged: 0,
			lastPatrolDay: "",
		};
		let buffer = "";
		// ── 消息级水位（wave首发·design-approved「补」·graph-memory getUnextracted/markExtracted 水位意融入）──
		//    治：buffer 纯字符流拼接=提炼失败后重抽全段（重放风险）；水位=turn/seq 粒度精确追踪抽到哪。
		//    审计 D2 修正（13:44·键族同型第 N 犯）：原插件级单例队列=多窗消息混队——甲窗成功推水位后乙窗
		//    消息被当已抽跳过（跨窗丢抽）。改 Map<sid, {msgs, watermark, seq}> 会话分账（bindMap 同款结构）。
		//    sid 键源=session.id（firehose 主键·L996 sessionOfLastTurn 同源）；env LEGION_A12_OFF 一键回退旧 buffer 路。
		const pendingBySid = new Map(); // sid -> { msgs: [{turn,role,text,seq}], watermark: number, seq: number, lastTs: number }
		const A12_OFF = !!process.env.LEGION_A12_OFF;
		// ── 提炼缓冲落盘持久化（wave·23:12 approved·治 41 发重启清缓冲·extract 四连空手案）──
		//    内存分账序列化落 organ_meta('extract_buffer')：启动恢复+节流落盘（5s）+成功/清账强制落盘。
		//    帽：落盘总量 2000 条（超丢最旧会话账）·LEGION_A25_OFF 回退纯内存态。
		const A25_OFF = !!process.env.LEGION_A25_OFF;
		let a25LastFlush = 0,
			a25FlushWarnAt = 0; // 后者=8-31 F3-4 warn 节流戳（60s 免刷屏·与 vecChannelErrors 同款）
		function a25Flush(force) {
			try {
				if (A25_OFF) return;
				// 审计 D15 修正（23:20）：节流 5s→60s——全量序列化最坏 6MB（2000×3000 字）·5s 节流=72MB/min 写入放大；
				// 60s 节流（成功/清账仍强制即时）=最坏 6MB/min 可容·丢失窗 60s 内的消息由下次落盘补（重启丢 ≤60s 尾巴可容）。
				if (!force && Date.now() - a25LastFlush < 60000) return;
				// 8-31 长尾甲档③（F3-4）：节流戳挪写库成功后——原写库前消耗·失败则 60s 窗白耗重试再延一拍（丢失窗 ≤60s→120s）
				let total = 0;
				for (const a of pendingBySid.values()) {
					a.msgs = a.msgs.filter((m) => m.seq > a.watermark);
					total += a.msgs.length;
				} // 8-31 锈面修（审计补盲面）：帽口径=未抽段——修剪已提取消息（原全量计帽提前触发+dump 虚胖 955KB；已提取消息对恢复无价值）
				for (const [k, a] of pendingBySid)
					if (a.msgs.length === 0) pendingBySid.delete(k); // 空账顺手清（全已提炼·释放 Map 位·不占帽）
				while (total > 2000) {
					// 总量帽：丢最旧会话账（lastTs 最小者）——8-31 修：丢未抽段计数透出（原静默丢账暗流失·对齐 I6a 留账精神的观测面）
					let oldestK = null,
						oldestT = Infinity;
					for (const [k, a] of pendingBySid)
						if ((a.lastTs || 0) < oldestT) {
							oldestT = a.lastTs || 0;
							oldestK = k;
						}
					if (!oldestK) break;
					stats.a25DroppedUnextracted = (stats.a25DroppedUnextracted || 0) + 1;
					total -= pendingBySid.get(oldestK).msgs.length;
					pendingBySid.delete(oldestK);
				}
				const dump = {};
				for (const [k, a] of pendingBySid)
					dump[k] = {
						msgs: a.msgs,
						watermark: a.watermark,
						seq: a.seq,
						lastTs: a.lastTs,
					};
				db.prepare(
					"INSERT INTO organ_meta (k, v) VALUES ('extract_buffer', ?) ON CONFLICT(k) DO UPDATE SET v = excluded.v",
				).run(JSON.stringify(dump));
				a25LastFlush = Date.now(); // 写库成功才消耗节流窗（F3-4②）
			} catch (eF) {
				// 8-31 长尾甲档③（F3-4①）：吞错无出口修——计数+节流 warn（治本承诺静默失效=sentry无从发现）
				stats.a25FlushErrors = (stats.a25FlushErrors || 0) + 1;
				if (Date.now() - a25FlushWarnAt > 60000) {
					a25FlushWarnAt = Date.now();
					try {
						console.warn(
							"[living-memory] a25Flush failed (#" +
								stats.a25FlushErrors +
								"): " +
								String(eF).slice(0, 80),
						);
					} catch {}
				}
				a25LastFlush = 0; // 失败不耗窗·下轮立即重试
			}
		}
		try {
			// 启动恢复：跨重启对话积累不丢（核心——41 发重启案的治本）
			if (!A25_OFF) {
				const row = db
					.prepare("SELECT v FROM organ_meta WHERE k = 'extract_buffer'")
					.get();
				if (row) {
					const dump = JSON.parse(row.v);
					for (const [k, a] of Object.entries(dump)) {
						if (a && Array.isArray(a.msgs) && a.msgs.length > 0)
							pendingBySid.set(k, {
								msgs: a.msgs,
								watermark: Number(a.watermark) || 0,
								seq: Number(a.seq) || 0,
								lastTs: Number(a.lastTs) || 0,
							});
					}
					stats.bufferRestored = [...pendingBySid.values()].reduce(
						(n, a) => n + a.msgs.filter((m) => m.seq > a.watermark).length,
						0,
					);
				}
			}
		} catch (eRB) {
			stats.bufferRestoreErrors = (stats.bufferRestoreErrors || 0) + 1; // P2 fix（09-03 audit）：启动恢复失败透出（原静默=脏值积压全账丢失零观测·⑬ 出口）
			ctx.logger?.warn?.(
				"[living-memory] extract_buffer restore failed (#" +
					stats.bufferRestoreErrors +
					"): " +
					String(eRB).slice(0, 60),
			);
		}
		let extracting = false;
		let sessionOfLastTurn = "";
		const guard = { pausedUntil: 0 }; // LlmFailureGuard 状态（吸inbox⑤·401/403/404 熔断）
		const preguard = { pausedUntil: 0 }; // 件三车：AUDN 预裁独立熔断态（模型分家·与提炼 guard 解连坐）

		// ── wave AUDN 预裁决（design-approved四车·治 conflicts 92 案积压）：pending 案 LLM 四态预裁——
		//    只写建议（pre_verdict/pre_reason/pre_at）·maintainer终批才执行（主权不动）。通道复用 EXTRACT_API/MODEL/guard。
		//    LLM 自相矛盾防御（同车并入·kg_clean 教训）：verdict 枚举外或 verdict-reason 词面矛盾 → 降级 manual（保守解）。
		const PRECLASSIFY_VERDICTS = new Set([
			"merge-keep-new",
			"merge-keep-old",
			"false-positive",
			"manual",
		]);
		const PRECLASSIFY_CONTRA = [
			[/^merge-keep-new$/, /保留旧|保留老|旧条更|旧更优|keep[- ]old|应留旧/],
			[/^merge-keep-old$/, /保留新|新条更|新更优|keep[- ]new|应留新/],
			[/^false-positive$/, /确属重复|确实重复|应合并|is duplicate|same memory/],
		];
		async function preclassifyConflict(entry, cred) {
			if (preguard.pausedUntil && Date.now() < preguard.pausedUntil)
				return null; // 件三车审计补丁（09-09·对抗性实抓）：preguard 消费点——原只设不查=预裁熔断不生效（分家后失去共享 guard 检查面·404 后 10min 内照发再撞）
			const sys =
				'你是记忆冲突预裁决器。输入两条记忆条目（新/旧）与立案依据，判断处理建议。输出严格 JSON：{"verdict":"merge-keep-new|merge-keep-old|false-positive|manual","reason":"一句话依据"}。四态语义：merge-keep-new=同主题演进应合并保留新条；merge-keep-old=旧条更优保留旧条；false-positive=并非重复无需合并；manual=无法确定需人工裁决。不要输出 JSON 以外的任何文字。';
			const usr =
				"【新条】type=" +
				entry.nType +
				" space=" +
				entry.nSpace +
				" title=" +
				String(entry.nTitle).slice(0, 120) +
				"\ncontent=" +
				String(entry.nContent).slice(0, 600) +
				"\n【旧条】type=" +
				entry.oType +
				" space=" +
				entry.oSpace +
				" title=" +
				String(entry.oTitle).slice(0, 120) +
				"\ncontent=" +
				String(entry.oContent).slice(0, 600) +
				"\n【立案依据】" +
				String(entry.basis || "").slice(0, 120);
			let resp = null;
			for (let attempt = 0; attempt <= 2; attempt++) {
				try {
					resp = await fetch(EXTRACT_API, {
						method: "POST",
						headers: {
							"Content-Type": "application/json",
							Authorization: "Bearer " + cred.value,
						},
						body: JSON.stringify({
							model: PREVERDICT_MODEL, // AUDN 刀②（09-08 maintainer）：预裁独立旋钮——与 EXTRACT_MODEL 脱钩（提炼主链 flash 不动）
							messages: [
								{ role: "system", content: sys },
								{ role: "user", content: usr },
							],
							max_tokens: 2000, // AUDN 修③（09-08 对拍实抓）：V4 系推理形态——200 被思考链吃光致 content 恒空（v4-pro finish=length 实证）·提额 2000
							temperature: 0.1,
							stream: false,
							response_format: { type: "json_object" }, // AUDN 刀①（09-08 maintainer）：JSON 模式——治「非 JSON/解析失败」降级
						}),
					});
				} catch {
					if (attempt === 2) return null;
					await new Promise((r2) => setTimeout(r2, 800 * 2 ** attempt));
					continue;
				}
				if (resp.ok) break;
				const st = resp.status;
				if ([401, 403, 404].includes(st)) {
					preguard.pausedUntil = Date.now() + 10 * 60_000;
					return null; // 熔断（件三车病灶1·09-09：预裁独立 preguard——AUDN 刀②模型已分家 v4-pro/flash·预裁 404（模型 id 特有故障）连坐提炼主链不成立·解连坐）
				}
				if (![429, 500, 502, 503, 529].includes(st) || attempt === 2)
					return null;
				await new Promise((r2) => setTimeout(r2, 800 * 2 ** attempt));
			}
			if (!resp || !resp.ok) return null;
			try {
				const data = await resp.json();
				const raw = String(data?.choices?.[0]?.message?.content || "").trim();
				const m = raw.match(/\{[\s\S]*\}/);
				if (!m) return { verdict: "manual", reason: "输出非 JSON 降级" };
				const o = JSON.parse(m[0]);
				const verdict = String(o.verdict || "").trim();
				const reason = String(o.reason || "").slice(0, 200);
				if (!PRECLASSIFY_VERDICTS.has(verdict))
					return {
						verdict: "manual",
						reason: "枚举外降级：" + verdict.slice(0, 40),
					};
				for (const [vr, contraRe] of PRECLASSIFY_CONTRA)
					if (vr.test(verdict) && contraRe.test(reason))
						return {
							verdict: "manual",
							reason:
								"矛盾防御降级（reason 与 verdict 相左）：" +
								reason.slice(0, 120),
						};
				return { verdict, reason };
			} catch {
				return { verdict: "manual", reason: "解析失败降级" };
			}
		}

		// ── 提炼核心（DeepSeek 记忆专用 key · 外部直调 · 失败静默）──
		async function runExtract(opts) {
			if (extracting) return { skipped: true, reason: "extracting" };
			// ── step：maintenance window闸（提炼入口）——挂牌即跳过，摘牌恢复 ──
			//    step顺手件：info→warn（cordis 生产吞 info）+ 计数器（审计链）
			try {
				if (fs2.existsSync(SURGERY_FLAG)) {
					stats.surgerySkipCount = (stats.surgerySkipCount || 0) + 1;
					ctx.logger?.warn?.(
						`[living-memory] surgery-flag: skip extract (#${stats.surgerySkipCount})`,
					);
					return { skipped: true, reason: "surgery-flag" };
				}
			} catch {}
			// ── 水位路源（wave）：getUnextracted 同语义——只取 seq>watermark 的待抽段（本会话分账）。
			//    审计 D1 修正（13:44）：源优先级=手写显式源（opts.source）> 本窗水位段 > 旧 buffer 回落——
			//    防手写触发段的 historyText 被水位路劫持（auto=false 路必须用调用方指定源）。
			const _a12sid = (opts && opts.sid) || sessionOfLastTurn || "_anon"; // step-2（10:34 maintainer·治 D-新2 他窗不可提）：显式 sid 优先——memory organ特权用法可提他窗账
			const _a12acct = (!A12_OFF && pendingBySid.get(_a12sid)) || null;
			const _a12unextracted = _a12acct
				? _a12acct.msgs.filter((m) => m.seq > _a12acct.watermark)
				: [];
			let source = opts && typeof opts.source === "string" ? opts.source : "";
			let sourceViaWatermark = false;
			const unextractedWatermarkRef = { seqs: [] }; // 本次抽的 seq 集合（成功推水位用·作用域提升至 accepted 段可达）
			if (!source && _a12unextracted.length > 0) {
				// step-1（10:34 maintainer·治 D-新1 长窗死锁）：last-12k→first-12k-after-watermark 滚动消化——
				// 原尾窗帽致超长账只见尾部·尾部已手动入册→LLM 判无可记→0→水位永冻=死锁。改从头按消息累积取段·多轮逐段消化全账。
				const _taken = [];
				let _len = 0;
				for (const m of _a12unextracted) {
					const piece = `[${m.role} t=${m.turn}]\n${m.text}`;
					if (_len + piece.length > 12000 && _taken.length > 0) break;
					if (_taken.length === 0 && piece.length > 12000) {
						_taken.push({ ...m, text: String(m.text).slice(0, 12000) });
						_len = 12000;
						break;
					} // 审计 I4：首条超长截断（粘贴大文件面）——防段超帽打爆 LLM 输入
					_taken.push(m);
					_len += piece.length;
				}
				// ── wave assistant 过程叙述蒸馏（design-approved四车·治 12K 窗稀释——
				//    过程行（让我/接下来/我将…开头）占窗挤掉实质内容·12 词表行级滤除·仅滤 assistant 行）──
				let _distilled = 0;
				const _distill = _taken.map((m) => {
					if (String(m.role) !== "assistant") return m;
					const ls = String(m.text).split("\n");
					const keep = ls.filter((l) => {
						const t = l.trim();
						if (
							t &&
							/^(让我|接下来|我将|我现在|我来|我先|好的[，,]|明白[，,了]|正在|准备|首先|然后|最后)/.test(
								t,
							)
						) {
							_distilled += 1;
							return false;
						}
						return true;
					});
					return keep.length === ls.length
						? m
						: { ...m, text: keep.join("\n") };
				});
				if (_distilled > 0)
					stats.assistantDistillLines =
						(stats.assistantDistillLines || 0) + _distilled;
				const _takenF = _distill.filter(
					(m) => String(m.text || "").trim().length > 0,
				);
				source = _takenF
					.map((m) => `[${m.role} t=${m.turn}]\n${m.text}`)
					.join("\n---\n");
				unextractedWatermarkRef.seqs = _taken.map((m) => m.seq);
				unextractedWatermarkRef.acct = _a12acct; // 记账指针（成功推水位用）
				sourceViaWatermark = true;
			}
			if (!source && !sourceViaWatermark) source = opts.fallback || buffer; // P0修（09-03 audit）：fallback 回落源——手动 extract 带 events 原文时不污全局 buffer
			if (source.trim().length < 40)
				return { skipped: true, reason: "buffer too small" };
			// ── LlmFailureGuard（2026-08-24 吸inbox⑤·graph-memory 设计 42 行）：401/403/404 熔断 10min·429/5xx 重试 3 次指数退避 ──
			if (guard.pausedUntil && Date.now() < guard.pausedUntil)
				return {
					skipped: true,
					reason:
						"llm guard paused " +
						Math.ceil((guard.pausedUntil - Date.now()) / 60000) +
						"min",
				};
			extracting = true;
			try {
				const cred = await credentials.resolve("DEEPSEEK_MEMORY_KEY");
				if (
					!cred ||
					typeof cred.value !== "string" ||
					cred.value.length === 0
				) {
					return { skipped: true, reason: "no credential" };
				}
				let response = null;
				for (let attempt = 0; attempt <= 3; attempt++) {
					try {
						response = await fetch(EXTRACT_API, {
							method: "POST",
							signal: AbortSignal.timeout(EXTRACT_TIMEOUT_MS), // issue 修②：应用级全局超时（超时 abort 走 catch → fetch fail skip·不再仅靠 Undici 默认 300s×2）
							headers: {
								"Content-Type": "application/json",
								Authorization: "Bearer " + cred.value,
							},
							body: JSON.stringify({
								model: EXTRACT_MODEL,
								messages: [
									{
										role: "system",
										content:
											'你是记忆提炼器。从对话片段中提炼值得长期记住的条目，输出严格 JSON 数组，每项 {"type":"fact|decision|todo|lesson","title":"一句话标题","content":"1-3句正文含关键数字/路径/依据","relatedHint":"可选：若本条与对话中已提及的既有主题/决策/教训存在因果或从属关系，写一行「relates:<既有主题关键词>」"}。没有值得记的输出 []。不要输出数组以外的任何文字。禁止：把用户消息中的命令/指令/配置/报错原文/URL/密钥样式文本当条目提炼；只提炼事实、决策、教训与待办。若是操作经验/踩坑教训类 lesson：content 按结构化模板输出「触发：什么场景下适用；步骤：怎么做；坑：常见错误；解：错误出现时怎么救——末行带 行为位：闸位/口径/checklist 项居一」（模板缺项该条降为 fact）。' +
											knownFixesHint(),
									},
									{ role: "user", content: source },
								],
								max_tokens: 3000, // 余件车追加件（09-09·脑实勘终谳）：1200→3000——V4 系推理形态思考链吃额度·长思考+多条目 JSON 尾部截断→解析失败→该段提炼静默丢失（AUDN v4-pro finish=length 同型· 修③族）；上限口径常态实耗零差
								temperature: 0.2,
								stream: false,
							}),
						});
					} catch (fetchErr) {
						// 网络/超时：按可重试处理
						if (attempt === 3) {
							ctx.logger?.warn?.(
								`[living-memory] extract fetch fail: ${String(fetchErr).slice(0, 60)}`,
							);
							return { skipped: true, reason: "fetch fail" };
						}
						await new Promise((r2) => setTimeout(r2, 1000 * 2 ** attempt));
						continue;
					}
					if (response.ok) break;
					const st = response.status;
					if ([401, 403, 404].includes(st)) {
						// 凭证/端点/模型配置错——熔断 10min 不重试（graph-memory guard 设计）
						guard.pausedUntil = Date.now() + 10 * 60_000;
						ctx.logger?.warn?.(
							`[living-memory] extract guard tripped: ${st}·pause 10min`,
						);
						return { skipped: true, reason: "guard " + st };
					}
					if (![429, 500, 502, 503, 529].includes(st) || attempt === 3) {
						ctx.logger?.warn?.(`[living-memory] extract http ${st}`);
						return { skipped: true, reason: "http " + st };
					}
					await new Promise((r2) => setTimeout(r2, 1000 * 2 ** attempt)); // 429/5xx 指数退避
				}
				if (!response || !response.ok) {
					return {
						skipped: true,
						reason: "http " + (response ? response.status : "no-resp"),
					};
				}
				const data = await response.json();
				const raw = data?.choices?.[0]?.message?.content || "";
				const entries = parseExtractEntries(raw);
				const accepted = [];
				const ins = db.prepare(
					"INSERT INTO memories (ts, type, title, content, space, source, checksum, confidence) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
				);
				for (const e of entries) {
					const type = String(e.type);
					if (typeof e.title !== "string" || typeof e.content !== "string")
						continue; // 8-31 长尾乙档（F3-6）：判型前置——原 String(undefined)='undefined'（9 字符）恰好绕过长度闸=垃圾条目入库且后闸不可拦
					const title = stripUrls(String(e.title)).slice(0, 120);
					const content = stripUrls(String(e.content)).slice(0, 1500);
					if (!["fact", "decision", "todo", "lesson"].includes(type)) continue;
					if (title.length < 2 || content.length < 4) continue;
					// ── 计数旧值过滤（kg_grow 同款·design-approved「开始吸收」）：整题剥尾括号缀后=纯数字/数字+量词=瞬时计数快照非知识——不入正库（随系统增长必过时·治「抽屉N条/pendingN」类噪音）。
					//    宽容面：日期(含-)/版本(含.)/范围(5-50)/题含动词等其余信息不拦（仅整题纯计数态拦·counter countSkipCount）。
					if (
						/^\d+\s*(条|个|项|件|篇|张|次|份|款|组|台|人|天|小时|分钟|KB|MB|GB|条目|案)?$/.test(
							title.replace(/[（(][^）)]*[)）]\s*$/, "").trim(),
						)
					) {
						stats.countSkipCount = (stats.countSkipCount || 0) + 1;
						ctx.logger?.warn?.(
							`[living-memory] count-gate skip (#${stats.countSkipCount}): ${title.slice(0, 50)}`,
						);
						continue;
					}
					const sec = securityCheck(title, content);
					if (!sec.ok) {
						stats.rejectedCount += 1;
						ctx.logger?.warn?.(
							`[living-memory] security reject (${sec.reason}): ${title.slice(0, 50)}`,
						);
						continue;
					}
					// ── 敏感闸（wave·design-approved最小版）：directive原文/凭据/未裁冲突类不入 auto 提炼 ──
					if (SENSITIVE_RE.test(title) || SENSITIVE_RE.test(content)) {
						stats.sensitiveSkipCount = (stats.sensitiveSkipCount || 0) + 1;
						ctx.logger?.warn?.(
							`[living-memory] sensitive-gate skip (#${stats.sensitiveSkipCount}): ${title.slice(0, 50)}`,
						);
						continue;
					}
					// ── 四闸·extraction gate（2026-08-22）：auto-extract 生成 todo 前与闭环面核对——同题冲突降为 fact 防复活 ──
					let entryType = type;
					if (type === "todo") {
						const cc = closureCheck(db, title, content); // step C案：ts 参退役（②同日排除已删）
						if (cc.closed) {
							entryType = "fact";
							ctx.logger?.warn?.(
								`[living-memory] extract-gate: todo→fact（与闭环条 #${cc.by} 重叠 ${cc.overlap} 词）: ${title.slice(0, 50)}`,
							);
						}
					}
					// ── global 专属词路由（design-approved global 两刀·刀A 源头）：sessionOrgan 判不出时按词面路由，判不出才 global ──
					// 两级词表（brain 23:58 审理+主刀审计）：强词触发；弱词不单独路由；抑制词命中即留 global(design note)
					const routeByKeyword = (t, c) => {
						const text = String(t || "") + " " + String(c || "");
						if (ROUTE_INHIB_RE.test(text)) return null;
						const x = false,
							m = ROUTE_MAINT_RE.test(text);
						if (x && !m) return "xhs";
						if (m && !x) return "maintain";
						return null;
					};
					const organFallback = sessionOrgan(_a12sid); // step Z1（wave审计·00:29）：sid 提炼时归属取被提账会话——原恒 sessionOfLastTurn·step存量清算 40 段跨 12 账将错路由
					let targetSpace = organFallback;
					if (!organFallback) {
						const routed = routeByKeyword(title, content);
						if (routed) {
							targetSpace = routed;
							stats.routedCount = (stats.routedCount || 0) + 1;
							ctx.logger?.info?.(
								`[living-memory] route-by-keyword (#${stats.routedCount}): "${title.slice(0, 40)}" → ${routed}（sessionOrgan 未中·词面路由）`,
							);
						}
					}
					// P0修（09-03 audit）：中断重抽闸——水位在循环后推，中途异常已 COMMIT 条目下轮重抽防重放（checksum 提炼路同式 sha1(entryType+title+content)·轻闸挡逐字重·近重复由nightly patrol去重兜）
					if (
						db
							.prepare(
								"SELECT id FROM memories WHERE checksum = ? AND status = 'active' LIMIT 1",
							)
							.get(sha1(entryType + title + content))
					) {
						stats.extractDupSkipped = (stats.extractDupSkipped || 0) + 1;
						continue;
					}
					db.exec("BEGIN"); // step B-5：主+边同事务（防崩窗孤儿边）——try 内 ·catch 回滚
					// ── external 信任分级（投毒防线同款·论文 2606.24322）：批次级判定——提炼源（source 文本）URL≥3 = 外部粘贴为主 → 本批条目 confidence 0.5（内容可总结·来源属性保留防洗白·检索面外显）
					const extBatch =
						(String(source).match(/https?:\/\//g) || []).length >= 3;
					const insInfo = ins.run(
						nowIso(),
						entryType,
						title,
						content,
						targetSpace || "global",
						"auto:" + _a12sid,
						sha1(entryType + title + content),
						extBatch ? 0.5 : 1.0,
					); // step Z1②：source 归因同源 _a12sid；8-31 移植族修复①：run 返回值存 insInfo——node:sqlite 的 lastInsertRowid 在 run 结果上（statement 本体无·原 引用幻属性 NaN→NULL 致产边整路死·正库 0 条铁证）
					// ── 提炼产边（wave·16:47 approved·graph-memory 抽取产边意）：relatedHint「relates:<关键词>」→
					//    FTS 查同 space 既有条 top1 建 auto 边（edge_type='auto-llm'·weight 0.5 低档·instruction 带 hint 原文）。
					//    边供给从nightly patrol jaccard 单源→双源；目标查不到静默跳过；预算帽 5 边/轮。
					try {
						const rh = String(e.relatedHint || "").match(/relates[:：]\s*(.+)/);
						if (rh && rh[1]) {
							const kw = rh[1].trim().slice(0, 40);
							const tokens2 = tokenize(kw).slice(0, 4);
							const match2 = queryMatch(tokens2);
							if (match2) {
								const tgt = db
									.prepare(`SELECT m.id FROM memories_fts JOIN memories m ON m.id = memories_fts.rowid
                  WHERE memories_fts MATCH ? AND m.status='active' AND m.space = ? AND m.id != ?
                  ORDER BY bm25(memories_fts) LIMIT 1`)
									.get(
										match2,
										targetSpace || "global",
										Number(insInfo.lastInsertRowid),
									);
								if (tgt) {
									db.prepare(`INSERT INTO memories_edges (src, dst, edge_type, weight, valid_at, source_group, last_seen, instruction)
                    VALUES (?, ?, 'auto-llm', 0.5, ?, 'extract:' || ?, ?, ?)
                    ON CONFLICT(src, dst, edge_type) DO UPDATE SET last_seen = excluded.last_seen`).run(
										Number(insInfo.lastInsertRowid),
										tgt.id,
										nowIso(),
										_a12sid.slice(0, 40),
										nowIso(),
										"提炼关联: " + kw,
									); // 8-31 移植族修复①：insInfo 同源+source_group 归因从 sessionOfLastTurn 改 _a12sid（Z1 同型漏改·opts.sid≠当前窗时错归因）
									stats.autoEdges = (stats.autoEdges || 0) + 1;
								}
							}
						}
					} catch {}
					// Single-subject wording gate on the extraction path: warn + count, never block
					const bw = softBodyWarn(title, content);
					if (bw.length > 0) {
						stats.singleBodyWarns = (stats.singleBodyWarns || 0) + 1;
						ctx.logger?.warn?.(
							`[living-memory] single-body-warn (#${stats.singleBodyWarns}): 「${bw.join("、")}」in: ${title.slice(0, 40)}`,
						);
					}
					db.exec("COMMIT"); // step：主+边落毕提交
					accepted.push({
						type: entryType,
						title,
						...(bw.length > 0 ? { softWarn: bw } : {}),
					});
				}
				// step-1b：水位推进与条目产出解耦——LLM 解析成功即推（该段判无价值也是消化·防 0 产出死锁重看同段）。
				// 宁漏勿死锁：API 抖动空回应由 guard/重试网挡·此处只承接「真跑过且 parse 成功」。
				if (
					sourceViaWatermark &&
					unextractedWatermarkRef.acct &&
					unextractedWatermarkRef.seqs.length > 0
				) {
					const a = unextractedWatermarkRef.acct;
					a.watermark = Math.max(a.watermark, ...unextractedWatermarkRef.seqs);
					stats.watermarkAdvanced = a.watermark;
					a25Flush(true); // 消化强制落盘（水位持久）
				}
				if (accepted.length > 0) {
					buffer = ""; // 提炼成功后清空缓冲（旧路）
					// 配套（审计 D7）：信号在成功路径消化（与 水位同生命周期）——失败留队下轮再试
					if (stats.signalArmed) {
						signalQ = [];
						stats.signalArmed = false;
					}
				}
				stats.bufferedChars =
					buffer.length +
					(A12_OFF
						? 0
						: (() => {
								let n = 0;
								for (const a of pendingBySid.values())
									for (const m of a.msgs)
										if (m.seq > a.watermark) n += m.text.length;
								return n;
							})());
				stats.lastExtractAt = Date.now();
				stats.autoExtractCount += 1;
				stats.autoMemoryCount += accepted.length;
				return {
					extracted: accepted.length,
					titles: accepted.map((a) => a.title),
				};
			} catch (error) {
				try {
					db.exec("ROLLBACK");
				} catch {} // step：事务失败回滚（孤儿防）
				ctx.logger?.warn?.(`[living-memory] extract failed: ${String(error)}`);
				return { skipped: true, reason: String(error).slice(0, 80) };
			} finally {
				extracting = false;
			}
		}

		// 解析提炼输出（容错：裸 JSON 数组 / ```json 代码块）
		function parseExtractEntries(raw) {
			try {
				const parsed = JSON.parse(raw);
				if (Array.isArray(parsed))
					return parsed.filter((x) => x && typeof x === "object");
			} catch {}
			const fence = raw.match(/```(?:json)?\s*([\s\S]*?)```/);
			if (fence) {
				try {
					const parsed = JSON.parse(fence[1]);
					if (Array.isArray(parsed))
						return parsed.filter((x) => x && typeof x === "object");
				} catch {}
			}
			const bracket = raw.match(/\[[\s\S]*\]/);
			if (bracket) {
				try {
					const parsed = JSON.parse(bracket[0]);
					if (Array.isArray(parsed))
						return parsed.filter((x) => x && typeof x === "object");
				} catch {}
			}
			return [];
		}

		// ── 图谱引擎(design note)：PPR 查询相关加成+社区——graph-memory pagerank/community 意融入 ──
		//    图源=memories_edges 活边（cooccur 派生+explicit 人知·统一无向图）·30s 结构缓存（graph-memory 同款）；
		//    PPR：种子=当前命中行·teleport 回种子（带查询相关性·非均匀 PageRank）·damping 0.85·15 迭代——
		//    治step静态加成「所有 cooccur 伙伴平权」：与查询种子近的伙伴高分·远的低分。
		//    社区：Label Propagation（nightly patrol 13.5 段建表 memory_communities）·供水位泛化路。
		const PPR_GRAPH_CACHE_MS = 30_000;
		let pprGraphCache = { at: 0, adj: null };
		function pprBuildGraph(qKey, qTokens) {
			// ── wave KG 查询条件化边权（design-approved四车·MemORAI Dynamic Weighted PPR 意）──
			//    缓存改 query 键（qh 不匹配即重建·30s 同 query 命中）；边权乘 (1+0.5×overlap)——overlap=query tokens
			//    对「边 instruction ∪ 端点 title tokens」的覆盖率（查询相关边浮升·种子仍 FTS∪vec 不变）。
			//    qTokens 空（兜底路/LEGION_PPR_QUERY_OFF）→ 因子全 1=退化原行为。
			const qh =
				qTokens && qTokens.length ? qTokens.slice().sort().join("\u0001") : "";
			if (
				pprGraphCache.adj &&
				pprGraphCache.qh === qh &&
				Date.now() - pprGraphCache.at < PPR_GRAPH_CACHE_MS
			)
				return pprGraphCache.adj;
			const edges = db
				.prepare(`SELECT src, dst, weight, instruction FROM memories_edges WHERE invalid_at IS NULL
        AND src IN (SELECT id FROM memories WHERE status='active' AND (valid_to IS NULL OR valid_to > ?))
        AND dst IN (SELECT id FROM memories WHERE status='active' AND (valid_to IS NULL OR valid_to > ?))`)
				.all(nowIso(), nowIso()); // 审计③修（design-approved「修」·检索链改动）：活边图原只滤 status ⇒ 21 条沉底条（valid_to 已到点·status 仍 active）的 81 条边**全权重入图**，检索层 ×0.2 沉底意图在图传播层缺位。修＝条目轴同口径（valid_to IS NULL OR valid_to > now）与检索层沉底判据对齐（短串 'YYYY-MM-DD' 字典序 < 同日带时刻串 ⇒ 当日到点即排除）
			// （wave）：PPR 边权消费 validatedCount——重复验证的锚条边权浮升（graph-memory validated_count 浮权意）
			try {
				const vcs = db
					.prepare(
						"SELECT id, validated_count FROM memories WHERE validated_count > 1 AND status='active'",
					)
					.all();
				const vcMap = new Map(
					vcs.map((r) => [
						r.id,
						Math.min(1.5, 0.8 + 0.2 * Math.log(1 + Number(r.validated_count))),
					]),
				);
				for (const e of edges) {
					const f1 = vcMap.get(e.src);
					if (f1) e.weight = Number(e.weight) * f1;
					const f2 = vcMap.get(e.dst);
					if (f2) e.weight = Number(e.weight) * f2;
				}
			} catch {}
			// ── 查询因子：qTokens 非空时预取端点 title tokens+边 instruction tokens → 覆盖率因子 ──
			let qSet = null;
			let titleTok = null;
			if (qTokens && qTokens.length && !process.env.LEGION_PPR_QUERY_OFF) {
				qSet = new Set(qTokens);
				titleTok = new Map();
				try {
					const tRows = db
						.prepare("SELECT id, title FROM memories WHERE status='active'")
						.all();
					for (const r of tRows) titleTok.set(r.id, tokenize(String(r.title)));
				} catch {} // 全表 title 切分失败→因子路静默退场（原行为）
			}
			const qFactor = (e) => {
				if (!qSet) return 1;
				let inter = 0;
				const side = (arr) => {
					if (!arr) return;
					for (const t of arr) if (qSet.has(t)) inter++;
				};
				side(titleTok && titleTok.get(e.src));
				side(titleTok && titleTok.get(e.dst));
				if (e.instruction) {
					for (const t of tokenize(String(e.instruction)))
						if (qSet.has(t)) inter++;
				}
				return 1 + 0.5 * Math.min(1, inter / qSet.size); // 覆盖率帽 1·因子∈[1,1.5]
			};
			const adj = new Map(); // id -> [{to, w}]
			const add = (a, b, w) => {
				if (!adj.has(a)) adj.set(a, []);
				adj
					.get(a)
					.push({ to: b, w: Math.max(0.1, Math.min(3, Number(w) || 1)) });
			};
			for (const e of edges) {
				const qf = qFactor(e);
				add(e.src, e.dst, e.weight * qf);
				add(e.dst, e.src, e.weight * qf);
			}
			pprGraphCache = { at: Date.now(), adj, qh };
			return adj;
		}
		function pprScores(seedIds, qTokens) {
			try {
				const adj = pprBuildGraph(null, qTokens);
				if (!adj.size || !seedIds || seedIds.length === 0) return null;
				const d = 0.85,
					ITER = 15;
				const seed = new Set(seedIds);
				let pr = new Map();
				for (const id of seed) pr.set(id, 1 / seed.size);
				for (let it = 0; it < ITER; it++) {
					const next = new Map();
					for (const id of seed)
						next.set(id, (next.get(id) || 0) + (1 - d) / seed.size); // teleport 回种子
					for (const [id, score] of pr) {
						const nbrs = adj.get(id);
						if (!nbrs || nbrs.length === 0) {
							// 悬挂点：概率归种子（graph-memory 同法）
							for (const s of seed)
								next.set(s, (next.get(s) || 0) + (d * score) / seed.size);
							continue;
						}
						let wsum = 0;
						for (const n of nbrs) wsum += n.w;
						for (const n of nbrs)
							next.set(n.to, (next.get(n.to) || 0) + d * score * (n.w / wsum));
					}
					pr = next;
				}
				let max = 0;
				for (const v of pr.values()) if (v > max) max = v;
				if (max <= 0) return null;
				const out = new Map();
				for (const [k, v] of pr) out.set(k, v / max); // 归一 0~1
				return out;
			} catch {
				return null;
			}
		}

		// ── 观察：session/event firehose（计数 + 文本缓冲 + turn/end 限频自动提炼）──
		// ── 融合刀②·B inbox⇄living memory绑定 + A 开局三查（design-approved·观测段：仅提示不拦截）──
		//    审计修正 19:05：bind 按 sessionId 分账（原插件级单例——多会话并发跨污染：甲窗读inbox的信号会让乙窗永不告警）
		const bindMap = new Map(); // sessionId -> { inboxSeen, memCalled, userTurns, warnedA, warnedB }
		const compactionBySid = new Map(); // 09-03 压缩桥：sid -> 本会话压缩次数（刀② end 计数·刀⑤ 注入面读·sweep 2h 过期同扫）——⚠ 09-11 起降为**进程内兜底**，主计数在 organ_meta（见下）
		// ── 压缩序数持久化（design-approved「同意」＋同日**独立审计 B 路**加固）：
		//    病灶：原＝**进程内 Map 单源** ⇒ 进程重启即归零 ⇒ 同会话第二次压缩仍写「第 1 次」，
		//    与重启前那条**同名**（实测 09-11 本会话  20:11 与  22:08 双条同名·检索面旧条排前）。
		//    **审计加固两条**（B 路真缺陷 D2/D3 ／ 改进 I1）：
		//      ①**第三源自愈**：单 DB 键仍是单点（键被清/换库/写失败 ⇒ 重启重号），且**存量会话无回填**
		//        （本会话即为病灶案例：库中两条同名锚条、无计数键 ⇒ 单选持久键仍会写「第 3 条第 1 次」）
		//        ⇒ 取**库内既有锚条数**为事实源（`source='auto:<sid>' ∧ title LIKE '上下文压缩锚%'`）。
		//        三源取大：进程内 Map／organ_meta 键／库内锚条数——任一源活着即不重号。
		//      ②**读侧补出口**（⑬ 对称）：原 `catch { return Map }` 零出口＝DB 不可读时注入面**永久静默少报**。
		//    法同 bumpNudgeTotal（v+1 型 UPSERT 免竞态）；键=`compaction_count.<sid>`（可 grep·不与他键撞）。
		//    ⚠ 性能：`memories.source` **无二级索引**（全表扫 ~ms 级）⇒ 读侧**仅在键缺失时**才查第三源（键在即跳过）。
		const anchorCountOf = (sid) => {
			try {
				const r = db
					.prepare(
						"SELECT COUNT(*) c FROM memories WHERE source = ? AND title LIKE '上下文压缩锚%'",
					)
					.get("auto:" + sid);
				return Number(r && r.c) || 0;
			} catch {
				return 0; // ⑬：读失败按「无锚条」计（三源取大里的保守项·出口＝返回值）
			}
		};
		const nextCompactionN = (sid) => {
			const fallback = (compactionBySid.get(sid) || 0) + 1; // 进程内兜底
			const anchorN = anchorCountOf(sid) + 1; // 第三源：库内既有锚条数（存量会话/键被清/写失败时的事实源）
			try {
				db.prepare(
					"INSERT INTO organ_meta(k, v) VALUES(?, '1') ON CONFLICT(k) DO UPDATE SET v = CAST(CAST(v AS INTEGER) + 1 AS TEXT)",
				).run("compaction_count." + sid);
				const r = db
					.prepare("SELECT v FROM organ_meta WHERE k = ?")
					.get("compaction_count." + sid);
				const persisted = Number(r && r.v) || 0;
				// 三源取大（序数宁大勿小·防同号）：进程内／持久键／库内既有锚条数
				const n = Math.max(fallback, persisted, anchorN);
				// **回写**（审计 I1：DB 滞后不自愈）：持久键落后于任一源时刷到 n——否则读侧按滞后值少报
				if (n > persisted) {
					db.prepare(
						"INSERT INTO organ_meta(k, v) VALUES(?, ?) ON CONFLICT(k) DO UPDATE SET v = excluded.v",
					).run("compaction_count." + sid, String(n));
				}
				return n;
			} catch (eC) {
				stats.compactionBridgeErrors = (stats.compactionBridgeErrors || 0) + 1; // ⑬ catch 必须有出口（归因看下方 warn 文案）
				ctx.logger?.warn?.(
					"[living-memory] compaction counter persist failed (#" +
						stats.compactionBridgeErrors +
						"): " +
						String(eC).slice(0, 60),
				);
				return Math.max(fallback, anchorN); // 持久面失败也要吃第三源（审计 I1：DB 滞后不自愈）
			}
		};
		const currentCompactionN = (sid) => {
			// 注入面只读（不写）：三源取大（键缺失时才查第三源·免每轮全表 COUNT）
			let persisted = 0;
			try {
				const r = db
					.prepare("SELECT v FROM organ_meta WHERE k = ?")
					.get("compaction_count." + sid);
				persisted = Number(r && r.v) || 0;
			} catch (eR) {
				// ⑬ 出口（审计 D3）：读失败不再静默——计数＋warn 与写侧对称（复用既有键·零新增）
				stats.compactionBridgeErrors = (stats.compactionBridgeErrors || 0) + 1;
				ctx.logger?.warn?.(
					"[living-memory] compaction counter read failed (#" +
						stats.compactionBridgeErrors +
						"): " +
						String(eR).slice(0, 60),
				);
			}
			// AUDIT-FIX-20260912（修复车·审计 C 路第 4 项）：原式 `persisted === 0 ? anchorCountOf(sid) : 0`
			//   只在**键缺失**时才吃第三源 ⇒ 「键滞后」（键=2 而库内锚条=3）时注入面**少报**，与写侧
			//   `nextCompactionN` 的无条件三源取大**口径不一致**。修：读侧同样无条件三源取大。
			//   ⚠ 代价（诚实账）：`memories.source` 无二级索引 ⇒ 每轮注入面多一次全表 COUNT（~ms 级）；
			//   库量级上万条后应加索引（登记为候选优化），届时本注释同步更新。
			return Math.max(
				persisted,
				anchorCountOf(sid),
				compactionBySid.get(sid) || 0,
			);
		};
		// AUDIT-FIX-20260912（修复车·审计 C 路第 5 项·P0-2 主体）：**回落落键**。
		//   ⚠ **因果模型订正（审计 A 路 D1·2026-09-12）**：原注释称「锚条入册失败 ⇒ 序数丢失」**与实测
		//   不符**——`nextCompactionN` 在锚条 INSERT **之前**即已 UPSERT 写键并回写（v+1），故「仅锚条
		//   失败」时键仍在、重启后下一号与不落键**完全等价**（A/B 对照实测 key/n 全同）。真实增量面
		//   只有一处：**键写本身瞬时失败**（nextCompactionN 的 catch(eC) 路径·不落键）——**且**随后
		//   锚条/建边路径也失败（即 catch(eB) 被触发）时，回落落键才补上事实源（实测 NEW key=1 保留
		//   vs LEGACY key 缺失·下一号 2 vs 1）；**仅键写失败而锚条成功时本函数不被调用**（审计 A′ 实证），
		//   此时第三源（锚条数）兜住、无损害。叠加 MAX 语义即**幂等冗余**。
		//   记账口径：本函数是**二次机会垫片**——两个面**不可混述**（A″D8 订正）：
		//   **调用面**＝`catch (eB)` 触发即被调用（含「键写成功 + 锚条失败」·实测 __FB_CALLS=1）；
		//   **增量面**（真正改变事实源的场景）＝键写失败 **∧** `catch (eB)` 触发。
		//   为何必须独立成件：drill 探针只能触达模块作用域声明，内联回 catch 则**不可测**
		//   （自建防线必被独立验证）⇒ 抽成函数是真能测的前提。
		//   ⚠ 与之配套的 prune 侧「保信息闸」见 pruneOrganMeta——两道合璧才闭合本缺陷：
		//     落键保证「即使锚条没入册、事实源仍在」，保信息闸保证「该键不会被当冗余缓存清掉」。
		const fallbackWriteCompactionN = (sid, n) => {
			if (!sid || !(Number(n) > 0)) return false;
			try {
				db.prepare(
					"INSERT INTO organ_meta(k, v) VALUES(?, ?) ON CONFLICT(k) DO UPDATE SET v = CAST(MAX(CAST(v AS INTEGER), CAST(excluded.v AS INTEGER)) AS TEXT)",
				).run("compaction_count." + sid, String(Number(n)));
				return true;
			} catch (eF) {
				stats.compactionBridgeErrors = (stats.compactionBridgeErrors || 0) + 1; // ⑬ 出口（回落落键自身失败也计数）
				try {
					ctx.logger?.warn?.(
						"[living-memory] compaction counter fallback write failed (#" +
							stats.compactionBridgeErrors +
							"): " +
							String(eF).slice(0, 60),
					);
				} catch {}
				return false;
			}
		};
		// ── organ_meta 生命周期：按会话展开的计数键清理（2026-09-12 三车·裁 4＋3b）──────────────
		//    病根：`compaction_count.<sid>` **按会话展开**且写入后**永无回收面** ⇒ organ_meta 只增不减；
		//          同族的进程内 `compactionBySid` 有 2h sweep，持久面却是永久（B 路 D4／B-I3「2h 有界变永久」）。
		//    **安全性论证（本刀的核心·勿删此段）**：`nextCompactionN` 是**三源取大**
		//          （进程内 Map／本键／**库内既有锚条数** `anchorCountOf`）⇒ 本键本质是
		//          「免全表 COUNT 的**加速缓存**」，**删过期键不会致序数重号**（第三源兜住）。
		//    判据：保留「近 N 天仍有记忆写入的会话」（以 `memories.source='session:<sid>'` 的 ts 为准），
		//          其余 `compaction_count.*` 键删除。**不动** `compactionBySid` 的 sweep（进程内 2h 面·另一轴）。
		//    出口（⑬）：删失败有计数 + warn，不静默。
		const pruneOrganMeta = (days) => {
			const _d = Number(days) > 0 ? Number(days) : 7;
			// D2 修（审计 A 路·2026-09-12）：**时间口径对齐**——原 cut 用 `toISOString()`（UTC `Z`、
			//   含秒毫秒），而 `memories.ts` 经 nowIso() 写成 `+08:00` 到分钟（真库样本
			//   `2026-09-12T07:45+08:00`）⇒ 二者**字符串比较跨时区错位**（实测同分钟边界判窗外·
			//   窗实为 7d+8h·方向保守）。改为与 nowIso **同形**（本地时区到分钟 + `+08:00`）。
			//   ⚠ **勿再引入本地分量**（A″D9 订正）：本式与 nowIso 同为「+8h → getUTC* → 硬编码 +08:00」，
			//   六时区扫描（Shanghai/UTC/Tokyo/New_York/Berlin/Kiritimati）输出**恒定**、与 nowIso 逐值相等
			//   ⇒ 已与 TZ 无关。早先注释写「若迁移时区须同改」是**错的**——照办会把本地分量重新引入＝重造 bug。
			const _cutD = new Date(Date.now() - _d * 86400000 + 8 * 3600 * 1000);
			const _p2 = (x) => String(x).padStart(2, "0");
			const cut =
				_cutD.getUTCFullYear() + "-" + _p2(_cutD.getUTCMonth() + 1) + "-" + _p2(_cutD.getUTCDate()) +
				"T" + _p2(_cutD.getUTCHours()) + ":" + _p2(_cutD.getUTCMinutes()) + "+08:00";
			try {
				const r = db
					.prepare(
						//    **两源并集**：会话记忆 `session:<sid>`（前缀 8 字符）**∪** 锚条 `auto:<sid>`
						//    （前缀 5 字符）——只看前者会误删「只产锚条、未写普通记忆」会话的计数键
						//    （虽有三源兜底不致重号，但白丢加速缓存）。
						// AUDIT-FIX-20260912（修复车·审计 C 路第 5 项配套·**三态保信息闸**）：原判据只看
						//   memories 的时间窗，而「锚条 INSERT 失败」时库内无 `auto:<sid>` 锚条 ⇒ 该键会被当
						//   冗余缓存清掉，可它此刻**正是唯一事实源**（第三源＝锚条数已失效）⇒ 重号窗口。
						//   三态：①**孤儿键**（库内该 sid 零痕迹——连 session:/auto: 都没有）⇒ 真清（否则本刀要治的
		//   「只增不减」被重新打开）；②有痕迹且 `键值 <= 锚条数` ⇒ 键确是冗余缓存（第三源可兜）⇒ 清；
		//   ③有痕迹且 `键值 > 锚条数` ⇒ **有锚条丢失** ⇒ 保留。
		//   ⚠ **措辞订正（审计 A 路 D3·2026-09-12）**：①态**不属「保信息」面**——它是**有意清理**，安全性
		//   依据是「零痕迹 ⇒ 零锚条 ⇒ anchorCountOf=0 ⇒ 下一号自 1 起、无并存」，而非「该键不会被清」。
		//   **已知边界（诚实账）**：若外部删除或备份镜像回灌后旧锚条复现，重置序号会与旧锚条重号；
		//   该形态无现成触发路径，登记为边界而非缺陷。
						//   ⚠ 首版只写「②③」（缺 ① 逃逸体）⇒ **孤儿键永不可清**（drill 全量重跑当场抓红·
						//     修复车自伤第二例）——这正是「修毕必再跑一轮」的实证依据。
						"DELETE FROM organ_meta WHERE k LIKE 'compaction_count.%' " +
							"AND substr(k, 18) NOT IN (" +
							"SELECT substr(source, 9) FROM memories WHERE source LIKE 'session:%' AND ts >= ? " +
							"UNION SELECT substr(source, 6) FROM memories WHERE source LIKE 'auto:%' AND ts >= ?) " +
							"AND (NOT EXISTS (SELECT 1 FROM memories WHERE source = 'session:' || substr(k, 18) " +
							"OR source = 'auto:' || substr(k, 18)) " +
							"OR CAST(v AS INTEGER) <= (SELECT COUNT(*) FROM memories " +
							"WHERE source = 'auto:' || substr(k, 18) AND title LIKE '上下文压缩锚%'))",
					)
					.run(cut, cut);
				const n = Number(r && r.changes) || 0;
				stats.organMetaPruned = (stats.organMetaPruned || 0) + n;
				return n;
			} catch (eP) {
				stats.organMetaPruneErrors = (stats.organMetaPruneErrors || 0) + 1; // ⑬ catch 必须有出口
				ctx.logger?.warn?.(
					"[living-memory] organ_meta prune failed (#" +
						stats.organMetaPruneErrors +
						"): " +
						String(eP && eP.message ? eP.message : eP).slice(0, 80),
				);
				return 0;
			}
		};
		// ── 信号队列（wave）：graph-memory gm_signals 表意——提炼候选优先触发器（三真信号·40 帽）
		let signalQ = []; // {type, at, hint}
		const episodicRate = new Map(); // sessionId -> { turns, at }——read_episodic 轻频控（补件④：每窗 4 真用户轮 1 次）
		const INBOX_RE = CRIT.inboxPath; // inbox路径 pattern 已外置（2026-09-12 一车·读inbox动作识别）
		const M_EXEMPT_RE = CRIT.exemptShort; // option豁免短令词表已外置（单源——user/message 留档与 turn/end 判定两用·2026-09-12 一车）
		// ── 3 天窗刀（design-approved「按推荐」）：option nudge 两率持久化——观测窗 7→3 天·打戳即 SQL 原子自增 organ_meta（跨重启累计·v+1 型 UPSERT 免竞态·a25Flush 锁竞争教训同防）──
		const bumpNudgeTotal = (k) => {
			try {
				db.prepare(
					"INSERT INTO organ_meta(k, v) VALUES(?, '1') ON CONFLICT(k) DO UPDATE SET v = CAST(CAST(v AS INTEGER) + 1 AS TEXT)",
				).run(k);
			} catch {
				stats.nudgePersistErrors = (stats.nudgePersistErrors || 0) + 1; // ⑬ catch 必须有出口
			}
		};
		const readNudgeTotal = (k) => {
			try {
				const r = db.prepare("SELECT v FROM organ_meta WHERE k = ?").get(k);
				return r ? Number(r.v) || 0 : 0;
			} catch {
				return 0;
			}
		};
		// bindMap 过期清理由 sweepExpiredAccounts 统一承担（8-31 移植族修复④：原 session/end 监听=幻事件·宿主 SessionEventMap 无终结型事件·整段死码）
		ctx.on("session/event", (session, event) => {
			stats.firehoseSeen = true;
			stats.eventCount += 1;
			const t = String(event.type);
			// B/A 绑定观测：工具流检测（tool/call 事件带工具名与参数——识别读inbox与 memory 调用）
			try {
				const bindSid =
					session && typeof session.id === "string"
						? session.id
						: sessionOfLastTurn || "_anon";
				let bd = bindMap.get(bindSid);
				if (!bd) {
					bd = {
						inboxSeen: false,
						memCalled: false,
						userTurns: 0,
						warnedA: false,
						warnedB: false,
						at: Date.now(),
						turnMemCalled: false,
						lastMemUserTurn: -99,
						lastUserText: "",
						nudgeMShownAt: 0,
						_userMsgThisTurn: false,
					};
					bindMap.set(bindSid, bd);
				}
				if (t === "turn/start") {
					bd.turnMemCalled = false;
					bd.lastOrderText = "";
				} // option：新轮重置本 turn 已查戳+本轮directive留档（I-1）
				if (t === "tool/call") {
					const name = String(event.data?.name || event.data?.toolName || "");
					const argStr = JSON.stringify(
						event.data?.arguments || event.data?.args || {},
					).slice(0, 400);
					if (INBOX_RE.test(argStr)) bd.inboxSeen = true;
					if (name === "memory" || name === "memory_write") {
						// option：本 turn 已查戳+最近查轮（豁免面）+提示听从结算
						bd.memCalled = true;
						bd.turnMemCalled = true;
						bd.lastMemUserTurn = bd.userTurns;
						if (bd.nudgeMShownAt && Date.now() - bd.nudgeMShownAt < 600000) {
							stats.nudgeTurnFollowed = (stats.nudgeTurnFollowed || 0) + 1;
							bumpNudgeTotal("nudge_followed_total"); // 3 天窗刀：听从结算即持久化
							bd.nudgeMShownAt = 0;
						}
					}
					// ── 六信号桥(design note)：tool_error/user_correction/
					//    task_completed 三真信号入提炼候选优先队列——用户纠错与工具报错不再等限频窗·下轮 turn/end 即提炼。
					//    审计 D8 收紧（15:25·自伤预防）：误信号=伪优先触发（白耗提炼次数+signalQ 假账）——
					//    ①user_correction 补 user/message 真源判（纠错主面在用户消息非工具参数·graph-memory user_correction 语义正源）
					//    ②task_completed 词表收紧（「完成毕」易在指令复述中误中——需连「收官/wrap-up」语境）
					//    ③tool_error 只认 error 真值字段（status 字符串 'error' 为下游结果事件面·tool/call 段恒不中=死面删除）。
					try {
						if (!process.env.LEGION_SIGNALS_OFF) {
							const d = event.data || {};
							if (d.error === true || d.isError === true)
								signalQ.push({
									type: "tool_error",
									at: Date.now(),
									hint: String(name).slice(0, 60),
								});
							if (CRIT.taskCompleted.test(argStr) && name !== "memory") // 完成信号词表已外置（2026-09-12 一车）
								signalQ.push({
									type: "task_completed",
									at: Date.now(),
									hint: argStr.slice(0, 60),
								});
							if (signalQ.length > 40) signalQ = signalQ.slice(-40);
							stats.signalCount = signalQ.length;
						}
					} catch {}
				}
				// ── P0修（09-03 audit）：tool_error 死面根治——宿主 tool/call 无 error 字段（真值在 tool/result message.isError·dsh-session L308-313），补独立分支（信号通道接通）
				if (t === "tool/result" && !process.env.LEGION_SIGNALS_OFF) {
					try {
						const rm = event.data?.message || event.data || {};
						if (rm.isError === true || event.data?.isError === true) {
							signalQ.push({
								type: "tool_error",
								at: Date.now(),
								hint: String(event.data?.name || rm.name || "tool").slice(
									0,
									60,
								),
							});
							if (signalQ.length > 40) signalQ = signalQ.slice(-40);
							stats.signalCount = signalQ.length;
						}
					} catch (eTR) {
						stats.signalErrors = (stats.signalErrors || 0) + 1;
					}
				}
				// ── 8-31 移植族修复③：user_correction 从 tool/call 块内解放——原判 t==='user/message' 嵌在 t==='tool/call' 分支内恒假（纠错信号自上线即死·宿主实码对照settled）；user/message 经 session/event 通道同机到达，独立同层判定
				try {
					if (
						!process.env.LEGION_SIGNALS_OFF &&
						t === "user/message" &&
						event?.data?.source?.kind === "user" &&
						typeof event.data.content !== "undefined"
					) {
						const uc = event.data.content;
						const uText = Array.isArray(uc)
							? uc.map((b) => (b && b.text) || "").join("")
							: String(uc);
						if (/不对|错了|不是这样|重新|纠错|修正/.test(uText)) {
							signalQ.push({
								type: "user_correction",
								at: Date.now(),
								hint: uText.slice(0, 60),
							});
							if (signalQ.length > 40) signalQ = signalQ.slice(-40);
							stats.signalCount = signalQ.length;
						}
					}
				} catch {}
				if (t === "user/message" && event?.data?.source?.kind === "user") {
					// 审计修正：只计真用户轮（插件注入的 user/message 不计——同 dsh-compaction isDshUserTurn 判法）
					bd.userTurns += 1;
					bd._userMsgThisTurn = true; // option：本轮真用户消息戳
					const uc2 = event.data.content;
					bd.lastUserText = (
						Array.isArray(uc2)
							? uc2.map((b) => (b && b.text) || "").join("")
							: String(uc2 || "")
					).slice(0, 200); // option：directive文本留档（豁免面判定）
					const _ut = bd.lastUserText.trim();
					if (
						_ut.length > (bd.lastOrderText || "").trim().length &&
						!M_EXEMPT_RE.test(_ut)
					)
						bd.lastOrderText = bd.lastUserText; // 审计 I-1 修：另留「本轮最长非豁免文本」——末条短令掩护长令漏提示（连发场景）
				}
				// B 告警：读了inbox但至今未查living memory → turn/end 时提示（观测段）
				if (bd.inboxSeen && !bd.memCalled && !bd.warnedB && t === "turn/end") {
					bd.warnedB = true;
					bd.pendingNudge = "B"; // 回显step（21:43 maintainer·28375d 案治理）：观测升格——下轮注入面带可见提示（模型看得见·不再只进日志）
					ctx.logger?.warn?.(
						"[living-memory] nudge B watch[" +
							bindSid.slice(0, 12) +
							"]: inbox was read but memory was never queried — the two go together (also shown in the injected context)",
					);
				}
				// A 告警：前 2 真用户轮内零 memory 调用（a quick window一次待办查询即满足）
				if (
					bd.userTurns >= 2 &&
					!bd.memCalled &&
					!bd.warnedA &&
					t === "turn/end"
				) {
					bd.warnedA = true;
					if (!bd.pendingNudge) bd.pendingNudge = "A"; // 同上·升格注入可见
					ctx.logger?.warn?.(
						"[living-memory] nudge A watch[" +
							bindSid.slice(0, 12) +
							"]: two user turns in with no memory call — check memory first (also shown in the injected context)",
					);
				}
				// ── 8-31 option（轮内记忆先行闸·approved·观测级先行——刀②A/B 同法：先观测后拦截）：本轮有真用户令且未调 memory 且离上次查≥3 真用户轮且非豁免短令 → 下轮注入面提示
				if (
					t === "turn/end" &&
					bd._userMsgThisTurn &&
					!bd.turnMemCalled &&
					bd.lastOrderText
				) {
					// I-1 修：判定用「本轮最长非豁免文本」（原 lastUserText 末条覆盖·连发场景短令掩护长令漏提示）
					if (
						bd.lastOrderText.trim().length >= 8 &&
						!M_EXEMPT_RE.test(bd.lastOrderText.trim()) &&
						bd.userTurns - (bd.lastMemUserTurn ?? -99) >= 3
					) {
						if (!bd.pendingNudge) {
							bd.pendingNudge = "M";
							stats.nudgeTurnShown = (stats.nudgeTurnShown || 0) + 1;
							bumpNudgeTotal("nudge_shown_total"); // 3 天窗刀：展示打戳即持久化
						}
						ctx.logger?.warn?.(
							"[living-memory] 方案观测[" +
								bindSid.slice(0, 12) +
								"]：轮内指令未先查记忆库——记忆先行提示（观测级·不拦截）",
						);
					}
				}
				// ── 8-31 option（turn/end 主动率审计·②已批件复活）：真用户轮结算「主动查记忆率」——观测量化面（each space横向可比）
				if (t === "turn/end" && bd._userMsgThisTurn) {
					stats.memoryTotalTurns = (stats.memoryTotalTurns || 0) + 1;
					if (bd.turnMemCalled)
						stats.memoryActiveTurns = (stats.memoryActiveTurns || 0) + 1;
					bd._userMsgThisTurn = false;
				}
			} catch {}
			if (stats.recentTypes[stats.recentTypes.length - 1] !== t) {
				stats.recentTypes.push(t);
				if (stats.recentTypes.length > 15) stats.recentTypes.shift();
			}
			if (session && typeof session.id === "string")
				sessionOfLastTurn = session.id;
			// 文本缓冲：只收 user/assistant 的文本内容
			if (t === "user/message" || t === "assistant/message") {
				let text = "";
				try {
					const content =
						t === "user/message"
							? event.data?.content
							: event.data?.message?.content;
					if (Array.isArray(content)) {
						for (const block of content) {
							if (
								block &&
								typeof block === "object" &&
								typeof block.text === "string" &&
								(block.type === "text" ||
									block.type === "input_text" ||
									block.type === "output_text")
							) {
								text += block.text;
							}
						}
					}
				} catch {}
				if (text.length > 0) {
					buffer += (buffer ? "\n---\n" : "") + text.slice(0, 3000);
					if (buffer.length > 12000) buffer = buffer.slice(-12000);
					// ── 摄取面（wave·审计 D2 后=按会话分账入队）：消息级 {turn,role,text,seq} 结构化
					if (!A12_OFF) {
						const sid =
							(session && typeof session.id === "string"
								? session.id
								: sessionOfLastTurn) || "_anon";
						let acct = pendingBySid.get(sid);
						if (!acct) {
							acct = { msgs: [], watermark: 0, seq: 0 };
							pendingBySid.set(sid, acct);
						}
						acct.seq += 1;
						acct.lastTs = Date.now(); // 过期兜底扫时间戳（审计：死语句修正——msgs 不带 ts·账级 lastTs 单源）
						acct.msgs.push({
							turn: stats.eventCount,
							role: t === "user/message" ? "USER" : "ASSISTANT",
							text: text.slice(0, 3000),
							seq: acct.seq,
						});
						if (acct.msgs.length > 400) acct.msgs = acct.msgs.slice(-400); // 无界保护（每会话 400 帽·与 buffer 12k 同精神）
						let pend = 0;
						for (const a of pendingBySid.values())
							pend += a.msgs.filter((m) => m.seq > a.watermark).length;
						stats.pendingMsgsCount = pend;
						a25Flush(); // 摄取节流落盘（5s）
					}
					stats.bufferedChars = buffer.length;
				}
			}
			// 自动提炼触发：turn/end + 限频 + 批量阈值即时通道 + 信号优先通道
			// ── 双通道：①buffer≥600 即时；②信号队列有货（近 10min）优先触发不等限频——
			//    用户纠错/工具报错当场沉淀（graph-memory 信号驱动意·治「他窗照犯」）。
			//    审计 D7 修正（15:25）：runExtract 系异步不阻塞 firehose——原紧邻 `signalQ=[]` 在提炼**发起时**
			//    即清队（不等成败）→提炼失败（guard 熔断/网络退避）信号已丢=纠错白触发。修：清队移入提炼成功路径
			//    （watermark 推进同点·信号与水位同生命周期：成功才消化）。失败留队下轮 turn/end 再试。
			if (t === "turn/end" && buffer.trim().length > 200 && !extracting) {
				const now = Date.now();
				const batchReady = buffer.length >= 600;
				const signalReady =
					signalQ.length > 0 &&
					Date.now() - signalQ[signalQ.length - 1].at < 10 * 60_000;
				if (
					batchReady ||
					signalReady ||
					stats.lastExtractAt === null ||
					now - stats.lastExtractAt >= EXTRACT_INTERVAL_MS
				) {
					if (signalReady) {
						stats.signalTriggered = (stats.signalTriggered || 0) + 1;
						stats.signalArmed = true;
					} // armed=本轮提炼open item·成功路径消化
					runExtract({ auto: true });
				}
			}
			// ── 压缩↔living memory联动桥（design-approved「同意」·刀①②·压缩机制体检终报呈件）──
			//    实勘依据：dsh-compaction-basic compactSurfaceRegion——start 在摘要前 append（被压段原文仍在 events），
			//    end 带成败；session.append 无差别过 session/event firehose（dsh-session L1469-1476）→ 本监听零宿主改动。
			//    刀① start=压缩前清算：该 sid 未抽段（水位面=压缩前最后原文摄取）fire-and-forget 提炼入册——
			//    被压细节先落living memory再离窗。刀② end=检查点锚条入册+建边：被压段经此锚 read_episodic 可回流
			//    （source=auto:<sid> 满足 episodic 通道）。回退：LEGION_COMPACT_BRIDGE_OFF。
			if (!process.env.LEGION_COMPACT_BRIDGE_OFF && t === "compaction/start") {
				try {
					const cSid =
						session && typeof session.id === "string" ? session.id : null;
					if (cSid) {
						const acct = pendingBySid.get(cSid);
						const unextracted = acct
							? acct.msgs.filter((m) => m.seq > acct.watermark).length
							: 0;
						if (unextracted > 0) {
							stats.compactionChaseRuns = (stats.compactionChaseRuns || 0) + 1;
							(async () => {
								try {
									let g = 0;
									while (g < 10) {
										const r = await runExtract({ sid: cSid });
										if (!r || r.skipped) break;
										g++;
									}
								} catch (eC) {
									stats.compactionChaseErrors =
										(stats.compactionChaseErrors || 0) + 1;
									ctx.logger?.warn?.(
										"[living-memory] compaction chase failed (#" +
											stats.compactionChaseErrors +
											"): " +
											String(eC).slice(0, 60),
									);
								}
							})(); // ⑬：fire-and-forget 但 catch 有出口（计数+warn）
							ctx.logger?.info?.(
								"[living-memory] compaction/start 捕获[" +
									cSid.slice(0, 12) +
									"]：压缩前清算发起（未抽段 " +
									unextracted +
									"）",
							);
						}
					}
				} catch (eS) {
					// ⑬ 补（design-approved「同车修」精神扩及同族·** 改进刀① 处置**）：**审计 N1 订正**—— 原文末句为「候maintainer是否即修」⇒ 性质＝**候裁 9 天未决**（非「积压未落」欠账·与本窗  订正一致）
					//   09-03 压缩桥车审计列 2 条改进——刀① 本处 start 段外层 catch{} 无计数（chase 同步段失败静默）、
					//   刀② end 段建边内层 catch{}；刀② 已于本车第四笔（2cd9112）修复，刀① 按「同族全点枚举」亲验hard rule一并补，禁留同族残尾。
					//   复用既有 compactionBridgeErrors（**三源共用一键**·审计 I2 注记：start 段外层 eS／end 段建边内层 eEdge／end 段外层 eB ⇒ 计数只可加总·归因靠 warn 文案前缀）＋零新增计数键。
					stats.compactionBridgeErrors =
						(stats.compactionBridgeErrors || 0) + 1;
					try {
						ctx.logger?.warn?.(
							"[living-memory] compaction/start bridge failed (#" +
								stats.compactionBridgeErrors +
								"): " +
								String(eS).slice(0, 60),
						);
					} catch {} // ⑬ 豁免报备（审计 I1）：本 catch 仅护上方 warn 调用（调用点已带 ?.）——增计已在其前执行·出口经 render「bridge错」透出，非观测黑洞
				}
			}
			if (
				!process.env.LEGION_COMPACT_BRIDGE_OFF &&
				t === "compaction/end" &&
				!(event.data && event.data.error)
			) {
				// AUDIT-FIX-20260912（修复车·审计 C 路第 5 项）：**锚条入册失败时的序数事实源兜底**——
				//   cSid2B/cNB 在 try 外声明，供 catch(eB) 回落落键（块级 const 在 catch 不可见）。
				let cSid2B = null,
					cNB = 0;
				try {
					const cSid2 =
						session && typeof session.id === "string" ? session.id : null;
					if (cSid2) {
						const n = nextCompactionN(cSid2); // 09-11：持久计数（原 `(compactionBySid.get(...)||0)+1` 单源 ⇒ 重启归零同名漂移）
						cSid2B = cSid2; // AUDIT-FIX-20260912（C 路第 5 项）：登记供 catch(eB) 回落落键
						cNB = n;
						compactionBySid.set(cSid2, n);
						stats.compactionSeen = (stats.compactionSeen || 0) + 1;
						// 刀②：检查点锚条入册——被压段经此锚 read_episodic 回流（step通道·source=auto:<sid> 现成）
						const cSpace = sessionOrgan(cSid2) || "global";
						// 标题带 **sid 前 8 位**（09-11 同车）：同 space 多窗锚条互不混同·「搜上下文压缩锚」可辨识来源窗
						const cTitle =
							"上下文压缩锚：" +
							String(cSid2).replace(/^session-/, "").slice(0, 8) +
							" 第 " +
							n +
							" 次压缩";
						const cContent =
							"compaction/end 捕获（compactionId=" +
							String(event.data?.compactionId || "?").slice(0, 36) +
							"）——被压段原文在 session.jsonl.zstd 全量留盘，read_episodic 本条 id 可取压缩前窗口原文。";
						const cInfo = db
							.prepare(
								"INSERT INTO memories (ts, type, title, content, space, source, checksum) VALUES (?, 'fact', ?, ?, ?, ?, ?)",
							)
							.run(
								nowIso(),
								cTitle,
								cContent,
								cSpace,
								"auto:" + cSid2,
								sha1(cSid2 + n + cTitle),
							);
						// 建边：链回该会话近 3 条同源条目（UPSERT 同法·幂等）
						try {
							const nearRows = db
								.prepare(
									"SELECT id FROM memories WHERE source = ? AND id != ? ORDER BY id DESC LIMIT 3",
								)
								.all("auto:" + cSid2, Number(cInfo.lastInsertRowid));
							const cEdge =
								db.prepare(`INSERT INTO memories_edges (src, dst, edge_type, weight, valid_at, source_group, last_seen, instruction)
                VALUES (?, ?, 'explicit', 0.5, ?, 'compact-bridge', ?, '压缩检查点锚·被压段经此锚 read_episodic 回流')
                ON CONFLICT(src, dst, edge_type) DO UPDATE SET last_seen = excluded.last_seen`);
							for (const nr of nearRows)
								cEdge.run(
									Number(cInfo.lastInsertRowid),
									nr.id,
									nowIso(),
									nowIso(),
								);
							stats.compactionBridged = (stats.compactionBridged || 0) + 1;
						} catch (eEdge) {
							// ⑬ 补（design-approved「同车修」·压缩质检派生）：原 `catch {}` 空吞＝**建边失败静默**——
							//   锚条已入册而边缺失时 compactionBridged 与 compactionBridgeErrors **双不增** ⇒ 观测黑洞（⑬「空 catch 一律视为缺陷」同族）。
							//   修：异常计入既有 compactionBridgeErrors（**零新增计数键** ⇒ render「bridge错」与 stats return 四点接线已在位·无新键接线负担）+ warn 留痕。
							stats.compactionBridgeErrors =
								(stats.compactionBridgeErrors || 0) + 1;
							try {
								ctx.logger?.warn?.(
									"[living-memory] compaction anchor edge failed (#" +
										stats.compactionBridgeErrors +
										"): " +
										String(eEdge).slice(0, 60),
								);
							} catch {} // ⑬ 豁免报备（审计 I1·同族）：本 catch 仅护上方 warn 调用（调用点已带 ?.）——增计已在其前执行·出口经 render「bridge错」透出
						}
					}
				} catch (eB) {
					stats.compactionBridgeErrors =
						(stats.compactionBridgeErrors || 0) + 1;
					try {
						ctx.logger?.warn?.(
							"[living-memory] compaction bridge failed (#" +
								stats.compactionBridgeErrors +
								"): " +
								String(eB).slice(0, 60),
						);
					} catch {}
					// AUDIT-FIX-20260912（修复车·审计 C 路第 5 项）：**回落落键**（函数体见
					//   fallbackWriteCompactionN·与之配套的 prune 侧「保信息闸」见 pruneOrganMeta）。
					fallbackWriteCompactionN(cSid2B, cNB);
				}
			}
			if (t === "turn/end") sweepExpiredAccounts(); // 8-31 修复④：过期扫挂真事件每轮尾（原幻事件整段死码——防 Map 无界+过期账清算+落盘恢复在产）
		});

		// ── 吸收项·每轮自动召回（wave·13:07 maintainer融入令；源=graph-memory dsh.ts L472-556 三件套逐块融入）──
		//    机制：agent/inbox/claimed 捕获真用户令 → system-prompt/assemble（宿主契约=dsh-system-prompt waterfall
		//    assembly{sections,contexts,variables}）时以其为 query 跑living memory FTS 检索链 → one-pass 变量注入
		//    （{{var}} 占位+variables 存值——召回文本当数据不当模板源·防提示词注入·graph-memory prompt-data 同法）。
		//    纪律：注入=参考级慎用口径（同 v2.3 ⑤）·高精门 bm25≥0.18 只进强相关·同 query 5min 缓存·
		//    检索失败静默零阻断·env LEGION_AUTORECALL_OFF 一键关。红线：只挂 contexts 不动 sections（人格/directive层零碰）。
		const AUTO_RECALL_MAX = 4;
		const AUTO_RECALL_MIN_SCORE = 0.18;
		const autoLatestPrompt = new Map(); // agentId -> { text, at }（8-31 修复④：带捕获时点——过期扫按 at 判·治「无 cache 即删」误清 claimed→assemble 窗口新令）
		const autoRecallCache = new Map(); // agentId -> { query, at, text }
		try {
			ctx.on &&
				ctx.on("agent/inbox/claimed", (payload) => {
					try {
						const message = payload?.message ?? payload;
						if (message?.source?.kind !== "user") return;
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
		} catch {}
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
		function legionContributeAutoRecall(assembly, text) {
			try {
				const base = "legion_auto_recall";
				let name = base,
					n = 2;
				assembly.variables = assembly.variables || {};
				while (Object.hasOwn(assembly.variables, name)) name = base + "_" + n++;
				assembly.variables[name] = text;
				assembly.contexts.push({ name, text: "{{" + name + "}}" });
			} catch {}
		}
		try {
			ctx.on &&
				ctx.on("system-prompt/assemble", async (assembly, context, next) => {
					// P2 fix（09-03 audit）：双 next 根治——try 内早退支 return await next() 若 reject 被 eAR catch 吞后落尾部再调 next（cordis next 一次性）；
					// 修：早退支全走 runNextAR（打戳+不 await→reject 天然传播宿主 waterfall 不碰 catch）·尾部唯一条件调用点。
					let nextCalledAR = false;
					const runNextAR = () => {
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
							if (hit.text) legionContributeAutoRecall(assembly, hit.text);
							return runNextAR(); // P2 fix
						}
						const callerSpace = callerKey ? sessionOrgan(callerKey) : null;
						const tokens = qTokens(query); // item：实词优选（jieba 滤虚词+西文专名最前〔option遗产守〕+bigram 兜底）——原「option西文排序+slice(0,8)」与更早纯 slice 两版统一收编本函数
						const match = queryMatch(tokens);
						const ftsBase = `SELECT m.id, m.type, m.title, m.content, m.space, COALESCE(m.event_at, m.ts) AS eff_ts, bm25(memories_fts) AS rank FROM memories_fts JOIN memories m ON m.id = memories_fts.rowid WHERE memories_fts MATCH ? AND m.status = 'active'`; // option伴修：ftsBase 上提（原 if(match) 块内 const——主题词重查段块外引用 ReferenceError 静默·caught in drill）
						let picked = [];
						if (match) {
							const rows = callerSpace
								? db
										.prepare(
											ftsBase +
												` AND m.space IN (?, 'global') ORDER BY rank LIMIT 40`,
										)
										.all(match, callerSpace)
								: db.prepare(ftsBase + ` ORDER BY rank LIMIT 40`).all(match);
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
									const tt = new Set(tokenize(String(r.title)));
									return tokens.filter((tk) => tt.has(tk)).length >= 2;
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
							const properNouns = tokens.filter((t) =>
								/^[\x21-\x7e]{2,}$/.test(t),
							);
							if (properNouns.length > 0) {
								const pnMatch = properNouns
									.map((t) => '"' + t.replace(/"/g, "") + '"')
									.join(" OR ");
								const pnRows = callerSpace
									? db
											.prepare(
												ftsBase +
													` AND m.space IN (?, 'global') ORDER BY rank LIMIT 10`,
											)
											.all(pnMatch, callerSpace)
									: db
											.prepare(ftsBase + ` ORDER BY rank LIMIT 10`)
											.all(pnMatch);
								const seen0 = new Set(picked.map((r) => r.id));
								const pnPicked = pnRows
									.filter((r) =>
										properNouns.some((t) =>
											(
												String(r.title) +
												" " +
												String(r.content || "").slice(0, 200)
											)
												.toLowerCase()
												.includes(t.toLowerCase()),
										),
									)
									.map((r) => ({ ...r, score: 0.99, pnRescue: true }))
									.filter((r) => !seen0.has(r.id))
									.slice(0, 2);
								if (pnPicked.length > 0) {
									picked = [...pnPicked, ...picked].slice(0, AUTO_RECALL_MAX);
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
															` AND m.space IN (?, 'global') ORDER BY rank LIMIT 40`,
													)
													.all(match2, callerSpace)
											: db
													.prepare(ftsBase + ` ORDER BY rank LIMIT 40`)
													.all(match2);
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
													ov.some((t) => /^[\x21-\x7e]{2,}$/.test(t))
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
								if (vc && vc.value) {
									if (!vecCredCache.v || Date.now() - vecCredCache.t > 60000)
										vecCredCache = { v: vc.value, t: Date.now() }; // option伴修：embedOnce 凭据前置——vecCredCache 本由 vecRecallCore 填充·直接调 embedOnce 空 Bearer 401 静默（caught in drill·移植接口同构亲验闸同族）
									// item1：查询侧 instruct 同落此兜底（查询身份一致）——此路查 memories_vec
									// 旧表（v4·instruct 未探针）；400 则 catch 静默降级 FTS·留 A/B counter。
									const [qv] = await embedOnce(
										[String(query).slice(0, 1500)],
										undefined,
										INSTRUCT_QUERY,
									);
									const vsql =
										`SELECT v.id, v.embedding, m.type, m.title, m.content, m.space, COALESCE(m.event_at, m.ts) AS eff_ts FROM memories_vec v JOIN memories m ON m.id = v.id WHERE v.model_version = ? AND m.status='active'` +
										(callerSpace ? ` AND m.space IN (?, 'global')` : "");
									const vrows = callerSpace
										? db.prepare(vsql).all(VEC_MODEL, callerSpace)
										: db.prepare(vsql).all(VEC_MODEL);
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
						let text = picked.length
							? "## 记忆库自动召回（⚠ 参考级·与最新记忆相关，引用前以当轮实况与正本核对）\n" +
								picked
									.map(
										(r) =>
											"- 🕐" +
											String(r.eff_ts).slice(5, 10) +
											(r.vecFallback
												? " 🧬[语义召回·" + r.type + "·" + r.space + "] "
												: " [" + r.type + "·" + r.space + "] ") +
											r.title +
											"——" +
											String(r.content || "").slice(0, 80),
									)
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
										"SELECT id, title, ts FROM memories WHERE space = ? AND type IN ('fact','decision') AND status = 'active' AND ts <= ? ORDER BY id DESC LIMIT 1",
									)
									.get(stitchSpace, cutIso);
								if (last && !pickedIds.has(last.id)) {
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
						try {
							if (text)
								text +=
									"\n🔍 盲区自检：以上为词面命中——决策前问自己「与主题相关但未检索的维度」（邻近域/反例/更早先例）·不确定即再发 search 换词";
						} catch {}
						autoRecallCache.set(key, { query, at: Date.now(), text });
						if (text) legionContributeAutoRecall(assembly, text);
					} catch (eAR) {
						if (nextCalledAR) throw eAR; // P2 fix：next 已调则此 reject 系下游失败——传播不吞（防尾部双调）
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
					if (!nextCalledAR) return await next(); // P2 fix：唯一调用点
				});
		} catch {}

		// ── nightly patrol(design note)──
		// 整理四件事：①精确去重 ②高相似合并 ③handover note警报 ④todo 衰减
		// ③handover note警报（pressure>70%）④todo 衰减——详见函数体
		function nightPatrol(force) {
			// ── step：maintenance window闸（最顶部——force 也不跳过此闸；摘牌即恢复）──
			//    step顺手件：info→warn + 计数器
			try {
				if (fs2.existsSync(SURGERY_FLAG)) {
					stats.surgerySkipCount = (stats.surgerySkipCount || 0) + 1;
					ctx.logger?.warn?.(
						`[living-memory] surgery-flag: skip patrol (#${stats.surgerySkipCount})`,
					);
					return;
				}
			} catch {}
			const d = new Date();
			const hour = d.getHours();
			if (!force && (hour < PATROL_START_HOUR || hour >= PATROL_END_HOUR))
				return;
			const dayKey = `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}-${String(d.getDate()).padStart(2, "0")}`;
			if (!force && stats.lastPatrolDay === dayKey) return; // 每晚最多一次
			stats.lastPatrolDay = dayKey;
			let merged = 0;
			let mergedIds = [];
			let conflictsPending = 0;
			let patrolMergedOut = 0; // 8-31 锈面修⑤：本巡 执行器真合并条数（light sentry合法流转豁免面·active 计数合法流出）
			const mergeIds = new Set(); // step伴修 D1（caught in drill）：S1 step「上提」落错块——声明进了 conflicts 内层 try·L1651 外层引用 ReferenceError 夜夜被外层 catch 吞（活·①精确填账+审计清单死）。真上提至本层三面同见。
			try {
				const rows = db
					.prepare(
						"SELECT id, ts, type, title, space FROM memories WHERE status = 'active' AND source NOT LIKE 'mirror:%' ORDER BY id DESC",
					)
					.all(); // D13 自伤修正+step豁免（07:23 maintainer）：mirror 影子条不参与去重立单（source of record在 MEMORY.md·手写已在库——N7 471 伪案根治）
				// ── 3a 冲突三态化（2026-08-23 阶段三release note·hard rule：不静默删——原静默 merged 改道 pending pending ruling）──
				try {
					db.exec(`CREATE TABLE IF NOT EXISTS conflicts (
            conflict_id INTEGER PRIMARY KEY AUTOINCREMENT,
            new_id INTEGER NOT NULL, old_id INTEGER NOT NULL,
            basis TEXT NOT NULL, evidence TEXT,
            status TEXT NOT NULL DEFAULT 'pending',
            decided_at TEXT, decided_by TEXT, created_at TEXT NOT NULL)`);
					try {
						db.exec("ALTER TABLE memories ADD COLUMN superseded_by INTEGER");
					} catch {}
					// ── wave AUDN：预裁决三列（幂等 ALTER·照 closed_at 先例）
					try {
						db.exec("ALTER TABLE conflicts ADD COLUMN pre_verdict TEXT");
					} catch {}
					try {
						db.exec("ALTER TABLE conflicts ADD COLUMN pre_reason TEXT");
					} catch {}
					try {
						db.exec("ALTER TABLE conflicts ADD COLUMN pre_at TEXT");
					} catch {}
					// 七小件⑥：同题并案计数列（独立 try——caught in drill：原四 ALTER 共用一个 try，pre_* 已存在即抛错短路 ⇒ 后续列永不补·生产同病）
					try {
						db.exec(
							"ALTER TABLE conflicts ADD COLUMN merged_count INTEGER DEFAULT 1",
						);
					} catch {}
					// AUDIT-FIX-20260912（修复车·审计 C 路第 1 项真漏项）：冲突升级词并入三级链第 11 键
					//   `sentryCritical`（实测第 11 键）——原字面量含魂律令词且被净化链重写为 /policy|charter|…/i（未披露·
					//   flags 亦被改）⇒ 外置后 code 面零机改，公开面词表在 CRITERIA_PUBLIC 显式登记。
					const CRIT_RE = CRIT.sentryCritical; // 重大项词——sentry report升级标（三级链第 11 键）
					const insC = db.prepare(
						"INSERT INTO conflicts (new_id, old_id, basis, evidence, status, created_at) VALUES (?, ?, ?, ?, 'pending', ?)",
					);
					const seenC = new Map();
					// 8-31 锈面修（审计片6①b）：handover note/light sentry警报类历史条与当夜条 family 判据夜夜流水立单（L5 幂等只锁当日）——
					//    扩为「同题族已裁决/未决已立案即不重立」：警报族（标题前缀族）任一成员已入 conflicts（任意状态）则整族不再立单。
					const ALERT_FAM_RE = CRIT.alertFamily; // 警报族前缀已外置（公开版显式 2 分支＝有意裁剪·见方案 §3.4）
					const alertFamFiled = new Map(); // 族名 → 是否已立案（本巡缓存·一族一查）
					const alertFamOf = (t) => {
						const m = String(t || "").match(ALERT_FAM_RE);
						return m ? m[1] : null;
					};
					const qAlertFam = db.prepare(
						"SELECT COUNT(*) c FROM conflicts co JOIN memories mn ON mn.id = co.new_id JOIN memories mo ON mo.id = co.old_id WHERE mn.title LIKE ? OR mo.title LIKE ?",
					);
					const alertFamBlocked = (fam) => {
						if (!alertFamFiled.has(fam))
							alertFamFiled.set(fam, qAlertFam.get(fam + "%", fam + "%").c > 0);
						return alertFamFiled.get(fam);
					};
					// ── 七小件⑤：系统自动条同题族豁免闸（09-08 maintainer·conflicts 313 案 85%=三类自动条互撞噪声）──
					//    豁免面=「两侧均系统自动条」的互撞立案；自动条 vs 手写条真冲突仍立（勿step切）。
					const SYS_AUTO_RE = CRIT.sysAutoPrefix; // 系统自动条前缀已外置（2026-09-12 一车）
					const sysAutoOf = (t) => SYS_AUTO_RE.test(String(t || ""));
					// ── 七小件⑥：同题 pending 并案制（O(n²)→O(n)·schema 取加列 merged_count 案）──
					const prefixKeyOf = (t) => {
						// 固定 6 字前缀（caught in drill：贪婪整段在无标点短题=前缀全题 ⇒ 永不并案·⑥名存实亡）
						const s = String(t || "");
						return s.length >= 6 ? s.slice(0, 6) : null;
					};
					const qMergePend = db.prepare(
						"SELECT co.conflict_id FROM conflicts co JOIN memories mn ON mn.id = co.new_id WHERE mn.space = ? AND mn.title LIKE ? AND co.status = 'pending' ORDER BY co.conflict_id LIMIT 1",
					);
					const uMergeCount = db.prepare(
						"UPDATE conflicts SET merged_count = COALESCE(merged_count, 1) + 1 WHERE conflict_id = ?",
					);
					const fileC = (newId, oldId, basisTxt, evTxt, space, title) => {
						const pk = prefixKeyOf(title);
						if (pk) {
							const prior = qMergePend.get(space, pk + "%");
							if (prior) {
								uMergeCount.run(prior.conflict_id);
								return false;
							}
						}
						insC.run(newId, oldId, basisTxt, evTxt, nowIso());
						return true;
					};
					// ── 七小件⑦：实体差异守卫（cid613 guard/gate 假阳型）──
					//    ⚠ 只挂 family 段；614 型（实体抽取漏专有名词·差集空）不拦——抽取面候办。
					const TIME_ENT_RE = /^\d{2,8}$/;
					const entOfMap = new Map();
					for (const er of db
						.prepare(
							"SELECT me.memory_id AS mid, e.name AS nm FROM memories_entities me JOIN entities e ON e.entity_id = me.entity_id",
						)
						.all()) {
						if (!entOfMap.has(er.mid)) entOfMap.set(er.mid, new Set());
						entOfMap.get(er.mid).add(String(er.nm));
					}
					const diffExclEntities = (aId, bId) => {
						if (!entOfMap.has(aId) || !entOfMap.has(bId)) return false;
						const A = [...entOfMap.get(aId)].filter(
							(x) => !TIME_ENT_RE.test(x),
						);
						const B = [...entOfMap.get(bId)].filter(
							(x) => !TIME_ENT_RE.test(x),
						);
						return (
							A.some((x) => !B.includes(x)) && B.some((x) => !A.includes(x))
						);
					};
					// ── 614 词面二层（09-09 maintainer·深挖专项案①· 案底）：复合专名型冲突的 title 差异守卫 ──
					//    根因链（深挖settled 09:4x）：614 型=实体抽取漏中文复合专名→diffExclEntities 差集空→family 误立案；
					//    首版判据（token 集差集）在生产 jieba 形态下过强——gateKeywords max=12 截断使相似题词集
					//    异位截断·人为双侧独占→全豁免→family 段零立案（「五形态玄学」实为此·eval 误判系 jieba
					//    加载失败退化 bigram 形态）。修法=全文包含判独占：词（≥2 字）不作为子串出现在对方 title
					//    全文才算独占——「补遗/安全」类修饰差与截断差皆非独占·「澜沧江/怒江」类真专名独占成立。
					//    ⚠ 只挂 family 段——semantic 段词面本可不重叠·挂=废判据。漏立（盘账兜底）＜误立（人工噪音）。
					const titleDiffExcl = (tA, tB) => {
						const a = String(tA),
							b = String(tB);
						const A = gateKeywords(tA, 12),
							B = gateKeywords(tB, 12);
						if (A.length === 0 || B.length === 0) return false;
						return (
							A.some((x) => x.length >= 2 && !b.includes(x)) &&
							B.some((x) => x.length >= 2 && !a.includes(x))
						);
					};
					for (const r of rows) {
						// exact 判定（原静默段捕获面原样改道）
						const key = r.type + "\u0000" + r.title;
						if (seenC.has(key)) {
							const old = seenC.get(key);
							// 幂等（2026-08-26 修法包·wave 审计根因）：exact 段补 dup 查询——防每轮nightly patrol对同对重复条目重复立 pending（50/53 双立实证）
							// E3 快治（design-approved·brain亲验定性）：①无序对查重（两值同交——病序老行 [如 52 号 (2580,2773) 旧序] 穿透新序查重落空=今夜重立根因）②AND status='pending'——已裁决（approved/resolved）同对不再重立（流水型警报根治）
							const dupX = db
								.prepare(
									"SELECT COUNT(*) c FROM conflicts WHERE new_id IN (?, ?) AND old_id IN (?, ?) AND new_id != old_id AND status = 'pending'",
								)
								.get(old.id, r.id, old.id, r.id).c;
							if (dupX === 0 && !(sysAutoOf(r.title) && sysAutoOf(old.title)))
								// 七小件⑤：双侧系统自动条互撞豁免（单侧手写=真冲突仍立）
								fileC(
									old.id,
									r.id,
									"exact",
									"type+title 全同" + (CRIT_RE.test(r.title) ? "|⚠重大" : ""),
									r.space,
									r.title,
									nowIso(),
								); // （2026-08-26 修法包）：两参对调——rows 按 id DESC·先见=更新条=保留方装 new_id·后见=更旧条=被合方装 old_id（原颠倒会致 approved 执行面反向合并保旧弃新）
						} else seenC.set(key, r);
					}
					// family 判定（同 type+space 桶·标题 bigram jaccard≥0.5）：评测评族口径同源——只记 pending 不并
					const bySpace3a = new Map();
					for (const r of rows) {
						if (!bySpace3a.has(r.space)) bySpace3a.set(r.space, []);
						bySpace3a.get(r.space).push(r);
					}
					for (const list of bySpace3a.values()) {
						if (list.length < 2 || list.length > 3000) continue;
						const grams = list.map((r) => ({
							id: r.id,
							type: r.type,
							title: r.title,
							g: bigrams(r.title),
						}));
						for (let i = 0; i < grams.length; i++) {
							for (let j = i + 1; j < grams.length; j++) {
								if (grams[i].type !== grams[j].type) continue;
								const sim = jaccard(grams[i].g, grams[j].g);
								if (sim < 0.5) continue;
								if (String(grams[i].title) === String(grams[j].title)) continue; // exact 已记
								const famI = alertFamOf(grams[i].title),
									famJ = alertFamOf(grams[j].title);
								if (famI && famI === famJ && alertFamBlocked(famI)) continue; // 8-31 锈面修①b：警报同题族已立案/已裁决——不重立（流水单根治）
								if (sysAutoOf(grams[i].title) && sysAutoOf(grams[j].title))
									continue; // 七小件⑤：双侧系统自动条互撞豁免
								if (diffExclEntities(grams[i].id, grams[j].id)) continue; // 七小件⑦：实体差异守卫（cid613 假阳根治）

								if (titleDiffExcl(grams[i].title, grams[j].title)) continue; // 614 词面二层（深挖修后回挂）：全文包含判独占——真专名差豁免·修饰差不豁免
								const nid = Math.max(grams[i].id, grams[j].id),
									oid = Math.min(grams[i].id, grams[j].id);
								// 8-31 锈面修（审计片6①a）：E3 resolved 幂等声明与码不符——已裁决（resolved/approved/executed）对每夜重立流水单。
								//    立案前查重补「已裁决对不重立」：对齐 exact 判据 L1564 的 无序对查重（病序老行穿透防护同法）·status 不限（pending 未决豁免保留+裁决终态亦不重立）。
								const dup = db
									.prepare(
										"SELECT COUNT(*) c FROM conflicts WHERE new_id IN (?, ?) AND old_id IN (?, ?) AND new_id != old_id",
									)
									.get(nid, oid, nid, oid).c;
								if (dup === 0)
									fileC(
										nid,
										oid,
										"family",
										`title jaccard=${sim.toFixed(2)}|同${grams[i].type}同space`,
										list[0].space,
										grams[j].title,
										nowIso(),
									); // 七小件⑥：fileC 并案包装
							}
						}
					}
					// ── 语义去重第三判据(design note)：向量余弦≥0.92
					//    且同 type+同 space 才立 pending——「词面不重叠语义同物」（jaccard 漏网面·如 conda-env-create
					//    vs conda-create-environment）。仍走人工裁决（8-23 弃 semantic 因误报·type+space+0.92 三重收窄后
					//    复投）。挂 RRF 已有向量·零新嵌入成本（缺向量对跳过）；护栏：active 全量 O(n²) 桶帽 3000。──
					// S1 修（step）原上提点=conflicts 内层 try 域——stepcaught in drill L1651 外层引用 ReferenceError；
					// 声明已真上提至外层 conflictsPending 旁（本行删·/①精确/审计清单三面同见）。
					try {
						if (!process.env.LEGION_SEMANTIC_DEDUP_OFF) {
							const a10Excl = [...mergeIds]; // S1 同修：空集→无 NOT IN 子句（原 'NULL' 兜底=NOT IN (NULL) 恒假死路·声明上提后仍死·两处合医才活）
							try {
								// 件三车病灶6（09-09·三轮审计 §4.5）：mergeIds 填点（①精确/②高相似段）在 之后=恒空集·排除意图死——SQL 直查 pending 对补排除集（exact/family 已立案的 id 不进 防同对双立案·语义对齐零段序改动）
								for (const pc of db
									.prepare(
										"SELECT new_id, old_id FROM conflicts WHERE status = 'pending'",
									)
									.all()) {
									a10Excl.push(pc.new_id, pc.old_id);
								}
							} catch {}
							const vecRows = db
								.prepare(`SELECT v.id, v.embedding FROM memories_vec v JOIN memories m ON m.id = v.id
                WHERE v.model_version = ? AND m.status = 'active'${a10Excl.length ? ` AND m.id NOT IN (${a10Excl.map(() => "?").join(",")})` : ""}`)
								.all(VEC_MODEL, ...a10Excl);
							if (vecRows.length > 1 && vecRows.length <= 5000) {
								// wave 16:55 修：3000 帽在 3237 向量正库恒跳过（首夜零发现同被帽掩盖）——升 5000 对齐 cooccur HI_SIM_CAP
								const metaOf = new Map(rows.map((r) => [r.id, r]));
								let semanticFound = 0;
								// P1 修（design-approved·次批治本件1）：解码提外层一次——
								// 原内层每对重解码=790 万次 TypedArray 分配/轮→GC 停顿风暴。纯性能·零逻辑变更。
								const vecs = vecRows.map((r) => blobToF32(r.embedding));
								outer: for (let i = 0; i < vecRows.length; i++) {
									const a = vecs[i];
									for (let j = i + 1; j < vecRows.length; j++) {
										const b = vecs[j];
										if (cosine(a, b) < 0.92) continue;
										const ma = metaOf.get(vecRows[i].id),
											mb = metaOf.get(vecRows[j].id);
										if (
											!ma ||
											!mb ||
											ma.type !== mb.type ||
											ma.space !== mb.space
										)
											continue; // type-guard：同 type+同 space（graph-memory type-guard 意·误报收窄第一重）
										if (String(ma.title) === String(mb.title)) continue; // exact 已记
										if (sysAutoOf(ma.title) && sysAutoOf(mb.title)) continue; // 七小件⑤：双侧系统自动条互撞豁免
										const nid2 = Math.max(ma.id, mb.id),
											oid2 = Math.min(ma.id, mb.id);
										// 8-31 锈面修（审计片6①a）：semantic 判据同 family 病灶——已裁决对每夜重立。同法修：无序对查重+status 不限（已裁决不重立）·跨判据同对已立案亦不重立（basis 过滤退役）
										const dup2 = db
											.prepare(
												"SELECT COUNT(*) c FROM conflicts WHERE new_id IN (?, ?) AND old_id IN (?, ?) AND new_id != old_id",
											)
											.get(nid2, oid2, nid2, oid2).c;
										if (
											dup2 === 0 &&
											fileC(
												nid2,
												oid2,
												"semantic",
												`vec cos≥0.92|同${ma.type}同space`,
												ma.space,
												ma.title,
											)
										) {
											semanticFound += 1; // 七小件⑥：fileC 并案包装（true=新立案计帽·false=并入原案）
										}
										if (semanticFound >= 20) break outer; // nightly patrol预算帽：单轮 semantic 最多立 20 对（首周纪律）
									}
								}
								stats.semanticConflictsFound = semanticFound;
							}
						}
					} catch (e) {
						stats.a10SkipErrors = (stats.a10SkipErrors || 0) + 1;
						ctx.logger?.warn?.(
							"[living-memory] semantic scan failed (#" +
								stats.a10SkipErrors +
								"): " +
								String((e && e.message) || e).slice(0, 80),
						);
					} // ⑬ 纪律回溯修（8-38 审计）：S1 病灶本体的空 catch——TDZ 曾被它静默吞一日
					// ── 七小件②：死向量清理（09-08 maintainer·审计 §四：superseded/done 条向量残留——
					//    落码时实测 vec 5056 vs active 4779=277 条·每夜冗余参与 桶与余弦扫描）──
					try {
						const dv = db
							.prepare(
								"DELETE FROM memories_vec WHERE id IN (SELECT v.id FROM memories_vec v LEFT JOIN memories m ON m.id = v.id WHERE m.id IS NULL OR m.status != 'active')",
							)
							.run();
						const dc = db
							.prepare(
								"DELETE FROM memories_vec_chunks WHERE id IN (SELECT c.id FROM memories_vec_chunks c LEFT JOIN memories m ON m.id = c.id WHERE m.id IS NULL OR m.status != 'active')",
							)
							.run();
						stats.deadVecCleaned =
							(stats.deadVecCleaned || 0) + dv.changes + dc.changes;
						if (dv.changes + dc.changes > 0)
							ctx.logger?.warn?.(
								"[living-memory] dead-vec cleanup (#" +
									stats.deadVecCleaned +
									"): vec -" +
									dv.changes +
									" chunks -" +
									dc.changes,
							);
					} catch (e2v) {
						stats.deadVecSkipErrors = (stats.deadVecSkipErrors || 0) + 1;
					}
					// ── merge 执行器（wave·graph-memory mergeNodes 边迁移+自环清理意融入）：approved 裁决自动执行——
					//    人工主权不动（裁决仍maintainer），执行自动化（原人工 UPDATE）：①被合条 merged+superseded_by
					//    ②边迁移（被合条端点活边改挂保留方·显式因果边不断链）③自环清理（迁移后 src=dst 软失效）。
					//    幂等（已 merged 跳过）·executed 态终收（approved→executed 审计链）。
					try {
						const approvedRows = db
							.prepare(
								"SELECT conflict_id, new_id, old_id FROM conflicts WHERE status = 'approved' AND decided_at IS NOT NULL",
							)
							.all();
						// ── 刀A both-valid 第四态（design-approved·对标pending ruling①）：不可约冲突——保留双方·建 conflicts_with 边+终收 ──
						//    不合并（条目双活·as_of 各自正确·检索天然支持）·边=检索面关联可见（conflicts_with 型）；E3 查重豁免天然兼容（非 pending 不重立）；executed 复用=执行器已处理终态
						try {
							const bothValidRows = db
								.prepare(
									"SELECT conflict_id, new_id, old_id FROM conflicts WHERE status = 'both-valid'",
								)
								.all();
							for (const bv of bothValidRows) {
								try {
									db.prepare(
										"INSERT INTO memories_edges (src, dst, edge_type, weight, valid_at, last_seen, instruction) VALUES (?, ?, 'conflicts_with', 0.5, ?, ?, 'both-valid 不可约对·两者都对·as_of 各自正确') ON CONFLICT(src, dst, edge_type) DO UPDATE SET last_seen = excluded.last_seen",
									).run(bv.new_id, bv.old_id, nowIso(), nowIso());
									db.prepare(
										"UPDATE conflicts SET status = 'executed', decided_by = COALESCE(decided_by, 'patrol-executor-bothvalid') WHERE conflict_id = ?",
									).run(bv.conflict_id);
									stats.bothValidLinked = (stats.bothValidLinked || 0) + 1;
								} catch {
									stats.bothValidErrors = (stats.bothValidErrors || 0) + 1;
								}
							}
						} catch {}
						let executed = 0,
							edgesMigrated = 0;
						for (const ap of approvedRows) {
							const drop = db
								.prepare("SELECT id, status FROM memories WHERE id = ?")
								.get(ap.old_id);
							// 8-31 悬死修①（patrol-persist ⑤真红settled·正库 20 案）：人工 UPDATE 已执行（drop 已 merged）但 conflicts 留 approved——同步终收 executed（审计链闭合·approved 池清零）
							if (!drop || drop.status === "merged") {
								db.prepare(
									"UPDATE conflicts SET status = 'executed', decided_by = COALESCE(decided_by, 'patrol-executor-sync') WHERE conflict_id = ?",
								).run(ap.conflict_id);
								continue;
							}
							// 8-31 悬死修②（正库 4 案）：保留方自己已被后续案件 merged——沿 superseded_by 追链至终极保留方执行（原裁决意图的存续方）；断链/环→resolved 注链断
							let nid = ap.new_id;
							let keep = db
								.prepare(
									"SELECT id, status, superseded_by FROM memories WHERE id = ?",
								)
								.get(nid);
							let hops = 0;
							while (
								keep &&
								keep.status === "merged" &&
								keep.superseded_by &&
								hops < 10
							) {
								nid = keep.superseded_by;
								keep = db
									.prepare(
										"SELECT id, status, superseded_by FROM memories WHERE id = ?",
									)
									.get(nid);
								hops++;
							}
							if (!keep || keep.status === "merged") {
								db.prepare(
									"UPDATE conflicts SET status = 'resolved', decided_by = COALESCE(decided_by, 'patrol-executor-chain') WHERE conflict_id = ?",
								).run(ap.conflict_id);
								continue;
							}
							// 审计 D11 修正（15:41）：原多行 UPDATE 单语句——X-old 与 X-keep 同型边迁移后撞 UNIQUE(src,dst,edge_type)
							// →整语句回滚→执行器 catch 跳过→该案每晚重试每晚炸（卡死不幂等）。改逐行迁移：
							// 每条边独立事务位·撞 UNIQUE（目标对已有同型活边）则该边软失效让位（保留既有边·信息不丢可溯）。
							const oldEdges = db
								.prepare(
									`SELECT id, src, dst, edge_type FROM memories_edges WHERE invalid_at IS NULL AND (src = ? OR dst = ?)`,
								)
								.all(ap.old_id, ap.old_id);
							for (const e4 of oldEdges) {
								const nsrc = e4.src === ap.old_id ? nid : e4.src;
								const ndst = e4.dst === ap.old_id ? nid : e4.dst;
								if (nsrc === ndst) {
									db.prepare(
										"UPDATE memories_edges SET invalid_at = ? WHERE id = ?",
									).run(nowIso(), e4.id);
									continue;
								} // 迁移后自环：直接软失效
								const clash = db
									.prepare(
										`SELECT COUNT(*) c FROM memories_edges WHERE invalid_at IS NULL AND src = ? AND dst = ? AND edge_type = ? AND id != ?`,
									)
									.get(nsrc, ndst, e4.edge_type, e4.id).c;
								if (clash > 0) {
									db.prepare(
										"UPDATE memories_edges SET invalid_at = ? WHERE id = ?",
									).run(nowIso(), e4.id);
									continue;
								} // 目标对已有同型活边：让位软失效（防 UNIQUE 撞）
								try {
									db.prepare(
										"UPDATE memories_edges SET src = ?, dst = ?, last_seen = ? WHERE id = ? AND invalid_at IS NULL",
									).run(nsrc, ndst, nowIso(), e4.id);
									edgesMigrated += 1;
								} catch (ue) {
									// ── D11 补面（2026-08-29 22:26 预演雷settled）：表级 UNIQUE(src,dst,edge_type) 含软失效边——死边占键，clash 只查活边防不了死键撞（边 B 让位→边 C 同键迁移→炸整案回滚→每晚重试每晚炸）。
									//    撞键=该边让位软失效（信息不丢可溯·与 clash 让位同效）·stats 计数透出。
									db.prepare(
										"UPDATE memories_edges SET invalid_at = ? WHERE id = ?",
									).run(nowIso(), e4.id);
									stats.edgeYieldDeadKey = (stats.edgeYieldDeadKey || 0) + 1;
								}
							}
							db.prepare(
								"UPDATE memories SET status = 'merged', superseded_by = ?, closed_at = ?, valid_to = COALESCE(valid_to, ?) WHERE id = ?",
							).run(nid, nowIso(), nowIso(), ap.old_id); // ：闭环时刻随墓碑记·valid_to 事件轴缺省同刻（显式值优先·幂等不重刷）·8-31 悬死修②：保留方=追链后终极 nid
							db.prepare(
								"UPDATE conflicts SET status = 'executed', decided_by = COALESCE(decided_by, 'patrol-executor') WHERE conflict_id = ?",
							).run(ap.conflict_id);
							executed += 1;
						}
						stats.mergeExecuted = executed;
						stats.edgesMigrated = edgesMigrated;
						merged += executed;
						patrolMergedOut = executed; // D12（15:41）：统一覆盖写（executed=0 也清零·防旧值误导counter）·8-31 锈面修⑤：本巡合并流出量供light sentry豁免
					} catch (e3) {
						try {
							ctx.logger?.warn?.(
								"[living-memory] executor fail: " +
									String(e3).slice(0, 60),
							);
						} catch {}
					}
					conflictsPending = db
						.prepare(
							"SELECT COUNT(*) c FROM conflicts WHERE status = 'pending'",
						)
						.get().c;
					// ── wave AUDN 预裁段（pending 且未裁·预算帽 40/巡·LEGION_PRECLASSIFY_OFF 回退）：
					//    只写 pre_* 建议列不动 status——maintainer终批才执行（执行器主权不动）。
					if (!process.env.LEGION_PRECLASSIFY_OFF) {
						(async () => {
							try {
								if (Date.now() >= guard.pausedUntil) {
									const cred = await credentials.resolve("DEEPSEEK_MEMORY_KEY");
									if (cred && cred.value) {
										const cases0 = db
											.prepare(
												"SELECT co.conflict_id cid, mn.type nType, mn.space nSpace, mn.title nTitle, mn.content nContent, mo.type oType, mo.space oSpace, mo.title oTitle, mo.content oContent, co.basis FROM conflicts co JOIN memories mn ON mn.id = co.new_id JOIN memories mo ON mo.id = co.old_id WHERE co.status = 'pending' AND co.pre_verdict IS NULL LIMIT 40",
											)
											.all();
										let preverdicted = 0;
										for (const c0 of cases0) {
											const v0 = await preclassifyConflict(c0, cred);
											if (!v0) break; // 熔断/通道断即止（次夜续）
											db.prepare(
												"UPDATE conflicts SET pre_verdict = ?, pre_reason = ?, pre_at = ? WHERE conflict_id = ?",
											).run(v0.verdict, v0.reason, nowIso(), c0.cid);
											preverdicted += 1;
										}
										if (preverdicted > 0)
											ctx.logger?.info?.(
												"[living-memory] AUDN preclassify: " +
													preverdicted +
													" cases",
											);
									}
									// 预裁counter尾刷（IIFE 完成后=最终一致·治「主线赋值先于异步落库恒 0」时序）
									stats.conflictsPreverdicted = db
										.prepare(
											"SELECT COUNT(*) c FROM conflicts WHERE pre_verdict IS NOT NULL AND status = 'pending'",
										)
										.get().c;
								}
							} catch (ePC) {
								try {
									ctx.logger?.warn?.(
										"[living-memory] preclassify fail: " +
											String(ePC).slice(0, 60),
									);
								} catch {}
							}
						})();
					}
					// ── validatedCount（wave·graph-memory validated_count 意融入）：新条撞既有主题族→锚条 +1——
					//    「重复验证的知识自然浮权」。审计 D13 修正（16:04）：原全量对扫=同对每晚重加终身无界（30 天 vc
					//    失义）；graph-memory 原语义=新事件驱动（再抽取命中才强化）。修：i 侧只取**上次nightly patrol后新条**（patrol_last.at
					//    水位·本段执行时该键仍是昨晚值——末段才刷新·时序天然正确）·j 侧全量同 type+space 桶——同对终身
					//    只强化一次。首夜（无水位）零强化（保守正确·回填 vc=1 已另做）。PPR 消费不变。
					try {
						if (!process.env.LEGION_VALIDATED_OFF) {
							// （step）：ALTER+回填已挪 ensureFts 启动即建（原在此=重启后→首nightly patrol前消费面缺列；02:16 巡已自愈列·结构性归位）
							const lastAt = (() => {
								try {
									return String(
										JSON.parse(
											db
												.prepare(
													"SELECT v FROM organ_meta WHERE k='patrol_last'",
												)
												.get().v,
										).at || "",
									);
								} catch {
									return "";
								}
							})();
							if (lastAt) {
								const recent = rows.filter((r) => String(r.ts) > lastAt); // D13：新条侧（ISO 同格式字符串可序）
								if (recent.length > 0 && recent.length <= 2000) {
									const bfp = db.prepare(
										"UPDATE memories SET validated_count = validated_count + 1 WHERE id = ?",
									);
									let vcUp = 0;
									const vcCounted = new Set(); // P0修（09-03 audit）：同对去重——recent 内 A/B 互为 nr/old 防 anchor 双计
									const byKey = new Map();
									for (const r of rows) {
										const k = r.type + "\u0000" + r.space;
										if (!byKey.has(k)) byKey.set(k, []);
										byKey.get(k).push(r);
									}
									for (const nr of recent) {
										const list = byKey.get(nr.type + "\u0000" + nr.space) || [];
										const ng = bigrams(nr.title);
										for (const old of list) {
											if (old.id === nr.id) continue;
											if (jaccard(ng, bigrams(old.title)) >= 0.6) {
												const anchor = Math.max(nr.id, old.id);
												const vcPair = Math.min(nr.id, old.id) + "-" + anchor;
												if (vcCounted.has(vcPair)) continue;
												vcCounted.add(vcPair);
												bfp.run(anchor);
												vcUp += 1;
												if (vcUp >= 100) break; // nightly patrol预算帽
											}
										}
										if (vcUp >= 100) break;
									}
									stats.validatedUp = vcUp;
								}
							}
						}
					} catch (eVC) {
						stats.validatedErrors = (stats.validatedErrors || 0) + 1; // P2 fix（09-03 audit）：段异常透出（原静默=失败残留昨夜值假绿·对照 有 a10SkipErrors）
						ctx.logger?.warn?.(
							"[living-memory] validatedCount failed (#" +
								stats.validatedErrors +
								"): " +
								String(eVC).slice(0, 60),
						);
					}
					// ── 语义近邻边（wave·16:47 approved）：nightly patrol对 vec 近邻带建 'semantic' 边
					//    （weight 0.3 低档）——治「词面不重叠语义同物」的图死角（jaccard 建不了的边）。
					//    与 分职：是「疑似重复」pending ruling；是「相关近邻」直接建边。
					//    阈值实测校准（16:51 演练）：本库 embedding 分散度高——样本 max cos=0.8993·0.90 带天然空。
					//    首周带定 0.85-0.92（0.85 下含强相关·0.92 上归 ）·预算帽 30 边/轮·观察误建率再收。
					try {
						if (!process.env.LEGION_SEMANTIC_EDGE_OFF) {
							const vecRows2 = db
								.prepare(`SELECT v.id, v.embedding FROM memories_vec v JOIN memories m ON m.id = v.id
                WHERE v.model_version = ? AND m.status = 'active'`)
								.all(VEC_MODEL);
							if (vecRows2.length > 1 && vecRows2.length <= 5000) {
								// 同上：帽校准（O(n²) 3093²≈4.8M 对·实测 ~3s nightly patrol窗可容）
								const meta2 = new Map(rows.map((r) => [r.id, r]));
								let semEdges = 0;
								const insSE =
									db.prepare(`INSERT INTO memories_edges (src, dst, edge_type, weight, valid_at, source_group, last_seen, instruction)
                  VALUES (?, ?, 'semantic', 0.3, ?, 'patrol:semantic', ?, '语义近邻')
                  ON CONFLICT(src, dst, edge_type) DO UPDATE SET last_seen = excluded.last_seen`);
								// P2 修（同 P1）：解码提外层一次
								const vecs2 = vecRows2.map((r) => blobToF32(r.embedding));
								outer2: for (let i = 0; i < vecRows2.length; i++) {
									const a2 = vecs2[i];
									for (let j = i + 1; j < vecRows2.length; j++) {
										const c2 = cosine(a2, vecs2[j]);
										if (c2 < 0.85) continue;
										if (c2 >= 0.92) continue; // ≥0.92 归 去重管（疑似重复不建边）
										const ma2 = meta2.get(vecRows2[i].id),
											mb2 = meta2.get(vecRows2[j].id);
										if (!ma2 || !mb2 || ma2.space !== mb2.space) continue; // 同 space（放宽 type：近邻跨型合理）
										const s2 = Math.max(ma2.id, mb2.id),
											t2 = Math.min(ma2.id, mb2.id);
										// 审计 D14 修正（17:04）：原 UPSERT 已存在对也计数——第二夜起 i,j 序先遇旧对→30 帽被
										// 「刷 last_seen」吞噬→新边永建不出。修：已存在对跳过（不刷不计数·时态边本就保活）。
										const ex = db
											.prepare(
												"SELECT 1 FROM memories_edges WHERE src = ? AND dst = ? AND edge_type = 'semantic' AND invalid_at IS NULL",
											)
											.get(s2, t2);
										if (ex) continue;
										insSE.run(s2, t2, nowIso(), nowIso());
										semEdges += 1;
										if (semEdges >= 30) break outer2; // 首周预算帽（只数真新建）
									}
								}
								stats.semanticEdges = semEdges;
							}
						}
					} catch (eSE) {
						stats.semanticEdgeErrors = (stats.semanticEdgeErrors || 0) + 1; // P2 fix（09-03 audit）：段异常透出（同 族）
						ctx.logger?.warn?.(
							"[living-memory] semantic-edge failed (#" +
								stats.semanticEdgeErrors +
								"): " +
								String(eSE).slice(0, 60),
						);
					}
				} catch (error) {
					ctx.logger?.warn?.(
						`[living-memory] conflicts scan failed: ${String(error).slice(0, 80)}`,
					);
				}
				// ①精确去重（全库）：同 type+title，rows 按 id DESC——先见为新，后见标 merged
				//    【3a 改道注】merged 动作延至maintainer approved 时执行（首周纪律：全量人工pending ruling勿自动合并——release note§3）；
				//    高相似段同改道。历史已 merged 条不动（幂等迁移）。
				const seen = new Set();
				// mergeIds 声明已上提至 段前（S1 修·step）——此处仅填账
				for (const r of rows) {
					const key = r.type + "\u0000" + r.title;
					if (seen.has(key)) mergeIds.add(r.id);
					else seen.add(key);
				}
				// ②高相似合并（2026-08-22 P2 扩面·体检修复）：原 auto-extract 过滤为死路径（step space 规范化后
				//    该 space 不存在，恒空转）——扩为全库 active 的 space 桶内两两；护栏：单桶 >5000 条只做精确去重
				//    （O(n²) 保护，等阶段一评测集后可换 ANN）。阈值 0.85 不变（手动记忆误合并零容忍）。
				const bySpace = new Map();
				for (const r of rows) {
					if (mergeIds.has(r.id)) continue;
					if (!bySpace.has(r.space)) bySpace.set(r.space, []);
					bySpace.get(r.space).push(r);
				}
				const HI_SIM_CAP = 5000;
				for (const list of bySpace.values()) {
					if (list.length < 2 || list.length > HI_SIM_CAP) continue;
					const grams = list.map((r) => ({ id: r.id, g: bigrams(r.title) }));
					for (let i = 0; i < grams.length; i++) {
						if (mergeIds.has(grams[i].id)) continue;
						for (let j = i + 1; j < grams.length; j++) {
							if (mergeIds.has(grams[j].id)) continue;
							if (jaccard(grams[i].g, grams[j].g) >= 0.85)
								mergeIds.add(grams[j].id); // j 的 id 更小（更旧）
						}
					}
				}
				if (mergeIds.size > 0) {
					// 【3a 改道·首周纪律】自动 merged 挂起——对已入 conflicts pending 待maintainer；
					// approved 裁决执行面（人工触发）：UPDATE status='merged'+superseded_by=new_id。
					// 8-31 锈面修（审计片6②）：原此处无条件 merged=0 把 执行器真执行计数（上段 merged += executed）一并抹掉——
					//    3a 改道后自动合并本就不向 merged 记账，归零纯属破坏性；改不动前值（counter=执行器真账·发现数由 conflictsPending/mergedIds 承担）。
					mergedIds = [...mergeIds].sort((a, b) => a - b); // 发现清单保留入 stats（审计链）
				}
			} catch (error) {
				ctx.logger?.warn?.(
					`[living-memory] patrol merge failed: ${String(error).slice(0, 80)}`,
				);
			}
			// ③handover note警报：高压会话检测（>70% 建议换窗）
			const aged = [];
			try {
				const sessions = projcacheRows(); // 庚批修③（2026-09-08）：v5 双源（旧单文件 09-05 停更=压力哨 09-05 后恒空「无高压」静默假绿）
				for (const [sid, ent] of sessions.entries()) {
					const cp = ent.rows?.contextPressure?.val;
					if (!cp || !cp.contextWindow) continue;
					// E stale 过滤（09-08 brain receipt建议·🟠采纳）：mtime>48h 死档不计入高压告警（09-05 seed 残留「脑升级77%」型）——历史高压单列
					const stale = ent.mtimeMs && Date.now() - ent.mtimeMs > 48 * 3600e3;
					if (stale) continue; // 余件item一（09-09·三轮审计 §4.4-6 亲验实锤）：原只打 stale 标不过滤——死档仍入 aged 计数·补 continue 落实注释承诺
					// B 假零兜底（09-08 brain报障）：pressure=0||<surface（失败请求打空）→ surfaceTokens 下限——「变绿」比假绿更危险
					const base =
						(cp.pressureTokens || 0) === 0 ||
						(cp.pressureTokens || 0) < (cp.surfaceTokens || 0)
							? cp.surfaceTokens || 0
							: cp.pressureTokens || 0;
					if (!base) continue;
					const pct = base / cp.contextWindow;
					if (pct > 0.7)
						aged.push({
							sid,
							pct: Math.round(pct * 100),
							stale: stale || undefined,
							legacyWindow: cp.contextWindow < 500000 || undefined,
						}); // 老分母（262144 型）标注·与现役 1M 不可比
				}
			} catch (ePR) {
				stats.pressureReadErrors = (stats.pressureReadErrors || 0) + 1; // P2 fix（09-03 audit）：压力缓存读取失败透出（原静默=aged 空集「无高压」假绿·写库侧已有 pressureAlertErrors 此为读侧对照）
				ctx.logger?.warn?.(
					"[living-memory] session_projcache 读取失败(#" +
						stats.pressureReadErrors +
						"): " +
						String(ePR).slice(0, 60),
				);
			}
			if (aged.length > 0) {
				try {
					const title = "handover note警报：" + aged.length + " 个会话压力超 70%";
					const content =
						aged
							.map((a) => a.sid.slice(0, 13) + "=" + a.pct + "%")
							.join("；")
							.slice(0, 400) + " — consider opening a fresh window.";
					// L5 根治（design-approved·脑 08:02 漏点清单）：警报=状态流非事件史——今日同 title 已存在则 UPDATE 旧条（刷计数/content），不新建（根治每夜同题 pending 重立·E3 幂等只挡同对挡不住新 id 新对）
					const prev = db
						.prepare(
							"SELECT id FROM memories WHERE title LIKE 'handover note警报：%' AND source LIKE 'patrol:%' ORDER BY id DESC LIMIT 1",
						)
						.get(); // P0修（09-03 audit）：跨日去重——source 掺 dayKey 日变致每夜重立（正库 13 行同 checksum 实锤）
					if (prev) {
						db.prepare(
							"UPDATE memories SET ts = ?, title = ?, content = ?, checksum = ? WHERE id = ?",
						).run(nowIso(), title, content, sha1(title + content), prev.id); // 审计②R1：补 title 刷新——N 变化时计数不失真
					} else {
						db.prepare(
							"INSERT INTO memories (ts, type, title, content, space, source, checksum) VALUES (?, ?, ?, ?, ?, ?, ?)",
						).run(
							nowIso(),
							"todo",
							title,
							content,
							"memory-organ",
							"patrol:" + dayKey,
							sha1(title + content),
						);
					}
				} catch (ePA) {
					stats.pressureAlertErrors = (stats.pressureAlertErrors || 0) + 1; // P0修（09-03 audit）：换窗警报写失败漏警防线（⑬ 出口）
					ctx.logger?.warn?.(
						"[living-memory] handover note警报写库失败(#" +
							stats.pressureAlertErrors +
							"): " +
							String(ePA).slice(0, 60),
					);
				}
			}
			// ③todo 衰减：14 天以上待办标 aged（不进注入/时间线，可搜——遗忘是功能）
			let agedTodos = 0;
			try {
				const cutoff = Date.now() - 14 * 24 * 3600 * 1000;
				const todos = db
					.prepare(
						"SELECT id, ts FROM memories WHERE status = 'active' AND type = 'todo'",
					)
					.all();
				for (const t of todos) {
					const d = new Date(String(t.ts).replace(" ", "T"));
					if (!isNaN(d.getTime()) && d.getTime() < cutoff) {
						db.prepare(
							"UPDATE memories SET status = 'aged', closed_at = ?, valid_to = COALESCE(valid_to, ?) WHERE id = ?",
						).run(nowIso(), nowIso(), t.id); // ：衰减时刻随墓碑记·valid_to 事件轴缺省同刻（显式值优先·幂等不重刷）
						agedTodos += 1;
					}
				}
			} catch (error) {
				ctx.logger?.warn?.(
					`[living-memory] patrol todo-age failed: ${String(error).slice(0, 60)}`,
				);
			}

						let soulDrift = ""; // soul-integrity sentinel is site-specific and removed from the public build;
			// the variable stays because the patrol-completion log below references it.
// ── step：图谱共现组 ── 阶段二首步：增量化改造（design-approved阶段二·M4 硬依赖）──
			//    原全表 DELETE 重灌会冲掉时态边 invalid_at 语义（阶段二第二步）——改差量：
			//    ①组级脏标：昨夜水线（organ_meta cooccur_watermark）之后有新增条的 source 组才重算；
			//    ②死边清理：只删指向非 active 条目的边（含该组旧边）；
			//    ③表升级：+source_group/+created_at 列（时态边地基·平滑迁移不重建）。
			//    相似判定不变：同 source 组内标题 bigram jaccard≥0.4；决策链×1.5。
			let cooccurPairs = 0;
			try {
				// 平滑迁移：旧表加列（存在则跳过）
				try {
					db.exec("ALTER TABLE memories_cooccur ADD COLUMN source_group TEXT");
				} catch {}
				try {
					db.exec("ALTER TABLE memories_cooccur ADD COLUMN created_at TEXT");
				} catch {}
				const wmRow = db
					.prepare("SELECT v FROM organ_meta WHERE k = 'cooccur_watermark'")
					.get();
				const watermark = wmRow ? Number(wmRow.v) : 0;
				// ①脏组发现：水线后新增（id > watermark）的条目所属 source 组
				const dirtyRows = db
					.prepare(
						"SELECT id, type, title, source FROM memories WHERE status = 'active' AND id > ? AND source != ''",
					)
					.all(watermark);
				const dirtySources = new Set(dirtyRows.map((r) => String(r.source)));
				// 首跑（watermark=0 或脏组 > 总组 50%）→ 全量重建一次（等价旧行为）
				const allSources = db
					.prepare(
						"SELECT DISTINCT source FROM memories WHERE status = 'active' AND source != ''",
					)
					.all();
				const fullRebuild =
					watermark === 0 || dirtySources.size > allSources.length * 0.5;
				const targetSources = fullRebuild
					? allSources.map((r) => String(r.source))
					: [...dirtySources];
				// 终审 16:41 顺手清：delPair 死语句（历史残留·prepare 未 run——组级删除已由 phDel 承担）
				const insPair = db.prepare(
					"INSERT OR REPLACE INTO memories_cooccur (id_a, id_b, score, source_group, created_at) VALUES (?, ?, ?, ?, ?)",
				);
				const ph2 = db.prepare(
					"SELECT id, type, title FROM memories WHERE source = ? AND status = 'active'",
				);
				let newMaxId = watermark;
				for (const src of targetSources) {
					const list = ph2.all(src);
					// ②该组旧边先清（组级重算的原子性）
					delPairAll: {
						const ids = list.map((r) => r.id);
						if (ids.length > 0) {
							const phDel = db.prepare(
								`DELETE FROM memories_cooccur WHERE source_group = ?`,
							);
							phDel.run(src);
						}
						break delPairAll;
					}
					if (list.length < 2) continue;
					const grams = list.map((r) => ({ ...r, g: bigrams(r.title) }));
					const stamp = nowIso();
					for (let i = 0; i < grams.length; i++) {
						for (let j = i + 1; j < grams.length; j++) {
							const sim = jaccard(grams[i].g, grams[j].g);
							if (sim < 0.4) continue;
							const chain =
								(grams[i].type === "decision" &&
									["fact", "lesson"].includes(grams[j].type)) ||
								(grams[j].type === "decision" &&
									["fact", "lesson"].includes(grams[i].type)) ||
								(grams[i].type === "lesson" && grams[j].type === "lesson");
							const score = chain ? sim * 1.5 : sim;
							const a = Math.min(grams[i].id, grams[j].id),
								b = Math.max(grams[i].id, grams[j].id);
							insPair.run(a, b, Math.round(score * 1000) / 1000, src, stamp);
						}
					}
				}
				if (fullRebuild) {
					// 全量路径：清孤儿边（source_group 空的旧边=迁移遗留）
					db.exec(
						"DELETE FROM memories_cooccur WHERE source_group IS NULL OR source_group = ''",
					);
				} else {
					// 增量路径：清死边（指向非 active 的——done/merged 条目的边）
					db.exec(
						`DELETE FROM memories_cooccur WHERE id_a NOT IN (SELECT id FROM memories WHERE status='active') OR id_b NOT IN (SELECT id FROM memories WHERE status='active')`,
					);
				}
				// 水线推进：全库 active 最大 id
				const maxRow = db
					.prepare("SELECT MAX(id) m FROM memories WHERE status = 'active'")
					.get();
				newMaxId = Math.max(watermark, Number(maxRow.m || 0));
				db.prepare(
					"INSERT OR REPLACE INTO organ_meta (k, v) VALUES ('cooccur_watermark', ?)",
				).run(String(newMaxId));
				cooccurPairs = db
					.prepare("SELECT COUNT(*) c FROM memories_cooccur")
					.get().c;

				// ── 3b 衰减强化对冲（2026-08-23 阶段三release note·设计稿修正版两钉）──
				//    命中次数=差分口径（进程 Map 攒批→nightly patrol消费→清空·钉二）；merged/done 不参与（WHERE active）；
				//    未命中天数=nowIso − last_hit_at 持久锚（钉一·NULL 回退 ts=条目创建时刻即冷启动口径）；
				//    读写分离（release note§执行注意）：last_hit_at 随结算统一 UPDATE，search 侧零写库。
				try {
					try {
						db.exec("ALTER TABLE memories ADD COLUMN relevancy REAL");
					} catch {}
					try {
						db.exec("ALTER TABLE memories ADD COLUMN last_hit_at TEXT");
					} catch {}
					const stampB = nowIso();
					const hitsNow =
						globalThis.__legionHitMap instanceof Map
							? globalThis.__legionHitMap
							: new Map();
					// 冷启动修正（09:05 对冲 A/B -3.3pp 病理）：expect 条被「0.02×库龄天」压 0.92-1.00 与 decay 双重惩罚。
					// 修正口径：last_hit_at 为 NULL（从未被命中过也无结算史）→ relevancy=1.0 中性（冷启动不惩罚）；
					// 下行通道仅对「有 last_hit_at」（被命中过后沉寂）生效——沉寂的定义=曾经热过。
					// 量纲降档（09:08 双开关三组锁定对冲单变量 -4.1pp）：0.05/0.02 与 bm25 边界排名不同量纲，
					// 对 0.1 档加成差放大 4pp——降档 0.02 回弹/0.005 下行（双通道语义保留·边界影响≤0.5pp）。
					const settle = db.prepare(`UPDATE memories SET
            relevancy = CASE WHEN last_hit_at IS NULL AND :h = 0 THEN 1.0
              WHEN last_hit_at IS NULL THEN MIN(1.5, 1.0 + 0.02 * :h)
              ELSE MAX(0.5, MIN(1.5, 1.0 + 0.02 * :h - 0.005 * (CAST((julianday(:now) - julianday(COALESCE(last_hit_at, ts))) AS INTEGER)))) END,
            last_hit_at = CASE WHEN :h2 > 0 THEN :hitAt ELSE last_hit_at END
            WHERE id = :id AND status = 'active'`);
					let settled = 0;
					for (const r of db
						.prepare("SELECT id FROM memories WHERE status = 'active'")
						.all()) {
						const h = hitsNow.get(r.id) || 0;
						settle.run({ h, now: stampB, h2: h, hitAt: stampB, id: r.id });
						settled++;
					}
					hitsNow.clear();
					ctx.logger?.info?.(
						`[living-memory] relevancy settled: ${settled} entries`,
					);
				} catch (error) {
					ctx.logger?.warn?.(
						`[living-memory] relevancy settle failed: ${String(error).slice(0, 80)}`,
					);
				}

				// ── 阶段二第二步 a(design note)：时态边镜像 + 跨卷实体抽取 ──
				//    设计稿终版（三修并入）：edges 表 UNIQUE 冲突靶+UPSERT 只刷 weight/last_seen；
				//    last_seen 双路径保活（脏组重算 OR 轻量巡检）；实体全局不分卷+mention_count 跨卷累计。
				try {
					db.exec(`CREATE TABLE IF NOT EXISTS memories_edges (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            src INTEGER NOT NULL, dst INTEGER NOT NULL, edge_type TEXT NOT NULL,
            weight REAL NOT NULL, valid_at TEXT NOT NULL, invalid_at TEXT,
            source_group TEXT, last_seen TEXT NOT NULL, instruction TEXT,
            UNIQUE(src, dst, edge_type))`); // 8-31 长尾甲档①：instruction 列入建表——与 ensureFts 兜底两处定义同源（原nightly patrol建表无此列·新库首nightly patrol后带 instruction 写边持续失败至重启）
					db.exec(
						`CREATE INDEX IF NOT EXISTS idx_edges_src ON memories_edges(src) WHERE invalid_at IS NULL`,
					);
					db.exec(
						`CREATE INDEX IF NOT EXISTS idx_edges_dst ON memories_edges(dst) WHERE invalid_at IS NULL`,
					);
					const stamp = nowIso();
					// 边镜像：cooccur 全量 → edges UPSERT（只刷 weight/last_seen，valid_at/invalid_at 原值不动）
					const upEdge =
						db.prepare(`INSERT INTO memories_edges (src, dst, edge_type, weight, valid_at, source_group, last_seen)
            VALUES (?, ?, 'cooccur', ?, ?, ?, ?)
            ON CONFLICT(src, dst, edge_type) DO UPDATE SET weight = excluded.weight, last_seen = excluded.last_seen`);
					for (const e of db
						.prepare(
							"SELECT id_a, id_b, score, source_group FROM memories_cooccur",
						)
						.all()) {
						upEdge.run(e.id_a, e.id_b, e.score, stamp, e.source_group, stamp);
					}
					// 轻量巡检保活：active 边且端点 active → last_seen 刷新（未脏组的在役边防误失效——终版必修②）
					db.prepare(`UPDATE memories_edges SET last_seen = ? WHERE invalid_at IS NULL
            AND src IN (SELECT id FROM memories WHERE status='active')
            AND dst IN (SELECT id FROM memories WHERE status='active')`).run(
						stamp,
					);
					// 死边软失效：端点非 active → invalid_at（不物理删——时态完整）
					db.prepare(`UPDATE memories_edges SET invalid_at = ? WHERE invalid_at IS NULL
            AND (src NOT IN (SELECT id FROM memories WHERE status='active')
              OR dst NOT IN (SELECT id FROM memories WHERE status='active'))`).run(
						stamp,
					);
				} catch (error) {
					ctx.logger?.warn?.(
						`[living-memory] edges mirror failed: ${String(error).slice(0, 80)}`,
					);
				}
				// ── 社区检测（wave·graph-memory Label Propagation 意融入）：活边图上标签传播 → memory_communities 表
				//    （id→community 映射+社区代表）。供水位泛化路与未来可视化。幂等：全量重算（nightly patrol窗口·量小无压力）。
				try {
					db.exec(
						`CREATE TABLE IF NOT EXISTS memory_communities (mem_id INTEGER PRIMARY KEY, community INTEGER NOT NULL, is_rep INTEGER NOT NULL DEFAULT 0)`,
					);
					const edges = db
						.prepare(`SELECT src, dst FROM memories_edges WHERE invalid_at IS NULL
            AND src IN (SELECT id FROM memories WHERE status='active' AND (valid_to IS NULL OR valid_to > ?))
            AND dst IN (SELECT id FROM memories WHERE status='active' AND (valid_to IS NULL OR valid_to > ?))`)
						.all(nowIso(), nowIso()); // 审计③修（design-approved「修」）：社区检测活边图同口径（原只滤 status ⇒ 沉底条被重新纳为活节点·下轮nightly patrol复现）——与 PPR 段同一修法
					const adj = new Map();
					const nodes = new Set();
					for (const e of edges) {
						nodes.add(e.src);
						nodes.add(e.dst);
						if (!adj.has(e.src)) adj.set(e.src, []);
						if (!adj.has(e.dst)) adj.set(e.dst, []);
						adj.get(e.src).push(e.dst);
						adj.get(e.dst).push(e.src);
					}
					// Label Propagation：初始标签=自身 id·异步传播至稳定或 20 轮
					const label = new Map();
					for (const n of nodes) label.set(n, n);
					for (let it = 0; it < 20; it++) {
						let changed = 0;
						for (const n of nodes) {
							const cnt = new Map();
							for (const m2 of adj.get(n) || [])
								cnt.set(label.get(m2), (cnt.get(label.get(m2)) || 0) + 1);
							let best = label.get(n),
								bestC = -1;
							for (const [l, c] of cnt)
								if (c > bestC || (c === bestC && l < best)) {
									best = l;
									bestC = c;
								}
							if (best !== label.get(n)) {
								label.set(n, best);
								changed += 1;
							}
						}
						if (changed === 0) break;
					}
					// 社区代表=每社区最高 id（最新条目·graph-memory 无社区代表概念·我方以最新为锚）
					const repBy = new Map();
					db.exec("DELETE FROM memory_communities");
					const insC = db.prepare(
						"INSERT INTO memory_communities (mem_id, community, is_rep) VALUES (?, ?, ?)",
					);
					for (const n of nodes) {
						const c = label.get(n);
						if (!repBy.has(c) || n > repBy.get(c)) repBy.set(c, n);
					}
					for (const n of nodes)
						insC.run(n, label.get(n), repBy.get(label.get(n)) === n ? 1 : 0);
					stats.communitiesBuilt = new Set(label.values()).size;
					// ── 件5 面1 Saga 社区叙事摘要（design-approved「开」·B级5·Graphiti Saga 增量叙事意·规则版零 LLM）──
					//    ≥5 条成员的社区生成摘要（rep 标题｜N 条｜top3 成员片段 ≤120 字）→ organ_meta community_summaries（JSON·全量覆写幂等）
					try {
						const groups = db
							.prepare(
								"SELECT community, COUNT(*) n, MAX(is_rep) hasRep FROM memory_communities GROUP BY community HAVING n >= 5 ORDER BY n DESC LIMIT 80",
							)
							.all();
						const gTitle = db.prepare(
							"SELECT title FROM memories WHERE id = ? AND status = 'active'",
						);
						const summaries = {};
						for (const g of groups) {
							const repId = db
								.prepare(
									"SELECT mem_id FROM memory_communities WHERE community = ? AND is_rep = 1 LIMIT 1",
								)
								.get(g.community);
							const repT = repId
								? String(gTitle.get(repId.mem_id)?.title || "").slice(0, 40)
								: "";
							if (!repT) continue;
							const members = db
								.prepare(
									"SELECT m.title FROM memory_communities mc JOIN memories m ON m.id = mc.mem_id WHERE mc.community = ? AND m.status = 'active' AND mc.mem_id != ? ORDER BY m.id DESC LIMIT 3",
								)
								.all(g.community, repId ? repId.mem_id : 0);
							summaries[g.community] = {
								n: g.n,
								rep: repT,
								top: members.map((m) => String(m.title).slice(0, 18)),
							};
						}
						db.prepare(
							"INSERT INTO organ_meta (k, v) VALUES ('community_summaries', ?) ON CONFLICT(k) DO UPDATE SET v = excluded.v",
						).run(JSON.stringify(summaries));
						stats.sagaSummaries = Object.keys(summaries).length;
					} catch (e2) {
						stats.sagaSummaryErrors = (stats.sagaSummaryErrors || 0) + 1;
					}
				} catch (error) {
					ctx.logger?.warn?.(
						`[living-memory] communities failed: ${String(error).slice(0, 80)}`,
					);
				}
				try {
					db.exec(`CREATE TABLE IF NOT EXISTS entities (
            entity_id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            kind TEXT, source_space TEXT,
            mention_count INTEGER DEFAULT 1,
            first_seen TEXT NOT NULL, last_seen TEXT NOT NULL)`);
					db.exec(`CREATE TABLE IF NOT EXISTS memories_entities (
            memory_id INTEGER NOT NULL, entity_id INTEGER NOT NULL,
            PRIMARY KEY(memory_id, entity_id))`);
					// ── wave APEX-MEM 实体消解（design-approved四车）：幂等 ALTER 两列——
					//    aliases=变体名清单（JSON）/canonical_id=指向规范体（变体不删·历史保留）
					try {
						db.exec("ALTER TABLE entities ADD COLUMN aliases TEXT");
					} catch {}
					try {
						db.exec("ALTER TABLE entities ADD COLUMN canonical_id INTEGER");
					} catch {}
					const stamp2 = nowIso();
					// 实体抽取：①专有词表种子（DICT 双源合并词全数入库 kind 待抽样核后定）②TfIdf 增强路（title top-3）
					const seedNames = DICT.words.filter((w) => w && w.length >= 2);
					const upEnt =
						db.prepare(`INSERT INTO entities (name, kind, source_space, first_seen, last_seen) VALUES (?, ?, 'memory-organ', ?, ?)
            ON CONFLICT(name) DO UPDATE SET last_seen = excluded.last_seen`);
					// kind 分类（步骤 b 接入前置：kind IS NOT NULL 才进检索）：机制词=mech｜space/编制词=organ｜流程文档词=doc
					// kind 映射已外置至词表文件 kinds 字段（双源合并·②覆盖①）——原硬编码字面量含内部术语·随包即泄
					const KIND_MAP = Object.assign(Object.create(null), DICT.kinds);
					for (const n of seedNames)
						upEnt.run(n, KIND_MAP[n] || null, stamp2, stamp2);
					// TfIdf 增强路：标题关键词 top-3（kind=NULL 不接入检索·首期观察）
					// 2.x API：new TfIdf().extractKeywords(jieba实例, 句, topK)——实例上无 extract（沙箱 07:17 实证）
					try {
						const J = require("@node-rs/jieba");
						if (jieba && J && typeof J.TfIdf === "function") {
							const tfidf = new J.TfIdf();
							const titleRows = db
								.prepare(
									"SELECT id, title, space FROM memories WHERE status = 'active' AND id > (SELECT CAST(COALESCE((SELECT v FROM organ_meta WHERE k='entity_watermark'), '0') AS INTEGER))",
								)
								.all();
							const entId = db.prepare(
								"SELECT entity_id FROM entities WHERE name = ?",
							);
							const insLink = db.prepare(
								"INSERT OR IGNORE INTO memories_entities (memory_id, entity_id) VALUES (?, ?)",
							);
							const bump = db.prepare(
								"UPDATE entities SET mention_count = mention_count + 1, last_seen = ? WHERE entity_id = ?",
							);
							for (const r of titleRows) {
								const cands = new Set();
								for (const n of seedNames)
									if (String(r.title).includes(n)) cands.add(n);
								try {
									for (const kw of tfidf.extractKeywords(
										jieba,
										String(r.title),
										3,
									))
										if (kw.keyword && kw.keyword.length >= 2)
											cands.add(kw.keyword);
								} catch {}
								for (const name of cands) {
									upEnt.run(name, KIND_MAP[name] || null, stamp2, stamp2);
									const e = entId.get(name);
									if (e) {
										insLink.run(r.id, e.entity_id);
										bump.run(stamp2, e.entity_id);
									}
								}
							}
							db.prepare(
								"INSERT OR REPLACE INTO organ_meta (k, v) VALUES ('entity_watermark', ?)",
							).run(String(maxRow ? maxRow.m : 0));
						}
					} catch (eEnt) {
						stats.entityExtractErrors = (stats.entityExtractErrors || 0) + 1;
						ctx.logger?.warn?.(
							"[living-memory] entity extract failed (#" +
								stats.entityExtractErrors +
								"): " +
								String(eEnt).slice(0, 80),
						);
					}
					// ── wave 实体消解段（变体合并·canonical 收敛·LEGION_RESOLVE_OFF 回退）──
					//    域：kind 非空实体（接入检索面·防 TfIdf 噪音乱并）·判定=归一化等值/编辑距离≤2(≥4字)/字集 jaccard≥0.75(≥3字)
					//    方向：mention_count 高者为规范体；合并=canonical 指向+aliases 累积+计数并入+链接迁移（变体行保留）
					if (!process.env.LEGION_RESOLVE_OFF) {
						try {
							const normEnt = (s) =>
								String(s || "")
									.normalize("NFKC")
									.toLowerCase()
									.replace(/[\s\-_/]/g, "");
							const editDist2 = (a, b) => {
								const m = a.length,
									n = b.length;
								if (Math.abs(m - n) > 2) return 9;
								const dp = Array.from({ length: m + 1 }, (_, i) => [i]);
								for (let j = 1; j <= n; j++) dp[0][j] = j;
								for (let i = 1; i <= m; i++)
									for (let j = 1; j <= n; j++)
										dp[i][j] = Math.min(
											dp[i - 1][j] + 1,
											dp[i][j - 1] + 1,
											dp[i - 1][j - 1] + (a[i - 1] === b[j - 1] ? 0 : 1),
										);
								return dp[m][n];
							};
							const isVariant = (na, nb) => {
								const a = normEnt(na),
									b = normEnt(nb);
								if (!a || !b || a === b) return a === b && a.length > 0;
								if (a.length >= 4 && b.length >= 4 && editDist2(a, b) <= 2)
									return true;
								if (a.length >= 3 && b.length >= 3) {
									const sa = new Set(a),
										sb = new Set(b);
									let inter = 0;
									for (const c of sa) if (sb.has(c)) inter++;
									if (inter / (sa.size + sb.size - inter) >= 0.75) return true;
								}
								return false;
							};
							const ents = db
								.prepare(
									"SELECT entity_id, name, mention_count FROM entities WHERE canonical_id IS NULL AND kind IS NOT NULL ORDER BY mention_count DESC, entity_id",
								)
								.all();
							const getEnt = db.prepare(
								"SELECT entity_id, name, aliases, canonical_id, mention_count FROM entities WHERE entity_id = ?",
							);
							let resolvedN = 0;
							for (let i = 0; i < ents.length; i++) {
								const canon = getEnt.get(ents[i].entity_id);
								if (!canon || canon.canonical_id !== null) continue; // 已被并走（跳过·下巡链接面已收敛）
								for (let j = ents.length - 1; j > i; j--) {
									const varCand = ents[j];
									if (varCand.entity_id === canon.entity_id) continue;
									const vc = getEnt.get(varCand.entity_id);
									if (!vc || vc.canonical_id !== null) continue;
									if (!isVariant(canon.name, vc.name)) continue;
									// 合并：vc → canon
									let aliases = [];
									try {
										aliases = JSON.parse(
											getEnt.get(canon.entity_id).aliases || "[]",
										);
									} catch {}
									if (!aliases.includes(vc.name)) aliases.push(vc.name);
									db.prepare(
										"UPDATE entities SET aliases = ?, mention_count = mention_count + ? WHERE entity_id = ?",
									).run(
										JSON.stringify(aliases),
										vc.mention_count || 1,
										canon.entity_id,
									);
									db.prepare(
										"UPDATE entities SET canonical_id = ? WHERE entity_id = ?",
									).run(canon.entity_id, vc.entity_id);
									db.prepare(
										"INSERT OR IGNORE INTO memories_entities (memory_id, entity_id) SELECT memory_id, ? FROM memories_entities WHERE entity_id = ?",
									).run(canon.entity_id, vc.entity_id);
									db.prepare(
										"DELETE FROM memories_entities WHERE entity_id = ?",
									).run(vc.entity_id);
									resolvedN += 1;
								}
							}
							if (resolvedN > 0) {
								stats.entitiesResolved =
									(stats.entitiesResolved || 0) + resolvedN;
								ctx.logger?.info?.(
									"[living-memory] entity resolve: " +
										resolvedN +
										" variants merged",
								);
							}
						} catch (eRes) {
							try {
								ctx.logger?.warn?.(
									"[living-memory] entity resolve fail: " +
										String(eRes).slice(0, 60),
								);
							} catch {}
						}
					}
				} catch (error) {
					ctx.logger?.warn?.(
						`[living-memory] entities fill failed: ${String(error).slice(0, 80)}`,
					);
				}
			} catch (error) {
				ctx.logger?.warn?.(
					`[living-memory] cooccur failed: ${String(error).slice(0, 80)}`,
				);
			}

			// ── 实体-三元组 KG 增量转写（step·design-approved「升维大刀这个窗做」）──
			//    显式系新边→两端条目主实体投影→memory_triples（实体知识层·与条目图并存）。
			//    质量三闸：主实体非纯数字（TfIdf 噪音词「16/12」不进知识层）+非自环+表达式唯一索引幂等（COALESCE 防表级 UNIQUE NULL 坑——「uuid 必写」同族）。
			//    水线 kg_edge_watermark（bootstrap 存量先行·nightly patrol只增新边）。cooccur/semantic 统计边不转（防垃圾·置信度过滤精神）。
			try {
				db.exec(
					`CREATE TABLE IF NOT EXISTS memory_triples (triple_id INTEGER PRIMARY KEY AUTOINCREMENT, subject_id INTEGER NOT NULL, predicate TEXT NOT NULL, object_id INTEGER, object_text TEXT, valid_from TEXT, valid_to TEXT, ingested_at TEXT NOT NULL, invalidated_at TEXT, confidence REAL DEFAULT 1.0, source_memory INTEGER, source_edge INTEGER, extracted_by TEXT NOT NULL)`,
				);
				db.exec(
					`CREATE UNIQUE INDEX IF NOT EXISTS idx_triples_dedupe ON memory_triples(subject_id, predicate, COALESCE(object_id,0), COALESCE(object_text,''))`,
				);
				db.exec(
					`CREATE INDEX IF NOT EXISTS idx_triples_subject ON memory_triples(subject_id)`,
				);
				const KG_KINDS = [
					"explicit",
					"patches",
					"used_skill",
					"solved_by",
					"requires",
					"conflicts_with",
				];
				let kgWm = 0;
				try {
					const w = db
						.prepare("SELECT v FROM organ_meta WHERE k='kg_edge_watermark'")
						.get();
					kgWm = Number(w && w.v) || 0;
				} catch {}
				const kgEdges = db
					.prepare(
						`SELECT id, src, dst, edge_type FROM memories_edges WHERE invalid_at IS NULL AND id > ? AND edge_type IN (${KG_KINDS.map(() => "?").join(",")}) ORDER BY id`,
					)
					.all(kgWm, ...KG_KINDS);
				if (kgEdges.length > 0) {
					const mainEntRows = db.prepare(
						`SELECT e.entity_id, e.name FROM memories_entities me JOIN entities e ON e.entity_id = me.entity_id WHERE me.memory_id = ? ORDER BY e.mention_count DESC, e.entity_id ASC LIMIT 3`,
					);
					const mainEnt = (mid) =>
						(mainEntRows.all(mid) || []).find((r) => /[^0-9]/.test(r.name)) ||
						null;
					const kgSrc = db.prepare(
						"SELECT id, ts, event_at, confidence FROM memories WHERE id = ?",
					);
					const insTri = db.prepare(
						`INSERT OR IGNORE INTO memory_triples (subject_id, predicate, object_id, valid_from, ingested_at, confidence, source_memory, source_edge, extracted_by) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'edge-project')`,
					);
					let kgIn = 0;
					for (const e of kgEdges) {
						const s = mainEnt(e.src),
							o = mainEnt(e.dst);
						if (!s || !o || s.entity_id === o.entity_id) continue; // 无锚/自环=零知识
						const sm = kgSrc.get(e.src);
						if (!sm) continue;
						kgIn += insTri.run(
							s.entity_id,
							e.edge_type,
							o.entity_id,
							sm.event_at || sm.ts,
							nowIso(),
							sm.confidence ?? 1.0,
							e.src,
							e.id,
						).changes;
					}
					stats.triplesIn = kgIn;
					db.prepare(
						"INSERT OR REPLACE INTO organ_meta (k, v) VALUES ('kg_edge_watermark', ?)",
					).run(String(kgEdges[kgEdges.length - 1].id));
					if (kgIn > 0)
						ctx.logger?.info?.(
							`[living-memory] kg triples 增量入 ${kgIn}（显式边→实体知识层）`,
						);
				}
				try {
					stats.triplesTotal = db
						.prepare(
							"SELECT COUNT(*) c FROM memory_triples WHERE invalidated_at IS NULL",
						)
						.get().c;
				} catch {}
			} catch (error) {
				ctx.logger?.warn?.(
					`[living-memory] kg triples failed: ${String(error).slice(0, 80)}`,
				);
			}

			// ── Auto-Dreamer 抽象合成（step·2026-08-30 goal 五连刀·论文 2605.20616 抽象合成意）──
			//    **实勘改造**：题 token 聚簇在 DSH 形态不可行（碎片池 7 条/题面稀疏 0 簇·写入=浓缩field report式无 碎片层）
			//    ——改用 **语义边连通分量**（现成基础设施·零新计算）：分量≥3 → 合成「簇索引条」（source='dreamer'
			//    ·content=成员题列表+时间跨度）+explicit 边 0.6 链回各成员。**只新增不删除**（总案原文·防误杀）。
			//    幂等=成员已被 dreamer 边链过即跳（分量只增不减·边判稳）。帽 1 簇/巡（dreamerClusters counter）。
			try {
				const sEdges = db
					.prepare(
						"SELECT src, dst FROM memories_edges WHERE invalid_at IS NULL AND edge_type='semantic'",
					)
					.all();
				const parent12 = new Map();
				const find12 = (x) => {
					while (parent12.get(x) !== x) x = parent12.get(x);
					return x;
				};
				for (const e of sEdges) {
					if (!parent12.has(e.src)) parent12.set(e.src, e.src);
					if (!parent12.has(e.dst)) parent12.set(e.dst, e.dst);
					parent12.set(find12(e.src), find12(e.dst));
				}
				const groups = new Map();
				for (const x of parent12.keys()) {
					const r = find12(x);
					if (!groups.has(r)) groups.set(r, []);
					groups.get(r).push(x);
				}
				// 8-31 锈面修（审计片7⑥）：幂等前提「分量只增不减」被死边软失效打破（边失效后分量可缩可裂）——覆盖判定只认活边（invalid_at IS NULL）
				const linkedByDreamer = db.prepare(
					"SELECT COUNT(*) c FROM memories_edges WHERE edge_type='explicit' AND source_group='dreamer' AND dst = ? AND invalid_at IS NULL",
				);
				const memInfo = db.prepare(
					"SELECT id, title, space, ts FROM memories WHERE id = ? AND status = 'active' AND source != 'dreamer'",
				); // 再审修：簇索引条不回流簇（可将 dreamer 条连进分量→链式繁殖·防）
				let made = false;
				for (const members of [...groups.values()].sort(
					(a, b) => b.length - a.length,
				)) {
					if (made || members.length < 3) break;
					// 8-31 锈面修（审计片7⑥）：members[0] 采样式判据致簇覆盖漏建——改全量存在性校验：簇成员全被活 dreamer 边链过才跳过；
					//    有未覆盖成员即重建（新成员并入/死边失效致旧覆盖破洞均补建）——保「只新增不删除」原典（旧簇索引条不删）
					let coveredAll = members.length > 0;
					for (const id of members) {
						if (linkedByDreamer.get(id).c === 0) {
							coveredAll = false;
							break;
						}
					}
					if (coveredAll) continue; // 幂等：簇成员全覆盖（活边口径）
					const rows12 = members.map((id) => memInfo.get(id)).filter(Boolean);
					if (rows12.length < 3) continue;
					const span12 =
						rows12
							.reduce((a, m) => (m.ts < a ? m.ts : a), rows12[0].ts)
							.slice(0, 10) +
						"~" +
						rows12
							.reduce((a, m) => (m.ts > a ? m.ts : a), rows12[0].ts)
							.slice(0, 10);
					const titles12 = rows12
						.slice(0, 8)
						.map(
							(m) =>
								"·#" +
								m.id +
								" " +
								String(m.title)
									.replace(
										/^20\d{2}-\d{2}-\d{2}[ T]\d{2}:\d{2}\s*\[\w+\]\s*/,
										"",
									)
									.slice(0, 30),
						)
						.join("\n");
					const t12 = `Auto-Dreamer 语义簇（${rows12.length} 条·${rows12[0].space}·${span12}·首锚#${Math.min(...rows12.map((r) => r.id))}）`;
					const d12 = db
						.prepare(
							"INSERT INTO memories (ts, type, title, content, space, source, status, checksum, event_at) VALUES (?, 'fact', ?, ?, ?, 'dreamer', 'active', ?, ?)",
						)
						.run(
							nowIso(),
							t12,
							`语义近邻簇索引（${rows12.length} 条·${span12}）——检索一跳到簇·源条全保留：\n${titles12}`,
							rows12[0].space,
							sha1(nowIso() + t12),
							span12.slice(0, 10),
						);
					const e12 = db.prepare(
						`INSERT INTO memories_edges (src, dst, edge_type, weight, valid_at, source_group, last_seen, instruction) VALUES (?, ?, 'explicit', 0.6, ?, 'dreamer', ?, 'Auto-Dreamer 语义簇链回')`,
					);
					for (const m of rows12) {
						try {
							e12.run(Number(d12.lastInsertRowid), m.id, nowIso(), nowIso());
						} catch {}
					}
					stats.dreamerClusters = (stats.dreamerClusters || 0) + 1;
					made = true;
					ctx.logger?.info?.(
						`[living-memory] Auto-Dreamer 合成语义簇 ${rows12.length} 条（#${d12.lastInsertRowid}·只增不删）`,
					);
				}
			} catch (e12) {
				ctx.logger?.warn?.(
					`[living-memory] dreamer fail: ${String(e12).slice(0, 60)}`,
				);
			}

			// ── 在库 TTL 到点退出（step·2026-08-30 goal 五连刀·set_state_expiry 意）──
			//    声明过寿命的条（stale_state='ttl' 预写）valid_to 到点→nightly patrol批量转 aged（closed_at 墓碑·退出检索）。
			//    **只动 ttl 标条**——显式 valid_to 补记条（无标）继续走检索 ⏦ 软沉底（失效≠删除·P2 精神）。
			let ttlAgedCount = 0; // 8-31 锈面修⑤：本巡 TTL 到点退出条数（light sentry合法流转豁免面）
			try {
				const ttlHit = db
					.prepare(
						"UPDATE memories SET status='aged', closed_at = COALESCE(closed_at, ?), valid_to = COALESCE(valid_to, ?) WHERE stale_state='ttl' AND status='active' AND valid_to IS NOT NULL AND valid_to <= ?",
					)
					.run(nowIso(), nowIso(), nowIso());
				ttlAgedCount = Number(ttlHit.changes) || 0; // 8-31 锈面修⑤
				if (ttlHit.changes > 0) {
					stats.ttlExpired = (stats.ttlExpired || 0) + ttlHit.changes;
					ctx.logger?.info?.(
						`[living-memory] TTL 到点退出 ${ttlHit.changes} 条（aged·set_state_expiry）`,
					);
				}
			} catch (e13) {
				ctx.logger?.warn?.(
					`[living-memory] ttl fail: ${String(e13).slice(0, 60)}`,
				);
			}

			// ── 第 2.step：记忆链路light sentry（共现后·备份前；整段 try/catch——哨故障只记 alarm 不阻断nightly patrol）──
			//    Legacy-path sentinel: structure kept, inert unless RE_LONG/RE_SHORT are set.
			//    projcache 限定 cwd 类字段（§六修订①：通知快照存field report原文含历史字样，全文扫必假警报）。
			//    库哨/计数基线落 organ_meta（§六修订②：内存基线重启即丢）。只读只报不改(ops note)。
			try {
				const RE_LONG = /$^/g; // legacy-path sentinel: site-specific migration check, inert in the public build
				const RE_SHORT = /$^/g; // ditto
				const hitsOf = (t) => {
					const s = String(t || "");
					return (
						(s.match(RE_LONG) || []).length + (s.match(RE_SHORT) || []).length
					);
				};
				let pathHits = 0;
				const alarms = [];
				// ① 路径哨·全文类：G1 总纲、workspace.json、your module directoriesAGENTS.md（缺文件按现存数≥8 判·空文件即警报）
				const fullFiles = [
					path.join(os.homedir(), ".dsh", "AGENTS.md"),
					path.join(os.homedir(), ".dsh", "storages", "workspace.json"),
				];
				let moduleAgentFiles = [];
				try {
					moduleAgentFiles = fs2
						.readdirSync(MODULE_SCAN_DIR)
						.filter((n) => {
							try {
								return fs2
									.statSync(path.join(MODULE_SCAN_DIR, n))
									.isDirectory();
							} catch {
								return false;
							}
						})
						.map((n) => path.join(MODULE_SCAN_DIR, n, "AGENTS.md"));
				} catch {}
				let existingAgents = 0;
				let injectOk = true;
				for (const f of fullFiles) {
					try {
						const txt = fs2.readFileSync(f, "utf8");
						if (!txt.trim()) {
							injectOk = false;
							alarms.push("注入文件空: " + f.slice(-30));
							continue;
						}
						pathHits += hitsOf(txt);
					} catch {
						injectOk = false;
						alarms.push("注入文件不可读: " + f.slice(-30));
					}
				}
				for (const f of moduleAgentFiles) {
					try {
						const txt = fs2.readFileSync(f, "utf8");
						existingAgents += 1;
						if (!txt.trim()) {
							injectOk = false;
							alarms.push("模块说明书空: " + f.slice(-30));
							continue;
						}
						pathHits += hitsOf(txt);
					} catch {
						/* 该模块无 AGENTS.md——不计存在、不单报（按现存数门槛判） */
					}
				}
				if (existingAgents < 8) {
					injectOk = false;
					alarms.push(
						"模块 AGENTS.md 现存仅 " + existingAgents + " 份（门槛 8）",
					);
				}
				// ② 路径哨·projcache（限定 cwd 类字段；沙箱开关 LEGION_SENTINEL_PROJCACHE）
				let pcCount = 0;
				try {
					const pcPath =
						process.env.LEGION_SENTINEL_PROJCACHE ||
						path.join(
							os.homedir(),
							".dsh",
							"storages",
							"session_projcache.json",
						);
					const pc = pcPath.endsWith("session_projcache.json")
						? projcacheRows()
						: JSON.parse(fs2.readFileSync(pcPath, "utf8")); // 庚批修⑤（09-08 审计）：默认路径走 v5 双源 Map（旧单文件 stale 半盲）；drill 假投影旋钮（LEGION_SENTINEL_PROJCACHE 指定文件）仍直读
					const PATH_KEYS = /^(cwd|workspace|workdir|rootdir|path)$/i; // 第三轮审计修（09-09 pending ruling②·TDZ 根治）：两声明上提至 walkEnt 消费前——原序 walkEnt 体内调 walkPc/PATH_KEYS 于 const 声明前=每夜 TDZ ReferenceError 被 catch 吞成「projcache 不可读」假警报（庚批修⑤自伤·假绿族同族）
					const walkPc = (o, parentKey) => {
						if (o && typeof o === "object") {
							for (const [k, v] of Object.entries(o)) {
								if (
									typeof v === "string" &&
									(PATH_KEYS.test(k) || PATH_KEYS.test(String(parentKey || "")))
								) {
									pathHits += hitsOf(v);
									pcCount += 1;
								} else walkPc(v, k);
							}
						}
					};
					const walkEnt = (ent) => {
						walkPc(ent?.rows || {}, "");
						walkPc(ent?.identity || {}, "");
					};
					if (pc instanceof Map) {
						for (const ent of pc.values()) walkEnt(ent);
					}
					if (!(pc instanceof Map)) walkPc(pc, ""); // Map 态已逐 ent 走（修⑤）
				} catch {
					alarms.push("projcache 不可读");
				}
				// ③ 库哨：active 条数不倒退（基线 organ_meta 持久化）+ ftsReady
				const libCount = Number(
					db
						.prepare(
							"SELECT COUNT(*) AS c FROM memories WHERE status = 'active'",
						)
						.get().c,
				);
				let ftsReady = true;
				try {
					db.prepare("SELECT COUNT(*) AS c FROM memories_fts").get();
				} catch {
					ftsReady = false;
				}
				let libBase = null,
					pcBase = null;
				try {
					const r1 = db
						.prepare("SELECT v FROM organ_meta WHERE k = ?")
						.get("linkSentinel.libCount");
					const r2 = db
						.prepare("SELECT v FROM organ_meta WHERE k = ?")
						.get("linkSentinel.pcCount");
					libBase = r1 ? Number(r1.v) : null;
					pcBase = r2 ? Number(r2.v) : null;
				} catch {}
				// 8-31 锈面修（审计片7⑤）：「不倒退」误报——计划内衰减属合法流转（本巡 TTL 到点转 aged/14d 待办衰减/裁决合并执行），差值豁免后再报
				const legitOut = (agedTodos || 0) + ttlAgedCount + patrolMergedOut;
				if (
					libBase !== null &&
					libCount < libBase &&
					libBase - libCount > legitOut
				)
					alarms.push(
						"库条数倒退: " +
							libBase +
							"→" +
							libCount +
							"（合法流转已豁免 " +
							legitOut +
							" 条）",
					);
				// ── design-approved「发」·A 线option·档=a companion script）──
				// projcache=宿主投影缓存（dsh-session-projection-cache：never wrong only possibly stale·无定时 prune·
				// 会话删除/处置事件驱动收缩·投影 256 行>物理 147 会话=宿主常态）——「cwd 计数不倒退」方向守卫证伪：
				// 合法收缩全被当警报（09-03 -3/09-05 -26 两单均误报）。保留两有效哨面：pathHits 旧路径检测（L5245）
				// +「projcache 不可读」（L5253）；pcCount 观测面与基线写入保留（stats.linkSentinel 连续性·复用备料）。
				if (!ftsReady) alarms.push("FTS 不可用");
				if (pathHits > 0)
					alarms.push("旧路径命中 " + pathHits + " 处（双形态）");
				// ⑤ 办结状态位哨（design-approved option·lesson 「办结=改状态位非写叙述」）：
				//    active todo 被 solved_by 边指向＝报捷 fact 已链回而 todo 本体漏打 done 戳（僵尸复活通道）——
				//    只报不改，销账走space流程正路（词面自动闭环已死  closureCheck 自污染·显式边判据不重蹈）。
				let solvedLeak = 0;
				try {
					const leaks = db
						.prepare(
							`SELECT t.id AS tid FROM memories t JOIN memories_edges e
						 ON e.dst = t.id AND e.edge_type = 'solved_by'
						 WHERE t.status = 'active' AND t.type = 'todo' AND e.invalid_at IS NULL
						 LIMIT 20`,
						)
						.all();
					solvedLeak = leaks.length;
					if (solvedLeak > 0)
						alarms.push(
							"办结漏戳 todo " +
								solvedLeak +
								" 条: " +
								leaks
									.map((r) => "#" + r.tid)
									.join(" ")
									.slice(0, 120),
						);
				} catch {
					alarms.push("办结状态位哨查询失败");
				}
				// 基线落盘（首跑建立，此后比较不倒退）
				try {
					db.prepare(
						"INSERT OR REPLACE INTO organ_meta (k, v) VALUES (?, ?)",
					).run("linkSentinel.libCount", String(libCount));
					db.prepare(
						"INSERT OR REPLACE INTO organ_meta (k, v) VALUES (?, ?)",
					).run("linkSentinel.pcCount", String(pcCount));
				} catch {}
				stats.linkSentinel = {
					checkedAt: nowIso(),
					pathHits,
					libCount,
					ftsReady,
					injectOk,
					pcCount,
					solvedLeak, // option哨（09-07）：active todo 被 solved_by 边指向条数——0=干净
					alarm: alarms.length ? alarms.join("；").slice(0, 300) : "",
				};
				if (alarms.length > 0) {
					try {
						const title = ("记忆链路light sentry警报：" + alarms[0]).slice(0, 80);
						const content = (
							"nightly patrollight sentry发现：" +
							alarms.join("；") +
							"——哨只报不改，修复走ops/space流程。"
						).slice(0, 400);
						db.prepare(
							"INSERT INTO memories (ts, type, title, content, space, source, checksum) VALUES (?, ?, ?, ?, ?, ?, ?)",
						).run(
							nowIso(),
							"todo",
							title,
							content,
							"memory-organ",
							"patrol-sentinel:" + dayKey,
							sha1(title + content),
						);
					} catch {}
				}
				ctx.logger?.info?.(
					`[living-memory] link sentinel ok: pathHits=${pathHits} libCount=${libCount} pcCount=${pcCount} alarm=${alarms.length ? "YES(" + alarms.length + ")" : "none"}`,
				);
			} catch (error) {
				stats.linkSentinel = {
					checkedAt: nowIso(),
					pathHits: -1,
					libCount: -1,
					ftsReady: false,
					injectOk: false,
					alarm: ("哨故障: " + String(error)).slice(0, 120),
				};
				ctx.logger?.warn?.(
					"[living-memory] link sentinel failed: " + String(error).slice(0, 80),
				);
			}

			stats.nightPatrolCount += 1;
			stats.lastPatrolAt = Date.now();
			stats.lastPatrolMerged = merged;
			stats.lastPatrolMergedIds = mergedIds.slice(0, 50); // 回查清单（截 50 防 stats 膨胀）
			stats.conflictsPending = conflictsPending; // 3a sentry report字段
			stats.conflictsPreverdicted = db
				.prepare(
					"SELECT COUNT(*) c FROM conflicts WHERE pre_verdict IS NOT NULL AND status = 'pending'",
				)
				.get().c; // AUDN 预裁counter（wave）
			stats.conflictsPreclassifErrors = stats.conflictsPreclassifErrors || 0;
			stats.lastCooccurPairs = cooccurPairs;
			// ── step（同批）：nightly patrol存量清算段——治 698 条积压（每账每晚一段=36 夜负增长）；
			//    fire-and-forget 逐账 while 推段·预算帽全局 40 段/巡·单账 6 段·超余顺延次夜。LEGION_BACKLOG_OFF 回退。
			if (!process.env.LEGION_BACKLOG_OFF) {
				(async () => {
					let budget = 40;
					// step 脑追办②：清单序改「未抽量大者优先」（治后缘饿死根——原插入序使长滞大账恒排尾吃残量）+参与账数观察键
					const keys = [...pendingBySid.entries()]
						.map(([sid0, acct]) => [
							sid0,
							acct.msgs.filter((m) => m.seq > acct.watermark).length,
						])
						.filter((p) => p[1] > 0)
						.sort((a, b) => b[1] - a[1])
						.map((p) => p[0]);
					stats.backlogAccounts = keys.length;
					for (const sid0 of keys) {
						if (budget <= 0) break;
						let seg = 0;
						try {
							while (seg < 6 && budget > 0) {
								const acct = pendingBySid.get(sid0);
								if (!acct || acct.msgs.every((m) => m.seq <= acct.watermark))
									break;
								const r = await runExtract({ sid: sid0 });
								if (!r || r.skipped) break;
								budget--;
								seg++;
							}
						} catch (e) {
							stats.backlogErrors = (stats.backlogErrors || 0) + 1;
						} // 审计 I7：⑬ 出口
					}
					stats.backlogSegments = 40 - budget;
					// P0修（09-03 audit）：竞态死值修——同步段 patrol_last 写入时本值恒 0（赋值在 await 后），清算毕补写真值（最终一致）
					try {
						const plRow = db
							.prepare("SELECT v FROM organ_meta WHERE k='patrol_last'")
							.get();
						if (plRow) {
							const plObj = JSON.parse(plRow.v);
							plObj.backlogSegments = stats.backlogSegments;
							plObj.backlogAccounts = stats.backlogAccounts || 0;
							db.prepare(
								"INSERT OR REPLACE INTO organ_meta (k, v) VALUES ('patrol_last', ?)",
							).run(JSON.stringify(plObj));
						}
					} catch (ePB) {
						stats.backlogErrors = (stats.backlogErrors || 0) + 1;
					}
				})();
			}
			// ── patrol 历史凭证(ops note)：organ_meta 落库，重启不丢——at/merged/cooccurPairs/total 四字段，total 读旧值+1 滚动 ──
			try {
				let patrolTotal = 0;
				try {
					const prev = db
						.prepare("SELECT v FROM organ_meta WHERE k='patrol_last'")
						.get();
					if (prev) patrolTotal = Number(JSON.parse(prev.v).total) || 0;
				} catch {}
				pruneOrganMeta(7); // 2026-09-12 三车（裁 4＋3b）：organ_meta 生命周期——清 7 天内无活动的会话计数键（删键不致病·三源取大兜住序数）
				db.prepare(
					"INSERT OR REPLACE INTO organ_meta (k, v) VALUES ('patrol_last', ?)",
				).run(
					JSON.stringify({
						at: nowIso(),
						merged,
						cooccurPairs,
						total: patrolTotal + 1,
						backlogSegments: stats.backlogSegments || 0,
						backlogAccounts: stats.backlogAccounts || 0,
						a10SkipErrors: stats.a10SkipErrors || 0,
						a10Semantic: stats.semanticConflictsFound || 0,
						triplesIn: stats.triplesIn || 0,
						triplesTotal: stats.triplesTotal || 0,
					}), // step：+backlogAccounts；step：+KG 三元组counter
				); // step N-c2：counter随巡持久化（跨重启对账面）
			} catch {}
			ctx.logger?.info?.(
				`[living-memory] night patrol done: merged=${merged} cooccurPairs=${cooccurPairs} agedSessions=${aged.length} agedTodos=${agedTodos} soul=${soulDrift ? "DRIFT!" : "ok"} conflicts_pending=${conflictsPending}${conflictsPending > 0 ? "（sentry report：清单见 conflicts 表·首周全量人工pending ruling）" : ""}`,
			);
			// ── 3c S4 阈值计数器（阶段三·后置触发：闭环面候选 >1500 才启用预筛——nightly patrol只检测）──
			try {
				const closureCandidates = db
					.prepare(
						`SELECT COUNT(*) c FROM memories WHERE ts >= ? AND ((type='todo' AND status='done') OR ((type IN ('fact','decision','lesson')) AND (title LIKE '%销账%' OR content LIKE '%销账%' OR title LIKE '%收官%' OR content LIKE '%收官%' OR title LIKE '%闭环%' OR content LIKE '%闭环%')))`,
					)
					.get(
						(() => {
							const d = new Date(
								Date.now() + 8 * 3600 * 1000 - 72 * 3600 * 1000,
							);
							const p = (n) => String(n).padStart(2, "0");
							return `${d.getUTCFullYear()}-${p(d.getUTCMonth() + 1)}-${p(d.getUTCDate())}T${p(d.getUTCHours())}:${p(d.getUTCMinutes())}+08:00`;
						})(),
					).c;
				db.prepare(
					"INSERT OR REPLACE INTO organ_meta (k, v) VALUES ('closurecheck_candidates', ?)",
				).run(String(closureCandidates));
				if (closureCandidates > 1500)
					ctx.logger?.warn?.(
						`[living-memory] S4 threshold exceeded: closure candidates ${closureCandidates} > 1500 — closureCheck prefilter not enabled yet (tracked as a separate item)`,
					);
			} catch {}

			// ── 提炼质检栏（design-approved P0 提炼质量环）：patrol 顺写 organ_meta distiller_qc——首检值本窗初始化·周检周一晨窗（绿稳两月降月检） ──
			try {
				const qc = db
					.prepare("SELECT v FROM organ_meta WHERE k = 'distiller_qc'")
					.get();
				if (!qc) {
					db.prepare(
						"INSERT OR REPLACE INTO organ_meta (k, v) VALUES ('distiller_qc', ?)",
					).run(
						JSON.stringify({
							at: "2026-08-26T07:00+08:00",
							sample: 10,
							green: 9,
							yellow: 1,
							red: 0,
							verdict: "green",
							note: "首检 8-26 wave（周随机 10 条三问·唯黄=8-21 历史层豁免）；下次=周一晨窗",
						}),
					);
					ctx.logger?.info?.(
						"[living-memory] distiller_qc 初始化落库（提炼质检栏）",
					);
				}
			} catch {}

			// ── 吸收item③（2026-09-02）：tool_usage 遥测表 30 天轮转（执行档「nightly patrol顺带轮转·30 天帽」）──
			//    ts 为 nowIso() 同构本地 ISO 串——同构串字典序=时序；cutoff 在 JS 侧同格式算，禁混 SQLite datetime（时区口径不一）。
			try {
				const cutD = new Date(
					Date.now() + 8 * 3600 * 1000 - 30 * 24 * 3600 * 1000,
				);
				const pp = (n) => String(n).padStart(2, "0");
				const cutIso = `${cutD.getUTCFullYear()}-${pp(cutD.getUTCMonth() + 1)}-${pp(cutD.getUTCDate())}T${pp(cutD.getUTCHours())}:${pp(cutD.getUTCMinutes())}+08:00`;
				const delU = db
					.prepare("DELETE FROM tool_usage WHERE ts < ?")
					.run(cutIso);
				if (delU.changes > 0)
					ctx.logger?.info?.(
						`[living-memory] tool_usage 30天轮转: 清 ${delU.changes} 行`,
					);
			} catch {}

			// ── step：nightly patrol末尾在线备份（sqlite3 .backup，禁文件 cp——WAL 未 checkpoint 数据会丢）──
			//    备份源 = 本实例主库路径（生产=正库；沙箱演练=沙箱库，绝不触碰正库）。保留最近 7 天。
			try {
				const dayStamp = dayKey.replaceAll("-", "");
				fs2.mkdirSync(SNAPSHOT_DIR, { recursive: true });
				const dest = path.join(SNAPSHOT_DIR, `memory-${dayStamp}.sqlite3`);
				execFileSync("sqlite3", [DB_PATH, `.backup ${dest}`], {
					timeout: 60000,
				});
				// 7 天轮转：删除超期快照
				const cutoff = Date.now() - SNAPSHOT_KEEP_DAYS * 24 * 3600 * 1000;
				for (const name of fs2.readdirSync(SNAPSHOT_DIR)) {
					const m = name.match(/^memory-(\d{8})\.sqlite3$/);
					if (!m) continue;
					const t = Date.parse(
						`${m[1].slice(0, 4)}-${m[1].slice(4, 6)}-${m[1].slice(6, 8)}T00:00:00+08:00`,
					);
					if (!isNaN(t) && t < cutoff) {
						try {
							fs2.unlinkSync(path.join(SNAPSHOT_DIR, name));
						} catch {}
					}
				}
				ctx.logger?.info?.(`[living-memory] snapshot ok: ${dest}`);
			} catch (error) {
				ctx.logger?.warn?.(
					`[living-memory] snapshot failed: ${String(error).slice(0, 80)}`,
				);
			}

			// ── 备份巡检三证(design note)：TM 两证+快照自测 ──
			//    ① TM 目标在位 ② 库未排除(ops note)③ nightly patrol日快照在位（自测）。
			//    历史注记：07:15 批「双异盘」方案已被 TM 令覆盖撤销——首版「快照存在/可开/行数≥前日」旧口径代码见 备份/index.cjs.修法包前。
			//    顺序修正（design-approved「同意」一行修）：本段原在快照段之前——每日首巡时今日快照未写致 snapOk 恒 false·移至快照段后（先备份后查证）。
			try {
				let tmMounted = false,
					tmIncluded = false;
				try {
					const tmOut = execFileSync("tmutil", ["destinationinfo"], {
						timeout: 10000,
						encoding: "utf8",
					});
					tmMounted = /Mount Point\s*:/.test(tmOut);
				} catch {}
				try {
					const incOut = execFileSync("tmutil", ["isexcluded", DB_PATH], {
						timeout: 10000,
						encoding: "utf8",
					});
					tmIncluded = /\[Included\]/.test(incOut);
				} catch {}
				const snapToday = path.join(
					SNAPSHOT_DIR,
					`memory-${dayKey.replaceAll("-", "")}.sqlite3`,
				);
				const snapOk = fs2.existsSync(snapToday);
				const verdict = tmMounted && tmIncluded && snapOk ? "ok" : "FAIL";
				db.prepare(
					"INSERT OR REPLACE INTO organ_meta (k, v) VALUES ('backup_tri', ?)",
				).run(
					JSON.stringify({
						at: nowIso(),
						tmMounted,
						tmIncluded,
						snapOk,
						snapshot: snapToday,
						verdict,
						note: "TM两证+快照自测（8-26 08:06maintainer改口径）",
					}),
				);
				if (verdict !== "ok")
					ctx.logger?.warn?.(
						`[living-memory] 备份三证异常: tmMounted=${tmMounted} tmIncluded=${tmIncluded} snapOk=${snapOk}`,
					);
			} catch (error) {
				ctx.logger?.warn?.(
					"[living-memory] backup tri failed: " + String(error).slice(0, 60),
				);
			}

			// ── P0 同item：遥测周报常驻化（design-approved「同意」四点②）──
			//    手跑转正：a companion script nightly patrol自动跑——7 天闸（organ_meta usage_weekly_last ISO 戳比对）；
			//    回退开关 LEGION_USAGE_WEEKLY_OFF；沙箱演练态（LEGION_MODULE_SCAN_DIR 设）跳过——不触真库/真周报目录。
			//    ⑬ 纪律：catch 必有出口（warn+stats.usageWeeklyErrors 计数透出）·失败不阻塞nightly patrol。
			try {
				if (
					false && // usage-weekly disabled in public build (companion script not shipped)
					!process.env.LEGION_USAGE_WEEKLY_OFF &&
					!process.env.LEGION_MODULE_SCAN_DIR
				) {
					const lastW = db
						.prepare("SELECT v FROM organ_meta WHERE k = 'usage_weekly_last'")
						.get();
					const due =
						!lastW ||
						!Number.isFinite(Date.parse(String(lastW.v))) || // 09-03 自审补：非法值 NaN 比较恒 false=静默永不再跑——视非法为 due
						Date.now() - Date.parse(String(lastW.v)) > 7 * 24 * 3600 * 1000;
					if (due) {
						const script = path.join(
							os.homedir(), ".dsh", "dsh-living-memory", "modules",
							"memory organ",
							"工程",
							"usage-weekly.cjs",
						);
						const out = execFileSync(process.execPath, [script], {
							timeout: 60000,
							encoding: "utf8",
						});
						db.prepare(
							"INSERT OR REPLACE INTO organ_meta (k, v) VALUES ('usage_weekly_last', ?)",
						).run(nowIso());
						stats.usageWeeklyRuns = (stats.usageWeeklyRuns || 0) + 1;
						ctx.logger?.info?.(
							`[living-memory] usage-weekly ok（7天闸·落 a companion script）: ${String(out).trim().slice(-120)}`,
						);
					}
				}
			} catch (eW) {
				stats.usageWeeklyErrors = (stats.usageWeeklyErrors || 0) + 1;
				ctx.logger?.warn?.(
					`[living-memory] usage-weekly failed (#${stats.usageWeeklyErrors}): ${String(eW).slice(0, 80)}`,
				);
			}
		}
		// ── issue 修①（design-approved）：nightly patrol timer 纳入 Cordis 生命周期 ──
		//    病灶：裸 setInterval 无句柄无清理——热换后旧 timer 残留、旧库连接已关、新旧 patrol 双跑；
		//    修：ctx.effect 注册+清理函数回收+unref（不阻 headless 退出）。
		ctx.effect(() => {
			const timer = setInterval(nightPatrol, PATROL_CHECK_MS);
			timer.unref?.();
			return () => clearInterval(timer);
		});
		// 沙箱演练钩子（生产不设=零行为差）：LEGION_DRILL_PATROL=1 → apply 后立即强制nightly patrol一轮（flag 闸仍生效）
		if (process.env.LEGION_DRILL_PATROL === "1") {
			try {
				nightPatrol(true);
			} catch {}
		}

		// ── 工具一（step后为只读三 action）：memory 统一只读操作 ──
		//    写权移至 memory_write（preset 层挂载矩阵——memory organ/brain可见，其余space无此工具）。
		tools.register({
			name: "memory",
			description:
				"Read-only access to the memory store. When past records, decisions or lessons are relevant, search before answering (memory-first), and cite entries as [#id] with a timestamp so quotes stay checkable. Actions: search (hybrid keyword + vector recall) | timeline (reverse-chronological browse) | stats (self-check, incl. extraction and nightly-patrol counters) | read_episodic (replay the source-conversation window of an auto-extracted entry) | read_evolution (replay the source-document window of a mirror entry). Writes go through memory_write (role-mounted).",
			parameters: {
				type: "object",
				required: ["action"],
				properties: {
					action: {
						type: "string",
						enum: [
							"search",
							"timeline",
							"stats",
							"read_episodic",
							"read_evolution",
						],
						description:
							"Action: search | timeline | stats | read_episodic (replay an entry's source-conversation window) | read_evolution (replay a mirror entry's source-document window)",
					},
					id: { type: "number", description: "read_episodic 用：条目 id" },
					window: {
						type: "number",
						description: "read_episodic 用：抽取轮窗（默认 6·1-12）",
					},
					charCap: {
						type: "number",
						description: "read_episodic 用：字符帽（默认 3000·500-6000）",
					},
					space: {
						type: "string",
						description:
							"timeline: filter by space (optional). stats: pin the pinboardTop ranking to a space (optional; defaults to the caller's own space)",
					},
					query: {
						type: "string",
						description: "search：检索关键词（空格/逗号分隔）",
					},
					as_of: {
						type: "string",
						description:
							"可选·as_of 时间旅行（search/timeline 用）：YYYY-MM-DD[THH:mm] 时点快照——只看该时点前已入库(ts≤)且未闭环(closed_at＞)且未失效(valid_to＞)的条目；日期粒度=该日零点口径",
					},
					limit: {
						type: "number",
						description:
							"search：最多返回条数（默认 5，最大 20）；timeline：默认 10，最大 20",
					},
				},
			},
			output: {
				schema: { type: "object", additionalProperties: true },
				render: (args, value) => memoryRender(args, value),
			},
			execute: usageWrap(
				"memory",
				() => db,
				async (args, exec) => {
					const action = String(args.action || "");
					if (action === "search") {
						if (!args.query) return { error: "search 需要 query 参数" };
						const limit = Math.min(Number(args.limit) || 5, 20);
						// ── as_of 时点检索路（wave·design-approved「继续三车」·时序补全）──
						//    治面：审档/复盘需「当时库什么样」——三窗过滤 ts≤as_of ∧ COALESCE(closed_at,'9999')＞as_of
						//    ∧ COALESCE(valid_to,'9999')＞as_of（已入库∧未闭环∧未失效；'9999' 兜底与 +08:00 ISO 串
						//    字典序天然兼容；日期粒度=该日零点口径）。专用精简路在正常路之前 return：跳过 vec/rerank/
						//    spoken boost/cooccur/PPR/hop/community/audit gate（时点快照=词面审档场景·timeline24h 系当前窗
						//    无意义）；弃权提示保留。无 env 开关（纯新增参数·缺省零行为变）。decay 用 as_of 锚
						//    内联简化版（todo/essential/mirror 恒 1.0·fact·decision 28d·lesson 14d 统一档——时点快照=
						//    当时相对新旧·不套运营新鲜度 P3 分档；decayOf 定义在正常路后 TDZ 不可达·故内联）。
						const asOfRaw = String(args.as_of || "").trim();
						if (
							asOfRaw &&
							!/^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])(T(0[0-9]|1\d|2[0-3]):[0-5]\d)?$/.test(
								asOfRaw,
							)
						)
							return {
								error:
									"as_of 格式非法（YYYY-MM-DD[THH:mm]·同款闸）: " +
									asOfRaw.slice(0, 30),
							};
						if (asOfRaw) {
							const asOfMs = Date.parse(
								asOfRaw.includes("T")
									? asOfRaw + ":00+08:00"
									: asOfRaw + "T00:00:00+08:00",
							);
							const callerCwdA =
								exec?.agent?.session?.header?.cwd ||
								exec?.agent?.session?.cwd ||
								exec?.session?.header?.cwd ||
								exec?.cwd;
							const callerSpaceA = organFromPath(callerCwdA);
							const tokensA = qTokens(String(args.query));
							const matchA = queryMatch(tokensA);
							const asOfDecay = (row) => {
								if (row.type === "todo") return 1.0;
								if (row.essential === 1) return 1.0;
								if (String(row.source || "").startsWith("mirror:")) return 1.0;
								const anchorA = row.event_at || row.ts;
								const tA = Date.parse(String(anchorA).replace(" ", "T"));
								if (isNaN(tA) || isNaN(asOfMs)) return 1.0;
								const daysA = Math.max(0, (asOfMs - tA) / 86400000);
								const halfA =
									row.type === "decision" || row.type === "fact" ? 28 : 14;
								return Math.max(0.05, 0.5 ** (daysA / halfA));
							};
							let rowsA = [];
							if (matchA) {
								const ftsBaseA = `SELECT m.*, bm25(memories_fts) AS rank FROM memories_fts JOIN memories m ON m.id = memories_fts.rowid
                 WHERE memories_fts MATCH ? AND m.status IN ('active','merged','done')
                   AND m.ts <= ? AND COALESCE(m.closed_at, '9999') > ? AND COALESCE(m.valid_to, '9999') > ?`; // step+三案修补案2（09-09 approved）：候选池放宽纳 merged/done——时点窗按 closed_at 自然排除 as_of 后闭环（as_of 前仍活的闭环条正确回流）；**时点语义不沉底**（×0.1=用后见之明改写历史·违 as_of「还原当时」本义）——只透出纯标注给现状线索
								rowsA = db
									.prepare(
										ftsBaseA +
											(callerSpaceA
												? ` AND m.space IN (?, 'global') ORDER BY rank LIMIT 100`
												: ` ORDER BY rank LIMIT 200`),
									)
									.all(
										matchA,
										asOfRaw,
										asOfRaw,
										asOfRaw,
										...(callerSpaceA ? [callerSpaceA] : []),
									);
							}
							const weightedA = rowsA.map((r) => {
								const rrA = -Number(r.rank || 0);
								const baseA = Math.max(0, rrA / (1 + rrA));
								const decA = asOfDecay(r);
								return {
									row: r,
									base: baseA,
									decay: Math.round(decA * 1000) / 1000,
									w: baseA * spaceWeight(r.space, callerSpaceA) * decA,
								};
							});
							weightedA.sort((a, b) => b.w - a.w);
							const pickedA = weightedA.slice(0, limit);
							return {
								hits: pickedA.map((s) => ({
									id: s.row.id,
									ts: s.row.ts,
									type: s.row.type,
									title: s.row.title,
									space: s.row.space,
									score: Math.round(s.w * 1000) / 1000,
									base: Math.round(s.base * 1000) / 1000,
									decay: s.decay,
									...(s.row.superseded_by != null
										? { supersededBy: s.row.superseded_by }
										: {}), // 三案修补案2：纯标注不降权——时点上当时仍活·后见之明不改写历史排序（render ↖标自然消费）
									...(s.row.status === "done"
										? {
												doneMark:
													"办结" +
													(s.row.closed_at
														? " " + String(s.row.closed_at).slice(0, 10)
														: ""),
											}
										: {}), // 三案修补案2：办结标同款纯标注
									content: String(s.row.content).slice(0, 200),
								})),
								total: weightedA.length,
								via: "fts",
								recallMode: "as-of",
								asOf: asOfRaw,
								callerSpace: callerSpaceA || "unknown",
								...(pickedA.length === 0 || pickedA[0].w < 0.15
									? {
											abstainHint:
												"⚠ 该时点库中无强相关条目——勿强答勿拼凑引用；可换关键词重查或调整 as_of 后重试",
										}
									: {}),
							};
						}
						// ── step：调用者space（加权用）——header.cwd 官方路径（dsh-tool-fs 同源），探测不到则不加权 ──
						const callerCwd =
							exec?.agent?.session?.header?.cwd ||
							exec?.agent?.session?.cwd ||
							exec?.session?.header?.cwd ||
							exec?.cwd;
						const callerSpace = organFromPath(callerCwd);
						// ── step③：FTS 主路（读写同 tokenize）+ bm25 → 0~1 × space权重 ──
						// ── 检索升级 1a（2026-08-22 复核案·两段式+RRF 框架·零依赖版）──
						//    两段式（学术反例 arXiv 2606.11350：软加权防稀释弱于硬过滤）：caller space存在时
						//    段一硬过滤 space IN (caller,global) top100；可用候选 <limit*2 回退段二全库 top200。
						//    RRF 框架（k=60）：召回层多通道融合接口——fts 通道常驻，vec 通道预留（embedding 源
						//    未接时返回空=单路直通，行为与旧版一致；接源后 FTS top50 ∪ vec top50 融合）。
						let rows = null;
						let viaFts = false;
						let vecIds = []; // 作用域修正（16:59·幻引用第三犯自伤）：提升到两 try 外——原 const 在 FTS try 内·cooccur try 引用不可达=整段静默灭
						let recallMode = "full";
						let qTokensShared = null; // option（design-approved）：查询词外提——weighted 加权段（try 外）席内加成要用（同法·幻引用预防）
						let queryClass = "other"; // SelRoute：分类结果外提（return 透出·try 外声明防 FTS 异常态丢）
						try {
							const tokens = qTokens(String(args.query)); // item：实词优选（注释见自动召回段同源函数）
							qTokensShared = tokens; // option：外提席内加成用
							// SelRoute 六类分类（观测版）：只计数+透出·零权重零排序变更——加权链候观测期满分布settled再pending approval
							queryClass = classifyQuery(args.query);
							queryClassDist[queryClass] += 1;
							const match = queryMatch(tokens);
							if (match) {
								const ftsBase = `SELECT m.*, bm25(memories_fts) AS rank FROM memories_fts JOIN memories m ON m.id = memories_fts.rowid
                 WHERE memories_fts MATCH ? AND m.status IN ('active','merged','done')`; // step：候选池放宽纳 merged/done——语义性死亡/办结历史可回查（加权段 ×0.1 沉底/仅标·aged 仍硬排除）
								let ftsRows;
								if (callerSpace) {
									// 段一：硬过滤this space+global（LIMIT 100）
									const seg1 = db
										.prepare(
											ftsBase +
												` AND m.space IN (?, 'global') ORDER BY rank LIMIT 100`,
										)
										.all(match, callerSpace);
									if (seg1.length >= Math.max(limit * 2, 8)) {
										ftsRows = seg1;
										recallMode = "segmented-" + callerSpace;
									} else {
										ftsRows = db
											.prepare(ftsBase + ` ORDER BY rank LIMIT 200`)
											.all(match); // 段二：回退全库
									}
								} else {
									ftsRows = db
										.prepare(ftsBase + ` ORDER BY rank LIMIT 200`)
										.all(match);
								}
								// RRF 融合接口：vecRecall 通道预留（未接源=空数组，单路直通保持旧行为）
								vecIds = await vecRecallCore(
									db,
									credentials,
									String(args.query),
									50,
								); // 1a' 向量通道（RRF 第二路·故障时空通道降级·作用域提升：const→let 外提）
								let rrfBoost = null;
								if (vecIds.length > 0) {
									rrfBoost = new Map();
									const K = 2; // 2026-08-31 item1：k=60 vec 消音翻案——扫描实证 k=2 总召回 22.2%→80.6%（Qdrant 08-22 经验律：单真相关场景 k=2~5）；spoken 层对 k 免疫=词汇鸿沟须粒度/前缀/底座治
									ftsRows
										.slice(0, 50)
										.forEach((r, i) => rrfBoost.set(r.id, 1 / (K + i + 1)));
									vecIds.forEach((id, i) =>
										rrfBoost.set(id, (rrfBoost.get(id) || 0) + 1 / (K + i + 1)),
									);
								}
								rows = ftsRows.map((r) => {
									const rr = -Number(r.rank || 0); // bm25 负值·越负越相关 → rr 越大越优
									const ftsBase = Math.max(0, rr / (1 + rr)); // → (0,1) 单调升
									const vecPart =
										rrfBoost && rrfBoost.has(r.id)
											? rrfBoost.get(r.id) * 10
											: 0; // RRF 分映射（多通道时）
									const base = Math.max(ftsBase, vecPart);
									return { row: r, base, ftsBase, vecPart }; // step：双分拆存（吸收·成分不再被 max 吞）——vecPart=0 且 ftsBase 高=纯词面命中（巧合风险可判读·⑧弃权案素材）
								});
								// vec 独有命中（FTS 未中但语义近）注入候选：RRF 分×10×0.5 折扣——
								// 全量评测实证（75.8%<92.5%）：语义近似行不打折会顶掉跨域正确答案（加权链竞争失衡）
								if (rrfBoost) {
									const inFts = new Set(ftsRows.map((r) => r.id));
									const ph = db.prepare("SELECT * FROM memories WHERE id = ?");
									for (const id of vecIds) {
										if (inFts.has(id)) continue;
										const row = ph.get(id);
										if (
											row &&
											(row.status === "active" ||
												row.status === "merged" ||
												row.status === "done")
										) {
											// step：vec 独有行同放宽（与 FTS 候选池一致）
											const vb = (rrfBoost.get(id) || 0) * 5;
											rows.push({ row, base: vb, ftsBase: 0, vecPart: vb });
										} // step：vec 独有命中=纯语义列
									}
								}
								// ── 融合面option（design-approved）：前缀命中保底注入——spoken_prefix 与查询 bigram 字面交叠 ──
								//    过双锚门（交叠词≥2 且含 ≥1 非高频词）且未在候选 → base=injectBase 固定分注入（ftsBase 头部级·低于
								//    vec 排1 的 3.33）·帽 injectCap 条防挤正席。治「FTS 榜尾→融合 top5」晋席力（ 剪刀差案）。
								//    回退开关 LEGION_SPOKEN_BOOST_OFF（演练开关族）；参数面 SPOKEN_BOOST_* env 族可扫（沙箱标定用）。
								if (!process.env.LEGION_SPOKEN_BOOST_OFF) {
									try {
										const inRows = new Set(rows.map((r) => r.row.id));
										const spCands = db
											.prepare(
												"SELECT * FROM memories WHERE status IN ('active','merged','done') AND spoken_prefix IS NOT NULL AND spoken_prefix != ''", // step：spoken 路候选池同放宽
											)
											.all();
										const scored = [];
										for (const row of spCands) {
											if (inRows.has(row.id)) continue;
											const ov = spokenOverlap(row.spoken_prefix, tokens);
											if (ov > 0) scored.push({ row, ov });
										}
										scored.sort((a, b) => b.ov - a.ov);
										for (const s of scored.slice(0, SPOKEN_BOOST.injectCap)) {
											rows.push({
												row: s.row,
												base: SPOKEN_BOOST.injectBase,
												ftsBase: 0,
												vecPart: 0,
												spokenBoost: s.ov,
											}); // 成分标=step双分拆同法可判读
											inRows.add(s.row.id);
										}
									} catch {
										/* 前缀面故障不拖主路 */
									}
								}
								viaFts = true;
							}
						} catch (error) {
							rows = null; // FTS 异常 → 兜底旧扫描
						}
						if (rows === null) {
							const words = qTokens(String(args.query)); // 兜底也走分词器（CJK 整句不空转）·item 实词优选同源
							qTokensShared = words; // option：兜底路同外提（席内加成不因 FTS 异常缺席）
							const scan = db
								.prepare(
									"SELECT * FROM memories WHERE status IN ('active','merged','done') ORDER BY id DESC LIMIT 500", // step：scan 兜底路候选池同放宽（FTS 降级时行为一致）
								)
								.all();
							rows = [];
							for (const row of scan) {
								const hay = (row.title + " " + row.content).toLowerCase();
								let score = 0;
								for (const w of words)
									if (hay.includes(w.toLowerCase())) score += 1;
								if (score > 0) rows.push({ row, base: score / words.length });
							}
						}
						// ── option·线上 rerank 精排应用（design-approved「按推荐」·论证稿落地路径第 2 步）──
						//    挂点=融合候选封盘后/加权链前：候选帽 RERANK_CAND 按 base 取头 → rerankDocs 精排 →
						//    topN 以 relevance_score 为 base 独尺续链（未入列弃置·防量纲混排）；故障/无 key →
						//    rows 不动原链直通。仅 viaFts 融合路挂（scan 兜底路=FTS 异常态不叠外部依赖）。
						if (!process.env.LEGION_RERANK_OFF && viaFts && rows.length > 1) {
							try {
								const cand = [...rows]
									.sort((a, b) => b.base - a.base)
									.slice(0, RERANK_CAND);
								const scoreMap = await rerankDocs(
									credentials,
									String(args.query),
									cand.map((c) => ({
										id: c.row.id,
										text: (
											String(c.row.title || "") +
											"\n" +
											String(c.row.content || "")
										).slice(0, 1200), // 文档 30K token 硬限·1200 字保守截（正文 1-3 句规范内远足）
									})),
								);
								if (scoreMap) {
									const kept = [];
									for (const c of cand) {
										const s = scoreMap.get(c.row.id);
										if (s === undefined) continue;
										kept.push({
											...c,
											base: s,
											rerank: Math.round(s * 1000) / 1000,
										});
									}
									kept.sort((a, b) => b.base - a.base);
									rows = kept.slice(0, RERANK_TOPN);
								} // null=故障直通（rows 不动）
							} catch {
								/* rerank 段异常不拖主路（原链直通） */
							}
						}
						// ── step：时序衰减——三因子乘性融合 final = bm25归一 × 空间权重 × decay ──
						//    14 天半衰 0.5^(days/14)；decision/fact 衰减减半（28 天半衰·maintainer与机制事实是长期资产）；
						//    todo 不衰减（现行待办永在顶）；decay 下限 0.05（旧记忆沉底不归零——遗忘是排序不是删除）。
						const decayOf = (row) => {
							if (row.type === "todo") return 1.0;
							if (row.essential === 1) return 1.0; // 常驻核心（step·ORIGIN/CORE 意）：亲判核心不衰——mirror 同款恒 1.0
							if (String(row.source || "").startsWith("mirror:")) return 1.0; // step（18:41 maintainer）：进化史镜像=长期资产不衰减——旧而不过气（与 decision 56d 同精神·更彻底：进化史是space人格生长记录）
							// P1 事件钟锚（2026-08-26）：decay 按 event_at（事件真实时点）算，缺省回落 ts——补记条目不再凭空折损半衰
							const anchor = row.event_at || row.ts;
							const t = Date.parse(String(anchor).replace(" ", "T"));
							if (isNaN(t)) return 1.0;
							const days = Math.max(0, (Date.now() - t) / 86400000);
							// P3 space分档（design-approved·step wave）：xhs/fetcher fact 7d（对齐xhs TTL S 级·沉底≠消失下限 0.05+ttl 哨正门对冲）；
							// brain/maintain decision 56d（maintainer与机制决策是长期资产）；其余 fact/decision 28d·lesson 14d 照旧；
							// LEGION_DECAY_LEGACY=1 切回统一档（fair-ab A/B 对照开关·与 LEGION_RELEVANCY_OFF 同构）
							let half;
							if (process.env.LEGION_DECAY_LEGACY) {
								half = row.type === "decision" || row.type === "fact" ? 28 : 14;
							} else if (
								row.type === "fact" &&
								(row.space === "xhs" || row.space === "fetcher")
							)
								half = 7;
							else if (
								row.type === "decision" &&
								(row.space === "brain" || row.space === "maintain")
							)
								half = 56;
							else
								half = row.type === "decision" || row.type === "fact" ? 28 : 14;
							// item FadeMem 重要性调制半衰（design-approved A 级三件·FadeMem λ_n=λ_base·exp(-μ·I_n) 意）：
							// 验证越多忘得越慢——validated_count≥2（真验证·列 DEFAULT 1=未验证）半衰延长
							// half_eff=half×(1+0.35·ln(vc))·帽 2.0（vc=2→×1.24·vc=4→×1.48·vc≥8→×2.0）；
							// vc≤1 →ln(1)=0 零影响（默认条目不动·基线稳）；LEGION_FADEMEM_OFF 回退（开关族同构）
							if (!process.env.LEGION_FADEMEM_OFF) {
								const vcF = Number(row.validated_count) || 1;
								if (vcF > 1)
									half = half * Math.min(2.0, 1 + 0.35 * Math.log(vcF));
							}
							const base = Math.max(0.05, 0.5 ** (days / half));
							// ── 3b 对冲一档（阶段三）：relevancy 乘法（0.5~1.5·NULL=1.0 中性）——排序层一档，零重写 ──
							if (process.env.LEGION_RELEVANCY_OFF) return base; // 对冲开关（3b 验证环 A/B 专用）
							const rel =
								row.relevancy === null ||
								row.relevancy === undefined ||
								isNaN(Number(row.relevancy))
									? 1.0
									: Number(row.relevancy);
							return base * Math.min(1.5, Math.max(0.5, rel));
						};
						const weighted = rows.map((r) => {
							const dec = decayOf(r.row);
							let w = r.base * spaceWeight(r.row.space, callerSpace) * dec;
							// ── option（design-approved）：席内前缀交叠小加成（帽 seatBonusCap·加权后加·防爆）——
							//    已在候选且有前缀交叠的行浮一点治席内排序；保底注入行（spokenBoost 已含加成）不重复叠加。
							if (
								!process.env.LEGION_SPOKEN_BOOST_OFF &&
								qTokensShared &&
								!r.spokenBoost
							) {
								const ov = spokenOverlap(r.row.spoken_prefix, qTokensShared);
								if (ov > 0) w += Math.min(SPOKEN_BOOST.seatBonusCap, ov * 0.1);
							}
							return { ...r, decay: Math.round(dec * 1000) / 1000, w };
						});
						// ── 意图条件权重（wave·17:45 approved·Cortex query_router 意融入）：时间类 query
						//    抬时序信号——论文实证 temporal 类受益最大；我方「上次/最近/之前」类 query 高频。
						//    实现=temporal 意图时对非 todo 行的 decay 再开方（半衰折半·旧条沉更快·新条浮）——
						//    非 temporal 零改动零回归；识别词面与 PANZ/轴提示同源（不新造轮子）。
						//    SelRoute：词表改引 SELROUTE_TEMPORAL_RE 单源（六类 temporal 类同词面）——防双份词表漂移
						const isTemporalQ = SELROUTE_TEMPORAL_RE.test(
							String(args.query || ""),
						);
						if (isTemporalQ) {
							for (const r of weighted) {
								if (r.row.type === "todo") continue; // todo 不衰减恒 1.0（现行待办不动）
								r.w = r.w * (r.decay === undefined ? 1 : r.decay); // decay 平方化：旧条（低 dec）沉更快·新条相对浮
							}
						}
						// ── 四闸·retrieval gate（2026-08-22）：active todo 与近期闭环面同题 → w×0.2 沉底 + stale 标（僵尸压过新 fact 的病灶）──
						let staleMarked = 0,
							staleValidTo = 0; // P0修（09-03 audit）：valid_to 失效独立计数（不混入 staleTodos）
						// 双时态（step）：valid_to 已到点条目沉底×0.2（事件轴失效≠删除·与 P2 过时≠删除同精神）——Map 一次拉全量（现库稀疏·零成本）
						const validToMap = new Map();
						try {
							for (const v of db
								.prepare(
									"SELECT id, valid_to FROM memories WHERE valid_to IS NOT NULL AND status='active' AND valid_to <= ?",
								)
								.all(nowIso()))
								validToMap.set(v.id, v.valid_to);
						} catch {}
						for (const r of weighted) {
							const vt9 = validToMap.get(r.row.id);
							if (vt9) {
								r.w *= 0.2;
								r.stale = "⏦" + String(vt9).slice(0, 10);
								r.staleKind = "valid-to"; // P0修（09-03 audit）：透出层区分——原无 kind 被透出二分错标「已闭环未销账」
								staleValidTo += 1; // 独立计数·不混入 staleTodos
								continue;
							} // 已失效沉底·标 ⏦（valid_to 前缀·与 #closedBy/#newerFact 区分）
							// ── step（09-08 maintainer·召回状态化）：召回面消费状态列（洞①·改判③系数分档）──
							//    superseded_by 非空（merged·语义性死亡）→ ×0.1 沉底+supersededBy 字段——比 valid_to ×0.2 更狠（被取代是比时间旧更强的信号）·防同内容双召回·保留回查可见性；
							//    status='done'（todo 办结历史）→ 仅标不降权——语义不同质：办结回查正当（注入面/autoRecall 已滤 active·零干扰）。
							if (r.row.superseded_by != null) {
								r.w *= 0.1;
								r.supersededBy = r.row.superseded_by;
								continue;
							}
							if (r.row.status === "done")
								r.doneClosed = String(r.row.closed_at || "").slice(0, 10);
							if (r.row.type === "todo" && r.row.status === "active") {
								const cc = closureCheck(
									db,
									r.row.title,
									r.row.content,
									r.row.id,
								); // step：边判定前置（P1 事件钟锚随②退役·C案·同日判断影响面已消）
								if (cc.closed) {
									r.w *= 0.2;
									r.stale = "#" + cc.by;
									staleMarked += 1;
								}
							}
						}
						// ── Q3 延伸（2026-08-22 补件task brief·轻量版）：todo 命中存在更晚同题 fact → 标注「已有较新闭环见 #id」──
						//    与 stale 标互补：stale 查 72h 闭环面（含 done/销账类）；本条查「更晚的 fact」——防旧 todo 盖过新事实（审计环 2 变体）。
						for (const r of weighted) {
							if (
								r.row.type === "todo" &&
								r.row.status === "active" &&
								!r.stale
							) {
								try {
									const newer = db
										.prepare(
											"SELECT id, title, ts FROM memories WHERE status = 'active' AND type IN ('fact','decision','lesson') AND id > ? ORDER BY id DESC LIMIT 40",
										)
										.all(r.row.id);
									const kws = gateKeywords(
										r.row.title + " " + (r.row.content || ""),
										12,
									);
									for (const f of newer) {
										const hay = (
											String(f.title || "") +
											" " +
											String(f.content || "")
										).toLowerCase(); // 顺手件（8-26 修法包·wave §六）：原拼接残留空串——content 侧证据恒空
										let hits = 0;
										for (const k of kws) if (hay.includes(k)) hits += 1;
										if (hits >= 3) {
											r.stale = "#" + f.id;
											r.staleKind = "newer-fact";
											r.w *= 0.2;
											staleMarked += 1;
											break;
										}
									}
								} catch {}
							}
						}
						// ── step：共现伙伴召回加成——「想起这个必须想起那个」──
						//    命中集中已存在的伙伴：+0.1 排序加成并标注；未命中的伙伴：以 0.1×权重×衰减 注入（上限 5 条防灌水）
						let cooccurBoosted = 0;
						let cooccurInjected = 0;
						try {
							const hitIds = new Set(weighted.map((r) => r.row.id));
							// ── vec 种子进 PPR（wave·16:47 approved）：RRF 向量路命中并入种子集——
							//    治「半边图」（原只 FTS 命中进图·语义近邻不传播）。种子帽仍 20。
							const pprSeeds = [...hitIds];
							for (const vid of vecIds) {
								if (!hitIds.has(vid)) pprSeeds.push(vid);
							}
							const pprMap = pprScores(
								pprSeeds.slice(0, 20),
								tokenize(String(args.query || "")),
							); // +种子=FTS∪vec 命中·teleport 回种子——查询相关
							const partnerOf = new Map();
							for (const p of db
								.prepare("SELECT id_a, id_b FROM memories_cooccur")
								.all()) {
								if (!hitIds.has(p.id_a) && !hitIds.has(p.id_b)) continue;
								if (!partnerOf.has(p.id_a)) partnerOf.set(p.id_a, new Set());
								if (!partnerOf.has(p.id_b)) partnerOf.set(p.id_b, new Set());
								partnerOf.get(p.id_a).add(p.id_b);
								partnerOf.get(p.id_b).add(p.id_a);
							}
							for (const r of weighted) {
								const ps = partnerOf.get(r.row.id);
								if (!ps) continue;
								const inSet = [...ps].filter((x) => hitIds.has(x));
								if (inSet.length > 0) {
									const pp = pprMap ? pprMap.get(r.row.id) || 0 : 0;
									r.w += Math.min(0.3, 0.1 + 0.2 * pp); // 原静态 +0.1 → PPR 查询相关（近种子伙伴高分·远伙伴保底 0.1）
									r.ppr = Math.round(pp * 1000) / 1000;
									r.cooccur = inSet;
									cooccurBoosted += 1;
								}
							}
							const missingPartners = new Map();
							const allCandidateIds = new Set(weighted.map((r) => r.row.id)); // OC-1（08-32 批·脑 08:05）：missing 判定并入加权全集（FTS+vec 独有行）去重——原只对 FTS hitIds 致 vec 行可被重复注入一行
							for (const [, ps] of partnerOf) {
								for (const pid of ps) {
									if (!hitIds.has(pid) && !allCandidateIds.has(pid))
										missingPartners.set(pid, true);
								}
							}
							if (missingPartners.size > 0 && weighted.length > 0) {
								const idList = [...missingPartners.keys()].slice(0, 5);
								const ph = db.prepare("SELECT * FROM memories WHERE id = ?");
								for (const pid of idList) {
									const row = ph.get(pid);
									if (!row || row.status !== "active") continue; // 三案修补案3（09-09 approved）：enrich 注入面保持 active-only——范围决策非漏改（注入行在此后进入·不过主路 sink·同放宽须自带沉底·故不放宽）
									const pp = pprMap ? pprMap.get(pid) || 0 : 0;
									if (pprMap && pp <= 0.02) continue; // 远伙伴不注入（治灌水·graph-memory 高精门同精神）；无图回落旧平权
									const dec = decayOf(row);
									weighted.push({
										row,
										base: 0,
										decay: Math.round(dec * 1000) / 1000,
										w:
											(pprMap ? 0.08 + 0.12 * pp : 0.1) *
											spaceWeight(row.space, callerSpace) *
											dec,
										cooccur: ["partner"],
										...(pprMap ? { ppr: Math.round(pp * 1000) / 1000 } : {}),
									});
									cooccurInjected += 1;
								}
							}
						} catch {
							/* 共现表未就绪（首夜前）→ 跳过加成，不影响主路 */
						}

						// ── 阶段二第二步 b(design note)：跨卷实体多跳——「想起这个必须想起那个」的跨卷版 ──
						//    挂两段式第二段（终版必修③）；kind=NULL 实体不接入（防噪罩门）；
						//    实战收紧（stepb-drill 07:22 实证：15 条注入灌水挤掉正解 73.3%<85%）——
						//    ①全局注入上限 3 条 ②仅在命中集 <limit 时补位（伙伴/cooccur 优先，实体只补缺）③独立开关回退。
						let entityInjected = 0;
						const ENTITIES_RECALL = !process.env.LEGION_ENTITIES_OFF;
						if (ENTITIES_RECALL) {
							try {
								// 定型 v2（07:30 两轮实证修订）：与 cooccur 伙伴注入同构——独立小配额（3 条）低权 0.1 档注入。
								// 「缺时补」版在两段式下永不触发（段一≥8 恒成立）——实体跨卷价值零出口；
								// 同构对齐后挤位风险与伙伴注入同级（已被接受为设计特性·fair-ab 同库铁证零退化）。
								const shortfall = Math.min(3, Math.max(1, limit - 2)); // 独立配额 3（顺手件 8-26 修法包：清死变量 cur——shortfall 直算）
								{
									// 8-31 锈面修（审计 P0-11·实体 hop ph3 参数漂移）：命中集固定为初始 baseIds——原 NOT IN 占位符
									// 按 prepare 时 weighted.length 冻结，而 L2721 循环内 push 增长 weighted → 次实体调用实参数>占位符数
									// RangeError 被 catch 吞=注入静默截断（首实体正常·次实体必死）。+injectedHopIds 防同条多轮重注。
									const baseIds = weighted.map((r) => r.row.id);
									const injectedHopIds = new Set();
									const entRows = db
										.prepare(`SELECT DISTINCT e.entity_id FROM memories_entities me
                  JOIN entities e ON e.entity_id = me.entity_id
                  WHERE me.memory_id IN (${baseIds.map(() => "?").join(",") || "NULL"}) AND e.kind IS NOT NULL`)
										.all(...baseIds);
									const ph3 =
										db.prepare(`SELECT m.* FROM memories m JOIN memories_entities me ON me.memory_id = m.id
                  WHERE me.entity_id = ? AND m.status = 'active' AND m.id NOT IN (${baseIds.map(() => "?").join(",") || "NULL"})
                  ORDER BY m.id DESC LIMIT ?`); // 三案修补案3：实体 hop 注入面保持 active-only（范围决策非漏改·同 cooccur 伙伴面理由）
									for (const er of entRows) {
										if (entityInjected >= Math.min(shortfall, 3)) break;
										for (const row of ph3.all(
											er.entity_id,
											...baseIds,
											Math.min(shortfall, 3) - entityInjected,
										)) {
											if (injectedHopIds.has(row.id)) continue;
											injectedHopIds.add(row.id);
											const dec = decayOf(row);
											weighted.push({
												row,
												base: 0,
												decay: Math.round(dec * 1000) / 1000,
												w: 0.1 * spaceWeight(row.space, callerSpace) * dec,
												entityHop: true,
											});
											entityInjected += 1;
										}
									}
								}
							} catch {
								/* 实体表未就绪 → 跳过 */
							}
						}
						weighted.sort((a, b) => b.w - a.w);
						// ── 泛化召回路（wave·graph-memory recallGeneralized 意融入·插在 sort 后注入不参与排序）：精确路命中不足
						//    （<max(limit,4)）时社区兜底——命中条所在社区其它成员低权注入（0.08 档·配额 3·is_rep 优先）；
						//    社区表=nightly patrol建（首夜前无表静默跳过）；独立开关 LEGION_GENERALIZED_OFF。
						let communityInjected = 0;
						let sagaHint = ""; // 件5 面2：社区叙事脉络行（泛化+聚齐两路共用）
						if (
							!process.env.LEGION_GENERALIZED_OFF &&
							weighted.length > 0 &&
							weighted.length < Math.max(limit, 4)
						) {
							try {
								const comIds = db
									.prepare(
										`SELECT DISTINCT community FROM memory_communities WHERE mem_id IN (${weighted.map(() => "?").join(",")})`,
									)
									.all(...weighted.map((r) => r.row.id));
								if (comIds.length > 0) {
									const cl = comIds.map((c) => c.community);
									// 审计 D5 修正（15:04）：原 cands 无 space 过滤——社区跨空间成员可注入任意 caller 窗（违背 v2.4
									// caller-first「零other spaces噪音」立法·噪音泵复发面）。修：泛化注入仅限 caller 空间+global 成员
									// （与两段式段一同构）；他空间成员留在社区内不注入——cross-space动态只经 global 干净管道。
									const spaceFilter = callerSpace
										? ` AND m.space IN (?, 'global')`
										: "";
									const cands = db
										.prepare(`SELECT m.* FROM memory_communities mc JOIN memories m ON m.id = mc.mem_id
                  WHERE mc.community IN (${cl.map(() => "?").join(",")}) AND m.status='active'
                  AND m.id NOT IN (${weighted.map(() => "?").join(",")})${spaceFilter}
                  ORDER BY mc.is_rep DESC, m.id DESC LIMIT 3`) // 三案修补案3：社区泛化注入面保持 active-only（范围决策非漏改·同 cooccur 伙伴面理由）
										.all(
											...cl,
											...weighted.map((r) => r.row.id),
											...(callerSpace ? [callerSpace] : []),
										); // 8-31 移植族修复⑤：绑定序对齐占位符序（community IN→id NOT IN→spaceFilter）——原 callerSpace 插在 weighted ids 前=数量相等静默错配·主线恒空（审计 :memory: 实测复刻）
									for (const row of cands) {
										const dec = decayOf(row);
										weighted.push({
											row,
											base: 0,
											decay: Math.round(dec * 1000) / 1000,
											w: 0.08 * spaceWeight(row.space, callerSpace) * dec,
											communityPath: true,
										});
										communityInjected += 1;
									}
									weighted.sort((a, b) => b.w - a.w); // 审计 D6 修正（15:04）：注入后重排——原 sort 后 append 永垫底，命中近满时泛化条必被挤掉（注入失效面）
									// ── 件5 面2 Saga 摘要消费（升格·2026-09-07）：泛化注入附社区叙事脉络（Graphiti Saga 意——散条→主题脉络）──
									try {
										const smRaw = db
											.prepare(
												"SELECT v FROM organ_meta WHERE k = 'community_summaries'",
											)
											.get();
										const sm = smRaw ? JSON.parse(smRaw.v) : {};
										const hitSm = comIds
											.map((c) => sm[c.community])
											.find(Boolean);
										if (hitSm)
											sagaHint =
												"同主题脉络：" +
												hitSm.rep +
												"｜社区 " +
												hitSm.n +
												" 条（泛化注入自该簇）";
									} catch {}
								}
							} catch {
								/* 社区表未就绪（首夜前）→ 静默 */
							}
						}
						// ── 件5 面2b Saga 聚齐亮脉络（2026-09-07）：正常命中 top5 内 ≥2 条同社区且该社区≥5 条 → sagaHint 主题脉络（纯透出零权重）──
						if (!sagaHint) {
							try {
								const smRaw2 = db
									.prepare(
										"SELECT v FROM organ_meta WHERE k = 'community_summaries'",
									)
									.get();
								if (smRaw2) {
									const sm2 = JSON.parse(smRaw2.v);
									const top3 = weighted.slice(0, 5).map((x) => x.row.id); // 件5 修①：top3→top5 面（单 query 稳定召回同社区两条入 top3 概率天然低·debug 实测settled）
									const rows3 = db
										.prepare(
											`SELECT community FROM memory_communities WHERE mem_id IN (${top3.map(() => "?").join(",")})`,
										)
										.all(...top3);
									const cnt = {};
									for (const r of rows3)
										cnt[r.community] = (cnt[r.community] || 0) + 1;
									const hit = Object.entries(cnt).find(
										([c, k]) => k >= 2 && sm2[c],
									);
									if (hit)
										sagaHint =
											"同主题脉络：" +
											sm2[hit[0]].rep +
											"｜社区 " +
											sm2[hit[0]].n +
											" 条（命中聚齐）";
								}
							} catch {}
						}
						// ── 全低于阈值 → top1 + filtered-low-relevance（防假阴性）──
						const allLow =
							weighted.length > 0 &&
							weighted.every((x) => x.w < LOW_RELEVANCE_THRESHOLD);
						const picked = allLow
							? weighted.slice(0, 1)
							: weighted.slice(0, limit);
						// ── 3b 记录点（阶段三·前置盘点定案）：picked 命中进进程 Map 差分攒批（零阻塞零写库——nightly patrol统一消费）──
						//    P2（2026-08-26 step wave）：stale_state='review'/'retired' 条目不进命中 Map——过时条不回弹 relevancy（拉锯治本）
						try {
							if (!globalThis.__legionHitMap)
								globalThis.__legionHitMap = new Map();
							for (const s of picked)
								if (!s.row.stale_state)
									globalThis.__legionHitMap.set(
										s.row.id,
										(globalThis.__legionHitMap.get(s.row.id) || 0) + 1,
									);
						} catch {}
						// ── P5 检索提示级闸（2026-08-26 step wave·先提示后拦截·观察一周误伤率回投brain后定拦截级）──
						//   A 命中面：picked 中 fact/decision 的 eff_ts 超 30d 线 ≥2 条且过半 → axisHint 提示引用前 timeline 核对（与 P2 review 口径对齐·lesson/todo 不算）；
						//   B 盲区面（P2 发现补位：decay 沉底使超龄条检索不可见）：超龄相关条在 weighted 有而 picked 无（被挤出）≥2 → axisHint 提示沉底条数；
						//   只提示不拦不压 w——零排序影响。
						let axisHint = "";
						try {
							const d30 = new Date(
								Date.now() + 8 * 3600 * 1000 - 30 * 86400000,
							);
							const p2 = (n) => String(n).padStart(2, "0");
							const past30Line = `${d30.getUTCFullYear()}-${p2(d30.getUTCMonth() + 1)}-${p2(d30.getUTCDate())}T${p2(d30.getUTCHours())}:${p2(d30.getUTCMinutes())}+08:00`;
							const isAged = (x) =>
								(x.row.type === "fact" || x.row.type === "decision") &&
								String(x.row.event_at || x.row.ts) < past30Line;
							const pickedOld = picked.filter(isAged).length;
							if (
								picked.length > 0 &&
								pickedOld >= 2 &&
								pickedOld * 2 >= picked.length
							) {
								axisHint = `本批 ${pickedOld}/${picked.length} 条为 >30d 旧态条目，pending ruling/引用前建议 timeline 核对`;
							} else {
								const inIds = new Set(picked.map((x) => x.row.id));
								const sunkOld = weighted.filter(
									(x) => !inIds.has(x.row.id) && isAged(x),
								).length;
								if (sunkOld >= 2)
									axisHint = `另有 ${sunkOld} 条 >30d 旧态相关条沉底未展示（decay 压制），需要时 limit↑或 timeline 查`;
							}
						} catch {}
						return {
							hits: picked.map((s) => ({
								id: s.row.id,
								ts: s.row.ts,
								type: s.row.type,
								title: s.row.title,
								confidence:
									s.row.confidence !== undefined ? s.row.confidence : 1, // ：信任分透出（外部源 0.5·格式化器外显 ⚠外部源）
								space: s.row.space,
								score: Math.round(s.w * 1000) / 1000,
								base: Math.round(s.base * 1000) / 1000,
								ftsBase:
									Math.round(
										(s.ftsBase !== undefined ? s.ftsBase : s.base) * 1000,
									) / 1000,
								vecPart: Math.round((s.vecPart || 0) * 1000) / 1000, // step：双分直出（吸收·08:57 批）——scan 兜底路无拆分归词面列
								...(s.rerank !== undefined ? { rerank: s.rerank } : {}), // option：rerank 精排分透出（判读位·双分拆同族）
								...(s.decay !== undefined ? { decay: s.decay } : {}),
								...(s.row.stale_state
									? {
											staleMark: `🕐${String(s.row.stale_at || "").slice(5, 10)} 记·${s.row.stale_state === "review" ? "待复核" : s.row.stale_state}`,
										}
									: {}), // P2：过时条带 staleMark 头标（审计补②：独立字段名——旧 stale 字段归retrieval gate专用，防同名覆盖）
								...(s.cooccur ? { cooccur: s.cooccur } : {}),
								...(s.ppr !== undefined ? { ppr: s.ppr } : {}), // 查询相关图谱分透出（counter）
								...(/^auto:session-[0-9a-f-]+$/.test(String(s.row.source || ""))
									? {
											episodic:
												"原文可回流：memory action=read_episodic id=" +
												s.row.id,
										}
									: {}), // B级4 情境锚刀（design-approved「开」·AnchorMem 事实满射回 immutable context 意）：auto:session-* 条=蒸馏事实带原始情境指针——检索面透出回流指引·模型见 hint 知证据可升级原文（read_episodic 自带频控 4 轮/窗+charCap）
								...(String(s.row.source || "") === "dreamer"
									? {
											clusterHint: "簇索引条·成员可直达",
											clusterMembers: [
												...new Set(
													(
														String(s.row.content || "").match(/#\d+/g) || []
													).map((x) => Number(x.slice(1))),
												),
											].slice(0, 10), // B级6 粗到细刀（design-approved「继续」·dreamer 簇升格入检索路）：粗层=泛词命中簇条天然定位·细层=clusterMembers 结构化直达（源条 id 一跳）——纯透出零排序（件5/情境锚同构系列）
										}
									: {}),
								...(s.communityPath ? { communityPath: true } : {}), // 社区泛化注入标记
								...(s.entityHop ? { entityHop: true } : {}),
								...(s.stale
									? {
											stale:
												(s.staleKind === "newer-fact"
													? "已有较新事实条"
													: s.staleKind === "valid-to"
														? "已到点失效（valid_to）"
														: "已闭环未销账") +
												"（见 " +
												s.stale +
												"）·检索降权",
										}
									: {}),
								...(s.supersededBy !== undefined
									? { supersededBy: s.supersededBy }
									: {}), // step：被取代标（数据层字段· render 必消费）
								...(s.doneClosed !== undefined
									? {
											doneMark:
												"办结" + (s.doneClosed ? " " + s.doneClosed : ""),
										}
									: {}), // step：办结仅标（不降权·回查正当）
								content: String(s.row.content).slice(0, 200),
							})),
							total: weighted.length,
							via: viaFts ? "fts" : "scan",
							recallMode,
							callerSpace: callerSpace || "unknown",
							queryClass, // SelRoute：六类分类结果透出（观测·render/评测可读）
							...(axisHint ? { axisHint } : {}),
							...(sagaHint ? { sagaHint } : {}), // 件5 面2：Saga 社区叙事脉络（泛化/聚齐两路）
							...(cooccurBoosted + cooccurInjected > 0
								? {
										cooccur: {
											boosted: cooccurBoosted,
											injected: cooccurInjected,
										},
									}
								: {}),
							...(staleMarked > 0 ? { staleTodos: staleMarked } : {}),
							...(typeof staleValidTo !== "undefined" && staleValidTo > 0
								? { staleValidTo }
								: {}),
							...(entityInjected > 0
								? { entityHop: { injected: entityInjected } }
								: {}),
							...(allLow
								? {
										note:
											"filtered-low-relevance：全部加权分低于 " +
											LOW_RELEVANCE_THRESHOLD +
											"，仅返 top1 防漏",
									}
								: {}),
							// ── 显式弃权提示（wave·Cortex abstain 意融入）：命中不足且 top1 低置信 → 「库中无强相关·勿强答勿拼凑」。
							//    治幻觉引用（论文实证：答案不在库时平均相关分 0.926 反而更高——分数阈值不能判弃权·须显式提示模型）。
							...(picked.length === 0 || (picked[0].w < 0.15 && !allLow)
								? {
										abstainHint:
											"⚠ 库中无强相关条目——勿强答勿拼凑引用；可换关键词重查或 timeline 浏览",
									}
								: {}),
							// ── 补件 1：盘账姿势机器闸（2026-08-22 补件task brief·治审计环 1/4·Q1 折中=触发词+兜底不恒挂）──
							//    query 命中盘账类词，或 0 命中/全 todo 命中 → 附近 24h timeline 速览 5 条（Q2=24h 滚动窗）。
							...(() => {
								const q = String(args.query || "");
								const PANZ =
									/还有哪些|没做|待办|剩什么|剩几|进度|到哪了|做到哪|做了什么|盘点|盘账|推理到哪|下一步|未做/;
								const hit = PANZ.test(q);
								const allTodo =
									weighted.length > 0 &&
									weighted.every((x) => x.row.type === "todo");
								if (!hit && !allTodo && weighted.length > 0) return {};
								try {
									const since24h = nowIso24hAgo();
									const tl = db
										.prepare(
											"SELECT id, ts, type, title, space FROM memories WHERE status = 'active' AND ts >= ? ORDER BY id DESC LIMIT 5",
										)
										.all(since24h);
									return {
										panzhangHint: {
											triggered: hit
												? "query 命中盘账词"
												: weighted.length === 0
													? "兜底：0 命中"
													: "兜底：全 todo 命中",
											timeline24h: tl.map((r) => ({
												id: r.id,
												ts: r.ts,
												type: r.type,
												space: r.space,
												title: r.title,
											})),
										},
									};
								} catch {
									return {};
								}
							})(),
						};
					}
					// ── episodic 原文回流小刀(design note)：auto 条目按 source 回流会话原文窗 ──
					//    批注解①：zstd 解压走 python3 桥（系统 3.9 zstandard 实证在位·不引新依赖）；批注解②：stats 计数器 episodicReadCount；
					//    补件④（08:19）：轻频控——每窗（会话）每 4 真用户轮至多 1 次（防当检索器每轮刷）；回退 LEGION_EPISODIC_OFF。
					if (action === "read_episodic") {
						if (process.env.LEGION_EPISODIC_OFF)
							return {
								episodicContext: null,
								note: "episodic 通道已关（LEGION_EPISODIC_OFF）",
							};
						const id = Number(args.id);
						if (!id)
							return {
								error: "read_episodic 需要 id 参数（auto:session-* 条目）",
							};
						const row = db
							.prepare(
								"SELECT id, ts, title, source, space FROM memories WHERE id = ?",
							)
							.get(id);
						if (!row)
							return { episodicContext: null, note: "条目不存在 #" + id };
						const src = String(row.source || "");
						const mSid = src.match(/^auto:(session-[0-9a-f-]+)$/);
						if (!mSid)
							return {
								episodicContext: null,
								note:
									"该条目无会话原文可回流（source=" +
									(src || "空") +
									"——仅 auto:session-* 条目有原文）",
							};
						// 频控：每会话 4 真用户轮 1 次（D2 修正 8-26 逐字审计：fcKey 取 exec 真身 session id——sessionOfLastTurn 是「最后活动会话」·多窗并发会串频控（bindMap 分账同型病 L876 教训））
						const fcKey =
							exec?.agent?.session?.id ||
							exec?.session?.id ||
							sessionOfLastTurn ||
							"_anon";
						const fcIdKey = fcKey + ":" + id; // 8-31 approved：频控按 id 计——同 id 4 轮防重刷（原意图保留）·新 id 放行(design note)
						const bd = bindMap.get(fcKey);
						const nowTurns = (bd && bd.userTurns) || 0;
						if (episodicRate.has(fcIdKey)) {
							const last = episodicRate.get(fcIdKey);
							if (nowTurns - last.turns < 4) {
								stats.episodicThrottled = (stats.episodicThrottled || 0) + 1;
								return {
									episodicContext: null,
									note:
										"本窗已取过本条原文（" +
										(4 - (nowTurns - last.turns)) +
										" 轮后再可取）——请先消化或转记，勿当检索器每轮刷（8-31 起按 id 计：换 id 不受此限）",
								};
							}
						}
						const sessWorkspace = (() => {
							// session-id → 工作区目录名（sessionsDirMap 只映 space·此处需原目录）
							try {
								const base = path.join(os.homedir(), ".dsh", "sessions");
								for (const ws of fs2.readdirSync(base)) {
									try {
										if (
											fs2.statSync(path.join(base, ws, mSid[1])).isDirectory()
										)
											return ws;
									} catch {}
								}
							} catch {}
							return null;
						})();
						if (!sessWorkspace)
							return {
								episodicContext: null,
								note:
									"原文会话文件不在当前工作区索引（可能已归档/迁移）·指针：" +
									src,
							};
						const zst = path.join(
							os.homedir(),
							".dsh",
							"sessions",
							sessWorkspace,
							mSid[1],
							"session.jsonl.zstd",
						);
						if (!fs2.existsSync(zst))
							return {
								episodicContext: null,
								note: "原文文件已不在位·指针：" + zst,
							};
						const t0 = Date.now();
						try {
							const anchorMs =
								Date.parse(String(row.ts).replace(" ", "T")) || 0;
							const window = Math.min(
								Math.max(Number(args.window) || 6, 1),
								12,
							);
							const charCap = Math.min(
								Math.max(Number(args.charCap) || 3000, 500),
								6000,
							);
							const py = `import zstandard as zstd, json, sys
data = zstd.ZstdDecompressor().stream_reader(open(sys.argv[1], 'rb')).read().decode('utf-8', errors='replace')
lines = data.split('\\n')
anchor_ms, window, cap, probe = int(sys.argv[2]), int(sys.argv[3]), int(sys.argv[4]), sys.argv[5]
frames = []
for i, l in enumerate(lines):
    if not l.strip(): continue
    try: o = json.loads(l)
    except: continue
    t = o.get('type', '')
    if t == 'user/message':
        blocks = (o.get('data') or {}).get('content') or []
        txt = ' '.join(b.get('text', '') for b in blocks if isinstance(b, dict) and b.get('type') in ('text', 'input_text'))
        if txt.strip():
            s0 = txt.lstrip()
            sysf = s0.startswith('Time sampled while preparing') or s0.startswith('当前运行时上下文。此快照取代') or s0.startswith('<system-reminder>') or s0.startswith('<goal_round>') or s0.startswith('<goal_complete>')
            frames.append({'k': 'U', 'ms': o.get('time', 0), 't': txt, 'sys': sysf})
    elif t == 'assistant/chunk':
        c = ((o.get('data') or {}).get('chunk') or {})
        if c.get('type') == 'text': frames.append({'k': 'A', 'ms': o.get('time', 0), 't': c.get('text', '')})
idx = 0
anchor_hit = False
if anchor_ms:
    best, bd2 = 0, None
    for j, f in enumerate(frames):
        d = abs(f['ms'] - anchor_ms)
        if bd2 is None or d < bd2: bd2, best = d, j
    idx = best
    anchor_hit = bd2 is not None and bd2 < 3600 * 1000  # L2（08-32 批）：ts 锚距最近帧 <1h 才算真命中
else:
    for j, f in enumerate(frames):
        if probe and probe in f['t']: idx = j; anchor_hit = True; break
lo = max(0, idx - window); hi = min(len(frames), idx + window + 1)
out, used, sysk = [], 0, 0
for f in frames[lo:hi]:
    if f.get('sys'): sysk += 1; continue
    piece = ('【' + f['k'] + '】' + f['t'])
    if used + len(piece) > cap: piece = piece[:max(0, cap - used)]
    out.append(piece); used += len(piece)
    if used >= cap: break
head = ('（已折叠 ' + str(sysk) + ' 条系统帧：时序/上下文快照/轮注——step降密）\\n') if sysk else ''
print(json.dumps({'frames': len(frames), 'window': [lo, hi], 'anchorHit': anchor_hit, 'sysFolded': sysk, 'text': head + '\\n---\\n'.join(out)}, ensure_ascii=False))`;
							const out = execFileSync(
								"python3",
								[
									"-c",
									py,
									zst,
									String(anchorMs),
									String(window),
									String(charCap),
									String(row.title).slice(0, 12),
								],
								{
									timeout: 30000,
									encoding: "utf8",
									maxBuffer: 16 * 1024 * 1024,
								},
							);
							const parsed = JSON.parse(out);
							// L2（2026-08-27 08:32 批·脑 08:02 漏点）：双锚均失败（ts 解析失败或距最近帧>1h 且标题前缀原文不命中）→ 不返回会话开头无关帧·改 note 兜底
							if (!parsed.anchorHit) {
								stats.episodicAnchorMiss = (stats.episodicAnchorMiss || 0) + 1;
								return {
									episodicContext: null,
									note:
										"未能定位原文窗（ts 锚与标题锚均未命中）·指针：" +
										src +
										"·会话共 " +
										parsed.frames +
										" 帧可人工排查",
								};
							}
							let text = String(parsed.text || "");
							// D1 修正（8-26 逐字审计）：REJECT_PATTERNS 无 g 标志·单 replace 只脱首处——同凭据多次出现会漏脱。循环替换至全净。
							for (const p of REJECT_PATTERNS) {
								let guardN = 0;
								while (p.re.test(text) && guardN < 50) {
									text = text.replace(p.re, "[redacted]");
									guardN++;
								}
							}
							text = stripUrls(text); // step B-4：episodic 回流原文补套 URL 脱敏（原只过凭据 REJECT_PATTERNS——会话原文里的外链裸流回上下文）
							// 注记（D3）：【A】帧为 assistant/chunk 增量文本片段（非整轮）——顺序拼接可读·标注按帧非按轮
							stats.episodicReadCount = (stats.episodicReadCount || 0) + 1;
							episodicRate.set(fcIdKey, { turns: nowTurns, at: Date.now() });
							return {
								episodicContext: text || "（窗口内无文本帧）",
								session: mSid[1],
								frames: parsed.frames,
								windowRange: parsed.window,
								sysFolded: parsed.sysFolded || 0,
								elapsedMs: Date.now() - t0,
								id: row.id,
								title: row.title,
							}; // D1 审计修：sysFolded 透传（原漏·格式化器恒 undefined）
						} catch (error) {
							return {
								episodicContext: null,
								note:
									"原文解压/定位失败：" +
									String(error).slice(0, 80) +
									"·指针：" +
									src,
							};
						}
					}
					if (action === "read_evolution") {
						// ── step（18:41 maintainer·进化史回流）：进化史=超长会话——mirror 条目按source of record行号锚取 ±N 条原文窗 ──
						//    「既然会话原文能回流·情景能记得·进化史也能像会话原文一样被回流」（maintainer原话）
						const id = Number(args.id);
						if (!id)
							return { error: "read_evolution 需要 id 参数（mirror 条目）" };
						const row = db
							.prepare(
								"SELECT id, title, content, space, source, ts FROM memories WHERE id = ?",
							)
							.get(id);
						if (!row) return { error: "条目不存在 #" + id };
						if (String(row.source || "") !== "mirror:memory-md")
							return {
								error:
									"非进化史镜像条目（source=" +
									row.source +
									"）——read_evolution 专用 mirror 条·检索面用 search",
							};
						const c = String(row.content || "");
						const pi = c.indexOf("〔source of record：modules/");
						if (pi < 0)
							return {
								error:
									"source of record指针缺失（旧格式 mirror 条·直读对应space MEMORY.md）",
							};
						const pe = c.indexOf("〕", pi);
						const parts = c.slice(pi + 7, pe).split("/MEMORY.md L"); // 零正则解析（转义免疫）
						const dirName = parts[0],
							anchorLine = Number(parts[1]);
						const mdPath = require("node:path").join(
							os.homedir(), ".dsh", "dsh-living-memory", "modules",
							dirName,
							"MEMORY.md",
						);
						if (!fs2.existsSync(mdPath))
							return { error: "source of record文件不在位：" + mdPath };
						const lines = fs2.readFileSync(mdPath, "utf8").split("\n");
						// 窗边界：锚 ## 行 ±window 个 ## 条目（step设计：给「窗」不给孤条——一周进化脉络情景齐）
						const heads = [];
						for (let i = 0; i < lines.length; i++)
							if (/^## /.test(lines[i])) heads.push(i);
						// ── P1① 审计 修（2026-08-29：440 锚实测 200 漂=45% 静默错窗——刀③每日顶部插入累积·锚行号只是初值·题才是身份）──
						//    两段式：①锚行命中且题对→直接用 ②否则先精确题重定位（保留尾锚=dup 族唯一键）③再规格化题兜底④全失才报漂移。
						const evPrefix =
							/^## 20\d{2}-\d{2}-\d{2}[ T]\d{2}:\d{2}\s*\[\w+\]\s*/;
						const rowTitleX = String(row.title)
							.replace(/^20\d{2}-\d{2}-\d{2}[ T]\d{2}:\d{2}\s*\[\w+\]\s*/, "")
							.replace(
								/（(?:active|aged|retired|resolved|merged|superseded)） (?=\[#\d+\]$)/,
								"",
							)
							.trim(); // 余件item二（09-09·三轮审计 §4.4-1）：剥缀集对齐——原 (active|aged|retired|resolved) 漏 merged/superseded（入库剥缀集 L7303 单源有之）→手写区带缀条目三级匹配全失恒落回落窗·取并集六态归一
						const lineTitleX = (l) =>
							String(l)
								.replace(evPrefix, "")
								.replace(
									/（(?:active|aged|retired|resolved|merged|superseded)） (?=\[#\d+\]$)/,
									"",
								)
								.trim();
						const normEvTitle = (t) =>
							String(t)
								.replace(/(?:\s*\[#\d+\])+\s*$/, "")
								.trim();
						let hi = heads.findIndex((h) => h + 1 === anchorLine);
						if (hi < 0 || lineTitleX(lines[heads[hi]]) !== rowTitleX) {
							const byX = heads.findIndex(
								(h) => lineTitleX(lines[h]) === rowTitleX,
							);
							if (byX >= 0)
								hi = byX; // 精确题（含尾锚）重定位自愈
							else {
								const byN = heads.findIndex(
									(h) =>
										normEvTitle(lineTitleX(lines[h])) ===
										normEvTitle(rowTitleX),
								);
								// B 车（09-05  再审计真缺陷）：规格化兜底剥尾锚比较可配同题异 id 行——加 id 尾锚校验（带尾锚且≠本条 id=同题族误配→不采·落回落窗；无尾锚历史行为正）
								if (byN >= 0) {
									const am = String(lines[heads[byN]]).match(/\[#(\d+)\]\s*$/);
									hi = am && Number(am[1]) !== row.id ? -1 : byN;
								} else hi = byN; // -1=题确不在source of record/误配→落漂移错或回落窗
							}
						}
						if (hi < 0) {
							// ── 锚自愈回落窗（09-05 审计settled：mirror 条source of record可缺行——evo 入库晚于 P1① 重建+刀③追加滤 mirror:% 防回环=三因叠加族·三级匹配全失≠无料）──
							const w2 = Math.min(Math.max(Number(args.window) || 2, 1), 6);
							const neigh = db
								.prepare(
									"SELECT id, ts, type, title, content FROM memories WHERE space = ? AND status != 'deleted' ORDER BY id ASC",
								)
								.all(row.space);
							const ix = neigh.findIndex((n) => n.id === row.id);
							if (ix >= 0) {
								const lo2 = Math.max(0, ix - w2),
									hib2 = Math.min(neigh.length, ix + w2 + 1);
								const seg2 = neigh
									.slice(lo2, hib2)
									.map(
										(n) =>
											`## ${String(n.ts).slice(0, 16)} [${n.type}] ${n.title} [#${n.id}]\n${n.content}`,
									)
									.join("\n\n");
								// 审计自审即修（09-05）：回落窗=成功回流——同受重型回流频控（同 id 4 轮防重刷）+episodicReadCount 口径对齐（原旁路=连刷不拦+计数漂）
								const fcK3 =
									exec?.agent?.session?.id ||
									exec?.session?.id ||
									sessionOfLastTurn ||
									"_anon";
								const fcId3 = fcK3 + ":" + row.id;
								const bd3 = bindMap.get(fcK3);
								const nowT3 = (bd3 && bd3.userTurns) || 0;
								const rc3 = episodicRate.get(fcId3);
								if (rc3 && nowT3 - rc3.turns < 4) {
									stats.episodicThrottled = (stats.episodicThrottled || 0) + 1;
									return {
										error:
											"回流频控（回落窗同受·按 id 计）——" +
											(4 - (nowT3 - rc3.turns)) +
											" 轮后再取本条",
									};
								}
								episodicRate.set(fcId3, { turns: nowT3, at: Date.now() });
								stats.episodicReadCount = (stats.episodicReadCount || 0) + 1;
								stats.evoFallbackWindows = (stats.evoFallbackWindows || 0) + 1; // ⑬ 计数透出（回落窗使用数·观测面）
								return {
									evolutionContext: seg2.slice(
										0,
										Math.min(Number(args.charCap) || 3000, 6000),
									),
									file: "living memory in-store fallback window (absent from source of record; excluded by evo ingest, rebuild, and mirror filters)",
									anchorLine,
									windowRange: [neigh[lo2].id, neigh[hib2 - 1].id],
									entries: hib2 - lo2,
									id: row.id,
									title: row.title, // E 车（09-05）：回落窗 return 原缺 id/title——render L202 引用即 #undefined（step遗留·handover note E 项）
									space: row.space,
								};
							}
							return {
								error: "锚漂移且库内回落失败（条已删？）——用 search 重新定位",
							};
						}
						const window = Math.min(Math.max(Number(args.window) || 2, 1), 6);
						const lo = heads[Math.max(0, hi - window)],
							hib =
								hi + window + 1 < heads.length
									? heads[hi + window + 1]
									: lines.length; // step窗修：末条吃全到下一 ## 前（原切末条头部=半身）
						const seg = lines.slice(lo, hib).join("\n");
						const fcKey2 =
							exec?.agent?.session?.id ||
							exec?.session?.id ||
							sessionOfLastTurn ||
							"_anon"; // step作用域修：fcKey 定义在 read_episodic 分支内——本分支自算（同法·exec 真身优先）
						const fcIdKey2 = fcKey2 + ":" + (row.id || "x"); // 8-31 approved：read_evolution 同按 id 计（与 episodic 分流·同 id 防重刷新 id 放行）
						const bd2 = bindMap.get(fcKey2);
						const nowTurns2 = (bd2 && bd2.userTurns) || 0;
						const rc = episodicRate.get(fcIdKey2);
						if (rc && nowTurns2 - rc.turns < 4) {
							stats.episodicThrottled = (stats.episodicThrottled || 0) + 1;
							return {
								error:
									"回流频控（与 episodic 共享·8-31 起按 id 计）——" +
									(4 - (nowTurns2 - rc.turns)) +
									" 轮后再取本条",
							};
						}
						episodicRate.set(fcIdKey2, { turns: nowTurns2, at: Date.now() });
						stats.episodicReadCount = (stats.episodicReadCount || 0) + 1;
						return {
							evolutionContext: seg.slice(
								0,
								Math.min(Number(args.charCap) || 3000, 6000),
							),
							file: "modules/" + dirName + "/MEMORY.md",
							anchorLine,
							windowRange: [lo + 1, hib],
							entries: window * 2 + 1,
							id: row.id,
							title: row.title,
							space: row.space,
						};
					}
					if (action === "timeline") {
						const limit = Math.min(Number(args.limit) || 10, 20);
						// ── as_of 三窗（timeline 同款·wave时序补全）：格式闸+ts≤∧COALESCE(closed_at,'9999')＞∧COALESCE(valid_to,'9999')＞
						//    ——时点快照浏览（10-B search 路同口径）；无 as_of=原行为零变更。
						const asOfTl = String(args.as_of || "").trim();
						if (
							asOfTl &&
							!/^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])(T(0[0-9]|1\d|2[0-3]):[0-5]\d)?$/.test(
								asOfTl,
							)
						)
							return {
								error:
									"as_of 格式非法（YYYY-MM-DD[THH:mm]·同款闸）: " +
									asOfTl.slice(0, 30),
							};
						const tlWin = asOfTl
							? " AND ts <= ? AND COALESCE(closed_at, '9999') > ? AND COALESCE(valid_to, '9999') > ?"
							: "";
						const tlWinArgs = asOfTl ? [asOfTl, asOfTl, asOfTl] : [];
						let rows;
						if (args.space) {
							rows = db
								.prepare(
									"SELECT * FROM memories WHERE status = 'active' AND space = ?" +
										tlWin +
										" ORDER BY COALESCE(event_at, ts) DESC, id DESC LIMIT ?",
								)
								.all(String(args.space), ...tlWinArgs, limit); // D3 审计修（18:57）：id 序被镜像 440 条霸屏——真时间序（镜像老 ts 沉底·事件钟优先）
						} else {
							rows = db
								.prepare(
									"SELECT * FROM memories WHERE status = 'active'" +
										tlWin +
										" ORDER BY COALESCE(event_at, ts) DESC, id DESC LIMIT ?",
								)
								.all(...tlWinArgs, limit); // D3 同修
						}
						const total = db
							.prepare(
								"SELECT COUNT(*) AS c FROM memories WHERE status = 'active'" +
									tlWin,
							)
							.get(...tlWinArgs);
						return {
							entries: rows.map((r) => ({
								id: r.id,
								ts: r.ts,
								type: r.type,
								title: r.title,
								space: r.space,
							})),
							total: Number(total.c),
							...(asOfTl ? { asOf: asOfTl } : {}),
						};
					}
					if (action === "stats") {
						const row = db.prepare("SELECT COUNT(*) AS c FROM memories").get();
						const active = db
							.prepare(
								"SELECT COUNT(*) AS c FROM memories WHERE status = 'active'",
							)
							.get();
						// 裁一（design-approved option·another space案悬案一）：pinboardTop 缺省定向=caller 归属空间（与 🧭 归属行同源解析）——「stats 带 space」用法成实
						const statsCallerOrgan =
							organFromPath(
								exec?.agent?.session?.header?.cwd ||
									exec?.agent?.session?.cwd ||
									exec?.session?.header?.cwd ||
									exec?.cwd ||
									"",
							) || null;
						return {
							memoryCount: Number(row.c),
							activeCount: Number(active.c),
							firehoseSeen: stats.firehoseSeen,
							eventCount: stats.eventCount,
							recentTypes: stats.recentTypes.slice(),
							autoExtractCount: stats.autoExtractCount,
							autoMemoryCount: stats.autoMemoryCount,
							rejectedCount: stats.rejectedCount,
							bufferedChars: stats.bufferedChars,
							nightPatrolCount: stats.nightPatrolCount, // 本次启动计数
							patrolHistoryTotal: (() => {
								try {
									const r = db
										.prepare("SELECT v FROM organ_meta WHERE k='patrol_last'")
										.get();
									return r ? Number(JSON.parse(r.v).total) || 0 : 0;
								} catch {
									return 0;
								}
							})(), // 历史累计（持久化）
							triplesTotal: (() => {
								try {
									return db
										.prepare(
											"SELECT COUNT(*) c FROM memory_triples WHERE invalidated_at IS NULL",
										)
										.get().c;
								} catch {
									return 0;
								}
							})(), // step：实体 KG 活三元组（库直读最新口径）
							validToCount: (() => {
								try {
									return db
										.prepare(
											"SELECT COUNT(*) c FROM memories WHERE status = 'active' AND valid_to IS NOT NULL AND valid_to <= ?", // P0修（09-03 audit）：补 active 过滤+nowIso 绑参（与retrieval gate L6258 同口径）
										)
										.get(nowIso()).c;
								} catch {
									return 0;
								}
							})(), // step：已到点失效条数（事件轴）
							pinboardTop: (() => {
								try {
									// 🅱案(design note)：pinboard ranking候选榜首——stats 带 space 即定向本空间（触达面=每窗 stats 第一发；详表=a companion script <space>）
									// 裁一补口径（2026-08-31）：与 pinboard-float.cjs 对齐——复犯两源 vc≥2∨fam≥1（题面 jaccard 交叠≥3 词同题族）；缺省=caller 归属空间
									const sp = String(
										args.space || statsCallerOrgan || "memory-organ",
									);
									const rows = db
										.prepare(
											`SELECT id, title, validated_count vc, last_hit_at FROM memories WHERE space = ? AND type = 'lesson' AND status = 'active' AND valid_to IS NULL`,
										)
										.all(sp);
									const tk = (s) =>
										new Set(
											String(s)
												.split(/[^⺀-鿿\w]+/)
												.filter((w) => w.length >= 2),
										); // 与 pinboard-float.cjs 同则（[^⺀-鿿\w]=[^\u2E80-\u9FFF\w]）
									const tks = rows.map((r) => tk(r.title));
									let best = null;
									for (let i = 0; i < rows.length; i++) {
										let fam = 0;
										for (let j = 0; j < rows.length; j++) {
											if (i === j) continue;
											let inter = 0;
											for (const w of tks[i]) if (tks[j].has(w)) inter++;
											if (inter >= 3) fam++;
										}
										const repeats = Math.max(rows[i].vc || 1, fam + 1);
										if (
											repeats >= 2 &&
											(!best ||
												repeats > best.repeats ||
												(repeats === best.repeats &&
													String(rows[i].last_hit_at || "") >
														String(best.last_hit_at || "")))
										)
											best = { ...rows[i], repeats };
									}
									return best
										? `#${best.id} 复犯${best.repeats} ${String(best.title)
												.replace(
													/^20\d{2}-\d{2}-\d{2}[ T]\d{2}:\d{2}\s*\[\w+\]\s*/,
													"",
												)
												.slice(0, 32)}`
										: ""; // title 清理与 pinboard-float 同则（去日期+[lesson] 前缀）
								} catch {
									return "";
								}
							})(),
							lastPatrolAt: stats.lastPatrolAt,
							lastPatrolMerged: stats.lastPatrolMerged,
							lastCooccurPairs: stats.lastCooccurPairs || 0,
							cooccurPairs: (() => {
								try {
									return Number(
										db
											.prepare("SELECT COUNT(*) AS c FROM memories_cooccur")
											.get().c,
									);
								} catch {
									return 0;
								}
							})(),
							linkSentinel: stats.linkSentinel || null,
							episodicReadCount: stats.episodicReadCount || 0, // 批注解②（08:13）：read_episodic 用量计数——防黑盒工具无人知用量
							episodicThrottled: stats.episodicThrottled || 0,
							// ── 二轮审即修（22:33）：MP 吸收车三计数器补 stats 出口（⑬ 精神——计数不可见=半合规）──
							countSkipCount: stats.countSkipCount || 0, // 甲档 计数闸拦截数
							edgeYieldDeadKey: stats.edgeYieldDeadKey || 0, // D11 补面死键让位数
							vecChannelErrors: vecChannelErrors || 0, // 8-31 锈面修③：vec 通道故障累计（F0-1 静默死遥测透出）
							rerankErrors: rerankErrors || 0, // option：rerank 通道故障累计（vecChannelErrors 同族透出·降级直通可见）
							toolUsageErrors, // 吸收item：遥测写库失败累计透出（⑬ 出口闭环·09-02 审计真缺陷 1 即修）
							usageWeeklyRuns: stats.usageWeeklyRuns || 0, // P0 同item：周报常驻段跑/败计数透出（09-03 自审补——⑬ 计数不可见=半合规同型病）
							usageWeeklyErrors: stats.usageWeeklyErrors || 0,
							memoryActiveTurns: stats.memoryActiveTurns || 0,
							memoryTotalTurns: stats.memoryTotalTurns || 0, // option：主动查记忆率（真用户轮中有 memory 调用占比·观测面）
							spaceGateBlocked: spaceGateCounts.blocked,
							spaceGatePassed: spaceGateCounts.passed,
							spaceGateUnconfigured: spaceGateCounts.unconfigured, // 空间闸未配置降级放行数（公开派生面常态·内源恒 0） // option：空间白名单闸拦/放计数(ops note)
							nudgeTurnShown: stats.nudgeTurnShown || 0,
							nudgeTurnFollowed: stats.nudgeTurnFollowed || 0, // option：轮内记忆先行闸提示/听从计数（观测级·一周误伤率escalate后议拦截级）
							nudgeTotalShown: readNudgeTotal("nudge_shown_total"),
							nudgeTotalFollowed: readNudgeTotal("nudge_followed_total"), // 3 天窗刀（09-06 批）：两率跨重启累计（organ_meta）·观测窗 3 天期满escalate
							nudgePersistErrors: stats.nudgePersistErrors || 0,
							autorecallPnRescue: stats.autorecallPnRescue || 0, // 柱环4/梁4 预备：专名救援路触发数（**原零出口**——L4696 自增而 return/render 双缺·⑬「四点接线」违反·2026-09-11 接续窗补齐）
							autorecallKwRescue: stats.autorecallKwRescue || 0,
							autorecallVecFallback: stats.autorecallVecFallback || 0, // option：主题词重查救回/vec 近邻兜底命中计数
							dupWriteBlocked: dupWriteStat.blocked || 0, // 梁1b 同文幂等闸：memory_write 同文重复拒写数（模块级对象·09-11 kimi 21 连发案治本件②·⑬ 四点接线齐）
							injectErrors: stats.injectErrors || 0, // 8-31 锈面修④：注入段兜底失败计数（F2-2 吞错出口透出）
							a25ChaseErrors: stats.a25ChaseErrors || 0, // 甲批⑩ I6b chase 失败数
							bothValidLinked: stats.bothValidLinked || 0, // 刀A：不可约对建边数
							bothValidErrors: stats.bothValidErrors || 0, // 刀A：both-valid 处理失败数
							a25FlushErrors: stats.a25FlushErrors || 0, // 锁面连环透出（09-04 对账红⑥·原仅 warn 日志·⑬ 同法）
							// ── 09-03 压缩↔living memory联动桥·五键透出（⑬ 纪律：计数不可见=半合规）──
							compactionSeen: stats.compactionSeen || 0, // 刀② end 捕获（成功压缩次数）
							compactionBridged: stats.compactionBridged || 0, // 刀② 锚条入册+建边成功数
							compactionChaseRuns: stats.compactionChaseRuns || 0, // 刀① start 压缩前清算发起数
							compactionChaseErrors: stats.compactionChaseErrors || 0,
							compactionBridgeErrors: stats.compactionBridgeErrors || 0,
							organMetaPruned: stats.organMetaPruned || 0, // 三车：organ_meta 过期计数键清理数（四点接线·⑬）
							organMetaPruneErrors: stats.organMetaPruneErrors || 0, // AUDIT-FIX（A-D3）：错误侧补透出（原只自增+warn）
							// ── 09-03 P0 修复车十键透出（⑬ 纪律：计数不可见=半合规同型病·自立法不自犯）──
							entityExtractErrors: stats.entityExtractErrors || 0, // entity 段失败（原型链闸后仍兜底）
							g2ArchiveErrors: stats.g2ArchiveErrors || 0, // G2 归档失败（主卷不覆写保护在役）
							mirrorWmErrors: stats.mirrorWmErrors || 0, // mirror 水线读写失败
							autorecallErrors: stats.autorecallErrors || 0, // 召回整链异常（原静默假绿面）
							pressureAlertErrors: stats.pressureAlertErrors || 0, // 换窗警报写库失败
							extractDupSkipped: stats.extractDupSkipped || 0, // 中断重抽闸拦截数
							signalErrors: stats.signalErrors || 0, // tool/result 信号段异常
							// ── 09-03 P2 修复车九键透出（⑬ 纪律同法：计数不可见=半合规）──
							bufferRestoreErrors: stats.bufferRestoreErrors || 0, // extract_buffer 启动恢复失败
							validatedErrors: stats.validatedErrors || 0, // 段异常
							semanticEdgeErrors: stats.semanticEdgeErrors || 0, // 段异常
							pressureReadErrors: stats.pressureReadErrors || 0, // 压力缓存读段失败（「无高压」假绿防线）
							spokenFillWmErrors: stats.spokenFillWmErrors || 0, // spoken 观测键写失败
							extractEventsErrors: stats.extractEventsErrors || 0, // extract events 增强段异常
							conflictsPreverdicted: stats.conflictsPreverdicted || 0, // AUDN 预裁counter（wave·pending 已裁数）
							assistantDistillLines: stats.assistantDistillLines || 0, // wave 蒸馏行计数
							entitiesResolved: stats.entitiesResolved || 0, // wave 消解计数
							conflictsPreclassifErrors: stats.conflictsPreclassifErrors || 0,
							evoFallbackWindows: stats.evoFallbackWindows || 0, // 回落窗使用数——09-06 审计补接（乙批：赋值 L7143/透出读 L319 在而 return 漏=三点断一线永不显示·⑬ 自犯第三例根治；drill=render-stats-drill T0 动态断言新增键自动进面）
							closureCheckErrors, // 四闸核心异常累计（模块级·同 toolUsageErrors 族）
							surgerySkipVecEmbed, // 挂牌期惰性补嵌跳过
							surgerySkipWrite, // 挂牌期写入冻结
							queryClassDist: { ...queryClassDist }, // SelRoute 六类分布（观测一周·本期零权重变更）
							// ── counter白名单（step·07:30 maintainer·夜审wave F1 清单）：赋值面 35 键 return 仅 23——14 counter补全 ──
							bufferRestored: stats.bufferRestored || 0,
							semanticConflictsFound: stats.semanticConflictsFound || 0,
							mergeExecuted: stats.mergeExecuted || 0,
							edgesMigrated: stats.edgesMigrated || 0,
							validatedUp: stats.validatedUp || 0,
							semanticEdges: stats.semanticEdges || 0,
							communitiesBuilt: stats.communitiesBuilt || 0,
							sagaSummaries: stats.sagaSummaries || 0, // 件5 审计修①：四点接线补 return（赋值在而 return 缺=计数不可见族）
							sagaSummaryErrors: stats.sagaSummaryErrors || 0,
							signalTriggered: stats.signalTriggered || 0,
							sensitiveSkipCount: stats.sensitiveSkipCount || 0,
							autoEdges: stats.autoEdges || 0,
							lastExtractAt: stats.lastExtractAt || null,
							routedCount: stats.routedCount || 0,
							signalCount: stats.signalCount || 0,
							signalArmed: stats.signalArmed || false,
							// ── 归属自检（21:55 maintainer·28375d 体检放行事故根治）：caller 链透视——体检一查 stats 即暴露归属断裂（该窗 17:40 若有此字段·「全绿」出不来） ──
							callerIntrospect: (() => {
								try {
									// step（07:30 maintainer·abf8a31c 案 ）：主路改 exec 调用者头（与 search 路键同源）——原 sessionOfLastTurn 全局指针多窗竞态串台
									const execCwd =
										exec?.agent?.session?.header?.cwd ||
										exec?.agent?.session?.cwd ||
										exec?.session?.header?.cwd ||
										exec?.cwd ||
										"";
									const sid = sessionOfLastTurn || null;
									const organ =
										organFromPath(execCwd) || (sid ? sessionOrgan(sid) : null);
									const recentAuto = db
										.prepare(
											"SELECT id, space, source FROM memories WHERE source LIKE 'auto:session-%' ORDER BY id DESC LIMIT 5",
										)
										.all();
									const drift = recentAuto.filter((r) => {
										// 归属漂移面：auto 条目 space 与其会话 identity.cwd 应属space不一致
										const m = String(r.source).match(
											/^auto:(session-[0-9a-f-]+)$/,
										);
										if (!m) return false;
										try {
											const tbl = projcacheRows().get(m[1]); // 庚批修④（09-08 审计·stale 半盲闭合）
											const cwd = tbl?.identity?.cwd;
											if (!cwd) return false;
											const o = organFromPath(cwd);
											return o && o !== r.space;
										} catch {
											return false;
										}
									});
									return {
										session: sid,
										organResolved: organ,
										driftSample: drift.map((r) => ({
											id: r.id,
											space: r.space,
											source: String(r.source).slice(5, 26),
										})),
										driftCount: drift.length,
										verdict: !organ
											? "BROKEN: 本窗归属未解析（auto 将落 global）"
											: drift.length > 0
												? "WARN: 本窗健康但近 5 条 auto 有归属漂移 " +
													drift.length +
													" 条（历史伤·查 driftSample）"
												: "ok",
									};
								} catch {
									return null;
								}
							})(),
							autoRegisteredSpaces: autoRegistered.length,
							surgerySkipCount: stats.surgerySkipCount || 0,
							ftsReady: (() => {
								try {
									db.prepare("SELECT COUNT(*) AS c FROM memories_fts").get();
									return true;
								} catch {
									return false;
								}
							})(),
							jieba: !!jieba,
						};
					}
					return {
						error:
							"未知 action：" +
							action +
							"（只读三 action：search/timeline/stats；写库用 memory_write）",
					};
				},
			),
		});

		// ── 工具二：memory_extract（手动触发提炼）──
		tools.register({
			name: "memory_extract",
			description:
				"Trigger extraction manually: hand the recent conversation buffer to the extraction model (its own API key) and write the results straight into the store. Nightly consolidation runs on its own (02:00-06:00), so this is only needed to force an early pass.",
			// wave（09-07 maintainer·审计 P1-1）：sid 从 output.schema 移入 parameters——原错位致模型不可传参·他窗特权提炼失效
			parameters: {
				type: "object",
				properties: {
					sid: {
						type: "string",
						description:
							"Optional: extract another session's backlog by id (format session-xxx). Defaults to the current session.",
					},
				},
			},
			output: {
				schema: {
					type: "object",
					additionalProperties: false,
					properties: {
						extracted: { type: "number" },
						titles: { type: "array", items: { type: "string" } },
						skipped: { type: "boolean" },
						reason: { type: "string" },
					},
					required: ["extracted", "titles"],
				},
				render: (args, value) => memoryRender(args, value),
			},
			execute: usageWrap(
				"memory_extract",
				() => db,
				async (args, exec) => {
					// 增强：从调用者会话的事件日志取最近文本并入缓冲（重启后缓冲清零时也有料可提炼）
					let historyText = "";
					try {
						const events =
							exec?.agent?.session?.snapshotEvents?.() ?? // rc.1（0.1.2+·09-05 迁移）：按需 API 全量（dsh-session index.d.ts L184：返回 readonly SessionEvent[]·与旧 .events 同型消费）——0.1.1 下 events 已移除=静默零料(ops note)
							exec?.agent?.session?.events; // ≤0.1.1 回落（双源兼容·回滚场景不断料）
						if (Array.isArray(events)) {
							const parts = [];
							for (
								let i = events.length - 1;
								i >= 0 && parts.length < 24;
								i--
							) {
								const e = events[i];
								const content = e?.data?.content || e?.data?.message?.content;
								if (Array.isArray(content)) {
									let t = "";
									for (const b of content)
										if (b && typeof b.text === "string") t += b.text;
									if (t.trim()) parts.unshift(t.slice(0, 1500));
								}
							}
							historyText = parts.join("\n---\n").slice(-10000);
						}
					} catch (eEV) {
						stats.extractEventsErrors = (stats.extractEventsErrors || 0) + 1; // P2 fix（09-03 audit）：events 增强段异常透出（原静默=重启后缓冲清零时取料失败不可见）
						if (Date.now() - (stats._evWarnAt || 0) > 60000) {
							stats._evWarnAt = Date.now();
							ctx.logger?.warn?.(
								"[living-memory] extract events 增强段失败(#" +
									stats.extractEventsErrors +
									"): " +
									String(eEV).slice(0, 60),
							);
						}
					}
					// P0修（09-03 audit）：historyText 改 fallback 显式传入（原覆盖全局 buffer=他窗历史残留污染本窗后续自动提炼）
					stats.bufferedChars = buffer.length;
					// 审计 D1 配套（13:44）：手写显式提炼传 source——防被 水位路劫持（本窗历史必须用本调用源）
					const result = await runExtract({
						auto: false,
						sid: args.sid,
						fallback:
							historyText && historyText.trim() ? historyText : undefined,
					}); // P0修：fallback 回落（水位账优先消化照step语义） // step-2c（10:35）：解除 source:buffer 旧路劫持——原显式传旧路全局 buffer·水位路（step-1 滚动消化）从未接管手动路径；sid 透传他窗特权
					if (result.skipped)
						return {
							extracted: 0,
							titles: [],
							skipped: true,
							reason: result.reason,
						};
					return {
						extracted: result.extracted,
						titles: result.titles,
						skipped: false,
					};
				},
			),
		});
	},
};
