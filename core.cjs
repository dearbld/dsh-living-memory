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
const episodicAnchors = require("./episodic-anchor.cjs").createEpisodicAnchorStore({ nowIso });

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
// 阶段〇第 16 项(design note)：nightly patrol判窗所用的钟＝北京。patrol.cjs 取 nowIso()（固定 +08:00、无夏令时），与本常量同钟。
//   本常量是声明，不是旋钮：外部写闸（副脑记忆桥「nightly patrol窗拒写」）读它对齐判窗；在产没有它＝旧码按本机时区判窗。
const PATROL_TZ = "Asia/Shanghai";
const PATROL_CHECK_MS = 30 * 60 * 1000; // 每 30 分钟检查一次是否入窗
// ── 吸收项（wave·design-approved「抽取模型定向 V4-Flash」）──
// 官方 changelog 实证 ID：deepseek-v4-flash（2026-08-13 文本档上线·同端点同 key 零新凭据）。
// 原 deepseek-chat 通用档 → Flash 低成本档；env LEGION_EXTRACT_MODEL 可覆（观察期灵活回退）。
const EXTRACT_MODEL = process.env.LEGION_EXTRACT_MODEL || "deepseek-flash"; // 09-13 maintainer：v4-flash→**deepseek-flash**（实测产出**完全等价**〔5条/行为位1/数字5/路径1〕且更快 6.7s vs 8.0s·directive首选1）
// ── issue 修②（design-approved）：单次提炼请求应用级全局超时（ms·env 可调）──
//    防慢速响应长持 extracting 互斥锁（Undici 默认 300s×2 只是兜底·四 attempts 最坏 20 分钟）；
//    默认 120s 下四 attempts 最坏 8 分钟封顶。超时 abort 走既有 fetch fail → skip 路径。
const envIntCap = (raw, dflt) => {
	// 车三件②（W6 整数帽族统一助手·design-approved「发」）：非法/非正整数一律回落缺省（负值/小数/Infinity/NaN 全防护——局部 a10Num L9144 同构范式·提为模块级供全族用）
	// ⚠ 位置hard rule（gate-car TDZ 案·09-30）：本定义必须在**所有顶层用点之前**（首个用点 L65 EXTRACT_TIMEOUT_MS）——const 箭头不提升·后置定义＝模块加载即崩。
	const n = Number(raw);
	return Number.isInteger(n) && n > 0 ? n : dflt;
};
// ── 板机制六缺口车（10-18 memory organ交办 0927·副脑落码）：三开关，每次调用现读 env ──
//    LEGION_BOARD_HINT_OFF=1：甲批总闸（缺口 1 wrap-up断点列全号与销号样例、缺口 2/3 滞后与未open item拆键、缺口 5 换窗提示）——
//      设 1 即四处回本车前原样（注入文本、board_write receipt、stats 键、render 标签逐字同）；只认 "1"（同注入瘦身车新开关惯例）。
//    LEGION_BOARD_STALE_DAYS：本窗板「⚠陈旧」标阈值天数（缺口 4①）——正有限数才认，余皆按 3（＝本车前常量）。
//    LEGION_BOARD_GC_OPEN_DAYS：nightly patrol板 GC 对含未完项之板的保留天数（缺口 4②）——大于 30 的有限数才认（上限 3650 防日期越界），
//      不设或不认＝null，GC 走本车前原语句（超 30 天整行删）。
const boardHintOn = () => process.env.LEGION_BOARD_HINT_OFF !== "1";
const boardStaleDays = () => {
	const n = Number(process.env.LEGION_BOARD_STALE_DAYS);
	return Number.isFinite(n) && n > 0 ? n : 3;
};
const boardGcOpenDays = () => {
	const n = Number(process.env.LEGION_BOARD_GC_OPEN_DAYS);
	return Number.isFinite(n) && n > 30 ? Math.min(n, 3650) : null;
};
const EXTRACT_TIMEOUT_MS =
	envIntCap(process.env.LEGION_EXTRACT_TIMEOUT_MS, 120000);
const SPOKEN_MODEL = process.env.LEGION_SPOKEN_MODEL || "deepseek-chat"; // 09-03 裁③（maintainer B 案）：spoken-prefix 模型独立旋钮——修「EXTRACT_MODEL 不传导」契约破洞·缺省现状零行为变·两链独立调优（质量directive按需分配）
// ── AUDN 刀②（09-08 approved·braintask brief 20:1x）：预裁模型独立旋钮——治「枚举外/矛盾」判据质量降级 ──
//    与 EXTRACT_MODEL 脱钩（提炼主链 flash 不动·量大成本敏感；预裁 40 案/巡≈50-100K·谷时 2-4 元可忽略）。
//    缺省档＝**`deepseek-flash`**（design-approved：**`deepseek-v4-pro` 明日下线·立即停用**；directive档位优先级＝
//    **`deepseek-flash` ／ 智谱 GLM-5.3 优先**，百炼或其他其次 ⇒ 本档先落**同端点·零新凭据**的 flash；GLM 接入待评测后另车）。
const PREVERDICT_MODEL =
	process.env.LEGION_PREVERDICT_MODEL || "deepseek-flash"; // design-approved「智谱不用了，以后不会用了」⇒ 预裁默认由 glm-5.3 切 DeepSeek flash（原 09-13 切档理由与实测数字见 ·史档可查）；权重理由：DeepSeek 记忆钥实测 200、智谱 GLM_CODE_KEY 实测 401
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
// ── 线上 LLM 端点与凭据**可配**（design-approved「质量优先·更放开·用更调优的线上大模型」）──
//    三路（提炼主链 / AUDN 预裁 / spoken-prefix）**共用**此端点与凭据 ⇒ 一处可配即整链可换 provider：
//      （历史举例·2026-10-24 起作废）智谱曾用 LEGION_LLM_BASE_URL=https://open.bigmodel.cn/… ＋ LEGION_LLM_KEY_REF=GLM_CODE_KEY；maintainer「智谱不用了」后该 REF 已从凭据库删除，勿再引用
//      百炼：端点 …/compatible-mode/v1/chat/completions ＋ 相应 KEY_REF（qwen 系）
//    ⚠ 缺省**零行为变**（仍 deepseek 官方端点 + DEEPSEEK_MEMORY_KEY）；三路模型档仍各由
//      LEGION_EXTRACT_MODEL / LEGION_PREVERDICT_MODEL / LEGION_SPOKEN_MODEL 分别指定（换 provider 时需同步）。
const EXTRACT_API =
	process.env.LEGION_LLM_BASE_URL || "https://api.deepseek.com/chat/completions";
const LLM_KEY_REF = process.env.LEGION_LLM_KEY_REF || "DEEPSEEK_MEMORY_KEY";
// ── AUDN 预裁路**专属**端点与凭据（design-approved「采纳切档 glm-5.3」·D 项对照实证质量更优 5:1）──
//    三路共用的 EXTRACT_API/LLM_KEY_REF 保留给**提炼主链与 spoken 路**（主链 flash 不动）；预裁路单列
//    ⇒ 换档只覆本路两键·**不牵动他路**。缺省＝DeepSeek 端点 + DEEPSEEK_MEMORY_KEY（design-approved后·智谱全面退出）。
//    一键回退：LEGION_PREVERDICT_MODEL=deepseek-flash ＋ LEGION_PREVERDICT_BASE_URL=…deepseek… ＋
//    LEGION_PREVERDICT_KEY_REF=DEEPSEEK_MEMORY_KEY。
const PREVERDICT_API =
	process.env.LEGION_PREVERDICT_BASE_URL ||
	"https://api.deepseek.com/v1/chat/completions"; // 2026-10-24 同上：智谱端点全面退出（本处为全仓最后一处活引用）
const PREVERDICT_KEY_REF =
	process.env.LEGION_PREVERDICT_KEY_REF || "DEEPSEEK_MEMORY_KEY"; // design-approved从凭据库删除

// 三路输出预算**可配**（design-approved·GLM 适配）：推理形态档（glm-5.3 等）的思考链会吃光小额预算 ⇒ `content` 恒空
//   （实测：简单任务 3000 够出 468 字；**插件长 prompt 下 3000 被 reasoning 吃光** ⇒ finish=length＋content 空）。
//   缺省 **0 ＝各路原值**（spoken 200／预裁 2000／提炼 3000 ⇒ **零行为变**）；切推理档时经 env 提额（建议 ≥8000）。
const LLM_MAX_TOKENS = (() => {
	const n = Number(process.env.LEGION_LLM_MAX_TOKENS);
	return Number.isFinite(n) && n > 0 ? Math.floor(n) : 0;
})();
const llmBudget = (dflt) => LLM_MAX_TOKENS || dflt;
// ── 代理对安全截断（design-approved「接续」）──
//   缺陷：裸 slice(0,N)/slice(-N) 会在 emoji（UTF-16 代理对）中间切开 ⇒ 留下**孤立代理位**（U+D800–U+DFFF）。
//   实证链（drill 三档 200 对照 + 库内精确定位）：账内 2 处 U+D83D 孤立位（textLen=3000·pos=2999=切片刀口）
//     ⇒ JSON.stringify 输出 \ud83d（不成对）⇒ 服务端 JSON 解析器报 unexpected end of hex escape ⇒ **HTTP 400**。
//   400 不属 401/403/404 熔断面亦不属 429/5xx 重试面 ⇒ 不重试不熔断 ⇒ 每次必炸·不可自愈（水位永冻）。
//   三件套：①源头截断保对（O(1)）②出口纵深净化（治存量·幂等）③诊断面（warn 双写 console＋响应体）
const clipSurrogateTail = (s) => {
	if (typeof s !== "string" || s.length === 0) return s;
	const c = s.charCodeAt(s.length - 1);
	return c >= 0xd800 && c <= 0xdbff ? s.slice(0, -1) : s;
};
const clipSurrogateHead = (s) => {
	if (typeof s !== "string" || s.length === 0) return s;
	const c = s.charCodeAt(0);
	return c >= 0xdc00 && c <= 0xdfff ? s.slice(1) : s;
};
const stripLone = (s) =>
	typeof s === "string"
		? s.replace(
				/[\uD800-\uDBFF](?![\uDC00-\uDFFF])|(?<![\uD800-\uDBFF])[\uDC00-\uDFFF]/g,
				"\uFFFD",
			)
		: s;
const warnBoth = (ctx, m) => {
	try {
		ctx.logger?.warn?.(m);
	} catch {}
	try {
		console.warn?.(m);
	} catch {}
};
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
const COOCCUR_CORE_MIN = 0.4; // core/weak split line for co-occurrence edges (0.2.5: 09-14 插入区间随本切割误杀·冷启动 T5f 首抓补回)
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
function guardDefaultsKey(key) {
	try {
		const d = JSON.parse(fs2.readFileSync(GUARD_DEFAULTS_PATH, "utf8"));
		return d && typeof d === "object" ? d[key] : undefined;
	} catch {
		return undefined; // ②级缺失/损坏 → 回退原「空词表＝闸自然停用」行为（json 解析亦在内）
	}
}
// ── 序2 最小刀（09-15）：guard 漂移/写路拒counter——模块级（loadGuardRules 加载期赋值·须先于 GUARD 求值声明）──
let guardRulesDriftCount = 0; // ①②级分叉键数（0=无分叉·>0=②级裁定未落地·词表分级另案）
let guardRulesDriftKeys = ""; // 分叉键名（逗号分隔·诊断面）
let writeRejectedCount = 0; // 写路安检拒计数（原零计数·⑬计数不可见族）
let sourceOverrideCount = 0; // E 甲′ 落码车：source 归属保护覆盖计数（模块级·跨重启清零·L14180 区接线）
let softSameTopicWarn = 0; // C 落码车：软同题观测计数（闸① 同判据·模块级·L14180 区接线）
let echoWarnCount = 0; // 回声车（09-25 maintainer③）：fact/lesson 已办回声观测计数（closureCheck 复用·主路 todo 型closure gate不动）
let boardRowsPruned = 0; // 丁（09-27 approved）：板行 GC 删除计数（nightly patrol·超 30 天行·治 L6 无上限增长）
let globalDispatchCap = 0; // A-G 架构 A 件（09-27 approved）：流转账数量帽触发计数
let dispatchStale = 0; // C 件：nightly patrol观测——global 交办/pending ruling todo 超 48h 未闭环数（本轮值非累计）
let dispatchStale7d = 0; // C 件：超 7d 数(design note)
// ── D2/L9721 落码车（2026-09-24）：未来日期助手（模块级·与 L4209 TTL 串形同构——toISOString().slice(0,10) 日粒度）──
const isoAhead = (msAhead) => new Date(Date.now() + 8 * 3600e3 + msAhead).toISOString().slice(0, 10);
function loadGuardRules() {
	// ── option逐键回退（design-approved「同意 按建议」·与审计 D1 修同窗）──
	//   与 loadCriteriaRules 同构：①缺键 → ② → floor（「命中即整份取用该键」）——
	//   治「裁定落②级而生产读①级」全族（ 结构免疫·guard 判据分叉 2 键的根治）；
	//   D15 双面出声（顶层数组拒收＋sensitive 非串）与 drift 自检语义全保留；
	//   reject 合并语义保留（所取层 reject + FLOOR 恒追加·数据文件关不掉硬保底）。
	const layers = [];
	let firstOkPath = "floor";
	for (const p of [GUARD_RULES_PATH, GUARD_DEFAULTS_PATH]) {
		try {
			const parsed = JSON.parse(fs2.readFileSync(p, "utf8"));
			// D15（09-14 approved）：顶层为数组=配置结构错——显式拒收+warn（原 typeof 判不过数组⇒fail-open 静默空规则）
			if (Array.isArray(parsed)) {
				console.warn?.(
					`[living-memory] guard-rules 顶层为数组（非对象）——拒收该级回退下一级: ${p}`,
				);
				continue;
			}
			if (parsed && typeof parsed === "object") {
				layers.push(parsed);
				if (firstOkPath === "floor") firstOkPath = p;
			}
		} catch {} // missing file / corrupt JSON → try the next layer
	}
	const perKey = (k) => {
		for (const j of layers) if (j && j[k] !== undefined) return j[k];
		return undefined;
	};
	// sensitive：字符串直吃·逐层（D15 同点：找到的首个值非字符串⇒warn＋继续下层）
	let sens;
	{
		let sv = perKey("sensitive");
		if (sv !== undefined && typeof sv !== "string") {
			console.warn?.(
				`[living-memory] guard-rules sensitive 键非字符串——跳该层继续回退`,
			);
			sv = undefined;
			for (const j of layers)
				if (j && typeof j.sensitive === "string" && j.sensitive.trim()) {
					sv = j.sensitive;
					break;
				}
		}
		const sSrc =
			typeof sv === "string" && sv.trim() ? sv : GUARD_FLOOR.sensitive;
		try {
			sens = new RegExp(sSrc, "i");
		} catch {
			sens = new RegExp(GUARD_FLOOR.sensitive, "i");
		}
	}
	// ── 序2 最小刀①② drift 自检（语义保留·layers 化）：①②级同在时逐键 diff ──
	if (layers.length >= 2) {
		try {
			const [l1, l2] = layers;
			const driftKeys = [];
			const s1 = typeof l1.sensitive === "string" ? l1.sensitive : "";
			const s2 = typeof l2.sensitive === "string" ? l2.sensitive : "";
			if (s1 && s2 && s1 !== s2) driftKeys.push("sensitive");
			const r1 = guardReject(l1.reject)
				.map((x) => x.re.source)
				.sort()
				.join("|");
			const r2 = guardReject(l2.reject)
				.map((x) => x.re.source)
				.sort()
				.join("|");
			if (r1 !== r2) driftKeys.push("reject");
			const w1 = guardWordList(l1.singleBodyWarn)
				.map((x) => x.source)
				.sort()
				.join("|");
			const w2 = guardWordList(l2.singleBodyWarn)
				.map((x) => x.source)
				.sort()
				.join("|");
			if (w1 !== w2) driftKeys.push("singleBodyWarn");
			if (driftKeys.length) {
				guardRulesDriftCount = driftKeys.length;
				guardRulesDriftKeys = driftKeys.join(",");
				console.warn?.(
					`[living-memory] guard-rules ①②级分叉（${driftKeys.join(",")}）——逐键回退下生产取①级值·②级差异未落地（词表分级另案·勿径行合并）`,
				);
			}
		} catch {}
	}
	return {
		reject: guardDedupe(
			guardReject(perKey("reject")).concat(guardReject(GUARD_FLOOR.reject)),
		),
		sensitive: sens,
		sensitiveWarn: guardWordList(perKey("sensitiveWarn"))[0] || null, // option（09-15·D1 修后数组形态）：L2 魂红线只 warn 不跳——逐键回退覆盖（①缺键自然落②）
		singleBodyWarn: guardWordList(perKey("singleBodyWarn")), // option：guardDefaultsKey 特例退役（逐键天然涵盖「①缺键→②」）
		source: firstOkPath,
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
	// wave 车二-8c（10-17·落码规格 2.5）：待办留存词——未闭待办标题含这些词时留到 45 天（与草案 Q26 交叠，只取标题词这一小块）
	lifeTodoHold: { re: "呈裁|交办|候.{0,6}裁", flags: "" },
	taskCompleted: { re: "task_completed", flags: "" },
	recallStopwords: { re: "已?(完成|办毕|办结|完结|更新|记录|说明|情况|事项|内容|工作|小结|汇总|总结|备忘|备注|处理|日常|杂项|其他|临时|测试|验证|整理|自查)", flags: "g" },
	// wave 车二-3（10-17·落码规格 2.3）：专名救援停用表——自动召回专名路、主题词补位与前瞻的西文单桥都不认这些词
	//   （诊 L65：of 中 offline；USD、KYC、本机路径段占两席）。整词匹配、不分大小写；开放集，memory organ可在数据文件里调
	//   盲审处置（should）：路径段泛词（users、admin、harness、home、tmp、private、var、library、applications）移出——路径段只由
	//   inject.cjs c2PathToks 按查询里真实的路径子串剔；放进全局停用表会误伤「${VAR}」「admin 权限」这类非路径用法（RS-026 #26873）
	pnRescueStop: {
		re: "^(?:of|the|to|and|for|with|usd|hkd|cny|rmb|hk|us|kyc|dsh|md|json|txt|log|pdf|png|jpg|http|https|www|com|cjs|js)$",
		flags: "i",
	},
	inboxPath: { re: "[/\\\\]inbox[/\\\\]|[/\\\\]inbox[/\\\\]", flags: "" },
	exemptShort: { re: "^(继续|同意|好|嗯|行|可以|OK|ok|多谢|谢谢|收到|是|对)\\s*[。.!！~～]?$", flags: "" },
	alertFamily: { re: "(?!)", flags: "" }, // AUDIT-FIX（A-D2）：原 "$^" 实测 test("")===true ⇒ 反向误判空标题
	sysAutoPrefix: { re: "(?!)", flags: "" }, // AUDIT-FIX（A-D2）同上
	// 车二-9 块一 1-a（10-25 落码·规格 wave第二批规格-20261024.md L133）：注入席要排的三类系统条判据。
	//   只排 J12 那三类（压缩锚／本窗任务账快照／压缩总结），handover note警报与 Dreamer 语义簇**不排**；
	//   nightly patrol用的 sysAutoPrefix 一字不动，免得悄悄放宽冲突豁免面。FLOOR 照 sysAutoPrefix 先例写 (?!)（永不匹配＝不误排），
	//   内部词面由包内 criteria-rules.default.json 供给。
	sysRowTitle: { re: "(?!)", flags: "" },
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
	// wave 车二-8a（10-17·落码规格 2.5）：生命周期保护判据三词表（lifeProtect 用）——
	//   lifeStateTitle 状态句(design note)；
	//   lifeMilAnchor directive锚词（与 extract.cjs _mil 同词）；lifePtrTitle 标题指针类（超出诊 L128 字面，可经 LEGION_LIFE_PTR_OFF=1 单关）
	// 盲审处置（should）补「未回、缺口、仅缺、还缺、尚缺」；处置十九收窄：「缺口」「未回」宽词会把实质结论、教训、业务事实（「海外业务四项缺口：真实库存…」
	//   「改码未回抄手册即传毒」）判成状态句、只认 essential 而到期老化——「缺口」只认过程态（零（新）缺口；缺口后紧跟已／仍／未／待／尚、全补／全闭／全治／全处置），
	//   「未回」不认未回溯／抄／退／滚／填／写／归／顾／头／收。本键管 lifeProtect、8-d 寿命分档、状态句不计命中与 8-c 不吃 30 天留存四处；askGateGap（闸内剔缺口条）另键不动
	lifeStateTitle: { re: "^[候待]|^本窗|^下一步|候您|待您|未回(?![溯抄退滚填写归顾头收])|零新?缺口|缺口(?:已|仍|未|待|尚|全(?:补|闭|治|处置))|仅缺|还缺|尚缺", flags: "" },
	lifeMilAnchor: { re: "军令|命令|指令|批示|裁决", flags: "" },
	lifePtrTitle: { re: "指针|落点|锚点", flags: "" },
	// ── 序2 三新键（design-approved「记忆面呈·脑附议并加两点）同意」·closure gate三案＋门控 P0 合车）──
	sameTopicOverlapMin: 3, // 门控同题判据：title token 交叠 ≥N 词 ⇒ 同题（与 pinboard-float.cjs 的 fam/inter>=3 同源常量——改须两侧同改＋drill 断言同值·防漂移）
	autoWaitBigramJaccardMin: 0.5, // 丁项（10-07 maintainer·四十七审再检查）：提炼 v2 fuzzy 查重字级 bigram Jaccard 阈——语序颠倒型兜底（实案 0.583·序数撞 0.692 由序数防护拦·贴阈误伤面 0.4 被挡——drill 断言同值）
	closureFreshDoneHours: 24, // ④-a 销号时效衰减：done todo 销号后 N 小时内不作闭环证据（⚠ 外推counter·首期只报数 closureGateFreshDoneSkip·跑一段取本机分布再定终值）
	essentialAnchorSeatDays: 3, // C-1③（09-15 批 C·maintainer）：锚点类（title 前缀「锚点：」）essential 条 ts 超 N 天退出常驻席（仍保标与 decay 免疫·只退席——治「一旦标记永久占席」·13/17 条 ts 早于 09-13 实证）
	closureAnchorStopwords: {
		// ① 停用表：锚定门 title 关键词剔除跨主题通用词——⚠ 开放集·治标（新通用词会再撞·非治本）；
		//   首期收词＝B2 v2.2 已治词（候/复盘/验收）＋ 09-14 误拦词（space/receipt/对账/整改/核销）
		re: "^(?:候|复盘|验收|space|receipt|对账|整改|核销)$",
		flags: "",
	},
	// wave 车二-1（10-17·落码规格 2.1）：开口前retrieval gate七键（askgate.cjs 用）——
	//   askGateAsk 直接索要句式；askGateWait 登记式(design note)；askGateSkip 宾语含这些词＝要的是决定不是信息（只用于 T-A、T-C）；
	//   askGateStop X 词停用（整词）；askGateSyn 闸内小同义表（源串按「;」「=」拆组，不进召回主路）；askGateGap 缺口条剔除（标题判）；askGatePtr 指针路词
	askGateAsk: {
		re: "(?:请|烦请|麻烦|劳烦|需要|还需|需)您[^。！？\\n]{0,8}?(?:提供|给(?:我|出)|发(?:我|来|一下)|告(?:诉|知)|补(?:充|给|一下)|上传|贴(?:出|一下)|报(?:一下|给我))|您(?:能否|可否|方便)[^。！？\\n]{0,6}?(?:提供|给|发|告诉|补|上传)",
		flags: "",
	},
	askGateWait: { re: "候您(?:提供|给|补|发|回|确认|亲填)?|待您(?:提供|给|补|发|回)|仅缺|还缺|尚缺|就差|(?:\\d+\\s*天)?未回", flags: "" },
	askGateSkip: { re: "裁|批|定夺|拍板|点头|选哪|同意|授权", flags: "" },
	askGateStop: {
		re: "^(?:您|提供|一下|信息|资料|需要|请|烦请|麻烦|劳烦|您|还需|告诉|告知|补充|上传|发来|给我|方便|能否|可否|还缺|仅缺|尚缺|就差|候您|待您|确认|一个|这个|那个|我们|你们|已经)$",
		flags: "i",
	},
	askGateSyn: { re: "住址=地址=门牌;进货价=采购价=成本价;重量=克重=称重=裸重;护照=资料页=证件;自拍=视频", flags: "" },
	askGateGap: { re: "^[候待]|候您|待您|未回|缺口|仅缺|还缺", flags: "" },
	askGatePtr: { re: "指针|落点|锚点|核验件|交接件", flags: "" },
};
// AUDIT-FIX-20260912（A-D1/A-I1·独立审计 A 路）：
//   ①**flags 白名单**——`.test()`/`.match()` 面消费的键一律剥掉 `g`/`y`（带 lastIndex 状态 ⇒ 连续调用
//     交替 true/false·实测复现 `true,false,true,false`）；只有**消费面为 `.replace()`** 的键（replace 自行
//     重置 lastIndex）才放行 g。剥掉的 flag **不静默丢弃**：计入 `CRIT_FLAGS_DROPPED`（模块级收集·debug 可读）。
//   ②`re` **不再 trim**：`^ ` 这类以空白为语义的正则会被 trim 放大匹配面；空白仅用于「是否为空」判定。
const CRIT_G_OK = { recallStopwords: 1 }; // flags 含 g 且**消费面安全**的键白名单（逐键声明·可 grep）
const CRIT_FLAGS_DROPPED = []; // 被剥 flag 的键名（审计可见性出口）
function critCompile(entry, key) {
	// 序2（design-approved·closure gate三案＋门控 P0 合车）：数值参数键直通——
	//   sameTopicOverlapMin/closureFreshDoneHours 系阈值非正则（三级链「第一个提供该键的层」语义对数字同样成立）
	if (typeof entry === "number" && Number.isFinite(entry)) return entry;
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
// ── 车二-9 块一 1-a（10-25 落码·规格「wave第二批规格-20261024.md」二·2.2 1-a）：注入面与召回面共用的现行谓词与系统条判法 ──
//   为什么落在这里：函数要用 CRIT（上面 L540 才初始化），本处已在 CRIT 之后；inject.cjs 与 read.cjs 经 core 取用。
//   CUR_SQL：①未到点（valid_to 为空或晚于参数）②未被取代（superseded_by 为空）③stale_state 不属 superseded-auto／review／retired。
//     参数只有一个 ?（当前时刻）；调用方按各自 SQL 的参数顺序插它。
//   isSystemRow：J12 三类系统条——标题合 CRIT 新键 sysRowTitle，或来源以 taskledger: 开头。只排这三类，
//     handover note警报与 Auto-Dreamer 语义簇不在 J12 内，不排；压缩锚照样能被 search 找到、能回流，只是不占注入席。
const CUR_SQL =
	process.env.LEGION_INJ_CUR_FILTER_OFF === "1"
		? "(m.valid_to IS NULL OR m.valid_to > ?)" // 回滚（规格 2.3）：只看到点，旧口径一字不差
		// 中枢 10-25 10:51 定乙：stale_state 只排 superseded-auto；review／retired 不排——
		//   二者是nightly patrol「超龄未命中」标记（patrol.cjs 3686–3701／3774，只标不改 status），不是作废；一次排掉约 1.07 万条
		//   （在役四分之一）会让「没被召回→标 review→更不会被命中→回不来」成黑洞，与maintainer 09-24 要的多条经验召回相反。
		: "(m.valid_to IS NULL OR m.valid_to > ?) AND m.superseded_by IS NULL AND COALESCE(m.stale_state,'') <> 'superseded-auto'";
function isSystemRow(row) {
	if (process.env.LEGION_INJ_SYSROW_OFF === "1") return false; // 回滚（规格 2.3）：J12 未裁时部署在 plist 设 1
	if (!row) return false;
	const src = String(row.source == null ? "" : row.source);
	if (src.startsWith("taskledger:")) return true; // 本窗任务账快照（index.cjs 落库处来源即 taskledger:<sid>）
	const re = CRIT.sysRowTitle;
	return !!(re && re.test(String(row.title == null ? "" : row.title)));
}
// ── E 甲′（2026-09-24 落码车）：source 会话归属前缀保护·白名单正则（模块级·十五审甲路指认原方案件未声明位）──
//    形态：session-<sid>（含缩略形）／auto:session-…／manual:…／taskledger:…／patrol:… 均放行·其余非空 source 走覆盖计数路
const SRC_OK_RE = /^(?:session:|auto:|mirror:|manual:|taskledger:|patrol:|patrol-sentinel:|split:|handwrite$|dreamer$|session-[0-9a-f]{8}(?![0-9a-zA-Z-]))/; // 十七审候裁⑥：8hex 尾锚;
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
// Space-routing word list: emptied in the public build.
const ROUTE_XHS_RE = /$^/;
// Tuning constants: rescue-path top score & low-confidence abstain threshold.
const PN_RESCUE_SCORE = 0.99;
const ABSTAIN_TOP1_MIN = 0.15;
// Per-session extract parse-failure streaks (give up a segment after 3 in a row).
const EXTRACT_PF_STREAK = new Map();
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
// ══ 注入面净化（design-approved三件套）════════════════════
//  事故：宿主 @deepseek-ai/dsh-system-prompt 对 context/section 文本做**二次模板解析**
//  （GROUP_AT 匹配「双花括号＋中间任意非花括号字符」＋ VARIABLE_NAME=/^[a-z][a-z0-9_]*$/，
//   命中即抛 malformed 并**整轮崩**——错在宿主渲染期，插件侧 try/catch 拦不住）。
//  而本插件注入块直接拼接记忆库 title/content（数据即模板）⇒ 库内**一条**含字面花括号组的
//  数据即可瘫痪整窗（session-657518bd 自 turn28 起四轮连败log·凶器  标题）。
//  治法：凡拼入 context 文本的外部数据，出境前把「连续双花括号」破坏为不可解析形态
//  （插入零宽 U+200B）——显示形态不变（零宽不可见）、解析形态断裂（宿主正则不再匹配）。
const PROMPT_SAFE_STAT = { hits: 0 }; // 注入面净化命中计数（模块级·stats 面读取·四点接线）
function promptSafe(text) {
	const s = String(text == null ? "" : text);
	if (s.indexOf("{{") < 0 && s.indexOf("}}") < 0) return s; // 快路径：无花括号者零改动零开销
	PROMPT_SAFE_STAT.hits += 1;
	return s.replace(/\{\{/g, "{\u200b{").replace(/\}\}/g, "}\u200b}");
}
function promptSafeContext(build) {
	return promptSafe(build()); // 注入块 text 出口统一净化（返回值语义不变·异常照抛）
}
// 危险组判据（哨用·与宿主 GROUP_AT 同族）：任何「连续双花括号组」都会被宿主当模板变量解析——
// 空名/非法名 ⇒ malformed 崩；合法名但未注册 ⇒ unknown variable 崩。故判据=完整组存在性（不看名字内容）。
function hasUnsafePromptRef(text) {
	return /\{\{([^{}]*)\}\}/.test(String(text == null ? "" : text));
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

// ── obs 归一总账（design-approved「批」·memory organ自办·设计件 §6.2 obs 归一总账）──────────
//   实勘settled（10-09）：render/stats 键实测 243＝「子系统×状态」二维矩阵（设计九类预览仅覆盖 25%）——
//   本条归一的**类目视图**：不以删键为手段（零回归），新增 stats 分组面 obsGroups＋未归类计数。
//   规则顺序敏感（先命中先归）；未命中落「未归类」——未归类非零＝新子系统出现/规则待扩（观测信号）。
const OBS_GROUPS = [
	["板账", /板|board|wrap-up/i],
	["压缩桥", /压缩|总结入册|账快照|记忆活轮|记忆总轮|seen|桥(?!接)/i],
	["注入", /注|inject|blk|词路由|常识/i],
	["安检", /安检|敏感|凭|sec/i],
	["同题", /同题|dup|fuzzy|候|并|撞|门控|糊同降/i],
	["回声", /回声|echo/i],
	["常驻席", /常驻|席|essential|锚|专名救|主题词救/i],
	["提炼", /a25|^丢$|chase|buf|提炼|extract|蒸馏|水位|待抽|块回补|缓冲|解析败|done证|召回|补嵌/i],
	["夜巡", /patrol|nightly patrol|巡|A\d|^(?:截断|池|采样|游标|游标错)$|跳|扫钩|扫挂/i],
	["进化史", /evo|mirror|Wm|spoken|进化键/i],
	["图边", /图|边|社区|ppr|cooccur|hop|entity|共现|三元|双活|序号冲|语义冲|判据分叉|cross-space/i],
	["检索", /vec|fts|rrf|rerank|recall/i],
	["防护", /哨|冲突|conflict|禁|拦|拒|魂|高压|闸|遥测|积压|压力读|写增|质量仪|周报|主写失败|拆键|双验/i],
	["归档", /归档|g2|docs|剥离/i],
	["文书", /收件|inbox|流转|呈裁|pending ruling/i],
	["usage", /usage|hit|nudge|权|提示|听从|信号/i],
	["前瞻", /梦|dreamer|前瞻|fore/i],
	["token量", /tok|字节/i],
	["时钟", /轴|钟|时|天|日|年|龄|钳|回落窗|回读/i],
	["游标族", /游标|cursor|fam|hisim|Saga|signal|bridge|proper|拾取/i],
	["记忆库", /stale|TTL|validated|vc|容量|帽|降格|沉底|aged|销席|失效条|墓碑|元清理|清账|预裁|复核|闭环闸|单体检|工作区|归类|^\s*obs/i],
];
const OBS_KEYS = new Set(); // render 键自省登记集（_t 内 add·render 跑过即得全键清单·零维护）
function obsGroupOf(key) {
	const s = String(key == null ? "" : key);
	for (const [g, re] of OBS_GROUPS) if (re.test(s)) return g;
	return "未归类";
}
function obsLedger(keys) {
	const groups = {};
	let total = 0;
	for (const k of Array.isArray(keys) ? keys : []) {
		if (!k) continue;
		total++;
		const g = obsGroupOf(k);
		groups[g] = (groups[g] || 0) + 1;
	}
	return { total, groups, unclassified: groups["未归类"] || 0, groupCount: Object.keys(groups).length };
}

function obsLedgerFromRender() {
	// render 键自省账：OBS_KEYS 为空（本进程尚未渲染过工具卡）时主动跑一次 memoryRender 以登记全键（值传空对象·只构造 lines·零外部副作用）。
	if (OBS_KEYS.size === 0) { try { memoryRender({ action: "stats" }, {}); } catch (eOR) { void eOR; } }
	return obsLedger([...OBS_KEYS]);
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
					(v.triplesKgStopped ? "（已停建·开关默认停）" : "") + // ⑤ 三元组停建（10-24 fu-nao/order-fix）：计数旁注
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
				OBS_KEYS.add(String(label)); // obs 归一总账（10-09 maintainer·memory organ自办）：render 键自省登记（render 跑过即得全键清单·零维护零 IO）
				if (n) _tl.push(label + " " + n);
			};
			_t("压缩seen", v.compactionSeen);
			_t("桥bridged", v.compactionBridged);
			_t("chase跑", v.compactionChaseRuns);
			_t("chase错", v.compactionChaseErrors);
			_t("bridge错", v.compactionBridgeErrors);
			_t("总结入册", v.compactionSummarized);
			_t("板镜像", v.boardMirrored);
			_t("板镜跳", v.boardMirrorSkipped);
			_t("板镜像错", v.boardMirrorErrors);
			_t("板回写", v.boardWriteBack); // 架构 C（10-04）：board_write 回写 todo/write 事件数
			_t("板回写跳", v.boardWriteBackSkip); // append 不可达跳过
			_t("板回写错", v.boardWriteBackErrors); // 回写异常
			_t("板自源跳", v.boardMirrorSelfSrc); // ⑩A 自源事件不镜像（防降格覆写）
			_t("板态取错", v.boardSnapErrors); // 二十四审⚠️1 修：⑩C 板态取数错（render 面·非零即显）
			_t("工作区取错", v.wsSnapErrors); // O2（09-30 压缩面优化车）：工作区快照失败计数（F5·非零即显）
			_t("板账漂移", v.boardLedgerDrift); // L2（10-01）：②真漂移（板 completed ∧ 账 active·方向订正版·非零即显）
			_t("板账取错", v.boardLedgerErrors); // L2：三桶计算异常
			if (boardHintOn()) {
				// 板机制六缺口车·甲批（10-18）：缺口 2 滞后（板未完∧所open item已非 active·按板项计）、缺口 3 未open item拆两键（无号／号不中 todo 账）——总闸关不调（OBS_KEYS 与本车前同）
				_t("板账滞后", v.boardLedgerLag);
				_t("板账无号", v.boardLedgerUnlinkedNoId);
				_t("板账错号", v.boardLedgerUnlinkedBadRef);
			}
			_t("账快照", v.ledgerSnapshots);
			_t("a25锁", v.a25FlushErrors);
			_t("a25chase错", v.a25ChaseErrors);
			_t("entity错", v.entityExtractErrors);
			_t("g2归档错", v.g2ArchiveErrors);
			_t("压缩安检拒", v.compactBridgeSecReject); // 批⑰ 压缩直插路安检拒计数
			_t("压缩敏跳", v.compactBridgeSensitiveSkip); // 批⑰ 压缩直插路敏感闸跳计数
			_t("mirrorWm错", v.mirrorWmErrors);
			_t("召回错", v.autorecallErrors);
			_t("召回钩死", v.autorecallHookDead); // 批⑲ P1：召回双钩注册期异常（0=两钩注册成功）
			_t("扫挂错", v.sweepCallErrors); // v4b（10-03）：sweep turn/end 调用期异常兜底计数（⑬ render 接线）
			_t("扫钩死", v.sweepHookDead); // v4b（10-03）：sweep session/event 挂载注册期异常（同召回钩死族）
			_t("巡跨进程跳", v.patrolCrossSkip); // 批⑲ P2：nightly patrol跨进程互斥跳次数
			_t("截断", v.a12Truncated); // 批⑲ P5：账 400 帽截断计数
			_t("专名救", v.autorecallPnRescue);
			_t("主题词救", v.autorecallKwRescue);
			_t("vec兜底", v.autorecallVecFallback); // 柱环4/梁4 预备：自动召回三路救援触发数 render 消费补齐（原 KwRescue/VecFallback 有 return 无 render=模型不可见·PnRescue 连 return 都缺·2026-09-11 同车补齐·⑬ 四点接线）
			_t("席互斥", v.autorecallSeatDedup); // E-2（2026-09-12）：注入席互斥剔重数——同条不入「待办席」与「召回区」（ 型两席病灶观测位）
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
			_t("池", v.a10PoolSize); // P0-4（09-12）：候选池规模（active ∧ 有向量⁻排除 pending 对）
			_t("采样", v.a10Sampled); // 采样态实际参与条数（池 ≤ 帽时＝池规模·全池两两）
			_t("帽跳", v.a10SkippedDueToCap); // 被帽跳过未作 i 侧的条数（**>0 ⇒ 本巡为采样态**·原整段跳过时此面恒空）
			_t("signal错", v.signalErrors);
			_t("buf恢复错", v.bufferRestoreErrors);
			_t("validated错", v.validatedErrors);
			_t("语义边错", v.semanticEdgeErrors);
			_t("语义边帽跳", v.semanticEdgeSkippedDueToCap); // 批⑰ 降采样轮转面（本夜池越帽未入窗条数·0=全池在窗）
			_t("块回补", v.chunkBackfillDone); // 批⑰ B6：chunks nightly patrol回补入量（LEGION_CHUNK_BACKFILL_ON 闸·默认关）
			_t("块回补错", v.chunkBackfillErrors);
			_t("压力读错", v.pressureReadErrors);
			_t("spokenWm错", v.spokenFillWmErrors);
			_t("注入净化", v.promptSafeHits); // 2026-09-13 注入面净化车：拼入 context 前破坏「连续双花括号」的命中计数（0=库内无雷可净）
			_t("extractEv错", v.extractEventsErrors);
			_t("提炼解析败", v.extractParseFail); // 批⑰ 提炼三档解析全败计数（水位不推·下轮重抽面）
			_t("解析败弃段", v.extractParseFailGiveUp); // 批⑱ K1-3：连续 3 败放弃段计数（丢账留痕）
			_t("闭环闸错", v.closureCheckErrors);
			_t("闸拦", v.closureGateBlocked); // 序2：closure gate拦截总数——B-2 订正：done证≥5,000 且仍 0 才判失效（换班窗未满=insufficient-data）
			_t("单锚放", v.closureGateAnchorWeak); // 序2②：门槛 1→2 副作用观测
			_t("done证", v.closureGateDoneEvidence); // 序2④-a：证据池 done 累计——B-2 量纲注：**次·非条**（调用×每次扫到条数乘积·非去重·不可直推豁免率）
			_t("鲜销跳", v.closureGateFreshDoneSkip); // 序2④-a：时效豁免命中——B-2 量纲注：同上（次·非条·N 标定须去重口径另立·排批 C）
			_t("序号冲", v.closureGateSeqClash); // 判定精确化（#31515·10-06 maintainer）：踵次/编号冲突放行（同单位不同数字⇒不同题）
			_t("鲜销跳条", v.closureGateFreshDoneIds); // B-2②（批 C 候办）：④-a 豁免**去重条口径**（当日 Set size·量纲=条——N=24 标定用此键非上行）
			_t("hit错", v.hitMapErrors); // B-1④（批 C 候办）：hitMap 攒批/落盘失败（原空 catch 静默→可观测）
			_t("门控拦观", v.gateBlocked); // 序2：salience 观测态（首期只计数）
			_t("门控合并", v.gateMerged); // 序2：同题合并路命中
			_t("门控vc", v.gateVcBumped); // 序2：合并路 vc bump
			_t("门控边错", v.gateEdgeErrors); // 审计 A③（09-15）：B 升级路边失败
			_t("门控落回退", v.gateSpaceFallback); // A①（09-15）：sessionOrgan 判不出→词面/global 回落
			_t("提炼降格", v.autoTodoClosedSkip); // 批⑯（09-21）：extraction gate todo→fact 降格实落计数（禁新鲜豁免面）
			// ── 2331 step render（design-approved·四点接线第 3 点）──
			_t("禁立降格", v.autoTodoV2Demote); // 刀一①：auto todo 非候语义降 fact 计数
			_t("候并立", v.autoWaitMerged); // 刀一②：候面同题并而不立计数
			_t("候转正", v.autoWaitPromoted); // 刀一②：候语义 fact→todo 装计数
			_t("止血降格", v.autoStateDemote); // 补丁车项2（五十审 🟡-3）：四点接线 render 位
			_t("巡闸跳", v.patrolGateSkip); // nightly patrol写入闸拦截计数（写入面修·R1 车·接线③render）
			_t("糊同降", v.autoWaitFuzzyDup); // 丁项（10-07 maintainer·四十七审再检查）：候面 fuzzy 同题→fact 降格防双立计数（无损·宁漏勿误·非零即显）
			_t("候序撞", v.autoWaitSeqClash); // 丁项：序数对撞放行计数（踵|审|刀|案|批|件|车|令 同单位不同号⇒不并）
			_t("销叙跳", v.saoNarrSkip); // 刀一③：销叙事疑义才写 skip 计数
			_t("钳年拒", v.evYearClamped); // 年份钳制（10-04）：|event_at 年−ts 年|>1 拒记计数（提炼面）
			_t("钳年拒·写", v.mwYearClamped); // 年份钳制·手写面（2317 §二）：memory_write 传 event_at |年差|>1 拒记计数（organ_meta 键 mw_year_clamped·四点接线 render 面）
			_t("钳年改", v.evYearRewritten); // 年份改写·提炼面（10-06 1907 §一·maintainer按荐）：|=1 且月日同 ts ⇒ 改年计数
			_t("钳年改·写", v.mwYearRewritten); // 年份改写·手写面（10-06 1907 §一·与提炼链同款）：organ_meta 键 mw_year_rewritten·原子式
			_t("wrap-up警", v.boardCloseWarn); // wrap-up断点（板 v2 余面⑤）：board_write 全毕态检出板账漂移（板已毕∧账 active）提示计数（organ_meta 键 board_close_warn·四点接线 render 面）
			_t("obs未归类", v.obsUnclassified); // obs 归一总账（10-09 maintainer·memory organ自办）：stats 键按 20 类归组后未归类键数（0=规则全覆盖·非零=新子系统出现/规则待扩）
			_t("边沉底", v.edgeSinked); // 刀三：被 solved_by/duplicate_of 指向条检索沉底计数
			_t("沉底图错", v.validToMapErrors); // 批⑰ C13：valid_to 沉底面读取失败计数（裸 catch 补出口）
			_t("锚点退席", v.essentialSeatAgedOut); // C-1③（09-15 批 C）：锚点类超期退席计数（治「永久占席」）
			// ── PPR 四键 render(design note)──
			_t("图建ms", v.pprBuildMs); // PPR 建图耗时（ms·30s 缓存跨题复用）
			_t("图边数", v.pprEdges); // 入图活边数
			_t("图查ms", v.pprQueryFactorMs); // 查询因子块耗时（ms）
			_t("图短路", v.pprShortCircuit); // 🔴 双语义（B-4 settled·防 D13 型误读）：沙箱/新库态非零＝正常（无图短路）；生产态非零＝建图失败（异常）
			_t("魂线警", v.sensitiveWarnCount); // option第二步（09-15）：L2 魂红线词面 warn 计数
			_t("判据分叉", v.guardRulesDrift); // 序2最小刀：guard ①②级分叉键数
			_t("写路拒", v.writeRejectedCount); // 序2最小刀：写路安检拒计数
			_t("回声观测", v.echoWarnCount); // Y1（09-26 maintainer·十八审候裁四小项）：fact/lesson 已办回声观测计数（非零即显·零静默）
			_t("软同题", v.softSameTopicWarn); // Y1：C 闸软同题观测计数（闸①同判据·观测不阻断）
			_t("板行清", v.boardRowsPruned); // 丁（09-27 maintainer）：板行 GC 删除数（非零即显·零静默）
			if (v.boardGcSkipRows !== undefined) _t("板行留", v.boardGcSkipRows); // 板机制六缺口车·乙批（10-18）：设 LEGION_BOARD_GC_OPEN_DAYS 后nightly patrol GC 留下的超 30 天未完板数（organ_meta 键 board_gc_skip_rows·本轮值）；键不在不调
			_t("流转滞", v.dispatchStale); // C 件（09-27 approved）：cross-space流转 48h+ 滞留数
			_t("流转呈滞", v.dispatchStale7d); // C 件：7d+ 滞留(design note)
			_t("流转帽", v.globalDispatchCap); // A 件（二十二审🔴3 订正·四点补齐）：数量帽触发计数
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
			_t("质量仪错", v.usageQualityErrors); // 检索质量仪表（09-12 第一步）：聚合异常（⑬ 四点接线④render）
			_t("写增错", v.writeTimeEnhanceErrors); // 09-12 主案：写时增强（嵌入/建边）异常
			_t("提炼链", v.writeTimeReflectLinked); // 09-12 主案：提炼路反思建边命中数
			_t("补嵌跑", v.vecBackfillRuns); // 向量补嵌可见化（09-12 第二步诊断）：跑次数/成功条/无凭据跳过/异常
			_t("补嵌条", v.vecBackfillFilled);
			_t("补嵌无凭", v.vecBackfillNoCred);
			_t("补嵌错", v.vecBackfillErrors);
			_t("注入错", v.injectErrors);
			_t("注入块跳过", v.injectBlockErrors); // 批⑳ J8-7：注入面十处块级降级计数（⑬ render 接线）
			_t("拆键写", v.a25KeysWritten); // 批⑳ J7-8：extract_buffer 拆键后每次 flush 实写键数
			_t("墓碑清", v.a25TombCleared); // 2026-09-22 丁路F-4：墓碑清删数（本趟实删盘键）
			_t("墓碑跳", v.a25TombSkipped); // 2026-09-22 戊路W-1：放弃/闸拒跳过的墓碑数
			_t("vec通道错", v.vecChannelErrors);
			_t("vec超帽跳", v.vecSkippedDueToBudget); // 批㉙ option：vec 臂扫描超帽降级计数（>0＝预算帽已触发·LEGION_VEC_SCAN_CAP 调或库减脂议）
			_t("帽接近", v.vecCapNear); // 帽接近度哨（10-01）：chunks/cap ≥80% 未超帽预警（教训 #25409/#25411 码化·非零即显）
			// 帽接近度哨族（10-01·approved·库量驱动型 5 顶补齐·四点接线之 render 面·非零即显）
			_t("帽近·fam", v.familyCapNear);
			_t("帽近·hisim", v.hisimCapNear);
			_t("帽近·spk", v.spokenCapNear);
			_t("帽近·chunk", v.chunkCapNear);
			_t("帽近·vpool", v.vecPoolCapNear);
			_t("帽超·vpool", v.vecPoolCapOver); // 超帽对偶观测（10-02·截断封顶态非零即显）
			_t("帽近键错", v.capNearKeyErrors); // 落键失败不静默（⑬ 出口）
			_t("nightly patrol账错", v.patrolLedgerErrors); // R路车一⑤：patrol_last 主写失败计数（>0＝水位根基受损·J4-3 出口补齐）
			_t("主写失败标", v.__ledgerFail ? "⚠" + String(v.__ledgerFail) : undefined); // 车三件③（W7 标记法）＋车三审即修：本条 patrol_last 系主写失败后末段补写产物（混装可辨）——stats return 已透出字符串（at ×n·甲⚠1/乙🔴2 哑键根治）
			_t("跳过键错", v.vecScanSkipKeyErrors); // 车一补丁（补审 B-21）：vec_scan_skip 落键失败计数（原空吞）
			_t("fam帽跳", v.familySkippedDueToCap); // 车二件1a：family 桶超帽 i 侧跳过数（>0＝降采样在跑·LEGION_FAMILY_CAP 可调）
			_t("hisim帽跳", v.hiSimSkippedDueToCap); // 车二件1b：高相似桶超帽 i 侧跳过数（同上·LEGION_HISIM_CAP）
			_t("跳闸", v.a19SkippedDueToCap); // 车二件2：recent>2000 跳闸计数（「未跑/跑了0」可辨）
			_t("魂豁免压", v.soulExemptSuppressed); // 车二件4：豁免窗压「返回空」形态计数（死变量激活可观测）
			_t("哨抑", v.sentryAlarmSuppressed); // 车二补丁乙（十八审甲W1/乙-1）：light sentry同源被抑计数——原漏 render 行（4/5 失真）
			_t("哨自愈", v.sentryTtlHeal); // 漏点即修⑤（W5）：存量哨条 TTL 自愈条数（R15c 判据的伴生读数）
			_t("哨写错", v.sentryWriteErrors); // 漏点即修④（W4·哨的哨）：light sentry写库失败计数
			_t("rerank错", v.rerankErrors);
			_t("帽动态", v.queryCapDyn); // 帽分治：>8 放宽生效次数（与「帽保持」合看动态占比）
			_t("帽保持", v.queryCapKept); // 帽分治：≤8 保持次数
			_t("proper拾取", v.rerankProperPicked); // option观测位接线④（brain 10:3x 报件· 四点接线闭）：D 项 proper 类 rerank 拾取累计——非零即显·零静默
			_t("有边率", v.edgeCoveragePct); // 三哨①：目标≥80%（车三标尺）·D5 接线③——null/0 静默（零扰动纪律）·未测态由 return 层 edgeCoverageMeasured 键承载（T0b 可见）
			_t("哨错", v.sentryErrors); // 三哨计算故障计数
			_t("计数闸拦", v.countSkipCount);
			_t("回落窗", v.evoFallbackWindows);
			_t("evo入", v.evoMirrorIns); // E-4（09-12 批 A）：evo-mirror 本巡入条数（与「evo跳」同现 0/0 ⇒ 空转可辨）
			_t("evo跳", v.evoMirrorSkip);
			_t("stale池", v.stalePoolNow); // E-5（09-12 批 A）：当日已越 30d 线条数
			_t("stale+7", v.stalePoolT7); // 未来 7 天越线**累计**预测（含已越）
			_t("stale+14", v.stalePoolT14);
			_t("stale+30", v.stalePoolT30); // 洪峰曲线上界（**活数·禁写死**：口径与复算指针见手册 §五 洪峰行与 E-5 段注释；09-13 审计N1 订正）
			// E 主车（09-12 approved·E 洪峰容量化·⑬ 四点接线②render）：三观测位＋带宽/错误面
			_t("复核池", v.staleReviewPoolSize); // 池水位（review **待裁面**·option优先级已前置）
			_t("stale流转", v.staleAutoRetired); // 出海口吞吐（本巡 review→retired·option 60d 二级线）
			_t("pending ruling送", v.staleSinkSent); // 本巡pending ruling清单送出数（option·限额 N=LEGION_STALE_SINK_N）
			_t("pending ruling余", v.staleSinkBandwidth); // 带宽余量（0=打满·仍在洪峰；>0=池已见底）
			_t("stale容错", v.staleSinkErrors); // ⑬ 出口（E 主车段异常）
			// E-7（09-12 批 A·S4 订正）：API token 用量**五类出网**（嵌入／主提炼／前缀／精排／AUDN 预裁）
			//   ——进程累计·重启清零·**下界口径**（不含失败/重试消耗；`tok无usage`/`tokhttp错` 为口径可解释性）
			_t("tok嵌", v.apiUsageEmbed);
			_t("tok提", v.apiUsageExtract);
			_t("tok前", v.apiUsageSpoken);
			_t("tok排", v.apiUsageRerank);
			_t("tok总次", v.apiUsageCalls); // S10：带 usage 的成功响应数（原唯一有 return 无 render 者）
			_t("tok预裁", v.apiUsagePreverdict); // D2：AUDN 预裁（与主提炼分家）
			_t("tok无usage", v.apiUsageNoUsage); // D2/D8：成功但无 usage 字段——下界口径的可解释性
			_t("tokhttp错", v.apiUsageHttpErr); // D2/D8：失败/重试次数（option：错误类须有真自增点者入 render）
			_t("游标", v.a10RotSlot); // D8：轮转游标（判覆盖的关键量·原「四点接线」缺 render 位）
			_t("游标错", v.a10RotCursorErrors); // F2（二轮审计）：游标读/写失败计数（原死键）
			_t("fam游标错", v.famRotCursorErrors); // 十九审即修①（甲⚠1·四点补齐照族内范式）：family 游标读/写失败
			_t("hisim游标错", v.hiRotCursorErrors); // 十九审即修①：hisim 游标读/写失败
			_t("巡落账错", v.patrolLastPatchErrors); // F2：nightly patrol末段观测位补写失败计数（原死键）
			_t("cross-space聚错", v.crossOrganErrors); // （09-16 三案）：cross-space聚合/注入行失败计数
			_t("进化键错", v.evoMetricErrors); // （09-16 独立审计 P2）：五键段异常计数（原空吞·冻结平台防线）
			_t("前瞻推教", v.forelookLessons); // （09-16 三案）：前瞻召回推送教训条数（两周counter）
			_t("前瞻错", v.forelookErrors); // 前瞻召回失败计数
			_t("stale池错", v.stalePoolErrors); // D8：E-5 聚合异常计数（option错误类）
			// E-1（09-12 批 A）：注入总账（字节·最近/峰值）——审计「14 路零总预算」的账本
			_t("注ctx", v.injectCtxBytes);
			_t("注wf", v.injectWfBytes);
			_t("注峰", v.injectTotalMax); // S2：**峰值上界**（ctxMax+wfMax·非同轮实际量——跨轮拼接已废）
			_t("注超", v.injectOverBudget); // 只在设了 LEGION_INJECT_BUDGET_BYTES 且越线时非零
			// 注入瘦身车 W4（10-11·maintainer 10-03 方案 1324 四案＋统筹审核 W4·依据 设计/注入瘦身/落码task brief.md §3.10）：同轮账、按轮冻结、
			//   帽、D 案下沉两数与持久四键——标签一律以「注」起头（OBS_GROUPS 归「注入」·避开前两条规则的「板／压缩／桥」等字）
			_t("注轮", v.injectTurnBytes); // 同轮合并字节（最近一笔·状态段＋召回段）
			_t("注轮峰", v.injectTurnMax);
			_t("注轮p50", v.injectTurnP50); // 进程内环 512 笔·重启清零（长期基线以会话档为准）
			_t("注轮p95", v.injectTurnP95);
			_t("注冻命", v.injectFreezeHits); // 按轮冻结：同轮后续步原样返回的步数
			_t("注冻破", v.injectFreezeBreaks); // 白名单事件致状态段同轮重算次数
			_t("注冻未知", v.injectFreezeUnknown); // 无真 sid／无会话记录／首个 turn/start 前——退回每步重算
			_t("注补结", v.injectSettleLate); // 上一步未经钩子结算、由下一步状态段入口补结（重启后验收应为 0 增量）
			_t("注帽超", v.injectOverCap); // 同轮合并字节越帽（缺省只计不裁）
			_t("注保超", v.injectOverCapProtected); // 可选类退完仍超帽（保护项照发）
			_t("注裁", v.injectTrimmed); // LEGION_INJECT_TRIM_ON=1 时退出的可选项数
			_t("注召重", v.injectRecallDedup); // LEGION_INJECT_XDEDUP_ON=1 时因状态段已显而剔掉的召回侧条数
			_t("注计错", v.injectMeterErrors); // injectMeter.errors 出口（原只赋值无出口·RI-10）
			_t("注轴48h", v.injAxisFresh48); // D 案：轴计数移出注入·两数归 stats
			_t("注轴30d", v.injAxisAged30);
			_t("注召截累", v.injectRecallCutTotal); // 持久（organ_meta·跨重启）：保守 C 被截条数
			_t("注截读累", v.injectRecallCutReadTotal); // 持久：被截条随后被 read_episodic（漏召代理）
			_t("注召读累", v.injectRecallShownReadTotal); // 持久：已显条被 read_episodic
			_t("注轮中令累", v.injectMidTurnUserClaimsTotal); // 持久：轮中真用户消息被 claim 次数（白名单③）
			_t("闸未配", v.spaceGateUnconfigured);
			_t("预裁毕", v.conflictsPreverdicted);
			_t("蒸馏行", v.assistantDistillLines);
			_t("消解并", v.entitiesResolved);
			_t("预裁错", v.conflictsPreclassifErrors);
			_t("预裁API败", v.preverdictApiFails); // 10-24 加活②（#45875 停因三查第四节 2）：预裁端点失败（401/403/404 熔断＋非可重试或末次；10-25 起另含 fetch 抛错末次）计数；非零即显
			_t("社区簇", v.communitiesBuilt);
			_t("Saga摘", v.sagaSummaries);
			_t("Saga错", v.sagaSummaryErrors); // 件5 审计修①：render 透出补三标签（社区簇顺带补）
			_t("TTL退", v.ttlExpired); // 批⑲ P6 接线（原断线键）
			_t("Dreamer簇", v.dreamerClusters); // 批⑲ P6 接线（原断线键）
			_t("死vec清", v.deadVecCleaned); // 批⑲ P6 接线（原断线键）
			_t("死vec错", v.deadVecSkipErrors); // 批⑲ P6 接线（原断线键）
			_t("常识变", v.commonsenseChanged); // 批⑲ P6 接线（原断线键）
			_t("常识首", v.commonsenseFirstSnap); // 批⑲ P6 接线（原断线键）
			_t("常识错", v.commonsenseSkipErrors); // 批⑲ P6 接线（原断线键）
			_t("冲突超期", v.conflictPendingStale); // 批⑲ P6 接线（原断线键）
			_t("冲突挂", v.conflictsPending); // 批⑲ P6 接线（原断线键）
			_t("积压错", v.backlogErrors); // 批⑲ P6 接线（原断线键）
			_t("FTS重错", v.ftsRebuildErrors); // 批⑲ P6 接线（原断线键）
			_t("FTS重行", v.ftsRebuildRows); // 批⑲ P6 接线（原断线键）
			_t("回锚失", v.episodicAnchorMiss); // 批⑲ P6 接线（原断线键）
			_t("清账错", v.sweepErrors); // 批⑲ P6 接线（原断线键）
			_t("单体检", v.singleBodyWarns); // 批⑲ P6 接线（原断线键）
			_t("待抽条", v.pendingMsgsCount); // 批⑲ P6 接线（原断线键）
			_t("水位位", v.watermarkAdvanced); // 批⑲ P6 接线（原断线键）
			_t("丢", v.a25DroppedUnextracted); // 批⑲ P6 接线（原断线键）
			_t("inbox自", v.noteIngestAuto); // 批⑲ P6 接线（原断线键）
			_t("inbox件", v.noteIngestFiles); // 批⑲ P6 接线（原断线键）
			_t("边保活跳", v.edgeKeepAliveSkips); // 批⑲ P6 接线（原断线键）
			_t("跳", v.a10SkipErrors); // 批⑲ P6 二档（原半接线）
			_t("auto边", v.autoEdges); // 批⑲ P6 二档（原半接线）
			_t("积压账", v.backlogAccounts); // 批⑲ P6 二档（原半接线）
			_t("积压段", v.backlogSegments); // 批⑲ P6 二档（原半接线）
			_t("双活链", v.bothValidLinked); // 批⑲ P6 二档（原半接线）
			_t("缓冲复", v.bufferRestored); // 批⑲ P6 二档（原半接线）
			_t("边覆盖测", v.edgeCoverageMeasured); // 批⑲ P6 二档（原半接线）
			_t("边产死键", v.edgeYieldDeadKey); // 批⑲ P6 二档（原半接线）
			_t("边迁", v.edgesMigrated); // 批⑲ P6 二档（原半接线）
			_t("回读次", v.episodicReadCount); // 批⑲ P6 二档（原半接线）
			_t("回读节流", v.episodicThrottled); // 批⑲ P6 二档（原半接线）
			_t("末共现对", v.lastCooccurPairs); // 批⑲ P6 二档（原半接线）
			_t("末提炼", v.lastExtractAt); // 批⑲ P6 二档（原半接线）
			// 2026-09-22 巡检修（#18937 漏点 3）：linkSentinel 系对象——摘要透出（lib/pc/警），禁原样拼接（[object Object] 盲位）；null→0 守「全零零扰动」
			_t("链哨", v.linkSentinel ? ("lib" + (v.linkSentinel.libCount ?? "?") + "/pc" + (v.linkSentinel.pcCount ?? "?") + (v.linkSentinel.alarm ? "/警" : "")) : 0);
			_t("记忆活轮", v.memoryActiveTurns); // 批⑲ P6 二档（原半接线）
			_t("记忆总轮", v.memoryTotalTurns); // 批⑲ P6 二档（原半接线）
			_t("合并执", v.mergeExecuted); // 批⑲ P6 二档（原半接线）
			_t("提示险", v.promptUnsafe); // 批⑲ P6 二档（原半接线）
			_t("词路由", v.routedCount); // 批⑲ P6 二档（原半接线）
			_t("语义边", v.semanticEdges); // 批⑲ P6 二档（原半接线）
			_t("信号挂", v.signalArmed); // 批⑲ P6 二档（原半接线）
			_t("信号计", v.signalCount); // 批⑲ P6 二档（原半接线）
			_t("信号触", v.signalTriggered); // 批⑲ P6 二档（原半接线）
			_t("三元入", v.triplesIn); // 批⑲ P6 二档（原半接线）
			_t("vc升", v.validatedUp); // 批⑲ P6 二档（原半接线）
			// SelRoute：六类分布非零即显（⑬ 精神——观测面 render 可见·全零零扰动）
			if (v.queryClassDist) {
				const qc = Object.entries(v.queryClassDist).filter(
					([, n]) => Number(n) > 0,
				);
				if (qc.length)
					_tl.push("分类 " + qc.map(([k, n]) => k + "×" + n).join("/"));
			}
			// ── 检索质量仪表（09-12 第一步·maintainer「按建议逐项」）：近 7 天 search 真命中成分一行（非空即显·零样本扰动）──
			//    读法：词面=词面通道真命中条占比·语义=纯语义条(ftsBase=0∧vecPart>0)·兜底=社区泛化/前缀保底/主题词重查注入；
			//    「词零命中」=该次检索词面通道零命中（=向量/图的价值面）；「仅兜底」=词面与语义双双落空（全靠兜底凑数）。
			if (v.usageQuality) {
				const _uq = v.usageQuality;
				lines.push(
					"  📊 检索质量（近 " +
						_uq.days +
						" 天 search " +
						_uq.n +
						" 次）：词面 " +
						_uq.lexPct +
						"%·语义 " +
						_uq.vecPct +
						"%·兜底 " +
						_uq.fbPct +
						"%｜词零命中 " +
						_uq.lexZeroPct +
						"%·仅兜底 " +
						_uq.fbOnlyPct +
						"%｜p95 " +
						_uq.p95 +
						"ms",
				);
			}
			if (_tl.length) lines.push("  📊 透出：" + _tl.join("·"));
			// wave 复活口·阶段 1（10-18）：取全文一行（get 关时 stats 不出这几键；全零不显）。不走 _t：_t 会把标签登记进 OBS_KEYS、改 obsGroups
			if (v.memGetCount) lines.push("  🧠 取全文 get " + v.memGetCount + " 次（已老化 " + (v.memGetAged || 0) + "·计命中 " + (v.memGetHits || 0) + "）");
			// wave 复活口·阶段 2（10-18）：复活候选一行（复活关时 stats 不出这几键；全零不显）
			if (v.reviveRecorded && typeof v.reviveRecorded === "object") {
				const _rr = v.reviveRecorded;
				const _sa = v.reviveSkipAdmin || {}; // 阶段 5：管理窗不记的 get、引用（全零不显）
				const _saN = Number(_sa.get) || Number(_sa.cite) ? "·管理窗不记 get " + (Number(_sa.get) || 0) + "·引用 " + (Number(_sa.cite) || 0) : "";
				if (v.revivePending || Number(_rr.get) || Number(_rr.recall) || Number(_rr.cite) || v.reviveRecordErrors || _saN)
					lines.push(
						"  ♻ 复活候选：get " + (Number(_rr.get) || 0) + "·重提 " + (Number(_rr.recall) || 0) + "·引用 " + (Number(_rr.cite) || 0) + "·待复活 " + (v.revivePending || 0) + (v.reviveRecordErrors ? "·记错 " + v.reviveRecordErrors : "") + _saN,
					);
				// 阶段 3：nightly patrol复活一行（本进程跑过复活段、或持久累计非零、或出过错才显）
				if (v.reviveLast != null || Number(v.reviveTotal) || v.reviveErrors) {
					const _sk = Object.entries(v.reviveSkip || {}).map(([k, n]) => k + " " + n).join("、");
					lines.push(
						"  ♻ nightly patrol复活：上次 " + (v.reviveLast ?? "—") + " 条·累计 " + (Number(v.reviveTotal) || 0) + (_sk ? "·跳过 " + _sk : "") + (v.reviveCapHit ? "·超帽 " + v.reviveCapHit + " 次" : "") + (v.reviveErrors ? "·出错 " + v.reviveErrors : ""),
					);
				}
				if (Number(v.reviveManualTotal)) lines.push("  ♻ 手工改期／复活（memory_write renewTargetId）：累计 " + Number(v.reviveManualTotal) + " 条"); // 阶段 4
			}
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
		} else if (a.action === "get" && v && v.kind === "memory-get") {
			// wave 复活口·阶段 1（10-18）：取全文卡。kind 标只由 read.cjs get 分支给（LEGION_MEM_GET_OFF=1 时 get 不受理，回「未知 action」，照旧落到末尾 JSON）；
			//   须在下面「已入册」receipt分支之前（get 结果也带 id 与 ts）
			if (v.error || v.found === false) lines.push("🧠 取全文：" + (v.error || v.note || "不可取"));
			else {
				lines.push(
					"🧠 取全文 #" +
						v.id +
						" [" +
						[v.type, String(v.ts || "").slice(5, 10), v.space, v.status === "aged" ? "已老化（aged）" : v.status, v.ttl ? "ttl" : ""].filter(Boolean).join("·") +
						"] " +
						(v.title || ""),
				);
				lines.push(
					"  状态：status=" +
						v.status +
						"｜stale_state=" +
						(v.stale_state || "空") +
						"｜valid_to=" +
						(v.valid_to || "未设") +
						"｜closed_at=" +
						(v.closed_at || "空") +
						(v.stale_by ? "｜stale_by=" + String(v.stale_by).slice(0, 40) : ""),
				);
				lines.push(
					"  字节 " +
						v.offset +
						"–" +
						v.end +
						"／共 " +
						v.totalBytes +
						(v.nextOffset !== null && v.nextOffset !== undefined ? "（未完，续读：memory action=get id=" + v.id + " offset=" + v.nextOffset + "）" : ""),
				);
				lines.push(String(v.content || ""));
			}
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
								(v.closedTodo.boardRows
									? "·刷板 " + v.closedTodo.boardRows + " 行"
									: "") +
								"）——step closeTargetId"
						: "⚠ 办结闭环未成：" +
								String(v.closedTodo.reason || "") +
								"——step",
				); // step：闭环结果 render 消费
			if (v.renewTarget)
				lines.push(
					v.renewTarget.ok && v.renewTarget.to
						? "✔ " +
								(v.renewTarget.act === "revive" ? "复活" : "改期") +
								"：#" +
								v.renewTarget.id +
								" " +
								String((v.renewTarget.from && v.renewTarget.from.status) || "?") +
								"→active、valid_to " +
								String((v.renewTarget.from && v.renewTarget.from.valid_to) || "空") +
								"→" +
								String(v.renewTarget.to.valid_to) +
								"（" +
								String(v.renewTarget.reason || "") +
								"·本窗今日 " +
								v.renewTarget.today +
								"/" +
								v.renewTarget.cap +
								"）——wave renewTargetId"
						: "⚠ 改期／复活未成：" + String(v.renewTarget.reason || "") + "——wave renewTargetId",
				); // wave 阶段 4：手工改期／复活结果 render（未带 renewTargetId 时无此行）
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

// ── wave 复活口·阶段 1（10-18·wave对照与wave草纲（修订1018）下篇一改点 1）：按 id 取全文 get ──
//    开关 LEGION_MEM_GET_OFF 只认 "1"（值非空又不是 1 时 index.cjs 装配处打一次 warn、按未设处理）；关时 memory 工具参数、说明与 action 判法逐字同底座。
//    单次至多 MEM_GET_BYTES 字节（UTF-8）、MEM_GET_LINES 行（工具卡渲染帽 200 行之内），按返回的 nextOffset（字节）续读。
//    切点先回退到字符首；未到末尾时优先落在换行、空格或制表符之后（回看至多 256 字节），少把长串（如凭据形态的长随机串）劈成两半——
//    桥的截断补遮只认 hits、episodicContext、evolutionContext，get 段切点处的残串要桥另补（见落码记录）。起点落在多字节字符中间时前移到下一字符首。纯函数
const memGetOn = () => process.env.LEGION_MEM_GET_OFF !== "1";
const MEM_GET_BYTES = 8000;
const MEM_GET_LINES = 120;
function memGetChunk(text, offset) {
	const buf = Buffer.from(String(text == null ? "" : text), "utf8");
	const total = buf.length;
	const o = Number(offset);
	let a = Number.isFinite(o) && o > 0 ? Math.floor(o) : 0;
	if (a > total) a = total;
	while (a < total && (buf[a] & 0xc0) === 0x80) a++; // 起点是 continuation 字节（10xxxxxx）→ 前移到下一字符首
	let b = Math.min(total, a + MEM_GET_BYTES);
	if (b < total) {
		while (b > a && (buf[b] & 0xc0) === 0x80) b--; // 切点回退到字符首（同 capRenderText）
		const lo = Math.max(a + 1, b - 256);
		for (let i = b - 1; i >= lo; i--) {
			const c = buf[i];
			if (c === 0x0a || c === 0x20 || c === 0x09) {
				b = i + 1;
				break;
			}
		}
	}
	let s = buf.subarray(a, b).toString("utf8");
	let nl = 0;
	for (let i = 0; i < s.length; i++)
		if (s.charCodeAt(i) === 10 && ++nl >= MEM_GET_LINES) {
			if (i + 1 < s.length) {
				s = s.slice(0, i + 1);
				b = a + Buffer.byteLength(s, "utf8");
			}
			break;
		}
	return { content: s, offset: a, end: b, totalBytes: total, nextOffset: b < total ? b : null };
}

// ── wave 复活口·阶段 2（10-18·草纲下篇一改点 2）：复活触发只记候选，不当场改条目 ──
//    三类触发（只认触发当下 status='aged' 且非待办的条——待办另有 14 天老化，复活了次夜照样再转，阶段 3 同样不复活）：
//      get＝被 memory get 取全文（tool/call 事件、首段；get 关时不认）；任何会话都算（子代理取全文也是用上；阶段 5 起管理窗除外，见 PRIV_SPACES 注）；
//      recall＝maintainer重提：真用户消息（source.kind=user、非子代理会话、非系统帧）按句切开，与调用窗空间（及 global）里 aged 条的标题比字二元组：
//        交集≥4 且 jaccard≥0.6 记一（0.6 取自nightly patrol 「新条撞既有主题族」的标题线）；每条消息至多 3 条。插件里分不出哪条提炼新条出自maintainer原话
//        （auto:session-* 同收用户与助手两侧，来源字段不带角色），故拿消息原句直接比，取保守法（几乎要整句复述标题才中）；调用窗空间认不出时不比；
//      cite＝被引用：主会话助手正文里的 #id（3–7 位、# 前不是字母数字下划线、/、&），每条至多 10 个（阶段 5 起管理窗助手正文除外）；maintainer原话里直接点的 #id 同记作 cite（各窗照记）。
//    候选落独立小表 revive_pending（id、首个触发类别与其后各类、次数、首末时刻、首个会话），nightly patrol复活段消费（阶段 3）；不动 memories——
//    与车二-8f 命中记账（当场写 organ_meta hit_pending）同一层级。连接 PRAGMA query_only=1（记忆桥只读连接即如此）时不写、不报错，只计 skipRO；
//    桥不跑 session/event，三类触发在桥上本就不发生。开关 LEGION_REVIVE_OFF 只认 "1"（index.cjs 装配处非 1 告警）；关时不建表、不记、stats 不出复活键
const reviveOn = () => process.env.LEGION_REVIVE_OFF !== "1";
// wave 复活口·阶段 5（10-18 中枢定）：双特权两窗（memory organ、brain）只此一份——tools.cjs option空间闸 spaceGatePriv（阶段 4 手工改期同用）与
//    阶段 5 管理窗判法共用。管理窗里 get 触发、助手回复 #id 引用不记复活候选（审库、销账、复筛时取全文或写 #id 多半是记账，不是用上），
//    只计 reviveStat.skipAdmin（分 get、cite，按本该记下的 aged 条数）；maintainer重提（recall）与maintainer原话点 #id 各窗照记；业务窗三类照旧。随 LEGION_REVIVE_OFF 走，无新开关
const PRIV_SPACES = Object.freeze(["brain", "memory-organ"]);
const privSpace = (s) => PRIV_SPACES.includes(s);
const REVIVE_RECALL_J = 0.6;
const REVIVE_RECALL_MAX = 3;
const REVIVE_CITE_MAX = 10;
const REVIVE_CITE_RE = /(?<![\w/&])#(\d{3,7})(?!\d)/g;
const REVIVE_SYS_RE = /^(?:Time sampled while preparing|当前运行时上下文。此快照取代|Current runtime context|<system-reminder>|<goal_round>|<goal_complete>|〔记忆闸)/; // 同 read_episodic 折叠系统帧的前缀
const reviveStat = { recorded: {}, skipRO: 0, errors: 0, recallNoSpace: 0, skipAdmin: { get: 0, cite: 0 } }; // 进程内
const revivePendingReady = new WeakSet();
function ensureRevivePending(db) {
	if (revivePendingReady.has(db)) return;
	db.exec("CREATE TABLE IF NOT EXISTS revive_pending(id INTEGER PRIMARY KEY, why TEXT NOT NULL, n INTEGER NOT NULL DEFAULT 1, first_at TEXT, last_at TEXT, sid TEXT)");
	revivePendingReady.add(db);
}
function reviveQueryOnly(db) {
	try {
		const r = db.prepare("PRAGMA query_only").get();
		return !!r && Number(Object.values(r)[0]) === 1;
	} catch {
		return true;
	}
}
// 记候选：ids 里当下 aged 的条 upsert 进 revive_pending（再触发 n+1、末时刻刷新、触发类别并列去重）；返回记下的条数
//    adminWin（阶段 5，可缺）：函数，有 aged 条要记时才调；回真＝管理窗，不记、只计 skipAdmin[why]
function reviveNote(db, ids, why, sid, adminWin) {
	if (!reviveOn() || !db) return 0;
	const list = [...new Set((Array.isArray(ids) ? ids : [ids]).map(Number).filter((x) => Number.isInteger(x) && x > 0))];
	if (!list.length) return 0;
	try {
		if (reviveQueryOnly(db)) {
			reviveStat.skipRO++;
			return 0;
		}
		const aged = db
			.prepare("SELECT id FROM memories WHERE status = 'aged' AND type != 'todo' AND id IN (" + list.map(() => "?").join(",") + ")")
			.all(...list)
			.map((r) => Number(r.id));
		if (!aged.length) return 0;
		if (typeof adminWin === "function" && adminWin()) {
			reviveStat.skipAdmin[why] = (reviveStat.skipAdmin[why] || 0) + aged.length;
			return 0;
		}
		ensureRevivePending(db);
		const at = nowIso();
		const up = db.prepare(
			"INSERT INTO revive_pending (id, why, n, first_at, last_at, sid) VALUES (?, ?, 1, ?, ?, ?) ON CONFLICT(id) DO UPDATE SET n = n + 1, last_at = excluded.last_at, why = CASE WHEN instr(',' || why || ',', ',' || excluded.why || ',') > 0 THEN why ELSE why || ',' || excluded.why END",
		);
		for (const id of aged) up.run(id, why, at, at, String(sid || "").slice(0, 48) || null);
		reviveStat.recorded[why] = (reviveStat.recorded[why] || 0) + aged.length;
		return aged.length;
	} catch {
		reviveStat.errors++;
		return 0;
	}
}
function reviveText(c) {
	if (typeof c === "string") return c;
	if (!Array.isArray(c)) return "";
	let s = "";
	for (const b of c) if (b && typeof b === "object" && typeof b.text === "string" && (!b.type || b.type === "text" || b.type === "input_text" || b.type === "output_text")) s += b.text;
	return s;
}
const reviveNorm = (s) => String(s || "").toLowerCase().replace(/[^\p{L}\p{N}]+/gu, "");
// 调用窗空间：会话头 cwd 主路，认不出再按会话 id 走 projcache(design note)
function reviveSpace(session, sid) {
	const h = (session && session.header) || {};
	return (h.cwd ? organFromPath(h.cwd) : null) || (sid ? sessionOrgan(sid) : null);
}
function reviveRecall(db, text, session, sid) {
	const raw = String(text || "").trim();
	if (raw.length < 4 || REVIVE_SYS_RE.test(raw)) return 0;
	const space = reviveSpace(session, sid);
	if (!space) {
		reviveStat.recallNoSpace++;
		return 0;
	}
	// 逐句取候选（每句自取检索词，免长消息检索词只取前几槽漏句）：句长（归一后）4–160，至多 8 句；每句 FTS 各取前 20 条同空间（及 global）非待办的 aged 条与在役条，
	// 标题与本句比：aged 条要交集≥4、jaccard≥0.6，且比本句最像的在役条更像（严格大于）——本句已有在役条接得住（如新旧两版同名规则只差一字），不复活旧条
	const clauses = raw
		.slice(0, 2000)
		.split(/[。！？!?；;\n\r]+/)
		.map((c) => [c, reviveNorm(c)])
		.filter(([, n]) => n.length >= 4 && n.length <= 160)
		.slice(0, 8);
	if (!clauses.length) return 0;
	const sel = (status) =>
		db.prepare(
			"SELECT m.id, m.title FROM memories_fts JOIN memories m ON m.id = memories_fts.rowid WHERE memories_fts MATCH ? AND m.status = '" + status + "' AND m.type != 'todo' AND m.space IN (?, 'global') ORDER BY bm25(memories_fts) LIMIT 20",
		);
	const stAged = sel("aged");
	const stActive = sel("active");
	const sim = (title, g) => {
		const tg = bigrams(reviveNorm(title));
		if (tg.size < 3) return 0;
		let inter = 0;
		for (const x of tg) if (g.has(x)) inter++;
		return inter >= 4 ? jaccard(tg, g) : 0;
	};
	const hit = [];
	for (const [c, n] of clauses) {
		const match = queryMatch(qTokens(c));
		if (!match) continue;
		const g = bigrams(n);
		const agedHits = [];
		for (const r of stAged.all(match, space)) {
			const j = sim(r.title, g);
			if (j >= REVIVE_RECALL_J) agedHits.push([Number(r.id), j]);
		}
		if (!agedHits.length) continue;
		let bestActive = 0;
		for (const r of stActive.all(match, space)) bestActive = Math.max(bestActive, sim(r.title, g));
		for (const [id, j] of agedHits) {
			if (hit.length >= REVIVE_RECALL_MAX) break;
			if (j > bestActive && !hit.includes(id)) hit.push(id);
		}
		if (hit.length >= REVIVE_RECALL_MAX) break;
	}
	return hit.length ? reviveNote(db, hit, "recall", sid) : 0;
}
function reviveCite(db, text, sid, adminWin) {
	const s = String(text || "").slice(0, 20000);
	if (!s.includes("#")) return 0;
	const ids = [];
	for (const m of s.matchAll(REVIVE_CITE_RE)) {
		const n = Number(m[1]);
		if (!ids.includes(n)) ids.push(n);
		if (ids.length >= REVIVE_CITE_MAX) break;
	}
	return ids.length ? reviveNote(db, ids, "cite", sid, adminWin) : 0;
}
// index.cjs session/event 调：按事件类分派三类触发（自带 try／catch，失败只计 errors）
function reviveOnEvent(db, session, t, event) {
	try {
		if (!reviveOn() || !db) return;
		const d = (event && event.data) || {};
		const sid = session && typeof session.id === "string" ? session.id : "";
		const h = (session && session.header) || {};
		const sub = h.origin === "subagent" || Number(h.delegationDepth) > 0;
		const adminWin = () => privSpace(reviveSpace(session, sid)); // 阶段 5：管理窗（双特权两窗）判法，只给 get 与助手回复引用用，有 aged 条要记时才算
		if (t === "tool/call") {
			if (!memGetOn() || String(d.name || d.toolName || "") !== "memory") return;
			let a = d.arguments !== undefined ? d.arguments : d.args !== undefined ? d.args : d.input; // 同 askgate.cjs tool/call 取参（另认 args）
			if (typeof a === "string") {
				try {
					a = JSON.parse(a);
				} catch {
					a = {};
				}
			}
			if (!a || typeof a !== "object" || a.action !== "get" || Number(a.offset) > 0) return;
			reviveNote(db, [a.id], "get", sid, adminWin);
		} else if (t === "user/message") {
			if (sub || !d.source || d.source.kind !== "user") return;
			const txt = reviveText(d.content);
			if (!txt || REVIVE_SYS_RE.test(txt.trim())) return;
			reviveRecall(db, txt, session, sid);
			reviveCite(db, txt, sid); // maintainer原话里直接点 #id，也算重提（记作 cite）
		} else if (t === "assistant/message") {
			if (sub) return;
			reviveCite(db, reviveText(d.message ? d.message.content : d.content), sid, adminWin);
		}
	} catch {
		reviveStat.errors++;
	}
}

// ── wave 车二-8a（10-17·落码规格 2.5）：生命周期保护判据——返回保护理由（ess／vc／hit／mil／ptr）或 null ──
//    状态句（lifeStateTitle）只认 essential；其余按序：essential=1 → ess；validated_count≥2 → vc；last_hit_at 在 30 天内 → hit；
//    标题或正文前 200 字含directive锚词 → mil；标题含指针类词（LEGION_LIFE_PTR_OFF=1 时不认）→ ptr。纯函数，不读写库
//    盲审处置（should，10-18）：vc 与 hit 只认跨日印证——last_hit_at 与 ts 同一北京日的不算（extract.cjs 当天同窗反复提炼的同题合并每次
//    vc+1 并刷新 last_hit_at，快照里续期 vc 条 55% 属此）；缺 ts 或 last_hit_at 时不作同日排除（nightly patrol近似对只加 vc、不刷 last_hit_at，本就跨日）
function lifeBjDay(v) {
	const s = String(v || "").trim();
	if (!/^\d{4}-\d{2}-\d{2}/.test(s)) return "";
	if (/(?:Z|[+-]\d{2}:?\d{2})$/.test(s)) {
		const t = Date.parse(s.replace(" ", "T"));
		if (Number.isFinite(t)) return new Date(t + 8 * 3600e3).toISOString().slice(0, 10);
	}
	return s.slice(0, 10);
}
function lifeProtect(row, nowMs) {
	if (!row) return null;
	const now = Number.isFinite(nowMs) ? nowMs : Date.now();
	const title = String(row.title || "");
	const ess = Number(row.essential) === 1;
	if (CRIT.lifeStateTitle && CRIT.lifeStateTitle.test(title)) return ess ? "ess" : null;
	if (ess) return "ess";
	const tsD = lifeBjDay(row.ts);
	const lhD = lifeBjDay(row.last_hit_at);
	const sameDay = !!tsD && !!lhD && tsD === lhD;
	if ((Number(row.validated_count) || 0) >= 2 && !sameDay) return "vc";
	if (row.last_hit_at && !sameDay) {
		const lh = Date.parse(String(row.last_hit_at).replace(" ", "T"));
		if (Number.isFinite(lh) && now - lh <= 30 * 86400e3) return "hit";
	}
	if (CRIT.lifeMilAnchor && CRIT.lifeMilAnchor.test(title + " " + String(row.content || "").slice(0, 200))) return "mil";
	if (process.env.LEGION_LIFE_PTR_OFF !== "1" && CRIT.lifePtrTitle && CRIT.lifePtrTitle.test(title)) return "ptr";
	return null;
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
// 分词版本戳（**模块级**·车二：ensureFts 与启动重建块须**同源引用**——防两处漂移；分词改动即须 bump）
//   ⚠ 位置：置于 porter2 段**之前**——drill 从「车二西文形态归一」锚点提取同源段求值，本行含 jieba 引用不宜入段
const TOKENIZER_VERSION = jieba ? "jieba2-cjk-stem-v2" : "cjk-bigram-stem-v2";
// ══ 车二：西文形态归一（标准 Porter2·零依赖·nltk 同构翻译版）══════════════
//  令源：design-approved「三问全按推荐」⇒ option（双侧归一）＋自实现标准 Porter2＋nightly patrol窗重建 FTS。
//  ⚠ 修正史（09-14 14:1x·独立审计触发·自证闭环）：首版三处真偏差——
//    ① R1/R2 区域相位错（官方＝「第一个元音后跟非元音」的非元音之后；首版误写「非元音后跟元音」的元音之后）
//    ② step1b 后缀竞争错（eed/eedly 与 ed/ing 必须按最长匹配互斥；eed 在 R1 外不得回落 ed 分支——feed→fe 假案）
//    ③ 缺官方 Exception 2 表（inning/outing/canning/herring/earring/proceed/exceed/succeed 及屈折）
//  修法：逐分支翻译 nltk SnowballStemmer("english")（Snowball 官方算法的编译产物）——
//    切片收缩模型（r1/r2 随词尾操作同步收缩/伸长）、stopwords 短路（nltk 英文为空集·故无表）、
//    两处生成器 quirk（step2 ation/iveness 族 r2 else="e"）、step5 四条件式。
//  对拍：nltk 全量对拍归零（≥400 词·drill ⑯ 钉子断言守）。
const PORTER2_VOWELS = "aeiouy";
const PORTER2_DOUBLE = ["bb", "dd", "ff", "gg", "mm", "nn", "pp", "rr", "tt"];
const PORTER2_LI_END = "cdeghkmnrt";
const PORTER2_S0 = ["'s'", "'s", "'"];
const PORTER2_S1A = ["sses", "ied", "ies", "us", "ss", "s"];
const PORTER2_S1B = ["eedly", "ingly", "edly", "eed", "ing", "ed"];
const PORTER2_S2 = [
	"ization",
	"ational",
	"fulness",
	"ousness",
	"iveness",
	"tional",
	"biliti",
	"lessli",
	"entli",
	"ation",
	"alism",
	"aliti",
	"ousli",
	"iviti",
	"fulli",
	"enci",
	"anci",
	"abli",
	"izer",
	"ator",
	"alli",
	"bli",
	"ogi",
	"li",
];
const PORTER2_S3 = [
	"ational",
	"tional",
	"alize",
	"icate",
	"iciti",
	"ative",
	"ical",
	"ness",
	"ful",
];
const PORTER2_S4 = [
	"ement",
	"ance",
	"ence",
	"able",
	"ible",
	"ment",
	"ant",
	"ent",
	"ism",
	"ate",
	"iti",
	"ous",
	"ive",
	"ize",
	"ion",
	"al",
	"er",
	"ic",
];
const PORTER2_SPECIAL = {
	skis: "ski",
	skies: "sky",
	dying: "die",
	lying: "lie",
	tying: "tie",
	idly: "idl",
	gently: "gentl",
	ugly: "ugli",
	early: "earli",
	only: "onli",
	singly: "singl",
	sky: "sky",
	news: "news",
	howe: "howe",
	atlas: "atlas",
	cosmos: "cosmos",
	bias: "bias",
	andes: "andes",
	// ── Exception 2（官方不变词族·首版漏）──
	inning: "inning",
	innings: "inning",
	outing: "outing",
	outings: "outing",
	canning: "canning",
	cannings: "canning",
	herring: "herring",
	herrings: "herring",
	earring: "earring",
	earrings: "earring",
	proceed: "proceed",
	proceeds: "proceed",
	proceeded: "proceed",
	proceeding: "proceed",
	exceed: "exceed",
	exceeds: "exceed",
	exceeded: "exceed",
	exceeding: "exceed",
	succeed: "succeed",
	succeeds: "succeed",
	succeeded: "succeed",
	succeeding: "succeed",
};
function porter2IsVowel(c) {
	return PORTER2_VOWELS.includes(c);
}
// R1/R2（官方定义·切片模型）：R1＝第一个「元音后跟非元音」的非元音之后；R2＝R1 内同法。
// 特例前缀 gener/arsen ⇒ R1=word[5:]；commun ⇒ R1=word[6:]（R2 仍在 R1 内普通找）。
function porter2R1R2(word) {
	let r1 = "",
		r2 = "";
	if (word.startsWith("gener") || word.startsWith("arsen")) r1 = word.slice(5);
	else if (word.startsWith("commun")) r1 = word.slice(6);
	else
		for (let i = 1; i < word.length; i++)
			if (!porter2IsVowel(word[i]) && porter2IsVowel(word[i - 1])) {
				r1 = word.slice(i + 1);
				break;
			}
	for (let i = 1; i < r1.length; i++)
		if (!porter2IsVowel(r1[i]) && porter2IsVowel(r1[i - 1])) {
			r2 = r1.slice(i + 1);
			break;
		}
	return [r1, r2];
}
function porter2Stem(raw) {
	let word = String(raw).toLowerCase();
	if (word.length <= 2) return word;
	if (PORTER2_SPECIAL[word]) return PORTER2_SPECIAL[word];
	word = word.replace(/[’‘‛]/g, "'");
	if (word.startsWith("'")) word = word.slice(1);
	if (word.startsWith("y")) word = "Y" + word.slice(1);
	for (let i = 1; i < word.length; i++)
		if (porter2IsVowel(word[i - 1]) && word[i] === "y")
			word = word.slice(0, i) + "Y" + word.slice(i + 1);
	let [r1, r2] = porter2R1R2(word);
	// STEP 0：撇号所有格（三切片同步）
	for (const suf of PORTER2_S0)
		if (word.endsWith(suf)) {
			word = word.slice(0, -suf.length);
			r1 = r1.slice(0, -suf.length);
			r2 = r2.slice(0, -suf.length);
			break;
		}
	// STEP 1a
	for (const suf of PORTER2_S1A)
		if (word.endsWith(suf)) {
			if (suf === "sses") {
				word = word.slice(0, -2);
				r1 = r1.slice(0, -2);
				r2 = r2.slice(0, -2);
			} else if (suf === "ied" || suf === "ies") {
				if (word.length - suf.length > 1) {
					word = word.slice(0, -2);
					r1 = r1.slice(0, -2);
					r2 = r2.slice(0, -2);
				} else {
					word = word.slice(0, -1);
					r1 = r1.slice(0, -1);
					r2 = r2.slice(0, -1);
				}
			} else if (suf === "s") {
				// 官方：删 s ⇔「s 之前、不紧邻 s 的部分」含元音（yes/bus/gas/this 不删）
				if (/[aeiouy]/.test(word.slice(0, -2))) {
					word = word.slice(0, -1);
					r1 = r1.slice(0, -1);
					r2 = r2.slice(0, -1);
				}
			}
			break;
		}
	// STEP 1b（互斥最长匹配：eed/eedly 与 ed/ing 族竞争——eed 在 R1 外不得回落）
	for (const suf of PORTER2_S1B)
		if (word.endsWith(suf)) {
			if (suf === "eed" || suf === "eedly") {
				if (r1.endsWith(suf)) {
					word = word.slice(0, word.length - suf.length) + "ee";
					r1 =
						r1.length >= suf.length
							? r1.slice(0, r1.length - suf.length) + "ee"
							: "";
					r2 =
						r2.length >= suf.length
							? r2.slice(0, r2.length - suf.length) + "ee"
							: "";
				}
			} else {
				const stem = word.slice(0, -suf.length);
				if (/[aeiouy]/.test(stem)) {
					word = stem;
					r1 = r1.slice(0, -suf.length);
					r2 = r2.slice(0, -suf.length);
					if (/(at|bl|iz)$/.test(word)) {
						word += "e";
						r1 += "e";
						if (word.length > 5 || r1.length >= 3) r2 += "e";
					} else if (PORTER2_DOUBLE.some((d) => word.endsWith(d))) {
						word = word.slice(0, -1);
						r1 = r1.slice(0, -1);
						r2 = r2.slice(0, -1);
					} else if (
						(r1 === "" &&
							word.length >= 3 &&
							!porter2IsVowel(word[word.length - 1]) &&
							!"wxY".includes(word[word.length - 1]) &&
							porter2IsVowel(word[word.length - 2]) &&
							!porter2IsVowel(word[word.length - 3])) ||
						(r1 === "" &&
							word.length === 2 &&
							porter2IsVowel(word[0]) &&
							!porter2IsVowel(word[1]))
					) {
						word += "e";
						if (r1.length > 0) r1 += "e";
						if (r2.length > 0) r2 += "e";
					}
				}
			}
			break;
		}
	// STEP 1c：结尾 y/Y → i（前为非元音）
	if (
		word.length > 2 &&
		(word[word.length - 1] === "y" || word[word.length - 1] === "Y") &&
		!porter2IsVowel(word[word.length - 2])
	) {
		word = word.slice(0, -1) + "i";
		r1 = r1.length >= 1 ? r1.slice(0, -1) + "i" : "";
		r2 = r2.length >= 1 ? r2.slice(0, -1) + "i" : "";
	}
	// STEP 2（R1 内）
	for (const suf of PORTER2_S2)
		if (word.endsWith(suf)) {
			if (r1.endsWith(suf)) {
				const cut = (n) => {
					word = word.slice(0, -n);
					r1 = r1.slice(0, -n);
					r2 = r2.slice(0, -n);
				};
				const rep1 = (to) => {
					word = word.slice(0, word.length - suf.length) + to;
					r1 =
						r1.length >= suf.length
							? r1.slice(0, r1.length - suf.length) + to
							: "";
				};
				const rep2 = (to, emptyIsE) => {
					r2 =
						r2.length >= suf.length
							? r2.slice(0, r2.length - suf.length) + to
							: emptyIsE
								? "e"
								: "";
				};
				if (suf === "tional") cut(2);
				else if (suf === "enci" || suf === "anci" || suf === "abli") {
					word = word.slice(0, -1) + "e";
					r1 = r1.length >= 1 ? r1.slice(0, -1) + "e" : "";
					r2 = r2.length >= 1 ? r2.slice(0, -1) + "e" : "";
				} else if (suf === "entli") cut(2);
				else if (suf === "izer" || suf === "ization") {
					rep1("ize");
					rep2("ize");
				} else if (suf === "ational" || suf === "ation" || suf === "ator") {
					rep1("ate");
					rep2("ate", true); // nltk 生成器 quirk：r2 不足长时置 "e"（照抄保一致）
				} else if (suf === "alism" || suf === "aliti" || suf === "alli") {
					rep1("al");
					rep2("al");
				} else if (suf === "fulness") cut(4);
				else if (suf === "ousli" || suf === "ousness") {
					rep1("ous");
					rep2("ous");
				} else if (suf === "iveness" || suf === "iviti") {
					rep1("ive");
					rep2("ive", true); // 同上 quirk
				} else if (suf === "biliti" || suf === "bli") {
					rep1("ble");
					rep2("ble");
				} else if (suf === "ogi" && word[word.length - 4] === "l") cut(1);
				else if (suf === "fulli" || suf === "lessli") cut(2);
				else if (suf === "li" && PORTER2_LI_END.includes(word[word.length - 3]))
					cut(2);
			}
			break;
		}
	// STEP 3（R1 内·ative 仅 R2）
	for (const suf of PORTER2_S3)
		if (word.endsWith(suf)) {
			if (r1.endsWith(suf)) {
				if (suf === "tional") {
					word = word.slice(0, -2);
					r1 = r1.slice(0, -2);
					r2 = r2.slice(0, -2);
				} else if (suf === "ational") {
					word = word.slice(0, word.length - suf.length) + "ate";
					r1 =
						r1.length >= suf.length
							? r1.slice(0, r1.length - suf.length) + "ate"
							: "";
					r2 =
						r2.length >= suf.length
							? r2.slice(0, r2.length - suf.length) + "ate"
							: "";
				} else if (suf === "alize") {
					word = word.slice(0, -3);
					r1 = r1.slice(0, -3);
					r2 = r2.slice(0, -3);
				} else if (suf === "icate" || suf === "iciti" || suf === "ical") {
					word = word.slice(0, word.length - suf.length) + "ic";
					r1 =
						r1.length >= suf.length
							? r1.slice(0, r1.length - suf.length) + "ic"
							: "";
					r2 =
						r2.length >= suf.length
							? r2.slice(0, r2.length - suf.length) + "ic"
							: "";
				} else if (suf === "ful" || suf === "ness") {
					word = word.slice(0, -suf.length);
					r1 = r1.slice(0, -suf.length);
					r2 = r2.slice(0, -suf.length);
				} else if (suf === "ative" && r2.endsWith(suf)) {
					word = word.slice(0, -5);
					r1 = r1.slice(0, -5);
					r2 = r2.slice(0, -5);
				}
			}
			break;
		}
	// STEP 4（R2 内·ion 前须 s/t）
	for (const suf of PORTER2_S4)
		if (word.endsWith(suf)) {
			if (r2.endsWith(suf)) {
				if (suf === "ion") {
					if ("st".includes(word[word.length - 4])) {
						word = word.slice(0, -3);
						r1 = r1.slice(0, -3);
						r2 = r2.slice(0, -3);
					}
				} else {
					word = word.slice(0, -suf.length);
					r1 = r1.slice(0, -suf.length);
					r2 = r2.slice(0, -suf.length);
				}
			}
			break;
		}
	// STEP 5（nltk 四条件式·非「短音节」式）
	if (r2.endsWith("l") && word[word.length - 2] === "l") word = word.slice(0, -1);
	else if (r2.endsWith("e")) word = word.slice(0, -1);
	else if (
		r1.endsWith("e") &&
		word.length >= 4 &&
		(porter2IsVowel(word[word.length - 2]) ||
			"wxY".includes(word[word.length - 2]) ||
			!porter2IsVowel(word[word.length - 3]) ||
			porter2IsVowel(word[word.length - 4]))
	)
		word = word.slice(0, -1);
	return word.replace(/Y/g, "y");
}

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
	// 车二（09-14 maintainer·option**双侧归一**）：原词**恒入**、词干**附加** —— 读写同源（本函数被写库与查询两侧共用）
	//   ①保精度：原词仍在索引 ⇒ 精确命中不受影响；②扩召回：词干并入 ⇒ 形态差（books↔book）可命中
	//   ③纯西文面：仅全 ASCII 字母 token 做词干（数字/下划线/混合串/非 ASCII 一律原样）——中文面零影响
	for (const w of s.split(/[^\w]+/)) {
		if (w.length < 2) continue;
		const lw = w.toLowerCase();
		out.add(lw);
		if (/^[a-z]+$/.test(lw)) {
			const st = porter2Stem(lw);
			if (st !== lw && st.length >= 2) out.add(st);
		}
	}
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
	// 帽分治动态放宽（approved2026-09-13·五件一item1）：≤8 零行为变·>8 放宽至 16
	// 16 系 LoCoMo 域内读数（中位10实词·65.9%题>8被截·-14.3pp 全归帽）——我方库上无对应证据·真链复标硬前置
	// env=8 是唯一「显式关闭动态」法（=现产逐字等价·一键回退）
	const QCAP = envIntCap(process.env.LEGION_QUERY_CAP, 0);
	if (QCAP > 0) return slots.slice(0, QCAP);
	// 修车-D6（冻结后修订）：计数挪出本函数至 search 主路（原按调用计——一次 search 多路〔A档/兜底/自动召回〕重复计）；此处纯函数零副作用
	return slots.length <= 8 ? slots : slots.slice(0, 16);
}
let queryCapDyn = 0, queryCapKept = 0; // 帽动态/保持计数（rerankErrors 同族：模块级→L10777 透出→render）
let rerankProperPicked = 0; // option车观测位（D 项·零权重变更）：proper 类查询经 rerank 拾取条数——同态趋势可比·禁拿关态值当治池证据（kickoff三禁区）

// ── SelRoute 六类路由正则版（wave·design-approved「同意」·观测版零权重变更）──
//    六类：ss-user/ss-assistant（单会话用户/助手侧）·multi-session（跨窗）·knowledge-update（知识
//    新鲜度）·temporal（词表单源复用）·other（兜底）。本期只分类+计数透出（stats.queryClassDist
//    ·观测一周）——权重路由（如 ss-user→FTS 单路权重升·knowledge-update→spoken 加成升）候分布settled
//    再pending approval，**不动排序链**。优先序=特异先泛化后（会话指向词＞知识新鲜度词＞泛时间词）——「上次」归
//    temporal（多会话指向由 multi-session 特异词「他窗/上个会话」承担·重叠面预期内·观测期归因看此）。
const SELROUTE_TEMPORAL_RE =
	/上次|最近|之前|昨天|前天|上周|上个月|今晚|今晨|哪天|什么时候|几点|几号|多久/;
// ── ④-b 回顾类词表(design note)──
//    用处：read.cjs 加权链 段——问句命中本表时，非 todo 行不再多乘一次 decay（问的是旧办法，再沉旧条方向反了）。
//    与上表重叠：「之前」「上次」两表都有——SELROUTE_TEMPORAL_RE 照旧管 queryClass 分类与 temporal 判定，本表只管「免二次衰减」。
//    判定顺序固定：先判回顾（本表）、后判 temporal（上表）；命中本表即跳过平方化，其余 temporal 词（最近、昨天、上周等）照旧。
//    两表同源留本文件，改词表两处一起看。开关：LEGION_TEMPORAL_BACKWARD_OFF 置 1 只关本表的跳过（回旧统一行为）。
const SELROUTE_BACKWARD_RE =
	/之前|上次|以前|早先|原先|原来|旧(?:的|版|办法|方法)|从前|当初|最初/;
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
	minAnchors: envIntCap(process.env.SPOKEN_BOOST_MIN_ANCHORS, 2),
	injectCap: envIntCap(process.env.SPOKEN_BOOST_INJECT_CAP, 3),
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
			max_tokens: llmBudget(200),
			temperature: 0.4,
		}),
		signal: AbortSignal.timeout(20000),
	});
	if (!resp.ok) throw new Error("spoken-prefix http " + resp.status);
	const data = await resp.json();
	addUsage("spoken", data); // E-7：前缀生成 token 用量（DeepSeek·spoken 链）
	return String(data?.choices?.[0]?.message?.content || "")
		.replace(/[\n、，,。;；|/]+/g, " ")
		.replace(/\s+/g, " ")
		.trim();
}
async function llmSpokenPrefix(title, content) {
	const t = clipSurrogateTail(String(title).slice(0, 120)), // 2026-09-22 甲-F3：代理对保对（出网 body）
		c = clipSurrogateTail(String(content).slice(0, 400)); // 2026-09-22 戊路W-3：代理对保对（t/c 同经 body）
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
	return clipSurrogateTail(txt.slice(0, 200)); // 200 字帽（FTS 体量闸）＋代理对保对（2026-09-22 乙路-③：帽位 U+FFFD 实证）
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
	const c = String(content || "");
	// 库减脂option（10-02 approved·brain交办 1436）：短条仅 seq=0 单块（阈值可配·缺省 600）
	// ——写入路（chunkMemory 调用点）与nightly patrol回填路（同函数）经函数集中改动自动同改（防两路分裂）。
	const shortMax = envIntCap("LEGION_CHUNK_SHORT_MAX", 600);
	if (t.length + c.length <= shortMax) {
		const whole = (t ? t + "\n" : "") + c.trim();
		return whole ? [whole] : [];
	}
	if (t) out.push(t.slice(0, 300));
	const parts = c
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
let vecChannelOff = 0, vecChannelOffWarnAt = 0; // 10-02 语分归零案补：vec 通道关断计数+10min 节流 warn（原静默 return [] 零痕迹·交办②可观测）
let vecChannelErrors = 0; // 故障累计（stats.vecChannelErrors 透出）
let vecSkippedDueToBudget = 0; // 批㉙ option（design-approved「按建议」·vecRecallCore 上限设计件）：超帽降级计数（stats 透出）
let vecLastWarnAt = 0; // 60s 节流窗（vecCredCache 同级手法）
let vecScanSkipWarnAt = 0; // A级四件②（全链再审甲J3）：超帽 warn/落键独立节流窗——原与通道错 warn 共用 vecLastWarnAt（互斥饿死面：通道错刚 warn 则 60s 内超帽不 warn 不落键）
let vecScanSkipKeyErrors = 0; // 车一补丁（09-29 补审 B-21）：超帽落键写失败计数（catch 原空吞——⑬ 计数透出）
let vecCapNear = 0; // 帽接近度哨（10-01 落码·approvedoption·教训 #25409/#25411「保险丝帽律」码化）：chunks/cap ≥80% 未超帽预警计数——帽类立法模板件
let vecCapNearWarnAt = 0; // 接近度 warn/落键独立节流窗（60s·照 vecScanSkipWarnAt 独立窗范式——禁与超帽窗共用防饿死）
let vecScanSkipKeyErrWarnAt = 0; // 车一补丁：落键失败 warn 独立 60s 节流窗（不与 vecLastWarnAt 互斥）
// ── 帽接近度哨族（10-01·approved·帽族适用面分治 #28365 裁定二·预排件＝a companion script…）──
//    **适用范围**（模板注释·与 vec 哨同族）：**库量驱动型**帽设哨（量随库增长·会顶死）；**静态配置型**帽标免不设哨（人设值不随库涨·顶到＝配置使然）。
//    设哨 5 顶＝LEGION_FAMILY_CAP／LEGION_HISIM_CAP／LEGION_SPOKEN_BACKFILL_CAP／LEGION_CHUNK_BACKFILL_CAP／LEGION_VEC_POOL_CAP；
//    标免 9 顶＝RERANK_CAND／RERANK_TOPN／QUERY_CAP／INJECT_BUDGET_BYTES／EXTRACT_TIMEOUT_MS／CONFLICT_PENDING_DAYS／EDGE_KEEPALIVE_DAYS／SPOKEN_BOOST_INJECT_CAP／SPOKEN_BOOST_MIN_ANCHORS。
//    判据＝量/cap ≥80% 且未超帽 → 60s 节流（**每哨独立窗**·照 vecScanSkipWarnAt 饿死面教训）warn＋organ_meta 落键含 pct。
const CAP_NEAR = { family: 0, hisim: 0, spoken: 0, chunk: 0, vecpool: 0, vecpoolOver: 0, keyErrors: 0 }; // 各哨计数＋超帽计数（vecpoolOver）＋落键失败计数（stats 透出）
const capNearWarnAt = Object.create(null); // 每哨独立 60s 节流窗（禁共用）
const capNearWatch = (connLike, key, cur, cap, hint) => {
	if (!(cap > 0) || !Number.isFinite(cur) || cur < cap * 0.8 || cur > cap)
		return false;
	CAP_NEAR[key] = (CAP_NEAR[key] || 0) + 1;
	if (Date.now() - (capNearWarnAt[key] || 0) > 60000) {
		capNearWarnAt[key] = Date.now();
		try {
			console.warn(
				"[living-memory] " +
					key +
					" 帽接近度预警 #" +
					CAP_NEAR[key] +
					" (" +
					cur +
					" ≥80% × cap " +
					cap +
					" · " +
					hint +
					")",
			);
		} catch {}
		if (!fs2.existsSync(SURGERY_FLAG)) {
			try {
				connLike
					.prepare(
						"INSERT OR REPLACE INTO organ_meta (k, v) VALUES (?, ?)",
					)
					.run(
						key + "_cap_near",
						JSON.stringify({
							n: CAP_NEAR[key],
							cur: cur,
							cap: cap,
							pct: Math.round((cur / cap) * 100),
							at: nowIso(),
						}),
					);
			} catch {
				CAP_NEAR.keyErrors += 1; // ⑬ 计数出口：落键失败不静默
			}
		}
	}
	return true;
};
// ── E-7（09-12 approved）：**API 用量/成本计数** ──
//    审计发现：全仓 `total_tokens`/`prompt_tokens`/`usage.` **命中 0** ⇒ API 成本**零可见化**
//    （嵌入/提炼/前缀/精排/预裁 **五类**出网调用各有消耗却无处看账）。实现＝各调用点读响应 `usage.total_tokens` 累加。
//    ⚠ **进程累计·重启清零**（与 vecChannelErrors 族同性质）；`calls` 只计「带 usage 的成功响应」。
//    ⚠ 独立审计 D2（09-12）**口径订正**：本账＝「**成功响应且带 usage 的调用下界**」——**不含**失败/重试消耗
//      （`!resp.ok` 抛点与 429/5xx 重试所耗 token 无出口）；`noUsage` 计「成功但无 usage 字段」者，
//      `httpErr` 计失败次数。原 `errors` 键（读属性对普通 JSON 不可能抛）实为**恒 0 假绿位** ⇒ 已删。
//    ⚠ 原 `extract` 实指向 **AUDN 预裁**调用 ⇒ 已分家：`preverdict`＝预裁／`extract`＝**主提炼**（D2 修正）。
const apiUsage = {
	embed: 0,
	extract: 0,
	preverdict: 0,
	spoken: 0,
	rerank: 0,
	calls: 0,
	noUsage: 0,
	httpErr: 0,
};
function addUsage(kind, data) {
	try {
		const t = Number(data && data.usage && data.usage.total_tokens);
		if (Number.isFinite(t) && t > 0) {
			apiUsage[kind] = (apiUsage[kind] || 0) + t;
			apiUsage.calls += 1;
		} else apiUsage.noUsage += 1; // D2：成功但无 usage ⇒ 显式计入（原静默跳过）
	} catch {
		apiUsage.noUsage += 1; // ⑬ 出口
	}
}
function noteApiFail() {
	apiUsage.httpErr += 1; // D2：失败/重试出口（无 usage 可读 ⇒ 只计次数·下界口径）
}
// ── E-1（09-12 approved）：**注入预算总账** ──
//    审计 §3-4 实勘：注入面 **14 路**（`systemPrompt.context('legion-memory-recall')` 10 路整块 ＋
//    `system-prompt/assemble` waterfall 4 路变量块）**零总预算**——仅 8 路有条数帽、**无任何字节/行数总帽**
//    （`MAX_RENDER_BYTES=50KB` 是工具面板帽·不作用于注入面），且唯一测量件 `inject-budget-probe.cjs` 已退役
//    ⇒ **连测量手段都缺**。本表＝「一处定义」；两钩子出口各测字节（last/max/samples）＝先把账建起来。
//    ⚠ 帽值**不凭空设**（尺子三问）：`LEGION_INJECT_BUDGET_BYTES` **未设 ⇒ 只测不告警**（观测期）；
//      待实测 max 出来后再据数据定帽＝先有账、后定尺。（`injectMeter.overBudget` 供告警计数。）
const INJECT_BUDGET_BYTES = envIntCap(process.env.LEGION_INJECT_BUDGET_BYTES, 0); // 0=观测期不告警
// ── 注入瘦身车 W4（10-11·maintainer 1324 四案＋统筹审核 W4·依据 设计/注入瘦身/落码task brief.md §3.8、§3.10）：同轮总帽 ──
//    瘦身在用时帽比「同轮合并总字节」（状态段＋召回段·每次装配一笔·recordInjectTurn），不再按路比（RI-10：一个帽分比两路·wf 路形同虚设）。
//    同一 env 名（02 号 L41：复用不另造）；缺省 4200＝重启后 71 轮记忆块字节 p95 3807×1.1 取整到百（2135 L44-45「P95 上浮 10%」·基线换会话档实测）；
//    0 与非法值回落缺省（envIntCap）。缺省只计不裁（裁剪另由 LEGION_INJECT_TRIM_ON=1 开·第 9 笔）。瘦身关时旧常量 INJECT_BUDGET_BYTES 与按路判原样。
const INJECT_TURN_CAP_BYTES = envIntCap(process.env.LEGION_INJECT_BUDGET_BYTES, 4200);
const injectMeter = {
	ctxLast: 0,
	ctxMax: 0,
	wfLast: 0,
	wfMax: 0,
	totalMax: 0,
	samples: 0,
	overBudget: 0,
	// 注入瘦身车 W4（10-11·依据 落码task brief §3.10）：同轮账字段——进程内计数·重启清零（长期基线以会话档为准）
	turnLast: 0, // 最近一笔同轮合并字节
	turnMax: 0,
	turnN: 0, // 重算步笔数（冻结命中只加 freezeHits）
	freezeHits: 0,
	freezeBreaks: 0,
	freezeUnknown: 0,
	settleLate: 0,
	overCap: 0,
	overCapProtected: 0,
	trimmed: 0,
	recallDedup: 0,
	errors: 0, // 显式初值（原只在 catch 里赋值·read 与 render 无出口）
	turnLog: [], // 本轮记录环（20 条·只记 id 与标签·不记正文）
};
function recordInjectBytes(kind, s, skipBudget) {
	try {
		const b = Buffer.byteLength(typeof s === "string" ? s : "", "utf8");
		if (kind === "ctx") {
			injectMeter.ctxLast = b;
			if (b > injectMeter.ctxMax) injectMeter.ctxMax = b;
		} else {
			injectMeter.wfLast = b;
			if (b > injectMeter.wfMax) injectMeter.wfMax = b;
		}
		// D1（独立审计 09-12 订正）：原 `totalLast = ctxLast + wfLast` **跨轮拼接**（某轮只触发一路时会把
		//   上一轮的另一路与本轮相加）⇒ 虚高。改为**峰值上界**语义：`ctxMax + wfMax`（明示「非同轮实际量」）。
		injectMeter.totalMax = injectMeter.ctxMax + injectMeter.wfMax;
		injectMeter.samples += 1;
		// D1 同修：超线判定原用跨轮 `totalLast` ⇒ 改为**按路判**（ctx／wf 各自 vs 预算），口径简单可解释
		//   注入瘦身车 W4（10-11）：第三参 skipBudget 为真（瘦身在用时 inject 各出口传 true）不再按路比——帽改比同轮合并总字节（recordInjectTurn）；两参旧调用行为不变
		if (!skipBudget && INJECT_BUDGET_BYTES > 0 && b > INJECT_BUDGET_BYTES)
			injectMeter.overBudget += 1;
	} catch {
		injectMeter.errors = (injectMeter.errors || 0) + 1; // ⑬ 出口
	}
	return s;
}
// ── 注入瘦身车 W4（10-11·依据 落码task brief §3.10）：同轮账——每次装配恰结算一笔（inject.cjs 钩子结算点调用；
//    状态段冻结命中只加 freezeHits 不进本函数）。越帽按裁前字节判（info.pre·TRIM_ON 时裁后字节另记 turnLast）。
//    本轮记录环只记 id 与标签（sid 前 8 位·轮次·策略版本·已显／被截／剔重 id·裁掉的项），不记正文。
const INJECT_TURN_RING = []; // 同轮合并字节环（512 笔·供 p50／p95）
function recordInjectTurn(bytes, info) {
	try {
		const b = Number.isFinite(bytes) && bytes > 0 ? Math.floor(bytes) : 0;
		injectMeter.turnLast = b;
		if (b > injectMeter.turnMax) injectMeter.turnMax = b;
		injectMeter.turnN += 1;
		INJECT_TURN_RING.push(b);
		if (INJECT_TURN_RING.length > 512) INJECT_TURN_RING.shift();
		const pre = info && Number.isFinite(info.pre) ? info.pre : b;
		if (pre > INJECT_TURN_CAP_BYTES) injectMeter.overCap += 1;
		const ids = (a) =>
			Array.isArray(a) ? a.filter((x) => Number.isInteger(x)).slice(0, 12) : [];
		const tags = (a) =>
			Array.isArray(a) ? a.map((x) => String(x).slice(0, 24)).slice(0, 6) : [];
		const o = info && typeof info === "object" ? info : {};
		injectMeter.turnLog.push({
			at: nowIso().slice(5, 16),
			sid: String(o.sid || "").slice(0, 8),
			turn: Number.isInteger(o.turn) ? o.turn : 0,
			ver: String(o.ver || "").slice(0, 16),
			st: o.st ? 1 : 0,
			why: String(o.why || "").slice(0, 16),
			bytes: b,
			pre: Number.isFinite(o.pre) ? Math.floor(o.pre) : b,
			shown: ids(o.shown),
			cut: ids(o.cut),
			xd: ids(o.xd),
			trim: tags(o.trim),
			late: o.late ? 1 : 0,
		});
		if (injectMeter.turnLog.length > 20) injectMeter.turnLog.shift();
	} catch {
		injectMeter.errors = (injectMeter.errors || 0) + 1; // ⑬ 出口
	}
}
// 同轮字节分位（最近秩法：升序第 ⌈p%·n⌉ 个）·无样本返回 0
function injectTurnPct(p) {
	try {
		if (!INJECT_TURN_RING.length) return 0;
		const a = INJECT_TURN_RING.slice().sort((x, y) => x - y);
		const i = Math.min(
			a.length - 1,
			Math.max(0, Math.ceil((Number(p) / 100) * a.length) - 1),
		);
		return a[i];
	} catch {
		injectMeter.errors = (injectMeter.errors || 0) + 1;
		return 0;
	}
}
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
					input: batch.map((t) => stripLone(t)), // 2026-09-22 戊路W-5：汇聚点净化（治 3056/chunkMemory 两入口）
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
		addUsage("embed", data); // E-7：嵌入 token 用量（DashScope·量最大者）
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
let vecChunkRowNMemo = { n: 0, at: 0 }; // 车三件①：超帽 COUNT 30s memo（B-04——原超帽态每次 search 白付全表 COUNT·实测热 0.07-0.22s）｜envIntCap 定义已上移文件头（TDZ 案·gate-car 抓）
async function vecRecallCore(conn, creds, query, topK) {
	try {
		const vtNowV = nowIso(); // 落码车硬排时刻（两扫描路同刻·2026-09-24）
		if (process.env.LEGION_VEC_ON !== "1" && process.env.LEGION_VEC_ON !== "query") { vecChannelOff += 1; if (Date.now() - vecChannelOffWarnAt > 600000) { vecChannelOffWarnAt = Date.now(); try { console.warn("[living-memory] vec 通道关断告警 #" + vecChannelOff + "（LEGION_VEC_ON 未设/非 1|query）——语义检索静默降级纯词面·语分恒 0 的根因位（10-02 语分归零案settled：plist env 丢失）；恢复＝plist EnvironmentVariables 加 LEGION_VEC_ON 后重启"); } catch {} } return []; } // option车：默认关断——原 return [] 零日志零计数（10-02 交办②补可观测·照 vecChannelErrors 同构·每 10 分钟记一次）
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
		// ── 车三件①（design-approved「发」·B-04+候选9）：帽检查前移＋COUNT 30s memo ──
		//    原序「补嵌→qvec→COUNT→帽判断」＝超帽降级态仍白付两笔（惰性补嵌喂胖它拒扫的表＋qvec 嵌入 API 算了即弃
		//    ——「拒扫的臂在喂胖它拒扫的表」#26374·「开销序须在容量闸前」通则第三例）。前移后：超帽态零补嵌零 qvec·直通降级。
		const VEC_SCAN_CAP = envIntCap(process.env.LEGION_VEC_SCAN_CAP, 80000); // 件②同车：整数帽族统一严格解析（W6——原裸 Number() 负值/小数/Infinity 无防护）；10-01 抬 50000→80000（approvedoption·标定：当日实测 chunks 57421·日均 +2.9k·80k 留 ~9 天余量——#25411「注释带标定库规模」律）
		if (now - vecChunkRowNMemo.at > 30000) {
			vecChunkRowNMemo = {
				n: Number(
					conn
						.prepare(
							`SELECT COUNT(*) c FROM memories_vec_chunks c JOIN memories m ON m.id = c.id WHERE c.model_version = ? AND LENGTH(c.embedding) > 0 AND m.status IN ('active','merged','done')`,
						)
						.get(VEC_CHUNK_MODEL)?.c,
				) || 0, // 防御取值：stub/异态 get 返 undefined 不炸（?.＋||0·vec-off-switch drill ④桩实证）
				at: now,
			};
		} // B-04：超帽态每次 search 不再白付全表 COUNT（EXPLAIN=SCAN·实测热 0.07-0.22s）
		const chunkRowN = vecChunkRowNMemo.n;
		// ── 帽接近度哨（10-01 落码·approvedoption·教训 #25409/#25411 码化）：chunks/cap ≥80% 且未超帽 → 60s 节流 warn＋organ_meta 落键 ──
		//    **模板适用范围**（10-01 approved分治）：**库量驱动型**帽设哨／**静态配置型**帽标免——两类清单与判据见本文件 `capNearWatch` 定义处注释（哨族通用实现）。
		//    与超帽路互斥（>cap 走原 budget-skip 路）；标定日现值 57421/80000=71.8% 在阈值下不触发（预期·载入后首验「贴现值判决输出」）。
		if (
			VEC_SCAN_CAP > 0 &&
			chunkRowN >= VEC_SCAN_CAP * 0.8 &&
			chunkRowN <= VEC_SCAN_CAP
		) {
			vecCapNear += 1;
			if (Date.now() - vecCapNearWarnAt > 60000) {
				vecCapNearWarnAt = Date.now();
				try {
					console.warn(
						"[living-memory] vec 帽接近度预警 #" +
							vecCapNear +
							" (chunks " +
							chunkRowN +
							" ≥80% × cap " +
							VEC_SCAN_CAP +
							" · 语义召回尚在·余量将尽——LEGION_VEC_SCAN_CAP 复评或库减脂)",
					);
				} catch {}
				if (!fs2.existsSync(SURGERY_FLAG)) {
					try {
						conn
							.prepare(
								"INSERT OR REPLACE INTO organ_meta (k, v) VALUES ('vec_cap_near', ?)",
							)
							.run(
								JSON.stringify({
									n: vecCapNear,
									chunks: chunkRowN,
									cap: VEC_SCAN_CAP,
									pct: Math.round((chunkRowN / VEC_SCAN_CAP) * 100),
									at: nowIso(),
								}),
							);
					} catch {
						vecScanSkipKeyErrors += 1; // 落键失败复用既有出口（⑬）
					}
				}
			}
		}
		if (chunkRowN > VEC_SCAN_CAP) {
			vecSkippedDueToBudget += 1;
			// R路车一①（design-approved「同意 按建议」·P10-1/R1 实勘增量）：超帽降级可见化——
			//    批㉘回填顶穿批㉙保险丝帽（09-29 实测 51783>50000·在产降级 14h+ 无人知）⇒ 照 L2127 vecChannelErrors
			//    同构补 60s 节流 warn＋organ_meta 落键（跨进程可读·nightly patrol可判·B5② 救援四计数持久化范式）。
			//    A级四件②（全链再审甲J3）：改用独立窗 vecScanSkipWarnAt——原共用 vecLastWarnAt 与通道错 warn 互斥（通道错刚 warn 则 60s 内超帽不 warn 不落键＝饿死面）。
			if (Date.now() - vecScanSkipWarnAt > 60000) {
				vecScanSkipWarnAt = Date.now();
				try {
					console.warn(
						"[living-memory] vec arm budget-skip #" +
							vecSkippedDueToBudget +
							" (chunks " +
							chunkRowN +
							" > cap " +
							VEC_SCAN_CAP +
							" · semantic recall degraded to lex-only · LEGION_VEC_SCAN_CAP 可调)",
					);
				} catch {}
				// 车一补丁（09-29 补审 B-21/B-08）：①落键须挂maintenance window闸（同函数惰性补嵌写点 L2030 surgeryOnVec 先例——
				//   「挂牌期检索只读」纪律·原缺口＝读路径写库无闸）②JSON 补 chunks/cap（跨重启后答得出「超多少」）
				//   ③catch 补计数出口（原空吞违 ⑬）。
				if (!fs2.existsSync(SURGERY_FLAG)) {
					try {
						conn
							.prepare(
								"INSERT OR REPLACE INTO organ_meta (k, v) VALUES ('vec_scan_skip', ?)",
							)
							.run(
								JSON.stringify({
									n: vecSkippedDueToBudget,
									chunks: chunkRowN,
									cap: VEC_SCAN_CAP,
									at: nowIso(),
								}),
							);
					} catch (e2k) {
						vecScanSkipKeyErrors += 1;
						if (Date.now() - vecScanSkipKeyErrWarnAt > 60000) {
							vecScanSkipKeyErrWarnAt = Date.now();
							try {
								console.warn(
									"[living-memory] vec_scan_skip key write failed #" +
										vecScanSkipKeyErrors +
										": " +
										String(e2k).slice(0, 60),
								);
							} catch {}
						}
					}
				}
			}
			return []; // 超帽降级＝词面独走（空通道直通·同 L1986 无 key 降级语义）——车三件①前移位：降级态零补嵌零 qvec
		}
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
			[clipSurrogateTail(String(query).slice(0, 1500))],
			VEC_CHUNK_MODEL,
			INSTRUCT_QUERY,
		); // 09-03 裁①：query 与 chunks 同空间（跨空间混查防线）·item1：查询侧 instruct（探针实证 cos=0.951 生效·qwen3.7 指令遵循）
		// 句级 max-sim：每条目取其 chunks 与 query 的最大余弦（迟交互同构·条目=多向量文档）
		// 车三件①（2026-09-30）：原「批㉙ option」扫描帽块（VEC_SCAN_CAP＋COUNT＋超帽降级分支）已整体前移至补嵌/qvec 之前——
		//    治「降级态仍白付惰性补嵌＋qvec 嵌入 API」（候选9/#26374·开销序须在容量闸前通则第三例）；批㉙设计语义不变（超帽＝降级词面独走·env LEGION_VEC_SCAN_CAP 可回调）。
		const allChunks = (() => {
			// ── 批㉚ option（design-approved「甲」·vec 上限设计件option）：候选预截——
			//    FTS 词面粗筛 ∪ 近期新条并集为池（帽 LEGION_VEC_POOL_CAP 缺省 500），余弦只算池内 chunks。
			//    🔴 闸默认关（LEGION_VEC_POOL_ON=1 才启用）：vec-eval 对拍实测（批㉙ vs 批㉚·同快照 120 对）——
			//    recall@5 85.8%→84.2%（-1.6pp）·均耗 2411→2762ms（未提速：瓶颈在嵌入 API 往返非余弦扫）
			//    ⇒ 现库规模（31k chunks）落码不值·码留闸关·库到十万级或 API 提速后再议开闸。
			if (process.env.LEGION_VEC_POOL_ON !== "1") {
				return conn
					.prepare(
						`SELECT c.id, c.embedding FROM memories_vec_chunks c JOIN memories m ON m.id = c.id WHERE c.model_version = ? AND LENGTH(c.embedding) > 0 AND m.status IN ('active','merged','done') AND (m.valid_to IS NULL OR m.valid_to > ?)`,
					)
					.all(VEC_CHUNK_MODEL, vtNowV); // 落码车硬排：valid_to 已过期条不进 vec 召回
			}
			const VEC_POOL_CAP = envIntCap(process.env.LEGION_VEC_POOL_CAP, 500);
			let poolSql = "";
			const poolArgs = [];
			try {
				const mt = queryMatch(qTokens(String(query)));
				const ftsPool = mt
					? conn
							.prepare(
								`SELECT rowid AS id FROM memories_fts WHERE memories_fts MATCH ? LIMIT ?`,
							)
							.all(mt, VEC_POOL_CAP)
					: [];
				const recentPool = conn
					.prepare(
						`SELECT id FROM memories WHERE status IN ('active','merged','done') ORDER BY id DESC LIMIT ?`,
					)
					.all(Math.min(200, VEC_POOL_CAP));
				const poolIds = [
					...new Set([...ftsPool.map((r) => r.id), ...recentPool.map((r) => r.id)]),
				];

				// 帽接近度哨（10-01 补·库量驱动型）：池大小 ≥80% 帽且未超 ⇒ 每查截断面接近
				capNearWatch(
					conn,
					"vecpool",
					poolIds.length,
					VEC_POOL_CAP,
					"向量池每查截断面接近·LEGION_VEC_POOL_CAP 复评",
				);
				// 超帽对偶路（10-02 落·廿九审甲路 ⚠1/#28825）：poolIds 去重上界 700>cap 500 ⇒ cur>cap 时近帽哨静默——
				//   此态恰为截断最深场景，照 family/vec 超帽范式补观测（独立 60s 窗·warn＋organ_meta vecpool_cap_over）。
				if (poolIds.length > VEC_POOL_CAP) {
					CAP_NEAR.vecpoolOver += 1;
					if (Date.now() - (capNearWarnAt.vecpoolOver || 0) > 60000) {
						capNearWarnAt.vecpoolOver = Date.now();
						try {
							console.warn(
								"[living-memory] vecpool 超帽观测 #" + CAP_NEAR.vecpoolOver + " (poolIds " + poolIds.length + " > cap " + VEC_POOL_CAP + " · 每查截断已封顶)",
							);
						} catch {}
						if (!fs2.existsSync(SURGERY_FLAG)) {
							try {
								conn.prepare("INSERT OR REPLACE INTO organ_meta (k, v) VALUES (?, ?)").run(
									"vecpool_cap_over",
									JSON.stringify({ n: CAP_NEAR.vecpoolOver, cur: poolIds.length, cap: VEC_POOL_CAP, at: nowIso() }),
								);
							} catch {
								CAP_NEAR.keyErrors += 1;
							}
						}
					}
				}
				if (poolIds.length > 0) {
					poolSql = ` AND c.id IN (${poolIds.map(() => "?").join(",")})`;
					poolArgs.push(...poolIds);
				}
			} catch {} // 池构建任何异常 → 空池Sql＝全表扫（现状等价·不劣化）
			return conn
				.prepare(
					`SELECT c.id, c.embedding FROM memories_vec_chunks c JOIN memories m ON m.id = c.id WHERE c.model_version = ? AND LENGTH(c.embedding) > 0 AND m.status IN ('active','merged','done') AND (m.valid_to IS NULL OR m.valid_to > ?)${poolSql}`,
				) // step：vec KNN 池同放宽（与 FTS 候选池一致·惰性回填写路保 active 纯净——merged 条 chunks 多在 active 期已建）；LENGTH>0：余件item一（09-09·三轮审计 §4.4-2）——空条目占位行 Buffer.alloc(0) 进 cosine 出 NaN 污染 bestById（父亲验实锤）·SQL 侧滤除
				.all(VEC_CHUNK_MODEL, vtNowV, ...poolArgs); // 落码车硬排（占位符序 model→valid_to→poolIds）
		})();
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
// ── directive型 todo 词表（注入席「顶替路」与 E-2「注入席互斥」**共用·单一source of record**）──────────
//    2026-09-12 审计真缺陷①修复：原为 section 回调内**局部**常量 ⇒ assemble 钩子（E-2）不可达
//    （与「席侧顶替路跨作用域幻引用前车」同族：跨作用域取不到对方局部）。而席侧顶替路命中的 todo
//    **无 space 限定、不受候选池 LIMIT 约束** ⇒ 可落在 E-2 池之外 ⇒ 「池 ⊇ 席最终集」
//    不变量被证伪（同条两席在该路径仍可达·且 autorecallSeatDedup 恒 0 假阴）。
//    ⇒ 提取为模块级，两路共用同一词源（零漂移）。
//    历史留证：8-31 长尾乙档（F2-4）RE 与 LIKE 提同一词源——原 RE 7 词/LIKE 5 词缺
//    「申请件/裁决」致该两词directive todo 顶替通道失效；>=2 放宽 >=1（单席态替换不扩预算）。
//    0.2.0 余件 ORD 泛化（09-10 approved）：英文分支短语级（防 request 泛词误伤）。
//    i flag：英文短语大小写不敏感（中文词不受影响）·SQLite LIKE 对 ASCII 天然不敏感=SQL 侧免 COLLATE。
const ORD_WORDS = [
	"申请件",
	"pending approval",
	"pending ruling",
	"awaiting approval",
	"awaiting approval",
	"裁决",
];
const ORD_WORDS_EN = [
	"\\bawaiting approval\\b",
	"\\bpending approval\\b",
	"\\bapproval request\\b",
	"\\bfor approval\\b",
	"\\bsubmitted for review\\b",
];
const ORD_TODO_RE = new RegExp(ORD_WORDS.concat(ORD_WORDS_EN).join("|"), "i");
const RERANK_MODEL = process.env.LEGION_RERANK_MODEL || "gte-rerank-v2";
const RERANK_TOPN = envIntCap(process.env.LEGION_RERANK_TOPN, 20); // 精排后入加权链条数（沙箱扫 10/20）
// D step（design-approved「同意 按建议」·D 项 rerank 双轨标定）：缺省 50 → **20**
//   依据＝**真链四态同库同刻**（this space自跑 120 对·产物 `a companion script`）：
//     OFF **88.3%**/0.718 ｜ **option CAND=20 90.0%**/0.707 ｜ 生产默认 CAND=50/TOPN=20 **85.0%**/0.688 ｜ 不裁剪 CAND=50/TOPN=50 86.7%/0.688
//   —— option vs 生产默认 **+5.0pp**（spoken +4.3／proper +7.0／multi 平）⇒ **帕累托支配**（五指标全胜）；机理＝「rerank 在 base
//   top20 内重排」有效，而「从 50 池精选 20」会打乱 base 好序（生产默认恰为 recall 最低组合）。
//   ⚠ 三态与brain独立复跑（18:2x pending approval件·异刻）**逐字一致**（85.0/0.688/76.6/87.7/100 等）；OFF 差 0.9~2.2pp＝**基线随库增长漂移**（已知现象）。
//   ⚠ 只改**缺省**：env `LEGION_RERANK_CAND` 覆盖语义不变（可随时回退）·TOPN 不动·未收回 proper 对 OFF 的 3.5pp 差
//   (design note)。
const RERANK_CAND = envIntCap(process.env.LEGION_RERANK_CAND, 20); // 喂 rerank 候选帽（原论证稿 top50；09-12 真链标定改 20）
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
						query: clipSurrogateTail(String(query).slice(0, 1000)), // query 4000 token 硬限＋代理对保对（2026-09-22 戊路W-2）
						documents: docs.map((d) => stripLone(d.text)), // 2026-09-22 戊路W-2：汇聚点净化
					},
					parameters: { top_n: docs.length, return_documents: false }, // 全量回分·截位在调用方
				}),
				signal: AbortSignal.timeout(10000), // embedOnce 15s 同族·精排延迟预算更紧
			},
		);
		if (!resp.ok) throw new Error("rerank http " + resp.status);
		const data = await resp.json();
		addUsage("rerank", data); // E-7：精排 token 用量
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
// ── 序2 同源 token 化（09-15 门控 P0）：与 pinboard-float.cjs 的 tk() 逐字同源 ──
//    （split(/[^\u2E80-\u9FFF\w]+/) 多字 token·非 jieba——轻量·批扫 2000 title 毫秒级；
//     改动须两侧同改＋drill 断言同值·同源常量 sameTopicOverlapMin 在 criteria 三级链）
function gateTitleTokens(text) {
	return new Set(
		String(text || "")
			.split(/[^⺀-鿿\w]+/)
			.filter((w) => w.length >= 2),
	);
}
// 批⑯b（design-approved option·#17050/#17101 双挂根因）：同题交集＝精确交 ∪ 长 token 子串互含——
//   整段 token 精确交时，标题前缀差一词（「memory organ注入面三处微修」vs「注入面三处微修」）即整段全异·交集实测 2<3 放行双挂；
//   option补：任一对 token 互为子串且短侧≥4 字亦计 1 交（精确交优先不重复计·误吸面=长 token 互含才计）。
//   ⚠ pinboard-float.cjs（离线分析脚本·工程/）另有旧精确交副本——非同源在线闸，另议不动（禁双闸口径漂移面外溢）。
function gateTokenInter(a, b) {
	let n = 0;
	for (const w of a) {
		if (b.has(w)) {
			n++;
			continue;
		}
		if (w.length >= 4) {
			let hit = false;
			for (const x of b) {
				if (x.length >= 4 && (x.includes(w) || w.includes(x))) {
					hit = true;
					break;
				}
			}
			if (hit) n++;
		}
	}
	return n;
}
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
function closureCheck(conn, title, content, todoId, noFreshDone) {
	// step C案：ts 参随②同日排除退役（原仅喂 todoT·P-1 双锚不一致影响面同消）
	// 批⑯(design note)：第 5 参 noFreshDone——auto 提炼路传 true 禁④-a新鲜销号豁免
	//   （豁免本意=手工路「销号后同专项下step」放行；auto 再造=LLM 见「已办结」叙述误挂·#17185/#17186 活证）；缺省=旧行为
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
		).filter((k) => !CRIT.closureAnchorStopwords.test(k)); // 审计 B① 修（09-15）：hits 面同滤停用表——原只滤锚定面（titleKw）·停用词经 content/bigram 进 kw 仍可凑 hits≥3（锚 2＋停用 2 反证）；滤后同题判定全链一致
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
				// 序2④-a（09-15）：取 closed_at 列供销号时效衰减判（done todo 销号后 N 小时内不作证据）
				"SELECT id, ts, type, title, content, closed_at FROM memories WHERE ts >= ? AND (" +
					"(type = 'todo' AND status = 'done') OR " +
					"((type = 'fact' OR type = 'decision' OR type = 'lesson') AND (title LIKE '%销账%' OR content LIKE '%销账%' OR title LIKE '%收官%' OR content LIKE '%收官%' OR title LIKE '%闭环%' OR content LIKE '%闭环%'))" +
					") ORDER BY id DESC LIMIT 500", // design-approved）
			)
			.all(since);
		// ── B-3（09-15 批 B 第 13 案·brainsettled「依据条选择偏差」）：证据按 type 优先级排序 ──
		//    done todo ＞ fact/decision ＞ lesson——使 PlanFence/闸拦的「见 #id」指向**真办结条**
		//    （原取循环首个词面命中条·不辨 type：#10537 被 lesson #10539 标过期而真依据是 fact #10529 活体案）。
		const _b3prio = { todo: 0, fact: 1, decision: 1, lesson: 2 };
		rows.sort((a, b) => (_b3prio[a.type] ?? 3) - (_b3prio[b.type] ?? 3));
		// ── v2.1 同题性锚定（2026-08-22 基线回归发现）：证据 title 须与待办 title ≥1 关键词重叠 ──
		//    病例：done 「注入瘦身方案awaiting approval」content 顺带提「某长尾话题速览下沉」3 词 → 跨主题误判 。
		//    修法：闭环是同题判定，题眼必须在双方 title 层对上；content 顺带词不算数。
		// 序2①（09-15 maintainer·closure gate三案）：锚定门停用表——title 关键词先剔跨主题通用词
		//   （⚠ 开放集·治标：新通用词会再撞·非治本——收词 B2 已治 3 词＋ 09-14 误拦 5 词·外置 criteria 三级链）
		const STOP_RE = CRIT.closureAnchorStopwords;
		const titleKw = gateKeywords(title, 8).filter((k) => !STOP_RE.test(k));
		// ── 判定精确化（design-approved「判定精确化」·#31515）：踵次/编号差异识别 ──
		//    双方 title 各提取「数字+序数单位」对（踵|审|刀|案|批|件|车|令）；同单位数字不同 ⇒ 不同题放行
		//    （编号=任务身份词·词面闸最强反证）。治 #31483：105 踵验收被误判与 99 踵同题（锚≥2+补证≥3 全过但踵次身份不同）。
		//    观测键 closureGateSeqClash＝⑬四点接线（赋值/return read.cjs/render core.cjs「序号冲」/drill）。
		const tSeq = new Map();
		{
			const SEQ_RE = /(\d+)\s*(踵|审|刀|案|批|件|车|令)/g;
			let m;
			const _t0 = String(title || "");
			while ((m = SEQ_RE.exec(_t0))) tSeq.set(m[2], m[1]);
		}
		// 序2④-a：销号时效衰减——N 外置（外推counter·首期只报数·跑一段取本机分布再定终值）
		const freshH = Number(CRIT.closureFreshDoneHours) || 0;
		let doneEv = 0;
		for (const r of rows) {
			if (String(title || "").trim() === String(r.title || "").trim()) continue; // 同名条（复述/追问）不算闭环证据
			if (r.type !== "todo") {
				// 证据面收紧：元类条目（关于事实的记忆）不作证据
				if (META_RE.test(String(r.title || "") + String(r.content || "")))
					continue;
				// ②同日排除已删（C 案·step）：同批互证由上方①元类全排承担；同日真 fact 为有效证据
			} else {
				doneEv++; // 序2观测：证据池 done todo 计数（closureGateDoneEvidence）
				// 序2④-a：done todo 销号后 N 小时内不作证据——治「销号条拦同专项后续待办」系统性死锁
				//   （09-14 实证 2：#10287 销号即拦step产出 todo）；同主题阶段推进紧接销号产生·真僵尸回流多在数日后（假设·counter验证）
				if (!noFreshDone && freshH > 0 && r.closed_at) { // 批⑯：auto 提炼路（noFreshDone=true）不走④-a豁免——手工/注入席路旧行为不动
					// closed_at 生产形态＝分钟精度带时区（「2026-09-14T23:35+08:00」·nowIso 同源）——slice(0,16)
					// （首版 slice(0,19) 取出「+08:」垃圾尾 ⇒ Date.parse NaN ⇒ 豁免恒不触·drill 构造反证抓出）
					const cMs = Date.parse(
						String(r.closed_at).slice(0, 16) + "+08:00",
					);
					if (!isNaN(cMs) && Date.now() - cMs < freshH * 3600 * 1000) {
						closureGateFreshDoneSkip++; // N 标定的数据源
						// B-2②（批 C 候办）：去重条口径——当日 Set（跨调用同 id 只计一条·render 透出 size·量纲=条）
						const _day = new Date(Date.now() + 8 * 3600e3)
							.toISOString()
							.slice(0, 10);
						if (closureGateDedupDay !== _day) {
							closureGateDedupDay = _day;
							closureGateFreshDoneIds.clear();
						}
						closureGateFreshDoneIds.add(r.id);
						continue;
					}
				}
			}
			const rTitle = String(r.title || "").toLowerCase();
			// 判定精确化（#31515）：序数单位对撞——同单位不同数字 ⇒ 不同题（continue 放行·计观测键）
			if (tSeq.size) {
				let _seqClash = false;
				const SEQ_RE2 = /(\d+)\s*(踵|审|刀|案|批|件|车|令)/g;
				let m2;
				while ((m2 = SEQ_RE2.exec(rTitle))) {
					const _pv = tSeq.get(m2[2]);
					if (_pv !== undefined && _pv !== m2[1]) {
						_seqClash = true;
						break;
					}
				}
				if (_seqClash) {
					closureGateSeqClash++;
					continue;
				}
			}
			// ── 序2②（09-15 maintainer）：锚定门槛 1→2·废单锚弱门（content 补证路）──
			//    治 09-14 实证 1（「space」单锚＋content 补证凑 3 hits 跨主题误拦）；
			//    B2 v2.2 的 α(≥2) 口径单独成立。副作用面（真同题但 title 词差异大被放过）由
			//    closureGateAnchorWeak 计数回看——该面词面闸本就无证据可判（词不同=证据不足）。
			const anchorHits = titleKw.filter(
				(k) => k.length > 1 && rTitle.includes(k),
			).length;
			if (anchorHits === 1) {
				closureGateAnchorWeak++; // ②副作用观测：单锚样本被放行计数
				// B-2④（批 C 候办）：撞词可观测——单锚弱放样本的锚词面落 organ_meta 最近 8 例环形
				//   （供回看「新通用词该不该进停用表」·治开放集「新词再撞无信号」·判据建议原样落）
				try {
					const _rec = {
						ts: nowIso(),
						anchor: String(title || "").slice(0, 40),
						kws: titleKw.slice(0, 6),
					};
					let _arr = [];
					try {
						_arr = JSON.parse(
							conn
								.prepare(
									"SELECT v FROM organ_meta WHERE k='closure_anchor_weak_recent'",
								)
								.get()?.v || "[]",
						);
					} catch {}
					if (!Array.isArray(_arr)) _arr = [];
					_arr.push(_rec);
					while (_arr.length > 8) _arr.shift();
					conn
						.prepare(
							"INSERT OR REPLACE INTO organ_meta (k, v) VALUES ('closure_anchor_weak_recent', ?)",
						)
						.run(JSON.stringify(_arr));
				} catch {}
			}
			if (anchorHits < 2) continue; // 序2②：单锚弱门废——α≥2 才认同题
			const hay = (rTitle + " " + String(r.content || "")).toLowerCase();
			let hits = 0;
			for (const k of kw) if (k.length > 1 && hay.includes(k)) hits += 1; // β：单字 token 不计 hits
			if (hits >= 3) {
				closureGateBlocked++; // 序2观测：拦截总数——B-2 订正口径（done证≥5,000 且仍 0 才判失效·换班窗未满=insufficient-data）
				return { closed: true, by: r.id, byTitle: r.title, overlap: hits };
			}
		}
		closureGateDoneEvidence += doneEv; // 序2观测：证据池 done todo 累计（④-a 豁免面基线）
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
let usageQualityErrors = 0; // 检索质量仪表（09-12 第一步）：聚合面异常累计（⑬ 出口闭环——静默 catch 必带计数透出）
// ── 向量补嵌可见化四计数（2026-09-12 第二步诊断·零行为改动）──
//    动机（实测）：auto:session-* 条无写时嵌入（提炼器自带 INSERT 不走 registerMemoryWrite ⇒ 不触），
//    全靠nightly patrol backfill 兜底；09-10 起 auto 条向量化断崖（09-10 3/115·09-12 17/274）⇒ 积压 553 条且日增。
//    而该段原只有 logger.info/warn（日志随重启覆盖=零可回溯）+ `if (c && c.value)` 无 else（凭据空静默跳过）
//    ⇒ 故障面全盲。四计数 + organ_meta 落账把「跑没跑/补了几条/凭据有没有/错没错」变成可查事实（⑬）。
let vecBackfillRuns = 0; // nightly patrol backfill 段跑次数（本进程累计）
let vecBackfillFilled = 0; // 累计补嵌条数
let vecBackfillNoCred = 0; // 凭据解析空而静默跳过次数（⑬ 出口——原无 else）
let vecBackfillErrors = 0; // 段内异常累计
let writeTimeEnhanceErrors = 0; // 09-12 主案：提炼路写时增强（嵌入/建边）异常累计（⑬ 出口）
// 二轮审计 D1-d（2026-09-12）：`err` 原每巡重置 ⇒ 失败后若次巡成功，故障史消失（与「跨重启可查失败原因」意图落差）
// ⇒ 增设「最近一次失败」跨巡保留位（成功不清），落账同时带出本巡 err 与最近失败 lastErr/lastErrAt。
let vecBackfillLastErr = null; // 最近一次失败摘要（跨巡保留·进程内）
let vecBackfillLastErrAt = null; // 最近一次失败时刻
// ── P2 修（09-03 audit）：模块级计数三键（toolUsageErrors 同族「静默降级透出」立法第一活体同法）──
let closureCheckErrors = 0; // closureCheck 顶层异常累计（四闸核心静默全放行的观测面）
// ── 序2 观测键四枚（design-approved「同意」）——closureCheck 系模块级函数不可达局部 stats·照 closureCheckErrors 先例 ──
let closureGateBlocked = 0; // 拦截总数——B-2 订正（09-15 批 B·brain自查同认）：判「闸失效假绿」须**累计调用 ≥N（done证≥5,000）且仍 0** 才报警；换班后观测窗未满＝insufficient-data 不判（原「归零即失效」硬口径缺最小观测量前置·每次换班必假报警）
let closureGateAnchorWeak = 0; // ②门槛 1→2 副作用观测：单锚样本被放行计数（真同题词差异大面回看）
let closureGateDoneEvidence = 0; // 证据池 done todo 累计（④-a 豁免面基线）
let closureGateFreshDoneSkip = 0; // ④-a 时效豁免命中（N=24 外推counter·标定数据源）
let closureGateSeqClash = 0; // 判定精确化（#31515·design-approved「判定精确化」）：踵次/编号冲突放行计数（「105 踵 vs 99 踵」同单位不同数字⇒不同题）
// ── B-2②/④（批 C 候办·09-15）——去重条口径 + 撞词可观测 ──
const closureGateFreshDoneIds = new Set(); // B-2②：④-a 豁免**去重条口径**（当日 Set——治「调用×条」乘积推不出豁免率·N=24 标定的真数据源）
let closureGateDedupDay = ""; // 当日键（YYYY-MM-DD·非当日清 Set 重开）
const hitMapErrorsStat = { n: 0 }; // B-1④（批 C 候办）：hitMap 攒批/落盘失败计数（进程口径·对象形态抗纠错；累计口径在 organ_meta['hit_map_errors']·写点双写）
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
			hitLex = null, // 检索质量仪表（09-12 第一步）：词面命中条数（ftsBase>0）
			hitVec = null, // 纯语义命中条数（ftsBase=0∧vecPart>0）
			hitFb = null, // 兜底填充条数（社区泛化/前缀保底/主题词重查/无分条）
			rc = -1;
		try {
			const out = await fn(args, exec);
			if (out && Array.isArray(out.hits)) {
				rc = out.hits.length;
				// 检索质量仪表（09-12 第一步）：命中成分三拆——词面 / 纯语义 / 兜底填充（社区泛化·前缀保底·主题词重查条无分）
				let _l = 0,
					_v = 0;
				for (const _h of out.hits) {
					const _bf = Number(_h && _h.ftsBase) || 0,
						_bv = Number(_h && _h.vecPart) || 0;
					if (_bf > 0) _l++;
					else if (_bv > 0) _v++;
				}
				hitLex = _l;
				hitVec = _v;
				hitFb = rc - _l - _v;
			} else if (out && Array.isArray(out.entries)) rc = out.entries.length;
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
							"INSERT INTO tool_usage (ts, tool, caller_space, action, duration_ms, result_count, success, error_kind, hit_lex, hit_vec, hit_fb) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
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
							hitLex,
							hitVec,
							hitFb,
						);
			} catch {
				toolUsageErrors++;
			}
		}
	};
}
// ── 检索质量仪表（design-approved「按建议逐项」）──
//    读 tool_usage 近 N 天 memory/search 且含成分列的行 → 真命中成分聚合。
//    口径：词面条占比=Σhit_lex/Σ条·语义条占比=Σhit_vec/Σ条·兜底条占比=Σhit_fb/Σ条；
//    词零命中率=hit_lex=0 的调用占比（词面单通道查不到的调用面=向量/图的价值面）；
//    仅兜底率=hit_lex=0∧hit_vec=0（全部靠社区泛化/前缀保底/主题词重查凑出）。
//    历史行（hit_lex NULL·09-12 前）不入分母——口径自本列投产起算，聚合面按 IS NOT NULL 过滤。
//    ⑬：异常累计 usageQualityErrors 透出，绝不拖垮 stats（静默降级透出立法同族）。
function usageQualityDigest(db, days) {
	const _d0 = days || 7;
	try {
		const _cut =
			new Date(Date.now() + 8 * 3600e3 - _d0 * 864e5)
				.toISOString()
				.slice(0, 16) + "+08:00";
		const _rows = db
			.prepare(
				"SELECT duration_ms, hit_lex, hit_vec, hit_fb FROM tool_usage WHERE ts >= ? AND tool='memory' AND action='search' AND hit_lex IS NOT NULL",
			)
			.all(_cut);
		if (!_rows.length) return null;
		let _l = 0,
			_v = 0,
			_f = 0,
			_lz = 0,
			_fo = 0;
		const _d = [];
		for (const _r of _rows) {
			const _hl = Number(_r.hit_lex) || 0,
				_hv = Number(_r.hit_vec) || 0,
				_hf = Number(_r.hit_fb) || 0;
			_l += _hl;
			_v += _hv;
			_f += _hf;
			if (_hl === 0) _lz++;
			if (_hl === 0 && _hv === 0) _fo++;
			_d.push(Number(_r.duration_ms) || 0);
		}
		_d.sort((a, b) => a - b);
		const _tot = _l + _v + _f;
		return {
			days: _d0,
			n: _rows.length,
			lexPct: _tot ? Math.round((_l * 100) / _tot) : 0,
			vecPct: _tot ? Math.round((_v * 100) / _tot) : 0,
			fbPct: _tot ? Math.round((_f * 100) / _tot) : 0,
			lexZeroPct: Math.round((_lz * 100) / _rows.length),
			fbOnlyPct: Math.round((_fo * 100) / _rows.length),
			p95: _d[Math.min(_d.length - 1, Math.floor(_d.length * 0.95))],
		};
	} catch {
		usageQualityErrors++;
		return null;
	}
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
// [拆④] db 单点化（2026-10-04）：index apply 闭包建库段（原 L1735-1764 逐字迁入·DB_PATH→dbPath 一处参数化）——
// host 主连接单点：new+PRAGMA（WAL/busy_timeout）+建表族+自注册日志。dbW（write 分支）轻连接不复用（拆而不变）。
function openDb(dbPath, ctx) {
	let db;
			db = new DatabaseSync(dbPath);
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
			episodicAnchors.initHost(db);
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
	return db;
}
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
		// ── 检索质量仪表（design-approved「按建议逐项」）：tool_usage 真命中成分三列 ──
		//    动机（本车实测·构造反证）：result_count 只记「返回条数」，而检索链兜底面（vec 独有注入 / 前缀保底 /
		//    主题词重查 / 社区泛化 ）恒把结果填满 ⇒ 「零结果率」结构性恒 0%（2010 行实测 0/629），
		//    只读 result_count = 假仪表（看不出一等检索质量）。
		//    三列：hit_lex=词面条数(ftsBase>0) / hit_vec=纯语义条数(ftsBase=0∧vecPart>0) / hit_fb=兜底填充条数。
		//    历史行 NULL=旧口径（聚合面按 hit_lex IS NOT NULL 过滤）；幂等 ALTER 静默（既有 stale_by 同族惯例）。
		for (const _c of ["hit_lex", "hit_vec", "hit_fb"]) {
			try {
				dbConn.exec(`ALTER TABLE tool_usage ADD COLUMN ${_c} INTEGER`);
			} catch {}
		}
		// ── ⑤-1 会话板持久层（design-approved「继续开」·研判二 C 档＋四点吸收①）──
		//    目标：把「板」从对话历史搬出——独立持久层（`todos/<sid>.json` 意）·last-write-wins·按 sid 一行；
		//    治「几轮后忘/换窗忘」（板不再随压缩/换窗失）（研判二四症状根因）。
		//    纪律：⑭ 结构变更一律 ensureFts（/ 教训——挂nightly patrol会留缺列/缺表窗口）；
		//    payload＝整表 JSON 串（`{todos:[{content,status,note?}]}`）·updated_at＝ISO 本地时刻。
		//    本步仅建表（零行为改动）·读写工具与注入回显见 ⑤-2/⑤-3。
		try {
			dbConn.exec(
				`CREATE TABLE IF NOT EXISTS session_boards (sid TEXT PRIMARY KEY, payload TEXT NOT NULL, updated_at TEXT NOT NULL)`,
			);
		} catch {}
		// ── I-7（2026-09-20 审计·改进）：闸①／①②级候选扫的复合索引 ──
		//    病灶：闸① 候选扫与 ①②级候选扫均走 `type=? AND space=? AND status='active'`，真库无 memories 复合索引
		//    ⇒ EXPLAIN 显示 `SCAN memories`（16654 行）·实测 4.69 ms/次，且**每条 todo 写入必跑一次**——
		//    与梁 1 闸自诩「纯字符串判断·零 DB 查询·守写路快径」的纪律相左。
		//    结构变更一律 ensureFts（/）·CREATE INDEX IF NOT EXISTS＝幂等·独立 try 不清全局。
		try {
			dbConn.exec(
				`CREATE INDEX IF NOT EXISTS idx_mem_type_space_status ON memories(type, space, status)`,
			);
		} catch {}
		// 批⑰ D14①（2026-09-21·J 路 J7-2）：常驻核心席 partial 索引——essential=1∧active 仅 33/16k，原每轮注入全索引扫＋TEMP B-TREE；
		//   partial 索引把席查询压到 33 行面。幂等 IF NOT EXISTS·独立 try 同族纪律。
		try {
			dbConn.exec(
				`CREATE INDEX IF NOT EXISTS idx_mem_essential_active ON memories(space, validated_count DESC, id DESC) WHERE essential = 1 AND status = 'active'`,
			);
		} catch {}
		// step VEC_MODEL 哨（B级P1·wave建议）：启动比对模型戳——不符 warn 提示重嵌（防跨批漂移·8-28 vec-eval 33.3% 案的同族预防）
		try {
			const vStamp = dbConn
				.prepare("SELECT v FROM organ_meta WHERE k='vec_model_stamp'")
				.get();
			// 车三件④（候选12·09-30 approved）：哨比对面改 VEC_CHUNK_MODEL——原比 VEC_MODEL（旧 v4 单向量表遗留常量 L1858）而 chunks 实际运行时=qwen3.7（09-03 裁①迁移毕·库实查全量 qwen3.7）⇒ 观测键失真（八维勘第一发即被误导实证其害）。
			if (!vStamp)
				dbConn
					.prepare(
						"INSERT OR REPLACE INTO organ_meta (k, v) VALUES ('vec_model_stamp', ?)",
					)
					.run(VEC_CHUNK_MODEL);
			else if (String(vStamp.v) === VEC_MODEL)
				// 一次性戳迁移：v4 系裁①前历史戳（非待办重嵌——chunks 已全量 qwen3.7）·启动幂等自愈·防改哨后每次启动误 warn
				dbConn
					.prepare(
						"INSERT OR REPLACE INTO organ_meta (k, v) VALUES ('vec_model_stamp', ?)",
					)
					.run(VEC_CHUNK_MODEL);
			else if (String(vStamp.v) !== VEC_CHUNK_MODEL)
				console.warn?.(
					`[living-memory] VEC_MODEL 切换告警：库=${vStamp.v} 运行时=${VEC_CHUNK_MODEL}——旧向量不参与 vec 路·请跑 reembed 全库重嵌`,
				);
		} catch {}
		dbConn.function("legion_bigram", bigramSpace); // 触发器在两角色连接上都可能触发
		dbConn.exec(
			`CREATE VIRTUAL TABLE IF NOT EXISTS memories_fts USING fts5(tokens)`,
		);
		// ── 微型件②：分词器版本戳（源：layered-memory FTS 版本戳·2026-08-21 B2 借鉴）──
		//    token 形态变更时防新旧索引混查——不匹配仅 warn 提示重建，不自动重建（maintainer措辞）。
		// TOKENIZER_VERSION（车二）：已提升至**模块级**——启动重建块与 ensureFts 同源引用，防两处漂移
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
// ── 主案（design-approved「按建议」·写时增强抽公共件·两写入点复用）──
//    根因（实测·跨层构造反证）：写时嵌入 与 反思建边（auto-link）原为 `registerMemoryWrite` 内联段，
//    而**提炼器 runExtract 自带 `INSERT INTO memories`**（不走统一写入口）⇒ 两增强对 auto 条**同时失能**：
//    auto 条缺向量 568（09-10 起断崖 3/115→17/274）·无活边覆盖仅 37.1%（手工条 60.4%）。
//    ⇒ 抽两公共件：手工写路与提炼路同源调用。语义与原内联段一致（见下各函数内注）。
//    ⚠ 防漂移：`writeTimeReflectLink` 的判据（题面 jaccard≥0.20 ∧ inter≥2 ⇒ auto-link 0.4）与手工路内联段**同判据**，
//    手工路因与「盖戳」逻辑耦合暂留内联——改一处必改两处，drill `after-write-enhance-drill` 含常量一致性断言。
//    写时嵌入（同构·含 chunks 句级路·fire-and-forget 语义由调用方 `void` 保证）
async function writeTimeEmbed(db, id, title, content, credResolve) {
	// 审计 D1（2026-09-12）：`LEGION_A14_OFF` 由「调用点判断」**下沉至函数内**——原只在手工路调用点判断，
	// ⇒ 提炼路调用绕过开关（应急回退关不掉仍打 API）。下沉后两调用点同受控（调用点判断保留=belt-and-braces·口径一致）。
	try {
		if (process.env.LEGION_A14_OFF) return;
		if (process.env.LEGION_VEC_ON !== "1") return; // option车(design note)：向量默认关断——本函数单条+chunks 两嵌入调用全停（=== "1" 严格开·"0"/"false"/空串均关·审计 P3⑤ 治）
		if (!credResolve) return;
		if (!vecCredCache.v || Date.now() - vecCredCache.t > 60000) {
			const c = await credResolve("EMBEDDING_BAILIAN_KEY");
			if (!c || !c.value) return;
			vecCredCache = { v: c.value, t: Date.now() };
		}
		const [emb] = await embedOnce([(title + " " + content).slice(0, 1500)]);
		if (!emb) return;
		db.prepare(
			"INSERT OR REPLACE INTO memories_vec (id, embedding, model_version, content_hash) VALUES (?, ?, ?, ?)",
		).run(
			Number(id),
			f32ToBlob(emb),
			VEC_MODEL,
			crypto.createHash("md5").update(String(title + " " + content).slice(0, 1500)).digest("hex"),
		); // content_hash 与nightly patrol门同源（md5·title+' '+content 前1500·8-31 移植族修复⑥同式）
		try {
			db.exec(
				`CREATE TABLE IF NOT EXISTS memories_vec_chunks (id INTEGER NOT NULL, seq INTEGER NOT NULL, chunk_text TEXT, embedding BLOB NOT NULL, model_version TEXT NOT NULL, PRIMARY KEY (id, seq, model_version))`,
			);
			const chunks = chunkMemory(title, content);
			if (chunks.length) {
				const cembs = await embedOnce(chunks, VEC_CHUNK_MODEL);
				const cins = db.prepare(
					"INSERT OR REPLACE INTO memories_vec_chunks (id, seq, chunk_text, embedding, model_version) VALUES (?, ?, ?, ?, ?)",
				);
				for (let ci = 0; ci < chunks.length && ci < cembs.length; ci++)
					cins.run(Number(id), ci, chunks[ci], f32ToBlob(cembs[ci]), VEC_CHUNK_MODEL);
			}
		} catch {
			writeTimeEnhanceErrors += 1; // ⑬ 出口（chunks 路失败不拖主嵌）
		}
	} catch {
		writeTimeEnhanceErrors += 1; // ⑬ 出口（原内联段空 catch·抽取时补出口）
	}
}
// 反思建边（A-MEM 写入即反思同构·零 LLM 零嵌入纯内存扫描）：命中返回 {j, dst}，否则 null
function writeTimeReflectLink(db, o) {
	try {
		const tk = (sv) => {
			const out = new Set();
			const str = String(sv);
			try {
				if (jieba)
					for (const w of jieba.cut(str, true)) if (w.trim().length >= 2) out.add(w.trim());
			} catch {}
			if (out.size === 0)
				for (const w of str.split(/[^\u2E80-\u9FFF\w]+/)) if (w.length >= 2) out.add(w);
			return out;
		};
		const nt = tk(o.title);
		if (nt.size < 2) return null;
		const cands = db
			.prepare(
				"SELECT id, title FROM memories WHERE status='active' AND space = ? AND type = ? AND id != ? LIMIT 2000",
			)
			.all(String(o.space), String(o.type), Number(o.id));
		let best = null,
			bestJ = 0,
			bestI = 0;
		for (const c of cands) {
			const ct = tk(c.title);
			let inter = 0;
			for (const w of nt) if (ct.has(w)) inter++;
			const j = inter / (nt.size + ct.size - inter || 1);
			if (j > bestJ) {
				bestJ = j;
				best = c;
				bestI = inter;
			}
		}
		if (best && bestJ >= 0.2 && bestI >= 2) {
			db.prepare(`INSERT INTO memories_edges (src, dst, edge_type, weight, valid_at, source_group, last_seen, instruction)
              VALUES (?, ?, 'auto-link', 0.4, ?, 'reflect', ?, ?)
              ON CONFLICT(src, dst, edge_type) DO UPDATE SET last_seen = excluded.last_seen`).run(
				Number(o.id),
				best.id,
				o.ts,
				o.ts,
				"写入即反思: 题交叠 jaccard=" + bestJ.toFixed(2),
			);
			return { j: bestJ, dst: best.id };
		}
		return null;
	} catch {
		writeTimeEnhanceErrors += 1; // ⑬ 出口
		return null;
	}
}
// ══ ⑩ 任务账导出器（design-approved「处置方案（step＋一闸·发车」）══
//    定位：治maintainer实勘所判「压缩总结里的待办清单是模型从叙述重构、非从权威账读出」——
//    三张清单（①本窗已办结 ②未闭环 todo ③候外部依赖）**一律由账态直接导出**（模型只排布润色）。
//    调用方：⑩D 注入面「本窗任务账」块／⑩C 压缩点快照条／⑩E 缺失哨——单一实现·三处共用。
//    口径：space＝空间；hours＝已办结时间窗（缺省 12h·`closed_at` 与 sinceIso 同为 22 字符 ISO ⇒ 字符串可比）；
//    limit＝每类条数帽。零数据＝空账（渲染侧按「零数据不输出」处理）。
// ── 2400 导出器 v2·三要素解析（源/态/流转·design-approved「和代码的设计统筹综合到一起」）──
//    源：①content「源＝…」约定句 ②结构边（剔 solved_by）③source 字段——推导不出标「源缺」提示补记
//    〔优先级订正 10-05 验证环：件二规格「边优先」系 fact 面视角；todo 面实证约定句优先——存量双在 2 条约定句全为真来源、solved_by 边指向销号条＝闭环证据非来源（#27466/#30665 实证）；规格差异随件二收官receipt报brain对齐〕
//    态：候谁（title「候X」提取）／卡点（content「卡＝」）／已闭环（closed_at）／在办
//    流转：content「流转＝<谁>→<做什么>」约定句·无则按候谁推导(design note)
// ── 候对象提取（候谁）·词表优先＋兜底截断（≤6字）｜板v2余面修（验证环🟡1候谁过贪/🟡2右括号粘附·10-05 maintainer甲·全量238条离线对照：失配8→1·右括号→0）──
//    词表序＝长词优先（alternation 左先）；兜底排除 ）) 治右括号粘附；候后允许空白（「候 2400」「候 docs_reindex」面）
//    同构对拍点：inject.cjs 板v2短注（在办/待办段两点）经 exports 引用本函数——逐字同一（#27098 同构须逐字核）
const WAIT_WHO_RE =
	/候\s*(您(?:亲裁|裁决|裁|批|示)?|值机台|大脑|生殖腺|创造窗|创造|记忆器官|信息抓取|小红书|美国运营|杂志照片|本地维护|视频|极简|夜巡|提炼|外部|各器官|父窗|排期|条件|重启|回执|docs_reindex|脑|办|裁|\d+|[^：:（(）)\s「」]{1,6})/;
function matchWaitWho(s) {
	return String(s || "").match(WAIT_WHO_RE);
}
function parseTaskElems(db, row) {
	const title = String(row.title || "");
	const content = String(row.content || "");
	let source = "";
	const mSrc = content.match(/源[＝=]\s*([^｜|\n]{1,40})/);
	if (mSrc) source = mSrc[1].trim();
	if (!source) {
		try {
			const e =
				db &&
				db
					.prepare(
						"SELECT src FROM memories_edges WHERE dst = ? AND edge_type IN ('subtask_of','part_of_campaign','depends_on') AND invalid_at IS NULL ORDER BY id DESC LIMIT 1", // 剔 solved_by：销号条＝闭环证据非来源（验证环实证·见段头注）
					)
					.get(row.id);
			if (e) source = "#" + e.src;
		} catch {}
	}
	if (!source && row.source) source = String(row.source).slice(0, 30);
	const mWait = matchWaitWho(title); // 板v2余面修：词表法候对象提取（旧贪婪10字过贪＋右括号粘附·WAIT_WHO_RE 注）
	const mStuck = content.match(/卡[＝=]\s*([^]{1,30})/);
	const state = row.closed_at
		? "已闭环"
		: mStuck
			? "卡点:" + mStuck[1].slice(0, 12)
			: mWait
				? "候" + mWait[1]
				: "在办";
	const mFlow = content.match(/流转[＝=]\s*([^]{1,40})/);
	let flow = mFlow ? mFlow[1].trim() : "";
	if (!flow) {
		if (row.closed_at) flow = "—";
		else if (mWait) flow = mWait[1].slice(0, 6) + "→裁量/receipt";
		else flow = "this space→推进";
	}
	return { source: source || "源缺", state, flow };
}
function exportTaskLedger(db, opts) {
	const o = opts || {};
	const space = String(o.space || "");
	const sid = String(o.sid || "");
	const hours =
		Number(o.hours || o.sinceHours) > 0 ? Number(o.hours || o.sinceHours) : 12;
	const lim = Number(o.limit) > 0 ? Number(o.limit) : 3;
	const sinceIso =
		new Date(Date.now() - hours * 3600 * 1000 + 8 * 3600 * 1000)
			.toISOString()
			.slice(0, 16) + "+08:00";
	const out = {
		space,
		sid,
		hours,
		sinceIso,
		done: [],
		open: [],
		waiting: [],
		n: { done: 0, open: 0, waiting: 0 },
	};
	if (!space) return out;
	try {
		out.done = db
			.prepare(
				"SELECT id, title, content, source, closed_at FROM memories WHERE type='todo' AND status='done' AND space=? AND closed_at >= ? ORDER BY closed_at DESC LIMIT ?",
			)
			.all(space, sinceIso, lim)
			.map((r) => ({ ...r, elems: parseTaskElems(db, r) })); // v2：三要素附行
		out.n.done =
			Number(
				(
					db
						.prepare(
							"SELECT COUNT(*) AS n FROM memories WHERE type='todo' AND status='done' AND space=? AND closed_at >= ?",
						)
						.get(space, sinceIso) || {}
				).n,
			) || 0;
		// E 路审计 真1 修（09-20 23:1x·⚠中）：原 `open` 与 `waiting` 同源 ⇒ `waiting ⊆ open` 必同条二现
		//   （实测 #16751 同轮占 ☐ 与 ⏳ 两行·320 字符预算约 1/3 被重复占用）⇒ open 剔「候」支，三清单**互斥**。
		out.open = db
			.prepare(
				"SELECT id, title, content, source, ts FROM memories WHERE type='todo' AND status='active' AND space=? AND title NOT LIKE '%候%' ORDER BY id DESC LIMIT ?",
			)
			.all(space, lim)
			.map((r) => ({ ...r, elems: parseTaskElems(db, r) })); // v2：三要素附行
		out.n.open =
			Number(
				(
					db
						.prepare(
							"SELECT COUNT(*) AS n FROM memories WHERE type='todo' AND status='active' AND space=? AND title NOT LIKE '%候%'",
						)
						.get(space) || {}
				).n,
			) || 0;
		out.waiting = db
			.prepare(
				"SELECT id, title, content, source, ts FROM memories WHERE type='todo' AND status='active' AND space=? AND title LIKE '%候%' ORDER BY id DESC LIMIT ?",
			)
			.all(space, lim)
			.map((r) => ({ ...r, elems: parseTaskElems(db, r) })); // v2：三要素附行
		// ── 2400 v2·全局视图（第二输出面）：候外部跨 space 汇总＋在办分布（brain全景用·帽 12 space）──
		out.global = {
			waitingBySpace: db
				.prepare(
					"SELECT space, COUNT(*) AS n FROM memories WHERE type='todo' AND status='active' AND title LIKE '%候%' GROUP BY space ORDER BY n DESC LIMIT 12",
				)
				.all(),
			openBySpace: db
				.prepare(
					"SELECT space, COUNT(*) AS n FROM memories WHERE type='todo' AND status='active' AND title NOT LIKE '%候%' GROUP BY space ORDER BY n DESC LIMIT 12",
				)
				.all(),
		};
		out.n.waiting =
			Number(
				(
					db
						.prepare(
							"SELECT COUNT(*) AS n FROM memories WHERE type='todo' AND status='active' AND space=? AND title LIKE '%候%'",
						)
						.get(space) || {}
				).n,
			) || 0;
	} catch {} // 导出失败＝空账（调用侧按零数据不输出·⑬ 由调用侧计数/出声）
	return out;
}

// 渲染「本窗任务账」块：≤400 字量级帽·每类 ≤limit 条·出处带记忆 id（可回溯·验收线②）
function renderTaskLedger(led, opts) {
	if (!led || !led.space) return "";
	const o = opts || {};
	const lim = Number(o.limit) > 0 ? Number(o.limit) : 3;
	if (led.n.done + led.n.open + led.n.waiting === 0) return "";
	const cut = (s, n) => {
		const t = String(s == null ? "" : s);
		// 2026-10-06 dc677316 案修：slice 按码元可截在代理对中间 → 孤立高位代理随注入入会话
		// → deepseek 官方端点拒收（400 空 body·连炸实证）——clipSurrogateTail 保对（同 09-22 甲-F3 精神·补注入面）
		return t.length > n ? clipSurrogateTail(t.slice(0, n)) + "…" : t;
	};
	const line = (arr, mark, n) =>
		arr && arr.length
			? mark +
				" " +
				n +
				" 条：" +
				arr
					.slice(0, lim)
					.map((r) =>
						"#" +
						r.id +
						" " +
						cut(r.title, 16) +
						(r.elems && mark === "⏳候外部"
							? "（" + cut(r.elems.flow, 14) + "）"
							: ""),
					)
					.join("｜")
			: ""; // 2400 v2：候项附流转短注（三要素短表注入形态·帽内）
	return (
		[
			// C1（design-approved「下step」·治名实不符）：口径实为 **caller space**（三张清单 SQL 全按 space 过滤·
			//   `sid` 参数仅透出未参与筛选——见 E 路 I6 与 F 路/子代理窗实测）⇒ 标题据实改「本 space」，免误导子代理窗。
			"\n## 本 space 任务账（账态导出·勿重建·近 " + led.hours + "h）",
			line(led.done, "✅已办结", led.n.done),
			line(led.open, "☐未闭环", led.n.open),
			line(led.waiting, "⏳候外部", led.n.waiting),
			"出处：#id（全量 memory action=search query=待办）",
		]
			.filter(Boolean)
			.join("\n") + "\n"
	);
}
// ── normSid（sid 归一唯一入口·模块级）：v → "session-<uuid>"（裸 uuid 补前缀·已带不动）──
//    车五即修（09-30 approved·O2/O3 同车·o2o3-drill U3 行为实证抓出）：原系 registerBoardTools
//    **局部**函数——⑩C 压缩点板态快照在 firehose 作用域调用 ⇒ ReferenceError ⇒ 板态段 09-27 落码以来
//    全失败（真库 17 条快照 0 条含[板态快照]·stats「板态取错」＝历次真压缩计数·机制在位≠在用实锤）⇒ 提升模块级单源。
const normSid = (v) => {
	const s = String(v == null ? "" : v).trim();
	return s ? (/^session-/.test(s) ? s : "session-" + s) : "";
};

// ══════════ [拆①] core 模块级段（2026-10-03 重构步①·L1-L3647 逐字搬入）══════════
// ── [拆①修正 v3] let 型共享态 getter/setter 活链接（38 组·含 queryCapKept 补漏）──
module.exports = {
	recordEpisodicAnchor: episodicAnchors.recordEpisodicAnchor,
	backfillEpisodicAnchors: episodicAnchors.backfillEpisodicAnchors,
	noteFreshEpisodicWrite: episodicAnchors.noteFresh,
	episodicAnchorOff: episodicAnchors.isOff,
	episodicAnchorStats: episodicAnchors.stats,
	get boardRowsPruned() { return boardRowsPruned; },
	set boardRowsPruned(v) { boardRowsPruned = v; },
	get closureCheckErrors() { return closureCheckErrors; },
	set closureCheckErrors(v) { closureCheckErrors = v; },
	get closureGateAnchorWeak() { return closureGateAnchorWeak; },
	set closureGateAnchorWeak(v) { closureGateAnchorWeak = v; },
	get closureGateBlocked() { return closureGateBlocked; },
	set closureGateBlocked(v) { closureGateBlocked = v; },
	get closureGateDoneEvidence() { return closureGateDoneEvidence; },
	set closureGateDoneEvidence(v) { closureGateDoneEvidence = v; },
	get closureGateFreshDoneSkip() { return closureGateFreshDoneSkip; },
	set closureGateFreshDoneSkip(v) { closureGateFreshDoneSkip = v; },
	get closureGateSeqClash() { return closureGateSeqClash; },
	set closureGateSeqClash(v) { closureGateSeqClash = v; },
	get dispatchStale() { return dispatchStale; },
	set dispatchStale(v) { dispatchStale = v; },
	get dispatchStale7d() { return dispatchStale7d; },
	set dispatchStale7d(v) { dispatchStale7d = v; },
	get echoWarnCount() { return echoWarnCount; },
	set echoWarnCount(v) { echoWarnCount = v; },
	get globalDispatchCap() { return globalDispatchCap; },
	set globalDispatchCap(v) { globalDispatchCap = v; },
	get guardRulesDriftCount() { return guardRulesDriftCount; },
	set guardRulesDriftCount(v) { guardRulesDriftCount = v; },
	get guardRulesDriftKeys() { return guardRulesDriftKeys; },
	set guardRulesDriftKeys(v) { guardRulesDriftKeys = v; },
	get jieba() { return jieba; },
	set jieba(v) { jieba = v; },
	get queryCapDyn() { return queryCapDyn; },
	set queryCapDyn(v) { queryCapDyn = v; },
	get queryCapKept() { return queryCapKept; },
	set queryCapKept(v) { queryCapKept = v; },
	get rerankErrors() { return rerankErrors; },
	set rerankErrors(v) { rerankErrors = v; },
	get rerankProperPicked() { return rerankProperPicked; },
	set rerankProperPicked(v) { rerankProperPicked = v; },
	get softSameTopicWarn() { return softSameTopicWarn; },
	set softSameTopicWarn(v) { softSameTopicWarn = v; },
	get sourceOverrideCount() { return sourceOverrideCount; },
	set sourceOverrideCount(v) { sourceOverrideCount = v; },
	get spCredCache() { return spCredCache; },
	set spCredCache(v) { spCredCache = v; },
	get spaceGateLastWarnAt() { return spaceGateLastWarnAt; },
	set spaceGateLastWarnAt(v) { spaceGateLastWarnAt = v; },
	get surgerySkipVecEmbed() { return surgerySkipVecEmbed; },
	set surgerySkipVecEmbed(v) { surgerySkipVecEmbed = v; },
	get surgerySkipWrite() { return surgerySkipWrite; },
	set surgerySkipWrite(v) { surgerySkipWrite = v; },
	get toolUsageErrors() { return toolUsageErrors; },
	set toolUsageErrors(v) { toolUsageErrors = v; },
	get usageQualityErrors() { return usageQualityErrors; },
	set usageQualityErrors(v) { usageQualityErrors = v; },
	get vecBackfillErrors() { return vecBackfillErrors; },
	set vecBackfillErrors(v) { vecBackfillErrors = v; },
	get vecBackfillFilled() { return vecBackfillFilled; },
	set vecBackfillFilled(v) { vecBackfillFilled = v; },
	get vecBackfillLastErr() { return vecBackfillLastErr; },
	set vecBackfillLastErr(v) { vecBackfillLastErr = v; },
	get vecBackfillLastErrAt() { return vecBackfillLastErrAt; },
	set vecBackfillLastErrAt(v) { vecBackfillLastErrAt = v; },
	get vecBackfillNoCred() { return vecBackfillNoCred; },
	set vecBackfillNoCred(v) { vecBackfillNoCred = v; },
	get vecBackfillRuns() { return vecBackfillRuns; },
	set vecBackfillRuns(v) { vecBackfillRuns = v; },
	get vecCapNear() { return vecCapNear; },
	set vecCapNear(v) { vecCapNear = v; },
	get vecChannelErrors() { return vecChannelErrors; },
	set vecChannelErrors(v) { vecChannelErrors = v; },
	get vecCredCache() { return vecCredCache; },
	set vecCredCache(v) { vecCredCache = v; },
	get vecScanSkipKeyErrors() { return vecScanSkipKeyErrors; },
	set vecScanSkipKeyErrors(v) { vecScanSkipKeyErrors = v; },
	get vecSkippedDueToBudget() { return vecSkippedDueToBudget; },
	set vecSkippedDueToBudget(v) { vecSkippedDueToBudget = v; },
	get writeRejectedCount() { return writeRejectedCount; },
	set writeRejectedCount(v) { writeRejectedCount = v; },
	get writeTimeEnhanceErrors() { return writeTimeEnhanceErrors; },
	set writeTimeEnhanceErrors(v) { writeTimeEnhanceErrors = v; },
	ABSTAIN_TOP1_MIN,
	BUILTIN_SPACES,
	CAP_NEAR,
	COOCCUR_CORE_MIN,
	CRIT,
	lifeProtect, // wave 车二-8a：生命周期保护判据（patrol 续期、待办留存用）
	memGetOn, // wave 复活口·阶段 1：get 开关（LEGION_MEM_GET_OFF 只认 1）
	memGetChunk, // wave 复活口·阶段 1：get 按字节帽与行帽切段（纯函数）
	MEM_GET_BYTES,
	MEM_GET_LINES,
	reviveOn, // wave 复活口·阶段 2：复活开关（LEGION_REVIVE_OFF 只认 1）
	reviveNote, // wave 复活口·阶段 2：记复活候选（只认 aged；只读连接上跳过）
	reviveOnEvent, // wave 复活口·阶段 2：session/event 三类触发分派
	reviveStat, // wave 复活口·阶段 2：候选记录计数（进程内）
	PRIV_SPACES, // wave 复活口·阶段 5：双特权两窗（memory organ、brain）名单，只此一份
	privSpace, // wave 复活口·阶段 5：是否双特权窗（tools.cjs 空间闸与阶段 5 管理窗判法共用）
	DB_PATH,
	DICT,
	DRILL_SANDBOX,
	DatabaseSync,
	EXTRACT_API,
	EXTRACT_INTERVAL_MS,
	EXTRACT_MODEL,
	EXTRACT_PF_STREAK,
	EXTRACT_TIMEOUT_MS,
	GUARD,
	INSTRUCT_QUERY,
	LLM_KEY_REF,
	LOW_RELEVANCE_THRESHOLD,
	MODULE_SCAN_DIR,
	ORD_TODO_RE,
	ORD_WORDS,
	ORD_WORDS_EN,
	ORGAN_DIR_MAP,
	PATROL_CHECK_MS,
	PATROL_END_HOUR,
	PATROL_START_HOUR,
	PATROL_TZ,
	PN_RESCUE_SCORE,
	PREVERDICT_API,
	PREVERDICT_KEY_REF,
	PREVERDICT_MODEL,
	PROMPT_SAFE_STAT,
	REJECT_PATTERNS,
	RERANK_CAND,
	RERANK_TOPN,
	ROUTE_INHIB_RE,
	ROUTE_MAINT_RE,
	ROUTE_XHS_RE,
	SELROUTE_TEMPORAL_RE,
	SELROUTE_BACKWARD_RE,
	SENSITIVE_RE,
	SNAPSHOT_DIR,
	SNAPSHOT_KEEP_DAYS,
	SPACES,
	SPACE_GATE_CONFIGURED,
	SPOKEN_BOOST,
	SRC_OK_RE,
	SURGERY_FLAG,
	TOKENIZER_VERSION,
	VEC_CHUNK_MODEL,
	VEC_MODEL,
	addUsage,
	apiUsage,
	autoRegistered,
	bigrams,
	blobToF32,
	boardGcOpenDays,
	boardHintOn,
	boardStaleDays,
	capNearWatch,
	chunkMemory,
	classifyQuery,
	clipSurrogateHead,
	clipSurrogateTail,
	closureCheck,
	closureGateFreshDoneIds,
	cosine,
	crypto,
	dupWriteStat,
	embedOnce,
	openDb,
	ensureFts,
	envIntCap,
	execFileSync,
	exportTaskLedger,
	f32ToBlob,
	fs2,
	gateKeywords,
	gateTitleTokens,
	gateTokenInter,
	hasUnsafePromptRef,
	hitMapErrorsStat,
	injectMeter,
	isoAhead,
	jaccard,
	knownFixesHint,
	llmBudget,
	llmSpokenPrefix,
	matchWaitWho,
	obsGroupOf,
	obsLedger,
	obsLedgerFromRender,
	OBS_GROUPS,
	memoryRender,
	normSid,
	noteApiFail,
	nowIso,
	nowIso24hAgo,
	organFromPath,
	os,
	path,
	porter2Stem,
	projcacheRows,
	promptSafe,
	promptSafeContext,
	CUR_SQL, // 车二-9 块一 1-a：注入面/召回面共用的现行谓词（inject、read 两侧解构取用）
	isSystemRow, // 车二-9 块一 1-a：J12 三类系统条判法（同上）
	qTokens,
	queryClassDist,
	queryMatch,
	recordInjectBytes,
	recordInjectTurn, // 注入瘦身车 W4（10-11）：同轮账
	injectTurnPct,
	INJECT_TURN_CAP_BYTES,
	renderTaskLedger,
	rerankDocs,
	securityCheck,
	sessionOrgan,
	sessionsDirMap,
	sha1,
	softBodyWarn,
	spaceGateCounts,
	spaceWeight,
	spokenOverlap,
	stripLone,
	stripUrls,
	tokenize,
	usageQualityDigest,
	usageWrap,
	vecRecallCore,
	warnBoth,
	writeTimeEmbed,
	writeTimeReflectLink,
};
