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
	ABSTAIN_TOP1_MIN,
	BUILTIN_SPACES,
	CAP_NEAR,
	COOCCUR_CORE_MIN,
	CRIT,
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
	qTokens,
	queryClassDist,
	queryMatch,
	recordInjectBytes,
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
} = core;

// deps 全 const/fn 直传（无 let 重赋值型——零 getter 零回调·#31831 边界核对毕）；调用点 setInterval/LEGION_DRILL_PATROL 在 index.cjs 装配区。
// [拆⑦·2026-10-04] 闸归一接入：nightly patrol 4 写入点过安检闸（pipeline.securityGate——判据 core 原语零新·原零安检的真增量·蓝图 §6.2）。
const _pipelineGate = require("./pipeline.cjs").securityGate;

// Keep the patrol synchronous: the child awaits node:sqlite's asynchronous backup
// before returning. No shell or sqlite3 CLI is involved, and the source is read-only.
const SNAPSHOT_CHILD = `
const { DatabaseSync, backup } = require('node:sqlite');
if (typeof backup !== 'function') {
  console.error('SQLite online backup is unavailable in this Node.js runtime (requires Node 22.16+ or 23.8+)');
  process.exit(1);
}
(async () => {
  const source = new DatabaseSync(process.argv[1], { readOnly: true });
  try { await backup(source, process.argv[2]); }
  finally { source.close(); }
  // Publish a standalone rollback-journal database, not a WAL-mode file.
  const result = new DatabaseSync(process.argv[2]);
  try {
    if (result.prepare('PRAGMA journal_mode=DELETE').get().journal_mode !== 'delete')
      throw new Error('Cannot finalize snapshot journal mode');
    const check = result.prepare('PRAGMA quick_check').all();
    if (check.length !== 1 || check[0].quick_check !== 'ok')
      throw new Error('Snapshot quick_check failed: ' + JSON.stringify(check).slice(0, 300));
  } finally { result.close(); }
})().catch(error => { console.error(error); process.exitCode = 1; });
`;
function createSnapshot(sourcePath, snapshotDir, dayStamp, keepDays) {
	if (!/^\d{8}$/.test(dayStamp)) throw new Error("Invalid snapshot date");
	fs2.mkdirSync(snapshotDir, { recursive: true });
	const dest = path.join(snapshotDir, `memory-${dayStamp}.sqlite3`);
	// A private, same-filesystem destination makes rename atomic and avoids
	// concurrent patrols sharing a partially written file.
	const temporaryDir = fs2.mkdtempSync(path.join(snapshotDir, ".snapshot-"));
	try {
		const temporary = path.join(temporaryDir, "memory.sqlite3");
		execFileSync(process.execPath, ["-e", SNAPSHOT_CHILD, sourcePath, temporary], {
			timeout: 60000,
			stdio: "pipe",
		});
		// Never replace a WAL-mode snapshot while its sidecars may contain data.
		if (fs2.existsSync(dest + "-wal") || fs2.existsSync(dest + "-shm"))
			throw new Error("Existing snapshot has SQLite WAL sidecars; refusing replacement");
		fs2.renameSync(temporary, dest);
	} finally {
		try { fs2.rmSync(temporaryDir, { recursive: true, force: true }); } catch {}
	}
	// Rotate only after the new snapshot has been published successfully.
	const cutoff = Date.now() - keepDays * 24 * 3600 * 1000;
	for (const name of fs2.readdirSync(snapshotDir)) {
		const m = name.match(/^memory-(\d{8})\.sqlite3$/);
		if (!m || name === path.basename(dest)) continue;
		const t = Date.parse(`${m[1].slice(0, 4)}-${m[1].slice(4, 6)}-${m[1].slice(6, 8)}T00:00:00+08:00`);
		if (!isNaN(t) && t < cutoff) {
			const old = path.join(snapshotDir, name);
			if (fs2.existsSync(old + "-wal") || fs2.existsSync(old + "-shm")) continue;
			try { fs2.unlinkSync(old); } catch {}
		}
	}
	return dest;
}
// wave 车二-8b：续期记账表 life_renew（首次写 old_valid_to、再续只加 n、改 last_at）。
//   盲审处置（should，10-18）：加 hits 列——上次续期后nightly patrol命中结算到的真命中数（read_episodic 回流同计），每次续期清零；
//   同一条续满 LIFE_RENEW_CAP 次后，除 essential 外再续须 hits>0（理由记 hit），否则不续、计 lifeRenewCapped。
//   last_hit_at 也被提炼同题合并刷新，分不出真命中，故帽后不看 last_hit_at 只看 hits。旧表（无 hits 列）就地补列
const LIFE_RENEW_CAP = 2;
function ensureLifeRenew(db) {
	db.exec(
		"CREATE TABLE IF NOT EXISTS life_renew(id INTEGER PRIMARY KEY, n INTEGER NOT NULL DEFAULT 1, first_at TEXT, last_at TEXT, old_valid_to TEXT, reason TEXT, hits INTEGER NOT NULL DEFAULT 0)",
	);
	const cols = db.prepare("PRAGMA table_info(life_renew)").all().map((c) => c.name);
	if (!cols.includes("hits")) db.exec("ALTER TABLE life_renew ADD COLUMN hits INTEGER NOT NULL DEFAULT 0");
}
// wave 复活口·阶段 3（10-18）：复活记账表 life_revive——每复活一次记一行原值（status、closed_at、valid_to，及 life_renew 原行 n、reason、last_at、hits；
//   lr_had=0 表示复活前 life_renew 没有这一条）。只增不改，数据面复原依据（每条按它最早一行回填＝首次复活前的原值，见落码记录「回滚三件」）。每夜帽 REVIVE_NIGHT_CAP 条
const REVIVE_NIGHT_CAP = 200;
function ensureLifeRevive(db) {
	db.exec(
		"CREATE TABLE IF NOT EXISTS life_revive(rid INTEGER PRIMARY KEY AUTOINCREMENT, id INTEGER NOT NULL, at TEXT, why TEXT, old_status TEXT, old_closed_at TEXT, old_valid_to TEXT, new_valid_to TEXT, lr_had INTEGER NOT NULL DEFAULT 0, lr_n INTEGER, lr_reason TEXT, lr_last_at TEXT, lr_hits INTEGER)",
	);
}
function setupPatrol({ ctx, stats, db, credentials, pendingBySid, guard, preguard, preclassifyConflict, runExtract, pruneOrganMeta }) {
	// [拆⑦] 闸 skip 计数＋warn（⑬ 可观测——单条 skip 不炸巡）
	const _patrolGateSkip = (g, label) => {
		stats.patrolGateSkip = (stats.patrolGateSkip || 0) + 1;
		ctx.logger?.warn?.(`[living-memory] patrol write-gate skip (#${stats.patrolGateSkip}·${g.kind}): ${label}`);
	};
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
			// 阶段〇第 16 项(design note)：判窗与 dayKey 显式按北京时间——
			//   nowIso() 恒为 +08:00 北京时刻（core.cjs PATROL_TZ 声明同钟）。原取本机 getHours()/getDate()：本机时区 10-01 改为
			//   America/New_York 后，nightly patrol漂到北京 14–18 时跑、dayKey 也按美东日切。
			const _bj = String(nowIso()); // YYYY-MM-DDTHH:MM+08:00
			const hour = Number(_bj.slice(11, 13));
			if (!force && (hour < PATROL_START_HOUR || hour >= PATROL_END_HOUR))
				return;
			const dayKey = _bj.slice(0, 10);
			// 批⑲ P2（J8-2）：跨进程互斥——原 stats.lastPatrolDay 纯进程内（多进程 timer 各挂 ⇒ 双跑）；改读 organ_meta（跨进程可见·读-判-写序）
			let _plDayDb = "";
			try {
				_plDayDb = String(
					db.prepare("SELECT v FROM organ_meta WHERE k='last_patrol_day'").get()?.v ||
						"",
				);
			} catch {}
			if (!force && _plDayDb === dayKey) {
				stats.patrolCrossSkip = (stats.patrolCrossSkip || 0) + 1;
				return; // 每晚最多一次（跨进程口径）
			}
			stats.lastPatrolDay = dayKey; // 进程内展示面保留
			// 批㉑ W2（M 路审计·P2 互斥时序失效）：占坑前移——原写回在nightly patrol尾部＋PATROL_CHECK_MS=30min 检查窗 ⇒
			//   nightly patrol主体 >30min 时他进程 tick 读到旧值恒双跑（「互斥」名不符实·损失=双倍耗时/写放大/锁竞争——恰是 J8-2 要治的）。
			//   改判定「该我跑」后**立即占坑**（他进程读到即跳·patrolCrossSkip）；尾部写回保留（幂等同值·belt-and-braces）。
			try {
				db.prepare(
					"INSERT OR REPLACE INTO organ_meta (k, v) VALUES ('last_patrol_day', ?)",
				).run(String(dayKey));
			} catch {}
			// ── 车二（09-14 approved）：**FTS 全量重建**（西文双侧形态归一生效的必经步骤）──
			//   背景：tokenize 改动后索引与运行时不兼容（TOKENIZER_VERSION bump）⇒ 须一次性全量重建；
			//   而重建只能走**本进程**（legion_bigram 是本进程注册的 SQL 函数 ⇒ CLI 无法重建）。
			//   **窗口＝nightly patrol**（maintainer·§七 待裁三问之第三问）：静默期低并发·一夜一次性完成。
			//   触发走「三步纪律」env：置 `LEGION_FTS_REBUILD=1` → 换班重启 → **毕后手动移除**（无自停）。
			//   复用启动回填同款 SQL（含 spoken_prefix·与触发器/回填三处同源）；重建后回写 tokenizer_version
			//   ＋organ_meta.fts_rebuild_last（观测位）。
			if (process.env.LEGION_FTS_REBUILD === "1") {
				try {
					db.exec("BEGIN");
					db.exec("DELETE FROM memories_fts");
					const nRb = db
						.prepare(
							"INSERT INTO memories_fts(rowid, tokens) SELECT id, legion_bigram(title || ' ' || content || ' ' || COALESCE(spoken_prefix, '')) FROM memories WHERE status != 'deleted'",
						)
						.run();
					const nRows = Number(nRb.changes);
					db.prepare(
						"INSERT OR REPLACE INTO organ_meta (k, v) VALUES ('tokenizer_version', ?)",
					).run(TOKENIZER_VERSION);
					db.prepare(
						"INSERT OR REPLACE INTO organ_meta (k, v) VALUES ('fts_rebuild_last', ?)",
					).run(
						JSON.stringify({
							at: nowIso(),
							rows: nRows,
							version: TOKENIZER_VERSION,
						}),
					);
					db.exec("COMMIT");
					stats.ftsRebuildRows = nRows; // 观测位（stats 面）
					ctx.logger?.warn?.(
						`[living-memory] fts rebuild: ${nRows} rows · version=${TOKENIZER_VERSION}（LEGION_FTS_REBUILD=1·**毕后请移除该 env**）`,
					);
				} catch (eRb) {
					try {
						db.exec("ROLLBACK");
					} catch {}
					stats.ftsRebuildErrors = (stats.ftsRebuildErrors || 0) + 1;
					ctx.logger?.warn?.(
						`[living-memory] fts rebuild failed: ${String((eRb && eRb.message) || eRb).slice(0, 90)}`,
					);
				}
			}
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
						"UPDATE conflicts SET merged_count = COALESCE(merged_count, 1) + 1, evidence = ? WHERE conflict_id = ?",
					);
					// ── 序2 A4-1（09-15 brain 00:59 件·maintainer闸判据体检）：并案幂等——evidence 尾标记 ‖m: 记已并入 id 集 ──
					//    治 conflict_id=908 案（真实同文 21 条·merged_count 虚增至 58≈2.8 倍：每轮nightly patrol对同批重复 +1）；
					//    原证据文本保留（尾标记前段不动）·同 id 不重复计数。
					const MERGE_MARK = " ‖m:";
					const mergeOnce = (cid, newId) => {
						const row = db
							.prepare("SELECT evidence FROM conflicts WHERE conflict_id = ?")
							.get(cid);
						const ev = String((row && row.evidence) || "");
						const mi = ev.indexOf(MERGE_MARK);
						const set =
							mi >= 0
								? ev.slice(mi + MERGE_MARK.length).split(/\s+/).filter(Boolean)
								: [];
						if (set.includes(String(newId))) return false; // 已并入·不重复 +1
						set.push(String(newId));
						uMergeCount.run(
							ev.split(MERGE_MARK)[0] + MERGE_MARK + set.join(" "),
							cid,
						);
						return true;
					};
					const fileC = (newId, oldId, basisTxt, evTxt, space, title) => {
						const pk = prefixKeyOf(title);
						if (pk) {
							const prior = qMergePend.get(space, pk + "%");
							if (prior) {
								mergeOnce(prior.conflict_id, newId); // A4-1：幂等并案（同 id 不重复 +1）
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
					// 车二补丁丙（再审 🔴-1·一处三病齐治）：五新键归零上提 conflicts 段首（exact 判定之前）——
					//   原补丁乙归零落 归零区（L9095）在 family 赋值点（L9036）之后＝「先累计后归零」当巡即抹（fam 观测断链·R15b 必落空）；
					//   且 soulExempt/sentry 两键原无归零（五键口径混装）——统一五键每巡快照（D5「归零必须在门之外」·exact/family//sentry/light sentry全段之前）。
					stats.familySkippedDueToCap = 0;
					stats.hiSimSkippedDueToCap = 0;
					stats.a19SkippedDueToCap = 0;
					stats.soulExemptSuppressed = 0;
					stats.sentryAlarmSuppressed = 0;
					stats.semanticEdgeSkippedDueToCap = 0; // A级四件④（全链再审乙F2·批⑰遗留）：帽跳族唯一无归零者——注释自称「本夜·0=全池在窗」实为跨巡累计·快照语义混装根治
					stats.a10PoolSize = 0; // B级②（甲J1）：族五键归零同提段首（原在 L9100 区＝归零点分裂）
					stats.a10Sampled = 0;
					stats.a10SkippedDueToCap = 0;
					stats.a10RotSlot = 0;
					stats.semanticConflictsFound = 0;
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
					let famCursorNext = null; // B级①：family 游标写点后移载体（段尾与成败绑定写回）
					for (const list of bySpace3a.values()) {
						if (list.length < 2) continue;
						const grams = list.map((r) => ({
							id: r.id,
							type: r.type,
							title: r.title,
							g: bigrams(r.title),
						}));
						// 车二件1a（design-approved「车二」·治 P4 ⚠-2「超帽即整段静默跳过零计数」同型第三例）：
						//   复用 骨架（降采样＋SkippedDueToCap 计数）——帽 env 化（LEGION_FAMILY_CAP·缺省 3000 不变）；
						//   ⚠ 语义边界（同款诚实记账）：超帽时 i 侧取「最新 FAM_CAP 条∪按步轮转片（跨巡覆盖旧条）」，
						//   帽外条仍可作 j 侧被撞（非完全不检）但不作 i 侧——原「整桶 continue」＝两机制全盲（memory-organ 7401 桶实证）。
						const FAM_CAP =
							envIntCap(process.env.LEGION_FAMILY_CAP, 3000);
						// 帽接近度哨（10-01 补·库量驱动型）：桶内条数 ≥80% 帽且未超 ⇒ 预警
						capNearWatch(
							db,
							"family",
							list.length,
							FAM_CAP,
							"族桶将满·LEGION_FAMILY_CAP 复评或库减脂",
						);
						let iGate = null;
						if (list.length > FAM_CAP) {
							const step3 = Math.ceil(list.length / FAM_CAP);
							// B级①（全链再审甲J2·轮转游标随迁）：原静态 gi%step ⇒「跨巡覆盖旧条」零保证（库静止时
							//   同一子集永不作 i 侧）——迁 a10_rot_cursor 范式：游标相位逐巡滑动（多超帽桶共享相位·简化记）；
							//   D6 诚实记账承继：id 序随库变 ⇒ 相位法为近似承诺非「rotN 巡必一轮」。
							let famSlot = 0;
							try {
								const rcF =
									db
										.prepare(
											"SELECT v FROM organ_meta WHERE k='family_rot_cursor'",
										)
										.get();
								famSlot = (((Number(rcF && rcF.v) || 0) + 1) % step3 + step3) % step3;
							} catch {
								stats.famRotCursorErrors =
									(stats.famRotCursorErrors || 0) + 1; // ⑬：游标读失败不拖扫描（退化槽 0·不推进）
							}
							iGate = new Set();
							for (let gi = 0; gi < grams.length; gi++)
								if (gi < FAM_CAP || (gi + famSlot) % step3 === 0)
									iGate.add(grams[gi].id);
							stats.familySkippedDueToCap =
								(stats.familySkippedDueToCap || 0) +
								(list.length - iGate.size);
							famCursorNext = famSlot; // 写点后移至段尾（D6：与成败绑定·扫描抛出不消耗相位）
						}
						for (let i = 0; i < grams.length; i++) {
							if (iGate && !iGate.has(grams[i].id)) continue; // i 侧采样门（帽外条可作 j 侧）
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
					if (famCursorNext !== null) {
						try {
							db.prepare(
								"INSERT OR REPLACE INTO organ_meta (k, v) VALUES ('family_rot_cursor', ?)",
							).run(String(famCursorNext)); // B级①段尾写（D6：扫描成功才推进相位）
						} catch {
							stats.famRotCursorErrors =
								(stats.famRotCursorErrors || 0) + 1;
						}
					}
					// ── 语义去重第三判据(design note)：向量余弦≥0.92
					//    且同 type+同 space 才立 pending——「词面不重叠语义同物」（jaccard 漏网面·如 conda-env-create
					//    vs conda-create-environment）。仍走人工裁决（8-23 弃 semantic 因误报·type+space+0.92 三重收窄后
					//    复投）。挂 RRF 已有向量·零新嵌入成本（缺向量对跳过）；护栏：active 全量 O(n²) 桶帽 3000。──
					// S1 修（step）原上提点=conflicts 内层 try 域——stepcaught in drill L1651 外层引用 ReferenceError；
					// 声明已真上提至外层 conflictsPending 旁（本行删·/①精确/审计清单三面同见）。
					try {
						// D5＋F3/F4（二轮审计补）：**归零必须在门之外** —— 三条「未跑到」路含 `LEGION_SEMANTIC_DEDUP_OFF`
						//   （原归零在门内 ⇒ 该路不归零）；并补 **主输出** `semanticConflictsFound`（原全仓从无 `=0`
						//   ⇒ 池 ≤1／段内抛错／关闸三路下留上一巡值，使「`a10Semantic:0 ∧ a10SkipErrors:0` 双零=not-run」
						//   这一 P0-4 判读签名本身失真）。归零后「未跑」显式呈现 0（配 `a10SkipErrors` 交叉判读）。
						// 车二补丁丙：五新键归零已上提 conflicts 段首（原在此处＝family 赋值后归零·当巡即抹——再审 🔴-1）
						// B级②（全链再审甲J1·归零点分裂根治）：族五键归零亦上提段首（与六新键合流 11 键群）——
						//   原两处归零点分裂（段首 vs 此处）中间夹 exact/family 段·该段抛错（L9850 catch 只 warn 无计数）时
						//   同一 patrol_last 内「fam键=0/缺」与「键=上巡值」口径分裂且无人可辨；D5 注释的「门之外」语义
						//   由段首位置承继（LEGION_SEMANTIC_DEDUP_OFF 开关在归零之后·关闸仍归零——判读签名保持）。
						if (!process.env.LEGION_SEMANTIC_DEDUP_OFF) {
							// P0-4（09-12 maintainer）三参数·一处定义 ＋ **env 可注入**（drill 用小帽验证采样态；
							//   解析严格化·吸取 C 车 D5 教训：非法/非正整数一律回落缺省，不静默塌 0）
							const a10Num = (raw, dflt) => {
								const n = Number(raw);
								return Number.isInteger(n) && n > 0 ? n : dflt;
							};
							const A10_FULL_CAP = a10Num(process.env.LEGION_A10_CAP, 5000);
							const A10_NEW_BUDGET = a10Num(process.env.LEGION_A10_NEW_BUDGET, 800);
							const A10_ROT_BUDGET = a10Num(process.env.LEGION_A10_ROT_BUDGET, 400);
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
                WHERE v.model_version = ? AND m.status = 'active' AND m.source NOT LIKE 'mirror:%'${a10Excl.length ? ` AND m.id NOT IN (${a10Excl.map(() => "?").join(",")})` : ""}
                AND (m.valid_to IS NULL OR m.valid_to > ?) ORDER BY m.id DESC`) // 十七审候裁⑦：对判池硬排 // P0-4（09-12）：id DESC ⇒ 超帽采样「新条优先」的序号语义（无序对判据与顺序无关）；批⑰ D14⑥：滤 mirror 影子条（474 条有向量恒落空占池·J4 补）
								.all(VEC_MODEL, ...a10Excl, nowIso());
							if (vecRows.length > 1) {
								// ── P0-4 治盲（design-approved「8 项全批」·审计 S2 实勘）──
								//    原 `if (len <= 5000)` 超帽即**整段跳过·无 else 出口零告警**（池 09-12 起 **5451 > 5000**
								//    ⇒ patrol_last `a10Semantic:0 ∧ a10SkipErrors:0` 双零＝典型 not-run·语义去重面为盲）；
								//    且与 09-06 前史「3000 帽在 3237 正库恒跳过」**同型复发**（下一行注释自陈）。
								//    修法＝**超帽降采样 ＋ 分片轮转**（不再整段跳过）：
								//      ①池 ≤ A10_FULL_CAP ⇒ 全池两两（**原语义不变**·样本＝null 走同一循环）
								//      ②池 > 帽 ⇒ 样本 ＝「最新 A10_NEW_BUDGET 条（新条是重复高发区·已按 id DESC 排序）」
								//        ∪「按 id 取模命中的轮转片（**除数是 A10_ROT_BUDGET·片实际大小 ≈ n/rotN 非硬帽**·
								//        游标 `a10_rot_cursor` 跨巡推进）」
								//      ③成本 ＝ |样本| × n 而非 n²（5451 池 ≈ 582 万对 vs 全对 1485 万对 ⇒ **比原全池路径更省**）
								//      ④**补 `a10SkippedDueToCap` 计数**（被帽跳过·本巡未作 i 侧参与的条数）＋ 池/样本/游标观测位
								//    ⚠ 语义边界（诚实记账）：帽外条**仍可作 j 侧**被撞（故非「完全不检」），但**不作 i 侧**
								//      ⇒ 每巡只保证「样本 × 全池」覆盖；全池两两覆盖靠轮转片跨巡累积——
								//      ⚠ **D6 审计订正：这是近似承诺、非「rotN 巡必轮一轮」**（`rotN=ceil(n/400)` 随池增长
								//      漂移 ⇒ 模数一变残类映射重排，可重片/漏片；`id % rotN` 均匀性未验证）。
								//      真承诺须作「片位点阵＋已完成位」；D7 另注：**对集合**与旧码逐对同，但**帽内先立案的
								//      20 对**因 `ORDER BY m.id DESC` 由近似 ASC 改为新条优先（有意为之）。
								const n10 = vecRows.length;
								let inSample = null;
								let a10Sampled = n10,
									a10Skipped = 0,
									a10RotSlot = 0,
									a10RotN = 1;
								if (n10 > A10_FULL_CAP) {
									a10RotN = Math.max(1, Math.ceil(n10 / A10_ROT_BUDGET));
									try {
										const rc = db
											.prepare(
												"SELECT v FROM organ_meta WHERE k='a10_rot_cursor'",
											)
											.get();
										a10RotSlot = ((Number(rc && rc.v) || 0) + 1) % a10RotN;
										// D6（独立审计 09-12）：**游标写点后移至扫描成功之后** —— 原「先写后扫」在扫描
										//   抛出时游标已消耗一格、该片零覆盖且不可回溯（与成败解耦）。此处只读只算，
										//   写点在段尾 `stats.semanticConflictsFound` 旁（与成败绑定）。
									} catch {
										a10RotSlot = 0; // ⑬ 出口：游标读失败不拖扫描（本巡退化槽 0·不推进）
										stats.a10RotCursorErrors = (stats.a10RotCursorErrors || 0) + 1; // F6：读失败亦计入（原注释称⑬却无自增点）
									}
									const inS = new Set();
									for (let i = 0; i < n10 && i < A10_NEW_BUDGET; i++)
										inS.add(i); // 新条（vecRows 已 id DESC）
									for (let i = 0; i < n10; i++)
										if (vecRows[i].id % a10RotN === a10RotSlot)
											inS.add(i); // 轮转片
									inSample = inS;
									a10Sampled = inS.size;
									a10Skipped = n10 - inS.size;
								}
								stats.a10PoolSize = n10;
								stats.a10Sampled = a10Sampled;
								stats.a10SkippedDueToCap = a10Skipped;
								// F6（二轮审计）：`a10RotSlot` **不在此赋值** —— 原在样本选择后即赋值 ⇒ 扫描抛错/写库失败时
								//   三面会显示「已推进至 S+1」而 DB 仍是 S（名实不符）。改在**成功写库之后**赋值（见段尾）。
								const metaOf = new Map(rows.map((r) => [r.id, r]));
								let semanticFound = 0;
								// P1 修（design-approved·次批治本件1）：解码提外层一次——
								// 原内层每对重解码=790 万次 TypedArray 分配/轮→GC 停顿风暴。纯性能·零逻辑变更。
								const vecs = vecRows.map((r) => blobToF32(r.embedding));
								outer: for (let i = 0; i < n10; i++) {
									if (inSample && !inSample.has(i)) continue; // 帽外条不作 i 侧（仍可作 j 侧被撞）
									const a = vecs[i];
									for (let j = 0; j < n10; j++) {
										if (i === j) continue;
										if (j < i && (!inSample || inSample.has(j))) continue; // 无序对去重（全池＝上半三角·采样＝样本内对只计一次）
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
								// ── 序2 A4-2（09-15 brain 00:59 件·maintainer闸判据体检）：pending 出海口——超 N 天 pending 投pending ruling清单 ──
								//    治「pending 永不裁决即永不重立 ⇒ 僵尸对静默积压」（exact 12 对最老 4 天·立案≠治理伪绿）；
								//    staleSink 同族：env LEGION_CONFLICT_PENDING_DAYS 缺省 7（counter）·超期只pending ruling不自动处置(design note)·日幂等。
								try {
									const pendDays = Math.max(
										1,
										envIntCap(process.env.LEGION_CONFLICT_PENDING_DAYS, 7),
									);
									const sincePend = new Date(
										Date.now() + 8 * 3600e3 - pendDays * 86400e3,
									)
										.toISOString()
										.replace("T", " ")
										.slice(0, 16);
									const stalePends = db
										.prepare(
											"SELECT co.conflict_id, co.new_id, co.old_id, co.basis, co.created_at FROM conflicts co WHERE co.status = 'pending' AND co.created_at < ? ORDER BY co.conflict_id LIMIT 30",
										)
										.all(sincePend);
									stats.conflictPendingStale = stalePends.length;
									if (stalePends.length > 0) {
										const dayKeyCP = nowIso().slice(0, 10);
										const linesCP = stalePends
											.map(
												(p) =>
													"- #" +
													p.conflict_id +
													"：new #" +
													p.new_id +
													" / old #" +
													p.old_id +
													"（" +
													p.basis +
													"·立案 " +
													String(p.created_at || "").slice(0, 16) +
													"）",
											)
											.join("\n");
										try {
											const fsCP = require("node:fs");
											const pathCP = require("node:path");
											// 批⑲ P7（J4-9·同族）：沙箱闸——原硬编码生产「brain inbox」腿未接 DRILL_SANDBOX（对照同族已闸腿）
											const dirCP = DRILL_SANDBOX
												? pathCP.join(os.tmpdir(), "living-drill-cp")
												: pathCP.join(
														os.homedir(), ".dsh", "dsh-living-memory", "modules",
														"brain",
														"inbox",
													);
											fsCP.mkdirSync(dirCP, { recursive: true });
											fsCP.writeFileSync(
												pathCP.join(
													dirCP,
													dayKeyCP +
														"-conflictspending ruling清单-pending超期" +
														pendDays +
														"d（" +
														stalePends.length +
														"对·A4-2出海口）.md",
												),
												"# conflicts pending ruling清单 · pending 超期出海口（A4-2·序2 09-15）\n\n" +
													"> 超期 " +
													pendDays +
													" 天未裁决的 pending 冲突——裁决权在maintainer·本清单不自动处置（counter·日幂等）\n\n" +
													linesCP +
													"\n",
											);
										} catch {}
									}
								} catch {}
								// D6（独立审计 09-12）：**扫描成功后才推进游标**（与成败绑定 ⇒ 承诺不虚耗一格）
								if (inSample) {
									try {
										db.prepare(
											"INSERT OR REPLACE INTO organ_meta (k, v) VALUES ('a10_rot_cursor', ?)",
										).run(String(a10RotSlot));
										stats.a10RotSlot = a10RotSlot; // F6：**写库成功后**才透出（观测值＝已提交槽·非「尝试槽」）
									} catch {
										stats.a10RotCursorErrors = (stats.a10RotCursorErrors || 0) + 1; // ⑬ 出口
									}
								}
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
								"DELETE FROM memories_vec WHERE id IN (SELECT v.id FROM memories_vec v LEFT JOIN memories m ON m.id = v.id WHERE m.id IS NULL OR m.status != 'active' OR (m.valid_to IS NOT NULL AND m.valid_to <= date('now')))", // 库减脂option（10-02）：清理判据扩 valid_to 到点（残留根因·实测 17 块）
							)
							.run();
						const dc = db
							.prepare(
								"DELETE FROM memories_vec_chunks WHERE id IN (SELECT c.id FROM memories_vec_chunks c LEFT JOIN memories m ON m.id = c.id WHERE m.id IS NULL OR m.status != 'active' OR (m.valid_to IS NOT NULL AND m.valid_to <= date('now')))", // 同上（两表同判据）
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
								// D13（09-14 翻案 #10273·approved撤退役）：COALESCE 保留人工裁值——执行器在役；
								//   判其死活勿以 decided_by 缺席/'patrol-executor' 之外的值为据（伪零判据·#10200 立案教训）
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
								// 10-24 加活①（memory organ 10-24 22:14《#45875 停因三查》第四节 1）：熔断判预裁自己的 preguard（index.cjs 件三车 09-09 为「预裁 404 不连坐提炼」设）；
								//    原判提炼链的 guard＝提炼 401/403/404 熔断时预裁整段连坐跳过、预裁自熔断时又照取凭据照进（再由 preclassifyConflict 首行挡回）。
								//    回退：LEGION_PREGUARD_FIX_OFF=1 回判 guard（只管这一处；只作用于 DSH 宿主进程）
								if (Date.now() >= (process.env.LEGION_PREGUARD_FIX_OFF === "1" ? guard : preguard).pausedUntil) {
									// 09-13 切档车：预裁路取**本路专属凭据**（原与提炼共用 LLM_KEY_REF ⇒ 切 GLM 后取错键＝同族第五处形态）
									const cred = await credentials.resolve(PREVERDICT_KEY_REF);
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
												const anchor = Math.min(nr.id, old.id); // 批⑰ B7（09-21 maintainer·J 路 B 级·父窗实锤）：原 Math.max 恒取 nr（新条 id 恒大）⇒ vc+1 恒加新条，与注释意图「新条撞既有主题族→锚条（既有条）+1」方向倒置；min＝旧条＝既有锚（十分位 0→222 梯度同向实锤在 B 级件）
												const vcPair = Math.min(nr.id, old.id) + "-" + Math.max(nr.id, old.id); // 批⑱ K 路修车（K1-2）：键须「对唯一」——批⑰ B7 改 anchor=min 后原式恒「min-min」⇒ 同锚多对误判为一对·vc 漏计（旧 Math.max 时键天然唯一）；键与 anchor 解耦
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
								} else if (recent.length > 2000) {
									// 车二件2（治 J4-12/P4 🟡-10·R2 册）：跳闸显式计数——原无 else ⇒「未跑」与「跑了 0」不可辨（stats 单例留上巡值＝假绿窗）
									stats.a19SkippedDueToCap = (stats.a19SkippedDueToCap || 0) + 1;
									ctx.logger?.warn?.(
										"[living-memory] recent window skip #" +
											stats.a19SkippedDueToCap +
											" (recent=" +
											recent.length +
											" > 2000 — patrol_last 主写失败或巡停数日即越帽)",
									);
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
					//    首周带定 0.85-0.92（0.85 下含强相关·0.92 上归 ）·预算帽 30 边/轮·观察误建率再收——**design-approved「同意 按建议」退役首周帽**（落地已 10 天·semantic 活边实测仅 228 条≈首周配额 30×7 ⇒ 图谱语义成分近缺）：改 `LEGION_SEMANTIC_EDGE_BUDGET` 可配·缺省 **300**/轮。
					try {
						if (!process.env.LEGION_SEMANTIC_EDGE_OFF) {
							const vecRows2 = db
								.prepare(`SELECT v.id, v.embedding FROM memories_vec v JOIN memories m ON m.id = v.id
                WHERE v.model_version = ? AND m.status = 'active' AND m.source NOT LIKE 'mirror:%' AND (m.valid_to IS NULL OR m.valid_to > ?)`) // 十七审候裁⑦：建边池硬排 // 批⑰ D14⑥：滤 mirror 影子条（与 池同滤）
								.all(VEC_MODEL, nowIso());
							// ── 批⑰ （design-approved「按推荐逐项发」·J 路 🔴J4-2）：复活——
							//    原 `length <= 5000` 守卫在池 11396+ 恒假＝整段静默跳过零计数（链死 10 天·semantic 末次建边 09-11）。
							//    修：越帽不降全撤，改**降采样轮转**——按日序错位取 1/step 薄片（逐夜覆盖全池）＋
							//    计数出口 semanticEdgeSkippedDueToCap（本夜未入窗条数·LEGION_A10_CAP 旋钮先例同族）。
							let vecPool2 = vecRows2;
							// design-approved「同意 按建议」·检索质量体检建议②）：**首周预算帽退役**——
							//   原硬编码 `semEdges >= 30` 系「首周观察期」临时帽，落地 10 天从未退役 ⇒ semantic 活边实测仅 228（≈30×7）
							//   ⇒ 图谱「词面不重叠语义同物」的边死角长期不修（段尾注释自陈「观察误建率再收」未收）。
							//   改 env 可配：LEGION_SEMANTIC_EDGE_BUDGET（非正整数回落 300）。成本可控——建边仍受 cos∈[0.85,0.92) 对筛限。
							const SEM_EDGE_BUDGET = (() => {
								const n = Number(process.env.LEGION_SEMANTIC_EDGE_BUDGET);
								return Number.isInteger(n) && n > 0 ? n : 300;
							})();
							// 批⑲-K1 并入（K1-5·10-07 maintainer归口自办）：硬编 5000 接 env 旋钮 LEGION_A22_CAP——
							//   LEGION_A10_CAP 先例同型（非法/非正整数回落缺省 5000·缺省不变＝零行为变·drill 可用小帽造行为级验证）；
							//   D6 同型限制注记：step 随池增长漂移 ⇒ 池容一变残类映射重排（可重片/漏片·逐夜覆盖≠严格轮换）——旋钮只调粒度不治漂移。
							const A22_CAP = ((raw, dflt) => {
								const n = Number(raw);
								return Number.isInteger(n) && n > 0 ? n : dflt;
							})(process.env.LEGION_A22_CAP, 5000);
							if (vecRows2.length > A22_CAP) {
								const step = Math.ceil(vecRows2.length / A22_CAP);
								const off = Math.floor(Date.now() / 86400e3) % step;
								vecPool2 = vecRows2.filter((_, i) => i % step === off);
								stats.semanticEdgeSkippedDueToCap =
									(stats.semanticEdgeSkippedDueToCap || 0) +
									(vecRows2.length - vecPool2.length);
							}
							if (vecPool2.length > 1) {
								// 同上：帽校准（O(n²) 3093²≈4.8M 对·实测 ~3s nightly patrol窗可容）
								const meta2 = new Map(rows.map((r) => [r.id, r]));
								let semEdges = 0;
								const insSE =
									db.prepare(`INSERT INTO memories_edges (src, dst, edge_type, weight, valid_at, source_group, last_seen, instruction)
                  VALUES (?, ?, 'semantic', 0.3, ?, 'patrol:semantic', ?, '语义近邻')
                  ON CONFLICT(src, dst, edge_type) DO UPDATE SET last_seen = excluded.last_seen`);
								// P2 修（同 P1）：解码提外层一次
								const vecs2 = vecPool2.map((r) => blobToF32(r.embedding));
								outer2: for (let i = 0; i < vecPool2.length; i++) {
									const a2 = vecs2[i];
									for (let j = i + 1; j < vecPool2.length; j++) {
										const c2 = cosine(a2, vecs2[j]);
										if (c2 < 0.85) continue;
										if (c2 >= 0.92) continue; // ≥0.92 归 去重管（疑似重复不建边）
										const ma2 = meta2.get(vecPool2[i].id),
											mb2 = meta2.get(vecPool2[j].id);
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
										if (semEdges >= SEM_EDGE_BUDGET) break outer2; // 2026-09-22：首周帽退役→可配（缺省 300·只数真新建）
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
					// ── 批⑰ B6（design-approved·J 路 B 级）：chunks nightly patrol回补通道——
					//    惰性补全只在 vec 查询时补（LIMIT 6/次）⇒ 存量缺口永远够不到＝语义臂数据面天花板。
					//    （K1-8 订正·批⑲-K1 并入 10-07：原注「72%（4355 条）」系批⑰落码时点快照·分母口径已不可复算——
					//     K 路 09-21 实测当时 active 16132／缺 chunks 4301≈26.7%·现算缺口以库为准）
					//    预算纪律：默认关(design note)＋每夜帽 LEGION_CHUNK_BACKFILL_CAP（缺省 1000 条——
					//    design-approved中型车「帽上调」：300→1000，缺口 3400 追赶 12 夜→4 夜；env 可覆盖回滚）
					//    ＋计数出口（chunkBackfillDone/Errors·return/render 同步）。空条目写占位行防永恒重试（惰性路同款对齐）。
					if (process.env.LEGION_CHUNK_BACKFILL_ON === "1") {
						// nightPatrol 非 async——IIFE 包裹异步段（11329 既有 vec 回填同款先例·不侵入函数签名）
						void (async () => {
							try {
							const bfCap = envIntCap(process.env.LEGION_CHUNK_BACKFILL_CAP, 1000);
							const missRows = db
								.prepare(
									`SELECT m.id, m.title, m.content FROM memories m LEFT JOIN memories_vec_chunks c ON c.id = m.id AND c.model_version = ? WHERE m.status='active' AND c.id IS NULL ORDER BY m.id DESC LIMIT ?`,
								)
								.all(VEC_CHUNK_MODEL, bfCap);
							// 帽接近度哨（10-01 补·库量驱动型）：本夜取出量 ≥80% 帽 ⇒ 缺口未清（超帽态另有 done 计数观察）
							capNearWatch(
								db,
								"chunk",
								missRows.length,
								bfCap,
								"块回填本夜作业量接近帽·缺口未清",
							);
							let bfOk = 0,
								bfErr = 0;
							if (missRows.length > 0) {
								const insB = db.prepare(
									"INSERT OR REPLACE INTO memories_vec_chunks (id, seq, chunk_text, embedding, model_version) VALUES (?, ?, ?, ?, ?)",
								);
								for (const r of missRows) {
									try {
										const chunks = chunkMemory(r.title, r.content);
										if (!chunks.length) {
											insB.run(r.id, 0, "", Buffer.alloc(0), VEC_CHUNK_MODEL);
											bfOk++;
											continue;
										}
										const embs = await embedOnce(chunks, VEC_CHUNK_MODEL);
										for (
											let i = 0;
											i < chunks.length && i < embs.length;
											i++
										)
											insB.run(r.id, i, chunks[i], f32ToBlob(embs[i]), VEC_CHUNK_MODEL);
										bfOk++;
									} catch {
										bfErr++;
									}
								}
							}
							stats.chunkBackfillDone = (stats.chunkBackfillDone || 0) + bfOk;
							if (bfErr > 0)
								stats.chunkBackfillErrors =
									(stats.chunkBackfillErrors || 0) + bfErr;
							} catch (eBF) {
								stats.chunkBackfillErrors = (stats.chunkBackfillErrors || 0) + 1;
								ctx.logger?.warn?.(
									"[living-memory] chunk-backfill failed (#" +
										stats.chunkBackfillErrors +
										"): " +
										String(eBF).slice(0, 60),
								);
							}
						})();
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
				const HI_SIM_CAP =
					envIntCap(process.env.LEGION_HISIM_CAP, 5000); // 车二件1b：帽 env 化（缺省不变）＋降采样＋计数（family 件1a 同法·治同型病灶）
				let hiCursorNext = null; // B级①：hisim 游标写点后移载体（段尾与成败绑定写回）
				for (const list of bySpace.values()) {
					if (list.length < 2) continue;
					const grams = list.map((r) => ({ id: r.id, g: bigrams(r.title) }));
					// 帽接近度哨（10-01 补·库量驱动型）：桶内条数 ≥80% 帽且未超 ⇒ 预警
					capNearWatch(
						db,
						"hisim",
						list.length,
						HI_SIM_CAP,
						"高相似桶将满·LEGION_HISIM_CAP 复评",
					);
					let iGate2 = null;
					if (list.length > HI_SIM_CAP) {
						const step2 = Math.ceil(list.length / HI_SIM_CAP);
						// B级①（甲J2 游标随迁·family 同法）：静态模数→游标相位逐巡滑动（范式·D6 近似承诺承继）
						let hiSlot = 0;
						try {
							const rcH =
								db
									.prepare(
										"SELECT v FROM organ_meta WHERE k='hisim_rot_cursor'",
									)
									.get();
							hiSlot = (((Number(rcH && rcH.v) || 0) + 1) % step2 + step2) % step2;
						} catch {
							stats.hiRotCursorErrors =
								(stats.hiRotCursorErrors || 0) + 1;
						}
						iGate2 = new Set();
						for (let gi = 0; gi < grams.length; gi++)
							if (gi < HI_SIM_CAP || (gi + hiSlot) % step2 === 0)
								iGate2.add(grams[gi].id);
						stats.hiSimSkippedDueToCap =
							(stats.hiSimSkippedDueToCap || 0) +
							(list.length - iGate2.size);
						hiCursorNext = hiSlot;
					}
					for (let i = 0; i < grams.length; i++) {
						if (mergeIds.has(grams[i].id)) continue;
						if (iGate2 && !iGate2.has(grams[i].id)) continue; // i 侧采样门（帽外条可作 j 侧·语义边界同款）
						for (let j = i + 1; j < grams.length; j++) {
							if (mergeIds.has(grams[j].id)) continue;
							if (jaccard(grams[i].g, grams[j].g) >= 0.85)
								mergeIds.add(grams[j].id); // j 的 id 更小（更旧）
						}
					}
				}
				if (hiCursorNext !== null) {
					try {
						db.prepare(
							"INSERT OR REPLACE INTO organ_meta (k, v) VALUES ('hisim_rot_cursor', ?)",
						).run(String(hiCursorNext)); // B级①段尾写（D6 与成败绑定）
					} catch {
						stats.hiRotCursorErrors =
							(stats.hiRotCursorErrors || 0) + 1;
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
							"SELECT id FROM memories WHERE title LIKE 'handover note警报：%' AND source LIKE 'patrol:%' AND status = 'active' ORDER BY id DESC LIMIT 1", // A级四件①（全链再审乙🔴F1·12 夜静默铁证）：补 status='active' 过滤——原无 ⇒ prev 恒选中最新一条（09-09 已 done 的死条 4615）·警报全刷进死条（库面 14 条 active=0）
						)
						.get(); // P0修（09-03 audit）：跨日去重——source 掺 dayKey 日变致每夜重立（正库 13 行同 checksum 实锤）
					if (prev) {
						db.prepare(
							"UPDATE memories SET ts = ?, title = ?, content = ?, checksum = ?, status = 'active', closed_at = NULL WHERE id = ?", // F1②：顺带复活兜底——若 prev 命中的是边缘态（并发闭环）也归 active（幂等无害）
						).run(nowIso(), title, content, sha1(title + content), prev.id); // 审计②R1：补 title 刷新——N 变化时计数不失真
					} else {
						// [拆⑦·2026-10-04] 闸归一：handover note警报 INSERT 前过安检闸（pipeline.securityGate·core 原语零新）
						const _g7 = _pipelineGate(title, content);
						if (!_g7.pass) _patrolGateSkip(_g7, "handover note警报");
						else
						db.prepare(
							"INSERT INTO memories (ts, type, title, content, space, source, checksum, valid_to, stale_state) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
						).run(
							nowIso(),
							"todo",
							title,
							content,
							"memory-organ",
							"patrol:" + dayKey,
							sha1(title + content),
							isoAhead(7 * 86400e3), // 落码车 L9721：handover note警报 todo 默认带 7 天短效（与 D2 同式·到点nightly patrol自动 aged）
							"ttl",
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
			// ③todo 衰减（wave 8-g 订正·按开关两态）：LIFE_GUARD 开：满 14 天先查闭环，未闭留到 30／45 天，老化后 search 可见（×0.1·带「已老化」标）；
			//   关（LEGION_LIFE_GUARD_OFF=1）：满 14 天一律 aged，search 不可见。两态 aged 都不进注入／时间线（遗忘是功能）
			let agedTodos = 0;
			try {
				const cutoff = Date.now() - 14 * 24 * 3600 * 1000;
				// wave 车二-8c（10-17·落码规格 2.5）：满 14 天的待办老化前先查闭环——closureCheck 判已闭 → 照旧 aged（不自动转 done，记 todoAgeClosed）；
				//   未闭且不满 30 天 → 留；未闭、不满 45 天且受保护（lifeProtect）或标题含 lifeTodoHold（pending ruling、交办、候…裁）→ 留（留记 todoAgeHeld）；其余照旧 aged。
				//   LEGION_LIFE_GUARD_OFF=1 时退回原法（满 14 天一律 aged）
				const c2LifeC = process.env.LEGION_LIFE_GUARD_OFF !== "1";
				const todos = db
					.prepare(
						c2LifeC
							? "SELECT id, ts, title, content, essential, validated_count, last_hit_at FROM memories WHERE status = 'active' AND type = 'todo'"
							: "SELECT id, ts FROM memories WHERE status = 'active' AND type = 'todo'",
					)
					.all();
				const nowC = Date.now();
				for (const t of todos) {
					const d = new Date(String(t.ts).replace(" ", "T"));
					if (!isNaN(d.getTime()) && d.getTime() < cutoff) {
						if (c2LifeC) {
							const ageMs = nowC - d.getTime();
							let closedC = false;
							try {
								closedC = !!closureCheck(db, t.title, t.content, t.id).closed;
							} catch {
								stats.todoAgeCheckErrors = (stats.todoAgeCheckErrors || 0) + 1;
							}
							if (closedC) stats.todoAgeClosed = (stats.todoAgeClosed || 0) + 1;
							else if (ageMs < 30 * 86400e3 && !(CRIT.lifeStateTitle && CRIT.lifeStateTitle.test(String(t.title || "")))) {
								// 盲审处置（nit）：30 天无条件留存不罩状态句标题(design note)；状态句照走下一支（受保护只认 ess、留存词 45 天）
								stats.todoAgeHeld = (stats.todoAgeHeld || 0) + 1;
								continue;
							} else if (
								ageMs < 45 * 86400e3 &&
								((typeof core.lifeProtect === "function" && core.lifeProtect(t, nowC)) || (CRIT.lifeTodoHold && CRIT.lifeTodoHold.test(String(t.title || ""))))
							) {
								stats.todoAgeHeld = (stats.todoAgeHeld || 0) + 1;
								continue;
							}
						}
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
			let mirrorWeak = 0; // 车三观测键载体（审计 rev4-D1 修：原声明于内层 try 块级出界 ⇒ 落账处 typeof 恒回 0 假零——上提至共现段顶级与 cooccurPairs 同级）
			let stalePurged = 0; // 车三补件②（09-14）：**共现边失效清理**计数（含 core 僵尸＋weak 残留）——**必须与 cooccurPairs 同级声明**（同 D1 教训：内层 let 出界 ⇒ 落账恒 0 假零）
			let mirrorErr = 0; // 车三补件③（09-14·审计 C/S2 面）：镜像事务失败计数出口——原仅 logger.warn，镜像失败夜 mirrorWeak 残留脏值（ROLLBACK 不回退 JS 变量）且「失败」不可见，与错误出口入账纪律不符
			try {
				// 平滑迁移：旧表加列（存在则跳过）
				try {
					db.exec("ALTER TABLE memories_cooccur ADD COLUMN source_group TEXT");
				} catch {}
				try {
					db.exec("ALTER TABLE memories_cooccur ADD COLUMN created_at TEXT");
				} catch {}
				// PPR 性能车 §三索引（brainkickoff·与 C 档同车）：partnerOf 查询 `WHERE score >= ?`——
				//   本表仅 UNIQUE(id_a,id_b) autoindex·score 无索引 ⇒ 现 1,417 行全扫无感，次夜 C 档后
				//   ＝ 9.8 万行**每题全扫**（未命中行也扫）。本索引幂等建·与 PPR 建图改造同车生效。
				try {
					db.exec(
						"CREATE INDEX IF NOT EXISTS idx_cooccur_score ON memories_cooccur(score)",
					);
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
					watermark === 0 || dirtySources.size > allSources.length * 0.5 ||
					process.env.LEGION_COOCCUR_FULLREBUILD === "1"; // 车三 C 档（task brief §四）：判据落定后存量补算走一次性全量（9.8 万对若分 13 夜慢跑新旧判据混用致指标跳变）——置此 env 一夜全量·毕后移除
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
					const COOCCUR_SIM_MIN = Number(process.env.LEGION_COOCCUR_SIM) || 0.1; // 车三①改判据(design note)：weak 档下限 0.10（全量重算可达 80.2%·brain十档实测）——core≥0.40 维持（score 天然分档）；回滚=LEGION_COOCCUR_SIM=0.4 一键回原判据
					for (let i = 0; i < grams.length; i++) {
						for (let j = i + 1; j < grams.length; j++) {
							const sim = jaccard(grams[i].g, grams[j].g);
							if (sim < COOCCUR_SIM_MIN) continue;
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
					// ── B-1②（批 C 候办·09-15）：消费入口恢复——organ_meta['hit_pending'] 并入 Map ──
					//    治「重启丢攒批」：进程 Map 重启清零，但落盘键在 ⇒ nightly patrol消费前∪进来（跨重启命中不丢·批 2 回收判据基础）
					let _restoredHits = 0;
					try {
						const _p = db
							.prepare("SELECT v FROM organ_meta WHERE k='hit_pending'")
							.get();
						if (_p?.v) {
							const _po = JSON.parse(_p.v);
							for (const [k, v] of Object.entries(_po || {})) {
								const n = Number(v) || 0;
								if (n > 0 && !hitsNow.has(Number(k)))
									hitsNow.set(Number(k), n);
							}
							_restoredHits = Object.keys(_po || {}).length;
						}
					} catch (e2) {
						// 观察缺口修（09-16 创造notify§三-2·同型空 catch）：nightly patrol入口恢复∪失败须可观测
						//   （原空吞 ⇒ 键在而 Map 未并进＝恢复路失败零信号——与 D-1 甲面同族·计数接入同一观测键）
						try {
							hitMapErrorsStat.n++;
						} catch {}
					}
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
					// wave 盲审处置（should）：本夜结算到的真命中记进 life_renew.hits（只动已续过的条；续期帽后再续凭它）——LIFE_GUARD 关时不记
					if (process.env.LEGION_LIFE_GUARD_OFF !== "1" && hitsNow.size > 0) {
						try {
							ensureLifeRenew(db);
							const upH = db.prepare("UPDATE life_renew SET hits = hits + ? WHERE id = ?");
							for (const [hid, hn] of hitsNow) if (Number(hn) > 0) upH.run(Number(hn), Number(hid));
						} catch (eHits) {
							// 补笔盲审处置（10-18·宿主契约 S2）：命中记账失败另计 lifeRenewHitsErrors 并打 warn——原记进 lifeRenewErrors 且静默，
							//   与「续期段整段回滚」混成一个数（那时续期段照跑），运维会误判当夜回滚；lifeRenewErrors 只留续期段
							stats.lifeRenewHitsErrors = (stats.lifeRenewHitsErrors || 0) + 1;
							ctx.logger?.warn?.(`[living-memory] wave 命中记账失败（life_renew.hits 本夜未记，续期段照跑；出错累计 ${stats.lifeRenewHitsErrors} 次）: ${String(eHits).slice(0, 60)}`);
						}
					}
					hitsNow.clear();
					// ── B-1②：消费毕删键（攒批生命周期闭环：写→落盘→恢复∪→消费→清键）──
					try {
						db.exec("DELETE FROM organ_meta WHERE k='hit_pending'");
					} catch {}
					ctx.logger?.info?.(
						`[living-memory] relevancy settled: ${settled} entries${_restoredHits > 0 ? ` (restored ${_restoredHits} from disk)` : ""}`,
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
					const upEdgeWeak = db.prepare(`INSERT INTO memories_edges (src, dst, edge_type, weight, valid_at, source_group, last_seen)
            VALUES (?, ?, 'cooccur_weak', ?, ?, ?, ?)
            ON CONFLICT(src, dst, edge_type) DO UPDATE SET weight = excluded.weight, last_seen = excluded.last_seen`); // 车三分档：weak（加权后 score<0.40·审计 rev4-J1 口径注）独立 edge_type——哨①零改自动计入（无 type 筛）；主图消费两处滤（PPR/社区）·防指标达标而主图污染
					db.exec("BEGIN"); // 车三批量：9.8 万对镜像包单事务（原逐条自提交·nightly patrol耗时不可控）
					try {
						for (const e of db
							.prepare(
								"SELECT id_a, id_b, score, source_group FROM memories_cooccur",
							)
							.all()) {
							if (e.score >= COOCCUR_CORE_MIN) upEdge.run(e.id_a, e.id_b, e.score, stamp, e.source_group, stamp); // 车三补件：判据常量化（与伙伴注入同源·防硬编码漂移）
							else {
								upEdgeWeak.run(e.id_a, e.id_b, e.score, stamp, e.source_group, stamp);
								mirrorWeak += 1;
							}
						}
						// 车三补件②（09-14 13:0x）：**weak 边生命周期闭环**——原镜像段只 UPSERT、全文件无任何
						//   `cooccur_weak` 删除路径 ⇒ ①回滚（LEGION_COOCCUR_SIM=0.4）后 edges 内 9.6 万 weak 行
						//   永久滞留 ⇒ 哨① edgeCoverage 虚高不降（「回滚了但账没回」）②某对升档 core 后旧 weak 行
						//   残留 ⇒ 同一对 cooccur＋cooccur_weak 双行并存（UNIQUE 含 edge_type 故不撞）。判据＝删
						//   「edges 有、cooccur 表已无对应 weak 对」的行——**未重算组的 weak 对仍在表内 ⇒ 不误删**
						//   （增量路径安全）；同事务内执行（与镜像原子）。
						// 车三补件②（09-14）：**共现边生命周期闭环**——原镜像段只 UPSERT、全文件无任何
						//   `cooccur_weak` 删除路径 ⇒ ①回滚（LEGION_COOCCUR_SIM=0.4）后 edges 内 9.6 万 weak 行
						//   永久滞留 ⇒ 哨① edgeCoverage 虚高不降（「回滚了但账没回」）②某对升档 core 后旧 weak
						//   行残留 ⇒ 同对 cooccur＋cooccur_weak 双行并存（UNIQUE 含 edge_type 故不撞）。
						//   ⚠ 审计车补强（09-14 13:1x·独立审计 B/E 面）两处：
						//     · **双向匹配**（F2）：原判据只按 (id_a,id_b) 有向匹配，而 merge 迁移会翻转端点序
						//       ⇒ 误删仍有效的 weak 边（审计实测 T4）。改 (src,dst) 双向 OR。
						//     · **core 侧对称**（F1）：原补件只闭 weak 半边 ⇒ core 僵尸边（表内已无此对，如真库
						//       ）每晚被保活刷 last_seen 并留在 PPR 主图。同一判据覆盖两型＝对称闭环。
						//   判据＝删「edges 有、而 cooccur 表已无**同档**对应对」的行——未重算组的对仍在表内
						//   ⇒ 不误删（增量安全）；同事务内执行（与镜像原子）。
						stalePurged = db
							.prepare(
								`DELETE FROM memories_edges WHERE edge_type IN ('cooccur','cooccur_weak')
                 AND NOT EXISTS (SELECT 1 FROM memories_cooccur c
                   WHERE ((c.id_a = memories_edges.src AND c.id_b = memories_edges.dst)
                       OR (c.id_a = memories_edges.dst AND c.id_b = memories_edges.src))
                     AND ((memories_edges.edge_type = 'cooccur' AND c.score >= ?)
                       OR (memories_edges.edge_type = 'cooccur_weak' AND c.score < ?)))`,
							)
							.run(COOCCUR_CORE_MIN, COOCCUR_CORE_MIN).changes;
						db.exec("COMMIT");
					} catch (eMirror) {
						db.exec("ROLLBACK");
						mirrorErr += 1; // 车三补件③（审计 C/S2）：**失败出口入账**——原仅 logger.warn ⇒ 镜像失败夜不可见（且 mirrorWeak 脏值无法辨识）
						throw eMirror;
					}
					// 轻量巡检保活：active 边且端点 active → last_seen 刷新（未脏组的在役边防误失效——终版必修②）
					// 批⑲ P3（J4-8·2026-09-21）：保活刷新降频——last_seen 全仓 36 处写侧零读侧（失效判据只看端点 status ⇒ 保活刷新无消费者），
					//   原每夜无条件刷 12.8 万行＝纯写放大（WAL 放大与 40 分钟巡耗时同向）；改每 N 夜一次
					//   （LEGION_EDGE_KEEPALIVE_DAYS·默认 7）；未来若引入 last_seen 老化消费须恢复每夜并给读侧设计。
					const _kaDays = envIntCap(process.env.LEGION_EDGE_KEEPALIVE_DAYS, 7);
					let _kaGo = true;
					try {
						const _kaLast = String(
							db.prepare("SELECT v FROM organ_meta WHERE k='edge_keepalive_at'").get()
								?.v || "",
						);
						const _kaMs = Date.parse(_kaLast);
						if (!isNaN(_kaMs) && Date.now() - _kaMs < _kaDays * 86400e3) {
							_kaGo = false;
							stats.edgeKeepAliveSkips = (stats.edgeKeepAliveSkips || 0) + 1;
						}
					} catch {}
					if (_kaGo) {
						db.prepare(`UPDATE memories_edges SET last_seen = ? WHERE invalid_at IS NULL
            AND src IN (SELECT id FROM memories WHERE status='active')
            AND dst IN (SELECT id FROM memories WHERE status='active')`).run(
							stamp,
						);
						try {
							db.prepare(
								"INSERT OR REPLACE INTO organ_meta (k, v) VALUES ('edge_keepalive_at', ?)",
							).run(nowIso());
						} catch {}
					}
					// 死边软失效：端点非 active → invalid_at（不物理删——时态完整）
					//    09-26 maintainer（十九审实证）：**solved_by 豁免 dst 判据**——其语义即「指向已办 todo」（dst 必 done），
					//    与 active 判据天然矛盾（库面 solved_by 有效 1044／失效 839 vs patches 801/145）；solved_by 只判 src active。
					db.prepare(`UPDATE memories_edges SET invalid_at = ? WHERE invalid_at IS NULL
            AND (edge_type IS NULL OR edge_type <> 'solved_by') -- step(09-27)·NULL 边界加固：现库 0 条·纯预防
					  AND (src NOT IN (SELECT id FROM memories WHERE status='active')
					    OR dst NOT IN (SELECT id FROM memories WHERE status='active'))`).run(
						stamp,
					);
					db.prepare(`UPDATE memories_edges SET invalid_at = ? WHERE invalid_at IS NULL
            AND edge_type = 'solved_by'
					  AND src NOT IN (SELECT id FROM memories WHERE status='active')`).run(
						stamp,
					);

					// ── 丁（maintainer 09-27·治 L6 无 GC）：板行 GC——超 30 天未更新行删除（窗口工作态·账态为权威）──
					try {
						const _dCutB = new Date(Date.now() + 8 * 3600e3 - 30 * 86400e3);
						const _cutB = _dCutB.toISOString().slice(0, 16) + "+08:00";
						const _openDaysB = core.boardGcOpenDays(); // 板机制六缺口车·乙批·缺口 4②（10-18）：不设或不认＝null，走下方原语句原样
						if (_openDaysB === null) {
						// step(09-27)·删前留痕（乙路 D2 补强）：将删行身份入 stats 观测面（≤8 条 sid）
					let _sidsB = [];
					try { _sidsB = db.prepare("SELECT sid FROM session_boards WHERE updated_at < ? LIMIT 8").all(_cutB).map((r) => r.sid); } catch (eSid) {
						ctx.logger?.warn?.("[living-memory] GC 留痕取数失败: " + String(eSid).slice(0, 60)); // 甲7 订正：不静默
					}
					const _infB = db.prepare("DELETE FROM session_boards WHERE updated_at < ?").run(_cutB);
					if (_infB && _infB.changes) stats.boardRowsPrunedSids = _sidsB;
						core.boardRowsPruned += _infB && _infB.changes ? _infB.changes : 0;
						} else {
							// 板机制六缺口车·乙批·缺口 4②（10-18）：设 LEGION_BOARD_GC_OPEN_DAYS=N（>30）——超 30 天的板中，含 pending/in_progress 项且未超 N 天者留，
							//   余者（全毕、空板、payload 坏、超 N 天）照删；删句带回 updated_at 条件（取数后被他窗刷新的板不误删）；
							//   留下数写 organ_meta board_gc_skip_rows（本轮值·覆盖写）。删前留痕同原口径（≤8 条 sid）
							const _cutOpenB = new Date(Date.now() + 8 * 3600e3 - _openDaysB * 86400e3).toISOString().slice(0, 16) + "+08:00";
							const _delB = [];
							let _skipB = 0;
							for (const _r of db.prepare("SELECT sid, payload, updated_at FROM session_boards WHERE updated_at < ?").all(_cutB)) {
								let _openB = false;
								try {
									const _p = JSON.parse(_r.payload);
									_openB = !!(_p && Array.isArray(_p.todos) && _p.todos.some((x) => x && (x.status === "pending" || x.status === "in_progress")));
								} catch {}
								if (_openB && String(_r.updated_at) >= _cutOpenB) _skipB++;
								else _delB.push(_r.sid);
							}
							// 盲审低-3：一句删（原子：中途出错一行不删、计数不乱；本段在 1809 那组事务之外）
							let _nDelB = 0;
							if (_delB.length) {
								const _iB = db
									.prepare("DELETE FROM session_boards WHERE updated_at < ? AND sid IN (" + _delB.map(() => "?").join(",") + ")")
									.run(_cutB, ..._delB);
								_nDelB = _iB && _iB.changes ? _iB.changes : 0;
							}
							if (_nDelB) stats.boardRowsPrunedSids = _delB.slice(0, 8);
							core.boardRowsPruned += _nDelB;
							db.prepare("INSERT INTO organ_meta (k, v) VALUES ('board_gc_skip_rows', ?) ON CONFLICT(k) DO UPDATE SET v = excluded.v").run(String(_skipB));
						}
					} catch (eBp) {
						ctx.logger?.warn?.("[living-memory] 板行 GC 失败: " + String(eBp).slice(0, 60));
					}
					// ── A-G 架构 C 件（09-27 approved）：cross-space流转超时哨 ──
					//    global 交办/pending ruling todo >48h 未 closed_at → dispatchStale；>7d → dispatchStale7d(design note)；
					//    滞留清单入 organ_meta dispatch_stale_list（零本机路径——发布面红线：催办不写承办inbox文件）。
					try {
						const _rows = db
							.prepare(
								"SELECT id, title, ts FROM memories WHERE space='global' AND type='todo' AND status='active' AND closed_at IS NULL AND (title LIKE '交办：%' OR title LIKE '呈裁：%') ORDER BY ts ASC",
							)
							.all();
						let _s48 = 0, _s7 = 0;
						const _list = [];
						for (const _r of _rows) {
							const _age = Date.now() - (Date.parse(_r.ts) || 0);
							if (_age > 48 * 3600e3) {
								_s48++;
								if (_age > 7 * 86400e3) _s7++;
								if (_list.length < 10) _list.push({ id: _r.id, t: String(_r.title).slice(0, 44) });
							}
						}
						core.dispatchStale = _s48;
						core.dispatchStale7d = _s7;
						if (_list.length)
							db.prepare(
								"INSERT OR REPLACE INTO organ_meta (k, v) VALUES ('dispatch_stale_list', ?)",
							).run(JSON.stringify(_list));
						else db.prepare("DELETE FROM organ_meta WHERE k='dispatch_stale_list'").run(); // 甲3 订正：空态清理（原只写不清）
					} catch (eDs) {
						ctx.logger?.warn?.("[living-memory] 流转哨失败: " + String(eDs).slice(0, 60));
					}
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
            AND edge_type != 'cooccur_weak' -- 车三分档：weak 边不进社区检测图（与 PPR 段同滤）
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
						const J = require("@node-rs/jieba"); // ④补审计甲路🔴1 修：原「@node-rs/core.jieba」系 3ab27c1 机械替换误伤字符串字面量——require 必抛 MODULE_NOT_FOUND 致 TfIdf 增强路每巡断供（b3b8c4a 迁此在产未修·2026-10-06 一行改回）
						if (core.jieba && J && typeof J.TfIdf === "function") {
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
										core.jieba,
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
			// ── ⑤ 三元组停建（10-24 13:51 maintainer准·中枢决策记录 13:51 条·并入 fu-nao/order-fix）──
			//    三元组只写不读（全插件唯一读处是 stats 计数）→ 投影默认停：LEGION_TRIPLES_KG_ON=1 才照旧建表、读边、投影、推水线（只认值 "1"）。
			//    停时不写 memory_triples、不动 kg_edge_watermark：存量不删不改，重开即从原水线续投（停得越久，重开当夜补投越多）。
			//    计数照读（stats.triplesTotal，patrol_last 同记）；stats.triplesKgStopped 标停建，stats 渲染行在 KG 计数旁注「已停建」。
			const KG_ON = process.env.LEGION_TRIPLES_KG_ON === "1";
			stats.triplesKgStopped = !KG_ON;
			try {
				if (KG_ON) {
					db.exec(
						`CREATE TABLE IF NOT EXISTS memory_triples (triple_id INTEGER PRIMARY KEY AUTOINCREMENT, subject_id INTEGER NOT NULL, predicate TEXT NOT NULL, object_id INTEGER, object_text TEXT, valid_from TEXT, valid_to TEXT, ingested_at TEXT NOT NULL, invalidated_at TEXT, confidence REAL DEFAULT 1.0, source_memory INTEGER, source_edge INTEGER, extracted_by TEXT NOT NULL)`,
					);
					db.exec(
						`CREATE UNIQUE INDEX IF NOT EXISTS idx_triples_dedupe ON memory_triples(subject_id, predicate, COALESCE(object_id,0), COALESCE(object_text,''))`,
					);
					db.exec(
						`CREATE INDEX IF NOT EXISTS idx_triples_subject ON memory_triples(subject_id)`,
					);
				} else stats.triplesIn = 0; // 停建夜：本夜入 0（长驻进程里不留上一夜的数）
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
				const kgEdges = !KG_ON
					? [] // 停建：不读边，下面投影与推水线整段不走
					: db
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
					// [拆⑦·2026-10-04] 闸归一：dreamer 簇条 INSERT 前过安检闸（含边链 e12 连跳——闸不过整簇不建）
					const _g7d = _pipelineGate(
						t12,
						`语义近邻簇索引（${rows12.length} 条·${span12}）——检索一跳到簇·源条全保留：\n${titles12}`,
					);
					if (!_g7d.pass) _patrolGateSkip(_g7d, "dreamer簇");
					else {
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
				}
			} catch (e12) {
				ctx.logger?.warn?.(
					`[living-memory] dreamer fail: ${String(e12).slice(0, 60)}`,
				);
			}

			// ── wave 车二-8b（10-17·落码规格 2.5）：nightly patrol续期——紧贴 之前，自带 try／catch ──
			//    取 active 且 valid_to 落在［现在−2 天，现在+36 小时］的条（自动 ttl 条与手写 fact 缺省 +30 天的软沉底条；下沿按日期、nightly patrol连漏时放宽；
			//    补笔②（10-18）排除被取代、退役、nightly patrol哨条与写入者自定的失效日，见取行处注），逐条判 core.lifeProtect；
			//    受保护的 valid_to = max(valid_to, 今天) + 30 天，形态随原值（10 字写日期，长的写北京 ISO「YYYY-MM-DDTHH:MM+08:00」）；
			//    记账表 life_renew（首次写 old_valid_to、再续只加 n、改 last_at——数据面回滚依据）；不写 stale_by（P2 复核段遇 stale_by 非空永不再标）。
			//    续期帽（盲审处置 10-18）：续满 LIFE_RENEW_CAP 次后，除 essential 外再续须 life_renew.hits>0（上次续期后的真命中），见 ensureLifeRenew 注。
			//    每夜帽 2000 条（超帽记 lifeRenewCapHit）；stats.lifeRenew 按理由分，organ_meta life_renew_total 累计。命中结算在本nightly patrol前段，读到的 last_hit_at 是当夜值。
			//    LEGION_LIFE_GUARD_OFF=1 时整段不跑
			//    盲审处置（should，10-18）：取行到写完包进一个 BEGIN IMMEDIATE 事务（valid_to 与 life_renew 同进同退——回滚 SQL 靠 life_renew 复原），
			//    出错 ROLLBACK；stats 与日志在 COMMIT 之后才记。已在事务中（db.isTransaction）时不另开。先例见本文件车三镜像单事务
			if (process.env.LEGION_LIFE_GUARD_OFF !== "1") {
				let txR = false;
				try {
					const lp = core.lifeProtect;
					if (typeof lp !== "function") throw new Error("lifeProtect 缺");
					ensureLifeRenew(db);
					if (!db.isTransaction) {
						db.exec("BEGIN IMMEDIATE");
						txR = true;
					}
					const nowR = Date.now();
					const bjR = (ms) => {
						const d = new Date(ms + 8 * 3600e3);
						const p2 = (n) => String(n).padStart(2, "0");
						return `${d.getUTCFullYear()}-${p2(d.getUTCMonth() + 1)}-${p2(d.getUTCDate())}T${p2(d.getUTCHours())}:${p2(d.getUTCMinutes())}+08:00`;
					};
					// 盲审处置（nit，10-18）：下沿按日期前 10 字比（在役带期条约九成是 10 字纯日期，整串比时「YYYY-MM-DD」<「YYYY-MM-DDT…」，下沿实际只到前一天）；
					//   下沿取 max(现在−7 天, min(现在−2 天, 上次nightly patrol−1 天))——nightly patrol连漏几夜时补上漏掉的窗（至多回看 7 天；无 patrol_last 时按 −2 天）
					let lastPatrolMs = NaN;
					try {
						const pl = db.prepare("SELECT v FROM organ_meta WHERE k='patrol_last'").get();
						if (pl && pl.v) lastPatrolMs = Date.parse(String(JSON.parse(pl.v).at || ""));
					} catch {}
					const loMs = Math.max(nowR - 7 * 86400e3, Math.min(nowR - 2 * 86400e3, Number.isFinite(lastPatrolMs) ? lastPatrolMs - 86400e3 : nowR - 2 * 86400e3));
					// 补笔②（10-18·中枢 03:26 定③）：续期取行的排除项，在 SQL 里排（不占每夜 2000 条帽）
					//   K3-01 被取代条：写时盖戳 stale_state='superseded-auto'（tools.cjs 盖戳把旧条 valid_to 写成当天，次夜即进窗）、P2 退役 retired、
					//     superseded_by 非空（合并执行器写，护栏；该列只由nightly patrol 段补建，先查列在不在）。码里没有 supersedes 边型（tools.cjs EDGE_KINDS）。
					//     取代类入边不排（补笔盲审处置·正确与回归 S1，取改法二）：码里另有「patches／solved_by 活入边＝已被后条取代」的口径
					//     （inject.cjs 顶注选条、常驻席，只管注入席位），但库面这两型边语义混杂——订正、勘误之外，锚点、换窗交接、lesson、销号条也挂在仍有效的原条上
					//     （10-18 快照窗内受保护的 56 条里，来源是订正类标题的只 10 条；本文件cross-space教训段还把other spaces指向 lesson 的这两型边当复用正信号），
					//     续期step全排会让仍有效的条到期老化。本车照续，交memory organ审读定（量账见补笔记录「盲审处置」节）
					let supByColR = false;
					try {
						supByColR = db.prepare("PRAGMA table_info(memories)").all().some((c) => c && c.name === "superseded_by");
					} catch {}
					//   K3-03 nightly patrol哨条：nightly patrol自写的条（来源 patrol:<日>、patrol-sentinel:<日>）与三类哨标题（sentry警报：、handover note警报：、记忆链路light sentry警报：，
					//     照侦察 K3-03 原文；较 存量自愈段（只认 patrol-sentinel: 来源与sentry、handover note两类标题）多认light sentry标题与 patrol: 来源，是其超集——
					//     补笔盲审处置·正确与回归 N5 订正原注「同自愈段的判法」）——哨条每天新立、7 天期，按标题近似给旧条加 vc，不排的话会被一续再续
					const exclR = [
						"COALESCE(m.stale_state, '') NOT IN ('superseded-auto', 'retired')",
						...(supByColR ? ["m.superseded_by IS NULL"] : []),
						"COALESCE(m.source, '') NOT LIKE 'patrol%'",
						"COALESCE(m.title, '') NOT LIKE 'sentry警报：%' AND COALESCE(m.title, '') NOT LIKE 'handover note警报：%' AND COALESCE(m.title, '') NOT LIKE '记忆链路light sentry警报：%'",
						//   K3-02 只续系统给的期限，写入者自定的失效日不续：①自动提炼的 ttl 条（来源 auto:<sid> 且 stale_state='ttl'，extract.cjs 落库处）；
						//     ②手写 fact 的缺省 +30 天（stale_state 为空、valid_to＝建档北京日 +30 天：tools.cjs 生命周期分级取北京日 +30，与 ts 同刻）；
						//     ③life_renew 已记的条（前次按①②续过，valid_to 已不是原值，照常再续、受续期帽管），但不含非 auto 的 ttl 标条（见下 F09）。
						//     手写显式 valid_to、手写 ttl_days 的条（ttl 标，不论类型、不论期限是否恰为建档日 +30）、其余来源（mirror:、drill: 等）的非缺省期限一律不续，
						//     到点照旧（ttl 条转 aged、无标条软沉底），交人工
						//   F09（10-24 中枢定，与 tools.cjs 写入顺序同车）：原②认 stale_state 为 ttl，是因为 F09 缺陷下 fact 带 ttl_days 时 tools 先补 +30、
						//     ttl 只打标不生效（10-18 盲审处置 K3-12 照认）。写入顺序改正后，ttl 标只来自调用方给的合法 ttl_days，属写入者自定的寿命：
						//     ②去掉 'ttl'；③补排「非 auto 且 ttl 标」（防被缺陷续过、复活或手工改期过的显式寿命条经 life_renew 一续再续；10-24 存量 0 条，属预防）。
						//     stale_state 可为 NULL，③的排除式取 COALESCE（裸 m.stale_state = 'ttl' 遇 NULL 得 NULL，会把 NULL 态的已续条也排掉）。①不动。
						//     已知限：调用方显式给的 valid_to 恰为建档日 +30、又没给 ttl_days 的 fact，与缺省同形，仍按②续（不加列不可辨，中枢 10-24 定记作已知限）。
						//     回退：LEGION_F09_TTL_OFF=1 回到原式
						process.env.LEGION_F09_TTL_OFF === "1"
							? "(lr.id IS NOT NULL OR (COALESCE(m.source, '') LIKE 'auto:%' AND m.stale_state = 'ttl') OR (m.type = 'fact' AND COALESCE(m.stale_state, '') IN ('', 'ttl') AND m.valid_to = date(substr(m.ts, 1, 10), '+30 days')))"
							: "((lr.id IS NOT NULL AND NOT (COALESCE(m.source, '') NOT LIKE 'auto:%' AND COALESCE(m.stale_state, '') = 'ttl')) OR (COALESCE(m.source, '') LIKE 'auto:%' AND m.stale_state = 'ttl') OR (m.type = 'fact' AND COALESCE(m.stale_state, '') = '' AND m.valid_to = date(substr(m.ts, 1, 10), '+30 days')))",
					];
					const rowsR = db
						.prepare(
							"SELECT m.id, m.ts, m.title, m.content, m.essential, m.validated_count, m.last_hit_at, m.valid_to, lr.n AS lr_n, lr.hits AS lr_hits FROM memories m LEFT JOIN life_renew lr ON lr.id = m.id WHERE m.status = 'active' AND m.valid_to IS NOT NULL AND substr(m.valid_to, 1, 10) >= ? AND m.valid_to <= ?" + // 盲审处置：带 ts（lifeProtect 跨日判）与 life_renew 的 n、hits（续期帽）
								exclR.map((c) => " AND " + c).join("") +
								" ORDER BY m.valid_to, m.id LIMIT 2001",
						)
						.all(bjR(loMs).slice(0, 10), bjR(nowR + 36 * 3600e3));
					const capHitR = rowsR.length > 2000;
					if (capHitR) rowsR.length = 2000;
					const updR = db.prepare("UPDATE memories SET valid_to = ? WHERE id = ? AND status = 'active'");
					const logR = db.prepare(
						"INSERT INTO life_renew (id, n, first_at, last_at, old_valid_to, reason, hits) VALUES (?, 1, ?, ?, ?, ?, 0) ON CONFLICT(id) DO UPDATE SET n = n + 1, last_at = excluded.last_at, hits = 0",
					);
					const stampR = nowIso();
					const todayR = bjR(nowR).slice(0, 10);
					let renewN = 0;
					let cappedN = 0;
					const byWhy = {};
					for (const r of rowsR) {
						const why0 = lp(r, nowR);
						if (!why0) continue;
						let why = why0;
						if (why0 !== "ess" && (Number(r.lr_n) || 0) >= LIFE_RENEW_CAP) {
							// 盲审处置（should）：续满帽后，再续只凭上次续期后的真命中
							if ((Number(r.lr_hits) || 0) > 0) why = "hit";
							else {
								cappedN++;
								continue;
							}
						}
						const v0 = String(r.valid_to);
						let v1;
						if (v0.length === 10) {
							const baseD = v0 > todayR ? v0 : todayR;
							v1 = new Date(Date.parse(baseD + "T00:00:00Z") + 30 * 86400e3).toISOString().slice(0, 10);
						} else {
							const t0 = Date.parse(v0.replace(" ", "T"));
							v1 = bjR(Math.max(Number.isFinite(t0) ? t0 : nowR, nowR) + 30 * 86400e3);
						}
						updR.run(v1, r.id);
						logR.run(r.id, stampR, stampR, v0, why);
						byWhy[why] = (byWhy[why] || 0) + 1;
						renewN++;
					}
					if (renewN > 0) {
						db.prepare(
							"INSERT INTO organ_meta (k, v) VALUES ('life_renew_total', ?) ON CONFLICT(k) DO UPDATE SET v = CAST(CAST(v AS INTEGER) + CAST(excluded.v AS INTEGER) AS TEXT)",
						).run(String(renewN));
					}
					if (txR) {
						db.exec("COMMIT");
						txR = false;
					}
					// COMMIT 之后才记 stats 与日志
					if (!stats.lifeRenew || typeof stats.lifeRenew !== "object") stats.lifeRenew = {};
					for (const [k, n] of Object.entries(byWhy)) stats.lifeRenew[k] = (stats.lifeRenew[k] || 0) + n;
					if (capHitR) stats.lifeRenewCapHit = (stats.lifeRenewCapHit || 0) + 1;
					if (cappedN > 0) stats.lifeRenewCapped = (stats.lifeRenewCapped || 0) + cappedN;
					stats.lifeRenewLast = renewN;
					// 补笔③（10-18·改定处置表 M3）：汇总行改走 warn（生产日志捕获只收 warn、error，原 info 行看不到），续 0 条也打——验收看得出续期段跑过；
					//   节流：每个北京日至多一行（进程全局键，模块热载不重置；nightly patrol本就每夜一次，防强制nightly patrol、热载重跑刷屏）。
					//   行内带当夜续期数（按理由）、续期帽挡下的条数、超每夜帽与否、本进程续期出错累计次数
					if (globalThis.__legionLifeRenewWarnDay !== todayR) {
						globalThis.__legionLifeRenewWarnDay = todayR;
						ctx.logger?.warn?.(
							`[living-memory] wave 续期 ${renewN} 条（${Object.entries(byWhy).map(([k, n]) => k + " " + n).join("、") || "无"}）；封顶不续 ${cappedN} 条；超每夜帽 ${capHitR ? "是" : "否"}；出错累计 ${stats.lifeRenewErrors || 0} 次（life_renew）`,
						);
					}
				} catch (eLR) {
					if (txR) {
						try {
							db.exec("ROLLBACK");
						} catch {}
					}
					stats.lifeRenewErrors = (stats.lifeRenewErrors || 0) + 1;
					ctx.logger?.warn?.(`[living-memory] wave 续期失败（出错累计 ${stats.lifeRenewErrors} 次，整段已回滚、当夜不续）: ${String(eLR).slice(0, 60)}`); // 补笔③：带出错累计数
				}
			}

			// ── wave 复活口·阶段 3（10-18·草纲下篇一改点 3、4）：nightly patrol复活段——紧贴车二-8 续期之后、之前，自带 try／catch ──
			//    消费 revive_pending（阶段 2 记的候选），按 first_at、id 先来先办，每夜至多 REVIVE_NIGHT_CAP 条（超帽的留到次夜，记 reviveCapHit）。
			//    逐条复核（触发到夜里状态可能已变）：条目还在、仍是 aged、不是待办（待办另有 14 天老化）；不复活被取代条（stale_state 为 superseded-auto、retired，
			//    superseded_by 非空，或是 superseded_by 活边的 src）、被 solved_by／patches 活边指向的 dst、duplicate_of 活边的 src、状态句标题（CRIT.lifeStateTitle）、
			//    nightly patrol哨条（来源 patrol*、三类哨标题，同续期段）。「活边」按该条转 aged 那一刻算（见取边处注）。不合的跳过、按理由计数；跳过与复活的候选都从 revive_pending 删去（再被触发会再记）。
			//    复活：status 回 active、closed_at 清空（stale_state 照旧——到新期限 照常再转，除非续期段续上）；valid_to＝max(今天, 原期)＋30 天，
			//    形态随原值（10 字写日期、长的写北京 ISO；原值空或认不出时写 10 字今天＋30 天）；life_renew 记 reason='revive'（已有行 n+1、last_at 刷新、
			//    reason 改记 revive、hits 清零，同续期段受续期帽管）；life_revive 记一行原值（数据面复原依据）；organ_meta life_revive_total 累计。
			//    整段一个事务：未在事务中开 BEGIN IMMEDIATE，已在事务中用 SAVEPOINT；出错整段回滚、计 reviveErrors、当夜不复活。COMMIT 之后才记 stats 与日志；
			//    汇总行走 warn，每北京日一行（复活 0 条、没有候选表也打——验收看得出本段跑过）。补嵌不在本段（见落码记录：复活条的向量块等nightly patrol补嵌选条）。
			//    LEGION_REVIVE_OFF=1 时整段不跑
			if (core.reviveOn()) {
				let txV = null;
				try {
					const nowV = Date.now();
					const bjV = (ms) => {
						const d = new Date(ms + 8 * 3600e3);
						const p2 = (n) => String(n).padStart(2, "0");
						return `${d.getUTCFullYear()}-${p2(d.getUTCMonth() + 1)}-${p2(d.getUTCDate())}T${p2(d.getUTCHours())}:${p2(d.getUTCMinutes())}+08:00`;
					};
					const todayV = bjV(nowV).slice(0, 10);
					const plus30 = (d10) => new Date(Date.parse(d10 + "T00:00:00Z") + 30 * 86400e3).toISOString().slice(0, 10);
					const newValidTo = (v0) => {
						const s = v0 == null ? "" : String(v0);
						if (/^\d{4}-\d{2}-\d{2}$/.test(s) && Number.isFinite(Date.parse(s + "T00:00:00Z"))) return plus30(s > todayV ? s : todayV);
						const t0 = s ? Date.parse(s.replace(" ", "T")) : NaN;
						if (s.length > 10 && Number.isFinite(t0)) return bjV(Math.max(t0, nowV) + 30 * 86400e3);
						return plus30(todayV);
					};
					const stampV = nowIso();
					let revivedN = 0;
					let capHitV = false;
					let leftV = 0;
					const skipV = {};
					const byWhyV = {};
					const hasPend = !!db.prepare("SELECT 1 AS x FROM sqlite_master WHERE type = 'table' AND name = 'revive_pending'").get();
					if (hasPend) {
						if (!db.isTransaction) {
							db.exec("BEGIN IMMEDIATE");
							txV = "tx";
						} else {
							db.exec("SAVEPOINT c3_revive");
							txV = "sp";
						}
						const pend = db.prepare("SELECT id, why FROM revive_pending ORDER BY first_at, id LIMIT ?").all(REVIVE_NIGHT_CAP + 1);
						capHitV = pend.length > REVIVE_NIGHT_CAP;
						if (capHitV) pend.length = REVIVE_NIGHT_CAP;
						let supByColV = false;
						try {
							supByColV = db.prepare("PRAGMA table_info(memories)").all().some((c) => c && c.name === "superseded_by");
						} catch {}
						const getV = db.prepare("SELECT id, type, title, source, status, stale_state, valid_to, closed_at" + (supByColV ? ", superseded_by" : "") + " FROM memories WHERE id = ?");
						// 「活边」按转 aged 那一刻算：本文件前段「死边软失效」每夜把端点非 active 的边置 invalid_at（solved_by 只看 src），aged 条的这几型边转 aged 后次夜就失效了，
						//   只看 invalid_at IS NULL 等于不排。故认 invalid_at 为空、或不早于该条 closed_at（转 aged 时盖的墓碑时刻，两者同为 nowIso 形态）的边；closed_at 为空时凡有边都认（取保守）
						const liveAt = " AND (invalid_at IS NULL OR invalid_at = '' OR ? IS NULL OR invalid_at >= ?)";
						const inV = db.prepare("SELECT 1 AS x FROM memories_edges WHERE dst = ? AND edge_type IN ('solved_by', 'patches')" + liveAt + " LIMIT 1");
						const outV = db.prepare("SELECT edge_type FROM memories_edges WHERE src = ? AND edge_type IN ('duplicate_of', 'superseded_by')" + liveAt + " ORDER BY edge_type LIMIT 1");
						const delV = db.prepare("DELETE FROM revive_pending WHERE id = ?");
						let updV = null;
						let lrGetV = null;
						let lrUpV = null;
						let logV = null;
						for (const p of pend) {
							const m = getV.get(p.id);
							let skip = null;
							if (!m) skip = "gone";
							else if (m.status !== "aged") skip = "notAged";
							else if (m.type === "todo") skip = "todo";
							else if (m.stale_state === "superseded-auto" || m.stale_state === "retired") skip = "superseded";
							else if (supByColV && m.superseded_by != null) skip = "superseded";
							else if (inV.get(m.id, m.closed_at ?? null, m.closed_at ?? null)) skip = "patchedDst";
							else {
								const eo = outV.get(m.id, m.closed_at ?? null, m.closed_at ?? null);
								if (eo) skip = eo.edge_type === "duplicate_of" ? "duplicateSrc" : "superseded";
							}
							if (!skip && CRIT.lifeStateTitle && CRIT.lifeStateTitle.test(String(m.title || ""))) skip = "stateTitle";
							if (!skip && (String(m.source || "").startsWith("patrol") || /^(?:sentry警报|handover note警报|记忆链路light sentry警报)：/.test(String(m.title || "")))) skip = "sentinel";
							if (skip) {
								skipV[skip] = (skipV[skip] || 0) + 1;
								delV.run(p.id);
								continue;
							}
							if (!updV) {
								ensureLifeRenew(db);
								ensureLifeRevive(db);
								updV = db.prepare("UPDATE memories SET status = 'active', closed_at = NULL, valid_to = ? WHERE id = ? AND status = 'aged'");
								lrGetV = db.prepare("SELECT n, reason, last_at, hits FROM life_renew WHERE id = ?");
								lrUpV = db.prepare(
									"INSERT INTO life_renew (id, n, first_at, last_at, old_valid_to, reason, hits) VALUES (?, 1, ?, ?, ?, 'revive', 0) ON CONFLICT(id) DO UPDATE SET n = n + 1, last_at = excluded.last_at, reason = 'revive', hits = 0",
								);
								logV = db.prepare(
									"INSERT INTO life_revive (id, at, why, old_status, old_closed_at, old_valid_to, new_valid_to, lr_had, lr_n, lr_reason, lr_last_at, lr_hits) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
								);
							}
							const v0 = m.valid_to == null ? null : String(m.valid_to);
							const v1 = newValidTo(v0);
							const lr0 = lrGetV.get(m.id);
							updV.run(v1, m.id);
							lrUpV.run(m.id, stampV, stampV, v0);
							logV.run(m.id, stampV, String(p.why || ""), m.status, m.closed_at == null ? null : String(m.closed_at), v0, v1, lr0 ? 1 : 0, lr0 ? lr0.n : null, lr0 ? lr0.reason : null, lr0 ? lr0.last_at : null, lr0 ? lr0.hits : null);
							delV.run(p.id);
							for (const w of String(p.why || "").split(",").filter(Boolean)) byWhyV[w] = (byWhyV[w] || 0) + 1;
							revivedN++;
						}
						if (revivedN > 0) {
							db.prepare(
								"INSERT INTO organ_meta (k, v) VALUES ('life_revive_total', ?) ON CONFLICT(k) DO UPDATE SET v = CAST(CAST(v AS INTEGER) + CAST(excluded.v AS INTEGER) AS TEXT)",
							).run(String(revivedN));
						}
						leftV = Number(db.prepare("SELECT COUNT(*) AS c FROM revive_pending").get().c) || 0;
						if (txV === "tx") db.exec("COMMIT");
						else db.exec("RELEASE c3_revive");
						txV = null;
					}
					// COMMIT 之后才记 stats 与日志
					stats.reviveLast = revivedN;
					stats.reviveRevived = (stats.reviveRevived || 0) + revivedN;
					if (!stats.reviveByWhy || typeof stats.reviveByWhy !== "object") stats.reviveByWhy = {};
					for (const [k, n] of Object.entries(byWhyV)) stats.reviveByWhy[k] = (stats.reviveByWhy[k] || 0) + n;
					if (!stats.reviveSkip || typeof stats.reviveSkip !== "object") stats.reviveSkip = {};
					for (const [k, n] of Object.entries(skipV)) stats.reviveSkip[k] = (stats.reviveSkip[k] || 0) + n;
					if (capHitV) stats.reviveCapHit = (stats.reviveCapHit || 0) + 1;
					if (globalThis.__legionReviveWarnDay !== todayV) {
						globalThis.__legionReviveWarnDay = todayV;
						ctx.logger?.warn?.(
							`[living-memory] wave 复活 ${revivedN} 条（触发 ${Object.entries(byWhyV).map(([k, n]) => k + " " + n).join("、") || "无"}，一条可占多类）；跳过 ${Object.entries(skipV).map(([k, n]) => k + " " + n).join("、") || "无"}；候选余 ${leftV} 条；超每夜帽 ${capHitV ? "是" : "否"}；出错累计 ${stats.reviveErrors || 0} 次（life_revive）`,
						);
					}
				} catch (eRV) {
					if (txV === "tx") {
						try {
							db.exec("ROLLBACK");
						} catch {}
					} else if (txV === "sp") {
						try {
							db.exec("ROLLBACK TO c3_revive");
							db.exec("RELEASE c3_revive");
						} catch {}
					}
					stats.reviveErrors = (stats.reviveErrors || 0) + 1;
					ctx.logger?.warn?.(`[living-memory] wave 复活失败（出错累计 ${stats.reviveErrors} 次，整段已回滚、当夜不复活）: ${String(eRV).slice(0, 60)}`);
				}
			}

			// ── 在库 TTL 到点退出（step·2026-08-30 goal 五连刀·set_state_expiry 意）──
			//    声明过寿命的条（stale_state='ttl' 预写）valid_to 到点→nightly patrol批量转 aged（closed_at 墓碑·退出检索）。
			//    **只动 ttl 标条**——显式 valid_to 补记条（无标）不转 aged（失效≠删除·P2 精神）。
			//    wave 8-g 订正（按开关两态）：LIFE_GUARD 开：过期无标条经 search 补充池可见（×0.2·带 ⏦），本段转出的 aged 条也可见（×0.1）；
			//    关（LEGION_LIFE_GUARD_OFF=1）：两类都被 search 硬排、不可见。注入面与向量路两态都硬排。
			let ttlAgedCount = 0; // 8-31 锈面修⑤：本巡 TTL 到点退出条数（light sentry合法流转豁免面）
			try {
				// B级④（全链再审丙-⚠C·存量自愈）：light sentry 7d TTL 只治增量——存量 11 条 active 哨条 stale_state=NULL
				//   永不被 TTL 段 aged（判据 stale_state='ttl'）——本句自愈：存量哨条补标 ttl＋7d valid_to（幂等·新条已带不命中）。
				//   漏点即修②（八维复查 W2）：范围扩三哨——sentry（title 前缀）与handover note存量同病（库面 id 10449 sentry条 14 天永活铁证）。
				const healHit = db.prepare(
					"UPDATE memories SET stale_state='ttl', valid_to = COALESCE(valid_to, ?) WHERE status='active' AND stale_state IS NULL AND (source LIKE 'patrol-sentinel:%' OR title LIKE 'sentry警报：%' OR title LIKE 'handover note警报：%')",
				).run(isoAhead(7 * 86400e3));
				stats.sentryTtlHeal = (stats.sentryTtlHeal || 0) + (Number(healHit.changes) || 0); // 漏点即修⑤（W5）：自愈独立计数——原与 TTL 主段同文本失败不可辨
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
				//    ── design-approved「继续」·执行总账⑦哨项·实勘驱动）──
				//    实勘（30 条 DISTINCT 逐一抽查）：该面多数为**边语义误用**而非真僵尸——①「转办承接」型
				//    （#15989←#16186「前提已变·由新条承接」）②「汇总去重」型（#12489←#13118「补销汇总条·去重」）
				//    ③「销报警自身」型（←//「sentry report警销号」）——3 类均非「已办未销」。
				//    ⇒ 加固两手（**仍只报不改**·不盲销）：①SQL 加 DISTINCT（同一目标多边时不再重复占位：
				//    #14242×5 类重复占位使可视条数虚降——** 共 4 条** solved_by 边（src 4944/5979/5980/5981·
				//    D 路审计 D-真4 亲验：21:49 已全撤·现有效 0）；原写「现为 3 条」系本车修注释时新立错锚·据实改口）
				//    ②文案如实标注「疑似·边语义待逐条核」。
				//    ── 2026-09-20 审计 D-3 修（A 路码面审计·中）──
				//    原 `LIMIT 20` 系**隐截断且被当权威读数**：本路 `solvedLeak` 恒 ≤20 ⇒ 字面不可能产生旧注释所称「原 42 ⇒ 现 30」
				//    （该读数系混引他路）。真库 as_of 重建（21:13 提交时刻回算）：**原始 50 行／DISTINCT 31** ⇒ 20 帽实为截断。
				//    修：①`LIMIT` 提到 100（≥ 真值·留余量）②达帽时文案显式标「已达帽」③旧「42⇒30」锚句已删（禁留错锚）。
				//    ── design-approved「按建议 甲」·专项 #16842 全量分类毕·**零行为改动**）──
				//    本哨判据**只覆盖「目标位＝todo」面**（连接条件落在 todo 表）⇒ **对「落差面」（dst＝fact/decision/lesson）天然失明——
				//    属设计性，非缺陷**（`solved_by` 的**生产消费面四处**：①此处按 dst=todo 反查 closureCheck ②常驻核心席入度排除
//    ③顶注选条入度排除 ④cross-space lesson 复犯统计面——G 路审计 订正：原写「只有两处」系单侧枚举，已改为全枚举·结论不变）。
				//    落差面实态（09-21 00:00:33 全量读数 789 条＝／B12／C727）：老约定＝「**收官条链回本次作战所涉各条**」
				//    （目标／依据／前序／锚点一并挂），与收窄后「只链 todo」不同源 ⇒ **存量判为历史语义层·声明不回溯**（先例 #16651）；
				//    09-20 之后新边一律只链 todo（新边 100% 落 dst=todo 已实证）。依据件＝a companion script
				//    （789条·三类判＋处置三选项）.md（含独立盲核 20 条·收敛：B 须「instruction 自陈补边＋valid_at 脚本形态」双证）。
				let solvedLeak = 0;
				try {
					const leaks = db
						.prepare(
							`SELECT DISTINCT t.id AS tid FROM memories t JOIN memories_edges e
						 ON e.dst = t.id AND e.edge_type = 'solved_by'
						 WHERE t.status = 'active' AND t.type = 'todo' AND e.invalid_at IS NULL
						 LIMIT 100`,
						)
						.all();
					solvedLeak = leaks.length;
					if (solvedLeak > 0)
						alarms.push(
							// 文案帽化：前缀由 66 字压到 ~34 字（原长前缀把 80 字 title 帽里的 id 列挤没·审计 N4），id 列限 120 字
							"办结漏戳疑似 todo " +
								solvedLeak +
								(solvedLeak >= 100 ? "（已达帽 100·真值更大）" : "") +
								" 条（待逐条核·含转办/汇总/销哨三类误建）: " +
								leaks
									.map((r) => "#" + r.tid)
									.join(" ")
									.slice(0, 120),
						);
				} catch {
					alarms.push("办结状态位哨查询失败");
				}
				// ── ⑩E 任务账快照缺失哨（step E·治「压缩后无账可读」且缺失可查·验收线③）──
				//    判据：最近一条「压缩总结」条是否配对「本窗任务账快照」条（标题键 <sid8>·<compactionId8>）。
				//    注：⑩C 上线前的历史压缩本无快照 ⇒ 文案显式标注「属预期·下次压缩起自愈」（不立假红）。
				try {
					const _lastSum = db
						.prepare(
							"SELECT title FROM memories WHERE title LIKE '压缩总结：%' ORDER BY id DESC LIMIT 1",
						)
						.get();
					if (_lastSum) {
						const _keyE = String(_lastSum.title)
							.replace(/^压缩总结：/, "")
							.trim();
						const _hasE = db
							.prepare("SELECT 1 AS x FROM memories WHERE title = ? LIMIT 1")
							.get("本窗任务账快照：" + _keyE);
						if (!_hasE)
							alarms.push(
								"压缩任务账快照缺失（最近压缩 " +
									_keyE +
									" 无配对快照·⑩C 前历史压缩属预期·下次压缩起自愈）",
							);
					}
				} catch {
					alarms.push("任务账快照缺失哨查询失败");
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
						// 车二件3（治 P4 ⚠-4/J4-4·连续四夜每夜新立实证）：①同源 24h 查重（sentry dupSoul 同法·防同夜双立）
						//   ②7d TTL（handover note警报同式·哨条到点自动 aged 退出——原永活积累·36 条实证）③被抑计数可观测。
						const dupSent =
							db
								.prepare(
									"SELECT COUNT(*) c FROM memories WHERE source = ? AND ts >= ? AND status = 'active'",
								)
								.get("patrol-sentinel:" + dayKey, nowIso24hAgo()).c;
						if (dupSent === 0) {
							// [拆⑦·2026-10-04] 闸归一：哨条 INSERT 前过安检闸
							const _g7 = _pipelineGate(title, content);
							if (!_g7.pass) _patrolGateSkip(_g7, "light sentry警报");
							else
							db.prepare(
								"INSERT INTO memories (ts, type, title, content, space, source, checksum, stale_state, stale_at, valid_to) VALUES (?, ?, ?, ?, ?, ?, ?, 'ttl', ?, ?)",
							).run(
								nowIso(),
								"todo",
								title,
								content,
								"memory-organ",
								"patrol-sentinel:" + dayKey,
								sha1(title + content),
								nowIso(),
								isoAhead(7 * 86400e3),
							);
						} else {
							stats.sentryAlarmSuppressed =
								(stats.sentryAlarmSuppressed || 0) + 1;
						}
					} catch (eSent) {
						// 漏点即修④（八维复查 W4·哨的哨）：light sentry INSERT 段原 catch{} 空吞——车二件3 新增三逻辑未同车补出口；
						//   同族三处（sentry L10155/handover note/TTL）皆有出口——本处补 stats+warn（照sentry范式）。
						stats.sentryWriteErrors = (stats.sentryWriteErrors || 0) + 1;
						try {
							ctx.logger?.warn?.(
								"[living-memory] sentinel-alert write failed (#" +
									stats.sentryWriteErrors +
									"): " +
									String(eSent).slice(0, 60),
							);
						} catch {}
					}
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
					try {
						// 批⑲ P2 写回：跨进程日戳（读-判-写序·写完即他进程可见）
						db.prepare(
							"INSERT OR REPLACE INTO organ_meta (k, v) VALUES ('last_patrol_day', ?)",
						).run(String(dayKey));
					} catch {}
				})();
			}
			let plMainWriteOk = true; // 车三件③(design note)：主写成败标志——catch 置 false·末段补写据此落 __ledgerFail（治「主写失败×末段补写＝上巡主字段＋当巡观测键」静默混装）
			// 车三件④(design note)：vec_scan_skip 键消费点——原全仓唯一写点、零 SELECT（「nightly patrol可判」名不副实·补审 B-07）。nightly patrol读键一行：n>0 报持续降级态（与检索路径 60s 节流 warn 互补·nightly patrol一次性汇总）。
			try {
				const _vsRow = db
					.prepare("SELECT v FROM organ_meta WHERE k='vec_scan_skip'")
					.get();
				if (_vsRow && _vsRow.v) {
					const _vs = JSON.parse(_vsRow.v);
					if (_vs.n > 0)
						console.warn(
							"[living-memory] vec budget-skip 持续态（nightly patrol读键）: n=" +
								_vs.n +
								" chunks=" +
								_vs.chunks +
								" cap=" +
								_vs.cap +
								" at=" +
								_vs.at +
								" ——语义召回降级中（车三①早退前移已减白付·治本议库减脂/帽调）",
						);
				}
			} catch {}
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
				// ── 注入面危险组哨（design-approved三件套③）──
				//    判据＝active 条里是否仍有「完整连续花括号组」——宿主模板解析命中即**整轮崩**；本车已在
				//    注入出口（promptSafe）与写入入口（memory_write／runExtract）双落净化，本哨是**兜底可见面**：
				//    粗筛走 SQL LIKE、精判按宿主同族正则在 JS 侧做；★只报告不改库（可见流转·不静默改数据）。
				try {
					const unsafeCand = db
						.prepare(
							"SELECT title, content, COALESCE(spoken_prefix,'') sp FROM memories WHERE status='active' AND (title LIKE '%{{%' OR content LIKE '%{{%' OR COALESCE(spoken_prefix,'') LIKE '%{{%')",
						)
						.all();
					let unsafeN = 0;
					for (const r of unsafeCand)
						if (
							hasUnsafePromptRef(r.title) ||
							hasUnsafePromptRef(r.content) ||
							hasUnsafePromptRef(r.sp) // 审计处置车（09-13·D1b）：扩 spoken_prefix 面——原只扫 title/content，与实测残余面（spoken_prefix）恰好错开
						)
							unsafeN++;
					stats.promptUnsafe = unsafeN;
					if (unsafeN > 0)
						ctx.logger?.warn?.(
							"[living-memory] prompt-unsafe refs: " +
								unsafeN +
								" active rows（有路绕闸写入·注入出口仍兜得住）",
						);
				} catch {}
									// ── 机检三哨（分流判据source of record §八·五件一item4·approved2026-09-13）：①有边率 ②随记入库率 ③open item ──
					let edgeCoveragePct = null, noteFilesToday = 0, autoNotesToday = 0; // 修车-D5：null=未测（首巡前）
					try {
						// 修车-D2（冻结后修订）：src-only 改 src∪dst＋滤失效边（invalid_at）＋滤端点非 active——原口径漏 dst-only·混失效/非活端点（真库基准 src-only 51.6 vs src∪dst 61.3）
						const ec = db.prepare("SELECT COUNT(DISTINCT u.sid) e, (SELECT COUNT(*) FROM memories WHERE status='active') a FROM (SELECT src sid FROM memories_edges WHERE invalid_at IS NULL OR invalid_at='' UNION SELECT dst sid FROM memories_edges WHERE invalid_at IS NULL OR invalid_at='') u JOIN memories m ON m.id=u.sid AND m.status='active'").get();
						// 修车-D5：仅计算成功才赋值＋edgeCoverageMeasured 标志（首巡前 null=未测·原伪 0 与真 0 不可辨）
						if (ec && ec.a) { edgeCoveragePct = Math.round(1000 * ec.e / ec.a) / 10; stats.edgeCoverageMeasured = true; }
						// 修车-D1（a）：当日窗基准改 nowIso 系（UTC+8 伪 ISO·与库内 ts 同系）——原真 UTC 与 ts 错位 18h（nightly patrol本地 02:00 时两窗重叠仅 18h）
						const todayStr = nowIso().slice(0, 10);
						const inbDir = path.join(os.homedir(), ".dsh", "dsh-living-memory", "modules", "memory organ", "inbox");
						// 修车-D1（b）：分子 mtime 同步换算 UTC+8 取日期（与 todayStr/ts 同系·双侧对称）
						noteFilesToday = fs2.readdirSync(inbDir).filter((f) => f.endsWith(".md") && new Date(fs2.statSync(path.join(inbDir, f)).mtimeMs + 8 * 3600e3).toISOString().slice(0, 10) === todayStr).length;
						autoNotesToday = Number(db.prepare("SELECT COUNT(*) c FROM memories WHERE source LIKE 'auto%' AND ts >= ?").get(todayStr).c) || 0;
					} catch (eSentry) { stats.sentryErrors = (stats.sentryErrors || 0) + 1; }
					stats.edgeCoveragePct = edgeCoveragePct; stats.noteIngestFiles = noteFilesToday; stats.noteIngestAuto = autoNotesToday; // 三哨值经 stats 中转（透出点跨作用域不可直引局部——首版踩坑）
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
						// P0-4（09-12）超帽降采样/轮转观测位：`a10SkippedDueToCap>0` ⇒ 本巡为**采样态**（非全池两两）
						a10PoolSize: stats.a10PoolSize || 0,
						a10Sampled: stats.a10Sampled || 0,
						a10SkippedDueToCap: stats.a10SkippedDueToCap || 0,
						a10RotSlot: stats.a10RotSlot || 0,
						// E-4（09-12 批 A）：evo-mirror 运行水线——空转(0/0)与已死(不执行)由此可辨（原仅条件日志）
						evoIns: stats.evoMirrorIns || 0,
						evoSkip: stats.evoMirrorSkip || 0,
						promptUnsafe: stats.promptUnsafe || 0, // 注入面危险组哨（09-13 净化车）：active 条里能被宿主解析成模板的「完整花括号组」残留数·★0=干净
						// E-5（09-12 批 A）：stale 池容量预警（当日实值 ＋ 未来 7/14/30 天越线预测）
						stalePoolNow: stats.stalePoolNow || 0,
						stalePoolT7: stats.stalePoolT7 || 0,
						stalePoolT14: stats.stalePoolT14 || 0,
						stalePoolT30: stats.stalePoolT30 || 0,
						// E 主车（09-12 maintainer·E 洪峰容量化）：池水位/出海口吞吐/带宽——落账面（重启不丢·与 stats 同源）
						staleReviewPoolSize: stats.staleReviewPoolSize || 0,
						staleAutoRetired: stats.staleAutoRetired || 0,
						staleSinkSent: stats.staleSinkSent || 0,
						staleSinkBandwidth: stats.staleSinkBandwidth || 0,
						staleSinkErrors: stats.staleSinkErrors || 0, // 09-12 审计判据项④：错误出口入主写（与末段补写同）
						// 机检三哨（分流判据source of record §八·五件一item4·maintainer 2026-09-13）
						edgeCoverage: edgeCoveragePct, // 哨①有边率（有条/活跃条·目标≥80%·基线44.9@09-12）——车三补边进度标尺
						cooccurBackfill: { pairs: cooccurPairs || 0, weakMirrored: mirrorErr > 0 ? null : (mirrorWeak || 0), stalePurged: mirrorErr > 0 ? null : (stalePurged || 0), mirrorErr: mirrorErr || 0, full: process.env.LEGION_COOCCUR_FULLREBUILD === "1", simMin: Number(process.env.LEGION_COOCCUR_SIM) || 0.1 }, // 车三观测键（审计 rev4-D2 修：归位 patrol_last 主账本——原误落 vec_backfill_last；C 档毕后须手动移除 env〔无自停〕）＋补件②③＋补审 E-1/P3 修：**mirrorErr>0 夜 weakMirrored/stalePurged 置 null**（镜像事务失败⇒两计数残留脏值不可信——null=自动可辨识·免读数者按源码注释猜）
						noteIngestFiles: noteFilesToday, // 哨②随记入库率·分子：当日inbox交接件数
						noteIngestAuto: autoNotesToday, // 哨②·分母侧：当日 auto 提炼条数（防表面合规·密度分化142×防）
						// 哨③死常识open item：tool_usage 无 entry 级命中数据（v2.22 系 per-query）——候检索单升级立哨（如实降级·不立死键）
						triplesIn: stats.triplesIn || 0,
						triplesTotal: stats.triplesTotal || 0,
						// ⑤ 三元组停建（10-24）：停建夜记 true；S2（盲审应改）起开态（LEGION_TRIPLES_KG_ON=1 照旧投影）也记 false，
						//    供 stats 优先据此判停没停（读取进程的环境可能与nightly patrol进程不同，如记忆桥删光 LEGION_*）；本轮没走到投影段时不记
						triplesKgStopped: typeof stats.triplesKgStopped === "boolean" ? stats.triplesKgStopped : undefined,
						vecScanSkip: core.vecSkippedDueToBudget || undefined, // R路车一①nightly patrol报红行：vec 超帽降级计数入 patrol_last(design note)
						patrolLedgerErrors: stats.patrolLedgerErrors || undefined, // 车一补丁（补审 B-22·四点补齐）：主写失败计数落账——原仅进程内 stats·跨重启失明
						// 车二补丁乙（十八审乙-5🔴·骨架三件套补第三件）：五新键落 patrol_last——原仅 stats 进程单例（重启清零）·跨重启观测断链
						familySkippedDueToCap: stats.familySkippedDueToCap || undefined,
						hiSimSkippedDueToCap: stats.hiSimSkippedDueToCap || undefined,
						a19SkippedDueToCap: stats.a19SkippedDueToCap || undefined,
						soulExemptSuppressed: stats.soulExemptSuppressed || undefined,
						sentryAlarmSuppressed: stats.sentryAlarmSuppressed || undefined,
						famRotCursorErrors: stats.famRotCursorErrors || undefined, // 十九审即修①
						hiRotCursorErrors: stats.hiRotCursorErrors || undefined,
						sentryTtlHeal: stats.sentryTtlHeal || undefined, // 漏点即修⑤（W5）：自愈条数落账（跨重启可读·R15c 伴生）
						sentryWriteErrors: stats.sentryWriteErrors || undefined, // 漏点即修④：light sentry写错落账
						vecScanSkipKeyErrors: core.vecScanSkipKeyErrors || undefined, // A级四件③（全链再审乙F5）：孪生键不齐补——patrolLedgerErrors 有落账而本键无（车一补丁同批次）
						semanticEdgeSkippedDueToCap: stats.semanticEdgeSkippedDueToCap || undefined, // A级四件④（全链再审乙F2）：帽跳族唯一无落账者补齐
					}), // step：+backlogAccounts；step：+KG 三元组counter
				); // step N-c2：counter随巡持久化（跨重启对账面）
			} catch (ePL) {
				// R路车一⑤（design-approved·P4 ⚠-6 首点）：主写裸 catch 补出口——patrol_last 系全系统水位根基
				//    （水位/nightly patrol凭证对账面），失败须可辨（J4-3 三轮在案）；stats 键接 render「nightly patrol账错」。
				stats.patrolLedgerErrors = (stats.patrolLedgerErrors || 0) + 1;
				plMainWriteOk = false; // 车三件③（W7）：主写失败标志置位——末段补写据此落 __ledgerFail 标记
				try {
					console.warn(
						"[living-memory] patrol_last ledger write failed #" +
							stats.patrolLedgerErrors +
							": " +
							String(ePL).slice(0, 80),
					);
				} catch {}
				// B级⑥（全链再审甲J4·主写失败夜兜底·与成败/warn 平级）：主写失败 ⇒ patrol_last 整体未写 ⇒
				//   全部新键跨重启失明——rescue 独立键留痕（对照 E-4/E-5 末段补写先例·此处为「主写自身失败」路径）。
				try {
					db.prepare(
						"INSERT OR REPLACE INTO organ_meta (k, v) VALUES ('patrol_ledger_fail', ?)",
					).run(
						JSON.stringify({
							at: nowIso(),
							n: stats.patrolLedgerErrors,
							err: String(ePL).slice(0, 80),
							vecScanSkip: core.vecSkippedDueToBudget || 0,
							famSkip: stats.familySkippedDueToCap || 0,
							hisimSkip: stats.hiSimSkippedDueToCap || 0,
							a19Skip: stats.a19SkippedDueToCap || 0, // 十九审即修③（丙⚠2 缩面补齐）：a19/soul/sentry 三键
							soulExemptSup: stats.soulExemptSuppressed || 0,
							sentryAlarmSup: stats.sentryAlarmSuppressed || 0,
							famRotErr: stats.famRotCursorErrors || 0, // 漏点即修③（八维复查 W3·第二次缩面补齐）：游标错两键＋vecSkipKeyErr＋semEdgeSkip 四键
							hiRotErr: stats.hiRotCursorErrors || 0,
							vecSkipKeyErr: core.vecScanSkipKeyErrors || 0,
							semEdgeSkip: stats.semanticEdgeSkippedDueToCap || 0,
						}),
					);
				} catch {}
				try {
					console.warn(
						"[living-memory] patrol_last ledger RESCUE key written #" +
							stats.patrolLedgerErrors,
					);
				} catch {}
			}
			ctx.logger?.info?.(
				`[living-memory] night patrol done: merged=${merged} cooccurPairs=${cooccurPairs} agedSessions=${aged.length} agedTodos=${agedTodos} soul=${soulDrift ? "DRIFT!" : "ok"} conflicts_pending=${conflictsPending}${conflictsPending > 0 ? "（sentry report：清单见 conflicts 表·首周全量人工pending ruling）" : ""} ledger=${stats.patrolLedgerErrors ? "FAIL#" + stats.patrolLedgerErrors : "ok"} vecSkip=${core.vecSkippedDueToBudget || 0}`,
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

			// ── 在线备份：包含 WAL 中已提交的数据；成功发布后才轮转旧快照。 ──
			let snapshotSucceeded = false;
			try {
				const dest = createSnapshot(DB_PATH, SNAPSHOT_DIR, dayKey.replaceAll("-", ""), SNAPSHOT_KEEP_DAYS);
				snapshotSucceeded = true;
				ctx.logger?.info?.(`[living-memory] snapshot ok: ${dest}`);
			} catch (error) {
				const detail = String(error?.stderr || "").trim() || String(error);
				ctx.logger?.warn?.(
					`[living-memory] snapshot failed: ${error?.code ? error.code + ": " : ""}${detail.slice(-500)}`,
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
				const snapOk = snapshotSucceeded && fs2.existsSync(snapToday);
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

			// ── 刀 2 补件（2026-08-22 三裁task brief §三.2）：nightly patrol批量补嵌——白昼新增条目夜间补齐向量 ──
			//    复用 vecRecallCore 的嵌入链路但独立跑（不依赖查询触发）；失败仅 warn 不阻塞nightly patrol。
			//    （nightPatrol 非 async——IIFE 包裹异步段，不侵入函数签名）
			(async () => {
				let _bfMissing = 0,
					_bfFilled = 0,
					_bfErr = null, // 09-12 审计 D1：段级可见面（供 finally 无条件落账）
					_bfBudget = 0,
					_bfOff = false, // D3：预算 0＝显式关闭（落账带 off 标·防与「全库补齐完毕」同形假绿）
					_bfPoolA = 0,
					_bfPoolB = 0,
					_bfTake = 0,
					_bfTakeA = 0,
					_bfTakeB = 0,
					_bfFilledA = 0,
					_bfFilledB = 0,
					_bfSkipped = 0,
					_bfHashFilled = 0; // 09-12 C 项辅案：预算/双池/分批各自可见面
				try {
					core.vecBackfillRuns += 1; // 09-12 可见化：本段真跑到的凭证
					// ── C 项辅案（design-approved「立即落码」）：预算化产能 ＋ 双池配额 7:3 ──
					//    为什么（实测·非推演）：原单批 `LIMIT 200` ＋ 单池排序 ⇒ ①active 缺口 586 条须 3 巡以上；
					//    ②**陈旧族 436 条在缺向量族清空前永远排不进 200 名**（按原口径复算：候选池 200 条 100% 为缺向量族）。
					//    设计：①预算 `LEGION_VEC_BF_BUDGET`（缺省 600·钳制 0..2000·0=显式关闭，仅跳过扫描与嵌入不跳过落账）
					//    ②双池按 7:3 配额 ＋ 余量互补（某池不足让给另一池）③嵌入按 100 条/批推进（批间可中断·部分成功留账）。
					//    D5（09-12 独立审计处置）：解析**严格化**——非法/越界值回落缺省并 warn
					//    （原 `Number.parseInt` 宽容解析致 `"-5"`/`"0x10"` 静默塌 0＝关闭、`"1e3"` 静默塌 1——与作者意图相反却不报错）
					const bfBudget = (() => {
						const raw = process.env.LEGION_VEC_BF_BUDGET;
						if (raw === undefined || raw === "") return 600;
						const n = Number(raw);
						if (!Number.isInteger(n) || n < 0 || n > 2000) {
							ctx.logger?.warn?.(
								`[living-memory] LEGION_VEC_BF_BUDGET 非法值「${String(raw).slice(0, 24)}」⇒ 回落缺省 600（合法面＝整数 0..2000）`,
							);
							return 600;
						}
						return n;
					})();
					_bfBudget = bfBudget;
					_bfOff = bfBudget <= 0 || process.env.LEGION_VEC_ON !== "1"; // D3：0＝显式关闭；option车闸4：VEC_ON 缺省关 ⇒ backfill 同停（off 标落账·置 "1" 开时自愈续跑）——审计 P0 订正：_bfOff 须被下方控制流消费（if (!_bfOff)）方真拦路
					const md5txt = (t) =>
						crypto
							.createHash("md5")
							.update(String(t).slice(0, 1500))
							.digest("hex");
					const backfillHash = db.prepare(
						"UPDATE memories_vec SET content_hash=? WHERE id=? AND (content_hash IS NULL OR content_hash='')",
					);
					// ① 零 API：hash 回填（全库口径·原只覆盖候选 200 条）——**移出预算闸**（零 API 零风险·D5 审计处置）
					//    独立审计**订正原注释因果**（原写「池 B 判定依赖它先完成，否则 hash 空行被误判陈旧」＝说反）：
					//    池 B 并不依赖它——池 B 的 `r.vh &&` 已短路排除 hash 空行；它的真实效果＝按**当前内容**盖戳
					//    ⇒ 这些行此后不再进池 B（其旧向量若对应旧内容＝永久隐身·与 hash-backfill.cjs 同款**已知取舍**·记录不修）
					for (const r of db
						.prepare(
							`SELECT m.id AS id, m.title || ' ' || m.content AS txt FROM memories m JOIN memories_vec v ON v.id = m.id AND v.model_version = ? WHERE m.status='active' AND (v.content_hash IS NULL OR v.content_hash='')`,
						)
						.all(VEC_MODEL)) {
						backfillHash.run(md5txt(r.txt), r.id);
						_bfHashFilled += 1;
					}
					if (_bfHashFilled > 0)
						ctx.logger?.info?.(
							`[living-memory] vec hash backfill: ${_bfHashFilled} rows (zero-API)`,
						);
					if (bfBudget > 0 && !_bfOff) { // 审计 P0 修：_bfOff（预算0 ∨ VEC_ON 关）为**真闸**——原仅 bfBudget>0 消费·VEC_ON 关态仍真调嵌入 API（/tmp/audit-rev2 实证 stub=2）
						// ② 池 A：缺向量（ORDER BY id DESC＝新条优先·原 v2「防新条目饿死」本意保留）
						const poolA = db
							.prepare(
								`SELECT m.id AS id, m.title || ' ' || m.content AS txt FROM memories m LEFT JOIN memories_vec v ON v.id = m.id AND v.model_version = ? WHERE m.status='active' AND v.id IS NULL ORDER BY m.id DESC`,
							)
							.all(VEC_MODEL);
						// ③ 池 B：有向量但内容已变（hash 不符）——md5 无 SQL 内建 ⇒ JS 侧全量扫描（active 6k 条·百 ms 级·每巡一次）
						//    ⚠ 判据与写入端**同函数同口径**（md5txt·String().slice(0,1500)）⇒ 重嵌后必收敛，不会每巡重复补
						//    ⚠ 规模阈值（诚实记账）：active **5 万条以上**时本全量扫描须改「分段轮转」（现口径每巡一次·内存峰值≈库文本量级）
						const staleB = db
							.prepare(
								`SELECT m.id AS id, m.title || ' ' || m.content AS txt, v.content_hash AS vh FROM memories m JOIN memories_vec v ON v.id = m.id AND v.model_version = ? WHERE m.status='active'`,
							)
							.all(VEC_MODEL)
							.filter((r) => r.vh && r.vh !== md5txt(r.txt))
							.sort((a, b) => b.id - a.id);
						_bfPoolA = poolA.length;
						_bfPoolB = staleB.length;
						_bfMissing = poolA.length + staleB.length; // 本巡候选总数（missing 口径后继：双池合计）
						// ④ 配额 7:3 ＋ 余量互补（某池不足时让给另一池）
						const quotaA = Math.ceil(bfBudget * 0.7);
						let takeA = poolA.slice(0, quotaA);
						let takeB = staleB.slice(0, bfBudget - quotaA);
						let bfRemain = bfBudget - takeA.length - takeB.length;
						if (bfRemain > 0 && takeA.length < poolA.length) {
							const add = Math.min(bfRemain, poolA.length - takeA.length);
							takeA = poolA.slice(0, takeA.length + add);
							bfRemain -= add;
						}
						if (bfRemain > 0 && takeB.length < staleB.length)
							takeB = staleB.slice(
								0,
								takeB.length + Math.min(bfRemain, staleB.length - takeB.length),
							);
						const targets = [
							...takeA.map((r) => ({ id: r.id, txt: r.txt })),
							...takeB.map((r) => ({ id: r.id, txt: r.txt })),
						]; // 审计 P3③：_p 字段已死（池循环改用 pool.tag）——删除
						_bfTake = targets.length;
						_bfTakeA = takeA.length;
						_bfTakeB = takeB.length;
						if (targets.length > 0) {
							const c = await credentials.resolve("EMBEDDING_BAILIAN_KEY");
							if (c && c.value) {
								core.vecCredCache = { v: c.value, t: Date.now() };
								const ins = db.prepare(
									"INSERT OR REPLACE INTO memories_vec (id, embedding, model_version, content_hash) VALUES (?, ?, ?, ?)",
								);
								const BF_BATCH = 100; // 批大小：embedOnce 内 10 条/请求 ⇒ 10 请求/批
								// option车④·C1 分池容错（2026-09-14）：A/B 池**独立跑独立 try**——单池超时/故障不连坐另一池·失败可续（次夜接余量）；池级超时省略（embedOnce 内建每请求超时·批循环天然中断点）
								for (const pool of [
									{ list: takeA, tag: "A" },
									{ list: takeB, tag: "B" },
								]) {
									if (!pool.list.length) continue;
									try {
										for (let off = 0; off < pool.list.length; off += BF_BATCH) {
											const batch = pool.list.slice(off, off + BF_BATCH);
											const embs = await embedOnce(
												batch.map((r) => String(r.txt).slice(0, 1500)),
											);
											for (let i = 0; i < batch.length; i++) {
												const e = embs[i];
												if (!e) {
													_bfSkipped += 1; // ⑬ 出口：批次返回不足（原码 f32ToBlob(undefined) 直接抛错·更粗）
													continue;
												}
												ins.run(batch[i].id, f32ToBlob(e), VEC_MODEL, md5txt(batch[i].txt));
												if (pool.tag === "A") _bfFilledA += 1;
												else _bfFilledB += 1;
											}
										}
									} catch (poolErr) {
										// 池级隔离：记错不 rethrow——另一池续跑（09-14 夜实证：B 池 TimeoutError 连坐整段 fillB=0）
										core.vecBackfillErrors += 1;
										_bfErr = (_bfErr ? _bfErr + " ; " : "") + `[pool${pool.tag}] ${String(poolErr).slice(0, 100)}`; // 审计 P3④：追加式——双池皆败时两错并见（原后写覆盖只剩 B）
										core.vecBackfillLastErr = _bfErr;
										core.vecBackfillLastErrAt = nowIso();
										ctx.logger?.warn?.(
											`[living-memory] vec backfill pool${pool.tag} failed（另一池续·失败可续跑）: ${String(poolErr).slice(0, 80)}`,
										);
									}
								}
								ctx.logger?.info?.(
									`[living-memory] vec backfill ok: ${_bfFilledA + _bfFilledB} entries (budget=${bfBudget}·pool A/B=${takeA.length}/${takeB.length}·filled A/B=${_bfFilledA}/${_bfFilledB}·hash ${_bfHashFilled})`,
								); // D1 处置：本行只报数不作结算——`_bfFilled` 统一在 finally 结算（见下）
							} else core.vecBackfillNoCred += 1; // ⑬ 出口（09-12 补）：原 if (c && c.value) 无 else——凭据空即静默跳过
						}
					} else
						ctx.logger?.warn?.(
							`[living-memory] vec backfill OFF (${_bfOff && bfBudget <= 0 ? "LEGION_VEC_BF_BUDGET=0" : "LEGION_VEC_ON 非 1〔含 query 档——写入/补嵌只认 1·09-16 小开案〕"}） —— 本巡不扫描不嵌入（落账带 off 标）`,
						);
				} catch (error) {
					core.vecBackfillErrors += 1;
					_bfErr = String(error).slice(0, 120); // 本巡失败摘要
					core.vecBackfillLastErr = _bfErr; // 二轮审计 D1-d：最近失败跨巡保留（成功不清）
					core.vecBackfillLastErrAt = nowIso();
					ctx.logger?.warn?.(
						`[living-memory] vec backfill failed: ${String(error).slice(0, 80)}`,
					);
				} finally {
					// ── D1（09-12 独立审计·真缺陷）**结算移入 finally**：原 `_bfFilled` 只在整段循环跑完后赋值，
					//    跨批中途抛错（如第 2 批 500）时前批已入库而 `filled` 落 0 ⇒ ①`filled = filledA + filledB` 不变量破裂
					//    ②**恰答错核账分叉**：`filled=0 ∧ errorsTot>0` 会被判成「凭据/嵌入故障」，实为「已补 N 条后中断」──
					_bfFilled = _bfFilledA + _bfFilledB; // 本巡成功数（部分成功也计入）
					core.vecBackfillFilled += _bfFilled; // 累计成功补嵌条数（本巡结算一次·成功/失败路径同径）
					// ── 09-12 审计 D1 修复：**无条件落账**（原只在成功路径 ⇒ 恰在故障时无凭据，与设计意图相反；
					//    实证：force nightly patrol+断网 ⇒ runs=1/errors=1 而 vec_backfill_last 键不存在）──
					//    字段：at（本巡时刻）/runs/missing（本巡候选数·故障时也能看「本可补多少」）/filled/noCred/errors/err（失败摘要）
					try {
						db.prepare(
							"INSERT OR REPLACE INTO organ_meta (k, v) VALUES ('vec_backfill_last', ?)",
						).run(
							JSON.stringify({
								at: nowIso(), // 本巡时刻
								missing: _bfMissing, // 本巡候选总数（池 A ＋ 池 B）
								filled: _bfFilled, // 本巡成功数
								// ── C 项辅案（09-12 approved）可见面：故障/截断时也能判「是配额切了·还是产能不足·还是批次掉了」──
								budget: _bfBudget,
								off: _bfOff, // D3：`true`＝本巡被显式关闭 ⇒ **核账分叉不适用**（防「假绿位」）
								done: !_bfOff && !_bfErr && _bfMissing === 0 && _bfTake === 0, // 8727-D2/D3 终闭＋审计 rev3 低危修：!_bfErr 防扫描抛错误绿（抛错时 missing/take 留初值 0 ⇒ 原 done=true 而「全库毕」未证实——err 在场可交叉辨识但 done 单看同形）
								poolA: _bfPoolA,
								poolB: _bfPoolB,
								take: _bfTake,
								takeA: _bfTakeA,
								takeB: _bfTakeB,
								filledA: _bfFilledA,
								filledB: _bfFilledB,
								skipped: _bfSkipped,
								hashFilled: _bfHashFilled,
								err: _bfErr, // 本巡失败摘要（null=本巡无失败）
								// 二轮审计 D1-c：**累计口径显式带 Tot 后缀**（原 runs/noCred/errors 三键与本巡值同列易误读；
								// 且「进程内累计·重启清零」——读账者不得据 runsTot 判「历史共跑几巡」）
								runsTot: core.vecBackfillRuns,
								noCredTot: core.vecBackfillNoCred,
								errorsTot: core.vecBackfillErrors,
								// 二轮审计 D1-d：跨巡保留的失败史（成功不清）
								lastErr: core.vecBackfillLastErr,
								lastErrAt: core.vecBackfillLastErrAt,
							}),
						);
					} catch {
						core.vecBackfillErrors += 1; // ⑬ 出口：落账失败也要可见
					}
				}
			})();

			// ── item nightly patrol回填（design-approved）：存量条目口语前缀批量生成——每巡 200 条新条优先 ──
			//    （写时 fire-and-forget 只覆新写入；存量靠本段消化。失败跳过下巡再试——counter organ_meta
			//    spoken_prefix_remain/spoken_prefix_filled 透出，remain 不降即人工介入。DeepSeek 通道·约 0.3 元/千条量级）
			//    design-approved中型车「帽上调」：50→200（缺省·env LEGION_SPOKEN_BACKFILL_CAP 可覆盖回滚）——
			//    覆盖 42.6%（缺 ~1 万条）追平 200 夜→50 夜。
			(async () => {
				try {
					if (process.env.LEGION_SPOKEN_OFF) return;
					const spCap = envIntCap(process.env.LEGION_SPOKEN_BACKFILL_CAP, 200);
					const spMissing = db
						.prepare(
							`SELECT id, title, content FROM memories WHERE status='active' AND spoken_prefix IS NULL ORDER BY id DESC LIMIT ?`,
						)
						.all(spCap);
					// 帽接近度哨（10-01 补·库量驱动型）：本夜取出量 ≥80% 帽 ⇒ remain 未降即人工介入
					capNearWatch(
						db,
						"spoken",
						spMissing.length,
						spCap,
						"口语前缀回填本夜作业量接近帽",
					);
					if (spMissing.length > 0) {
						const c = await credentials.resolve(LLM_KEY_REF);
						if (c && c.value) {
							core.spCredCache = { v: c.value, t: Date.now() };
							const spUp = db.prepare(
								"UPDATE memories SET spoken_prefix = ? WHERE id = ?",
							);
							let spFilled = 0;
							for (const r of spMissing) {
								try {
									const sp = await llmSpokenPrefix(r.title, r.content);
									if (sp) {
										spUp.run(promptSafe(sp), r.id); // 审计处置车（09-13·D1c）：nightly patrol回填路同挂净化
										spFilled++;
									}
								} catch {}
							}
							try {
								db.prepare(
									"INSERT OR REPLACE INTO organ_meta (k, v) VALUES ('spoken_prefix_fill', ?)",
								).run(
									JSON.stringify({
										at: nowIso(),
										filled: spFilled,
										// B级③（全链再审乙F3·remain 判据失真根治）：原 remain=spMissing.length-spFilled（帽内差·恒≈0）
										//   而「remain 不降即人工介入」判据因此失效（真实缺口 13936）——改全量池口径（vec_backfill_last.missing 先例）；
										//   读失败回退帽内差（⑬ 出口）。
										remain: (() => {
											try {
												return db
													.prepare(
														"SELECT COUNT(*) c FROM memories WHERE status='active' AND spoken_prefix IS NULL",
													)
													.get().c;
											} catch {
												return spMissing.length - spFilled;
											}
										})(),
									}),
								);
							} catch (eSW) {
								stats.spokenFillWmErrors = (stats.spokenFillWmErrors || 0) + 1; // P2 fix（09-03 audit）：观测键写失败透出（原静默=透出面丢失·remain 不降判据失效）
								ctx.logger?.warn?.(
									"[living-memory] spoken_prefix_fill 观测键写失败(#" +
										stats.spokenFillWmErrors +
										"): " +
										String(eSW).slice(0, 60),
								);
							}
							ctx.logger?.info?.(
								`[living-memory] spoken_prefix backfill: ${spFilled}/${spMissing.length} this patrol`,
							);
						}
					}
				} catch (error) {
					ctx.logger?.warn?.(
						`[living-memory] spoken_prefix backfill failed: ${String(error).slice(0, 80)}`,
					);
				}
			})();

			// ── D3（独立审计 09-12·**真缺陷**）观测位**末段补写** ──
			//    病：E-4 赋值（本段，晚）与 E-5 赋值（stale 段，更早但仍晚于主写点）均**晚于 `patrol_last` 主写**
			//    （7708-7736）⇒ 首巡必落 0、此后每巡写的是**上一巡值** —— 恰与本车「空转(0/0) 与 已死(不执行)可辨」
			//    的目标冲突（首巡假 0/0 正是要消灭的同形假信号）。修＝nightly patrol末段按 7681-7695 既有补丁手法再写一次
			//    （只更新观测键·`at`/`total`/主字段不动）。
			try {
				const plRow = db
					.prepare("SELECT v FROM organ_meta WHERE k='patrol_last'")
					.get();
				if (plRow && plRow.v) {
					const pl = JSON.parse(plRow.v);
					if (!plMainWriteOk) pl.__ledgerFail = { at: nowIso(), n: stats.patrolLedgerErrors || 0 }; // 车三件③(design note)：主写失败 ⇒ 本补写系「上巡主字段＋当巡观测键」混装——单键读法可辨（原静默混装·漏点 W7）；主写成功时新对象全新构造自然清除本标记
					pl.evoIns = stats.evoMirrorIns || 0;
					pl.evoSkip = stats.evoMirrorSkip || 0;
					pl.stalePoolNow = stats.stalePoolNow || 0;
					pl.stalePoolT7 = stats.stalePoolT7 || 0;
					pl.stalePoolT14 = stats.stalePoolT14 || 0;
					pl.stalePoolT30 = stats.stalePoolT30 || 0;
					pl.staleReviewPoolSize = stats.staleReviewPoolSize || 0; // E 主车：末段补写补齐（首巡不落 0·同 D3 手法）
					pl.staleAutoRetired = stats.staleAutoRetired || 0;
					pl.staleSinkSent = stats.staleSinkSent || 0;
					pl.staleSinkBandwidth = stats.staleSinkBandwidth || 0;
					pl.staleSinkErrors = stats.staleSinkErrors || 0; // 09-12 审计判据项④：错误出口同时入落账面（原只入 stats/render）
					// 09-13 审计D4 治本（**独立键**·第三次订正）：等待信号须「只被末段补写推进」——
					//   曾试 `patrol_last.at`（分钟精度撞车）→ `total`（属主写·只等于「开跑」）→ `pl.patchSeq`（**错**：主写每轮
					//   构造全新对象 ⇒ 覆盖后此键恒 1）。正解＝写**独立键** `organ_meta.patrol_patch`（主写不碰 ⇒ 仅末段补写 +1·单调）。
					pl.a10RotCursorErrors = stats.a10RotCursorErrors || 0; // F2：与 a10* 族同席（游标错可见于落账）
					// ── C 挂哨（批 C 候办·09-15）：conflicts open item可观测——pending 案数＋最老龄（小时）──
					//    治「conflicts 立案后无哨」：pending 长龄＝候裁积压可见（人工终裁主权的open item面·遥测/周报消费 patrol_last 即见）
					try {
						const _cRow = db
							.prepare(
								"SELECT COUNT(*) c, MIN(created_at) oldest FROM conflicts WHERE status = 'pending'",
							)
							.get();
						pl.conflictsPending = _cRow.c || 0;
						if (_cRow.oldest) {
							const _oMs = Date.parse(
								String(_cRow.oldest).slice(0, 16) + "+08:00",
							);
							pl.conflictsPendingOldestH = !isNaN(_oMs)
								? Math.max(0, Math.floor((Date.now() - _oMs) / 3600000))
								: -1;
						} else pl.conflictsPendingOldestH = 0;
					} catch {}
					// ── 进化度量五键（09-16 三案·maintainer·进化体系补全案）：每晚落 patrol_last ──
					//    治「进化只有账没有度量」——让「坑在变多么/教训真被用么/知识在流么」每晚可答（周报画曲线）。
					//    口径（第一版全 SQL 化·fs 面二版）：①lessonRecidivismEdges=近7d 复犯链回边数（explicit/auto-link→活跃 lesson）
					//    ②lessonCitationRate=近30d 被 search 命中 lesson 占比（last_hit_at=relevancy 结算写的条级命中史·%整数）
					//    ③extractYieldRate=auto 提炼条被命中占比（%整数）④lessonCreationRate=近7d 新 lesson 占比（pinboard换血上游代理）
					//    ⑤crossOrganEdges=跨空间边数（cross-space知识流库内代理·原案pinboard diff/上浮件数属 fs 面留二版）
					try {
						const _evIso = (msAgo) => {
							const d = new Date(Date.now() + 8 * 3600 * 1000 - msAgo);
							const p = (n) => String(n).padStart(2, "0");
							return `${d.getUTCFullYear()}-${p(d.getUTCMonth() + 1)}-${p(d.getUTCDate())}T${p(d.getUTCHours())}:${p(d.getUTCMinutes())}+08:00`;
						};
						const _cut7 = _evIso(7 * 86400000);
						const _cut30 = _evIso(30 * 86400000);
						const _act =
							db.prepare(
								"SELECT COUNT(*) c FROM memories WHERE type='lesson' AND status='active'",
							).get().c || 0;
						const _pct = (n, d) => (d > 0 ? Math.round((100 * n) / d) : 0);
						pl.activeLessons = _act;
						pl.lessonRecidivismEdges =
							db
								.prepare(
									"SELECT COUNT(*) c FROM memories_edges e JOIN memories m ON m.id = e.dst WHERE m.type = 'lesson' AND m.status = 'active' AND e.edge_type IN ('explicit','auto-link') AND e.valid_at >= ? AND e.invalid_at IS NULL",
								)
								.get(_cut7).c || 0; // P1 修（09-17 brain独立复验抓实）：.get() 空实参——node:sqlite 未绑 ? 静默当 NULL 返 0 不抛（catch 抓不到）⇒ 恒 0；补绑 _cut7（与键4 同病灶·漏修）
						pl.lessonCitationRate = _pct(
							db
								.prepare(
									"SELECT COUNT(*) c FROM memories WHERE type='lesson' AND status='active' AND last_hit_at IS NOT NULL AND last_hit_at >= ?",
								)
								.get(_cut30).c || 0, // P1 修（同上）：补绑 _cut30
							_act,
						);
						const _autoTot =
							db.prepare(
								"SELECT COUNT(*) c FROM memories WHERE source LIKE 'auto:%' AND status='active'",
							).get().c || 0;
						pl.extractYieldRate = _pct(
							db.prepare(
								"SELECT COUNT(*) c FROM memories WHERE source LIKE 'auto:%' AND status='active' AND last_hit_at IS NOT NULL",
							).get().c || 0,
							_autoTot,
						);
						pl.lessonCreationRate = _pct(
							db.prepare(
								// 审计修×2（09-16）：①.get() 漏传 _cut7（SQL ? 无绑定⇒异常被 catch 吞⇒键 4 永不落值）
								//   ②独立审计 P3 订正（09-16 夜）：真库 ts 系 T 形 96.7%（空格形仅 414 条 2026-08 历史遗留）——
								//     双侧 replace 同形化恢复分钟精度（原单侧法把 T 形比较降日粒度·边界日多计≈0.54pp）
								"SELECT COUNT(*) c FROM memories WHERE type='lesson' AND status='active' AND replace(ts, 'T', ' ') >= replace(?, 'T', ' ')",
							)
								.get(_cut7)
								.c || 0,
							_act,
						);
						pl.crossOrganEdges =
							db.prepare(
								"SELECT COUNT(*) c FROM memories_edges e JOIN memories a ON a.id = e.src JOIN memories b ON b.id = e.dst WHERE e.edge_type IN ('explicit','auto-link') AND e.invalid_at IS NULL AND a.space != b.space",
							).get().c || 0;
					} catch (eEV) {
						stats.evoMetricErrors = (stats.evoMetricErrors || 0) + 1; // ⑬ 出口（独立审计 P2·09-16：afe2b82 引入空吞·曾实吞 RangeError 致键4 永不落值——治根给出口）
					}
					// ── cross-space同族聚合（09-16 三案·maintainer·进化体系补全案）：nightly patrol跨空间聚三路 ──
					//    治四真缺口①「cross-space同族聚合层缺位」——同族坑cross-space重犯自动感知（行政链路外补记忆链路）。
					//    三路：①复犯边（patches/solved_by）src 跨空间入度榜 ②title 头 6 字跨空间族（共现交叠全 SQL 近似）
					//          ③族内词面同族（LIKE 通路并入族 SQL）→ patrol_last 三键（crossOrganLessonTop/crossOrganFamilies/crossOrganNew）
					//          ＋注入行（注入段消费·非空才显零打扰）。错误计数 crossOrganErrors（⑬ 出口·注入行共用）。
					try {
						const _coTot =
							db
								.prepare(
									"SELECT COUNT(DISTINCT m.id) c " +
										"FROM memories_edges e JOIN memories m ON m.id = e.dst JOIN memories s ON s.id = e.src " +
										"WHERE m.type = 'lesson' AND m.status = 'active' " +
										"AND e.edge_type IN ('patches','solved_by') AND e.invalid_at IS NULL AND s.space != m.space " +
										"AND NOT EXISTS (SELECT 1 FROM memories_edges d WHERE d.src = m.id AND d.edge_type='duplicate_of' AND d.invalid_at IS NULL)", // B1 落码车：duplicate_of 首次接辨型消费面（同题族不计cross-space教训席）
								)
								.get().c || 0; // 真值（无帽）——注入行 N 用真值（独立审计改进②：top 帽 5 恒显 5 误导）
						pl.crossOrganLessonTotal = _coTot;
						const _coD = new Date(Date.now() + 8 * 3600 * 1000 - 7 * 86400000);
						const _p2 = (n) => String(n).padStart(2, "0");
						const _coCut =
							_coD.getUTCFullYear() +
							"-" +
							_p2(_coD.getUTCMonth() + 1) +
							"-" +
							_p2(_coD.getUTCDate()); // ts 空格形·日粒度线（审计修②同形教训：T 形对空格形恒漏）
						const _coTop = db
							.prepare(
								"SELECT m.id, m.title, m.space, COUNT(DISTINCT s.space) spaces, COUNT(*) deg " +
									"FROM memories_edges e JOIN memories m ON m.id = e.dst JOIN memories s ON s.id = e.src " +
									"WHERE m.type = 'lesson' AND m.status = 'active' " +
									"AND e.edge_type IN ('patches','solved_by') AND e.invalid_at IS NULL AND s.space != m.space " +
									"AND NOT EXISTS (SELECT 1 FROM memories_edges d WHERE d.src = m.id AND d.edge_type='duplicate_of' AND d.invalid_at IS NULL) " + // B2 落码车：同 B1（top5 榜同治）
									"GROUP BY m.id ORDER BY deg DESC, m.id DESC LIMIT 5",
							)
							.all();
						pl.crossOrganLessonTop = _coTop.map((r) => ({
							id: r.id,
							space: r.space,
							deg: r.deg,
							spaces: r.spaces,
							title: String(r.title || "")
								.replace(/[\r\n{}]/g, " ")
								.slice(0, 40),
						}));
						const _coFam = db
							.prepare(
								"SELECT lower(substr(title, 1, 6)) head6, COUNT(*) members, COUNT(DISTINCT space) spaces, MAX(ts) lastTs " +
									"FROM memories WHERE type = 'lesson' AND status = 'active' AND length(title) >= 6 " +
									"GROUP BY head6 HAVING spaces >= 2 AND head6 != '' ORDER BY lastTs DESC",
							)
							.all();
						pl.crossOrganFamilies = _coFam.length;
						pl.crossOrganNew = _coFam.filter(
							(r) => String(r.lastTs || "").slice(0, 10) >= _coCut,
						).length;
					} catch (eCO) {
						stats.crossOrganErrors = (stats.crossOrganErrors || 0) + 1; // ⑬ 出口
					}
					// 末段补写完成戳（独立键·不进 patrol_last 主写面）：drill/运维可据此判定「本巡完整跑完」
					try {
						const _pp = db
							.prepare("SELECT v FROM organ_meta WHERE k='patrol_patch'")
							.get();
						db.prepare(
							"INSERT OR REPLACE INTO organ_meta (k, v) VALUES ('patrol_patch', ?)",
						).run(String((Number(_pp && _pp.v) || 0) + 1));
					} catch (ePP) {
						stats.patrolLastPatchErrors = (stats.patrolLastPatchErrors || 0) + 1; // ⑬ 出口
					}
					db.prepare(
						"INSERT OR REPLACE INTO organ_meta (k, v) VALUES ('patrol_last', ?)",
					).run(JSON.stringify(pl));
				}
			} catch (ePL) {
				stats.patrolLastPatchErrors = (stats.patrolLastPatchErrors || 0) + 1; // ⑬ 出口
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
	return { nightPatrol };
}

module.exports = { setupPatrol, ensureLifeRenew, ensureLifeRevive, createSnapshot }; // wave 阶段 4：两表建表句供 tools.cjs 手工改期／复活借用（只此一处建表）
