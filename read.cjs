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
	closureCheck,
	cosine,
	embedOnce,
	exportTaskLedger,
	gateKeywords,
	jieba,
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
queryClassDist, spaceWeight, SPOKEN_BOOST, CAP_NEAR, REJECT_PATTERNS, GUARD, apiUsage, injectMeter, spaceGateCounts, hitMapErrorsStat, fs2, path, os,
// [拆③ TS2304 补]（第15项扫出 27 名·≥3 阈值枚举盲区——core 导出面 15 名直解构）
ABSTAIN_TOP1_MIN, classifyQuery, spokenOverlap, RERANK_CAND, rerankDocs, RERANK_TOPN, SELROUTE_TEMPORAL_RE, COOCCUR_CORE_MIN, LOW_RELEVANCE_THRESHOLD, execFileSync, stripUrls, usageQualityDigest, dupWriteStat, PROMPT_SAFE_STAT, closureGateFreshDoneIds, autoRegistered,
SELROUTE_BACKWARD_RE, // ④-b（10-24 fu-nao/order-fix）：回顾类词表，与 SELROUTE_TEMPORAL_RE 同源 core.cjs
} = core;

// ── wave 车二-8f（10-17·落码规格 2.5）：read_episodic 成功回流一条 active 条 → 进程命中 Map +1 并落 hit_pending ──
//    同 search 3b 记录点口径（Map 首建时先从落盘键恢复；恢复失败本进程不覆盖落盘键）；窗口真去读原文，是比被 search 选中更强的「用过」信号。
//    自动召回展示、车二-1 闸文、车二-4 指针都只是展示，不走这里
function c2HitBump(db, id) {
	try {
		if (!(globalThis.__legionHitMap instanceof Map)) {
			globalThis.__legionHitMap = new Map();
			try {
				const p0 = db.prepare("SELECT v FROM organ_meta WHERE k='hit_pending'").get();
				if (p0?.v)
					for (const [k, v] of Object.entries(JSON.parse(p0.v) || {})) {
						const n = Number(v) || 0;
						if (n > 0) globalThis.__legionHitMap.set(Number(k), n);
					}
			} catch {
				globalThis.__legionHitRestoreFailed = true;
				hitMapErrorsStat.n++;
			}
		}
		const m = globalThis.__legionHitMap;
		m.set(Number(id), (m.get(Number(id)) || 0) + 1);
		if (!globalThis.__legionHitRestoreFailed)
			db.prepare("INSERT OR REPLACE INTO organ_meta (k, v) VALUES ('hit_pending', ?)").run(JSON.stringify(Object.fromEntries(m)));
		return true;
	} catch {
		try {
			hitMapErrorsStat.n++;
		} catch {}
		return false;
	}
}

// [拆③] deps 十参（+4：credentials/episodicRate/readNudgeTotal/pprScores＝apply 闭包面·TS2304 扫出；sessionOfLastTurn getter 化＝#31294 hard rule②）
function setupRead({ ctx, db, stats, bindMap, sessionOfLastTurn, runExtract, credentials, episodicRate, readNudgeTotal, pprScores }) {
	return async function execRead(action, args, exec) {
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
							// D-A 修（09-17 独立审计⚠中·探针 as_of 出口路零覆盖）：本路在 p0 之前独立 return——
							// 主路 _pMark/_prof 声明在本路后方（TDZ 不可达）⇒ 内联自备探针（env 直取·缺省 0 零开销）。
							const _profA = process.env.LEGION_SEARCH_PROF_ON ? Date.now() : 0;
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
							if (tokensA.length <= 8) core.queryCapKept += 1; else core.queryCapDyn += 1; // 修车-D6 补尾刀（审计 P3①）：as_of 独立出口同计（一次调用只走其一·「按查询计」全覆盖）
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
							if (_profA)
								ctx.logger?.info?.(
									`[living-memory] search-prof ${JSON.stringify({ as_of_total: Date.now() - _profA })}`,
								);
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
								...(pickedA.length === 0 || pickedA[0].w < ABSTAIN_TOP1_MIN
									? {
											abstainHint:
												"⚠ 该时点库中无强相关条目——勿强答勿拼凑引用；可换关键词重查或调整 as_of 后重试",
										}
									: {}),
							};
						}
						// ── option剖析探针（09-17 前置双刀·maintainer·创造窗 20:10 task brief）：search 路分段计时 ──
						//    env LEGION_SEARCH_PROF_ON 非空时打点（缺省零行为变）；P0..P5 六点段间差出口聚合 logger.info 一次。
						//    段差：fts=P1-P0（FTS+vec+加权+闸全链）｜ppr=P2-P1（共现+PPR）｜hop=P3-P2（实体多跳）｜a02=P4-P3（泛化+Saga）｜tail=P5-P4（兜底+组装）。
						const _profOn = !!process.env.LEGION_SEARCH_PROF_ON;
						const _prof = _profOn ? { t: {} } : null;
						const _pMark = (k) => {
							if (_prof) _prof.t[k] = Date.now();
						};
						_pMark("p0");
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
						let c2SupIds = null; // wave 车二-8e 盲审处置（must）：软沉底补充池并入的条 id——精排取头时排在其余候选之后（见精排段）
						let viaFts = false;
						let vecIds = []; // 作用域修正（16:59·幻引用第三犯自伤）：提升到两 try 外——原 const 在 FTS try 内·cooccur try 引用不可达=整段静默灭
						let recallMode = "full";
						let qTokensShared = null; // option（design-approved）：查询词外提——weighted 加权段（try 外）席内加成要用（同法·幻引用预防）
						let queryClass = "other"; // SelRoute：分类结果外提（return 透出·try 外声明防 FTS 异常态丢）
						try {
							const tokens = qTokens(String(args.query)); // item：实词优选（注释见自动召回段同源函数）
							if (tokens.length <= 8) core.queryCapKept += 1; else core.queryCapDyn += 1; // 修车-D6：按**查询**计（主路+as_of 两点·一次调用只走其一；兜底/自动召回不扰·原按调用计一次 search 多路重复计）
							qTokensShared = tokens; // option：外提席内加成用
							// SelRoute 六类分类（观测版）：只计数+透出·零权重零排序变更——加权链候观测期满分布settled再pending approval
							queryClass = classifyQuery(args.query);
							queryClassDist[queryClass] += 1;
							const match = queryMatch(tokens);
							if (match) {
								const vtNow = nowIso(); // 硬排时刻（一次提值·fts/vec 两路同刻·2026-09-24 落码车）
								const ftsBase = `SELECT m.*, bm25(memories_fts) AS rank FROM memories_fts JOIN memories m ON m.id = memories_fts.rowid
                 WHERE memories_fts MATCH ? AND m.status IN ('active','merged','done')`; // step：候选池放宽纳 merged/done——语义性死亡/办结历史可回查（加权段 ×0.1 沉底/仅标·aged 仍硬排除·硬排见各调用点尾追谓词）
								let ftsRows;
								if (callerSpace) {
									// 段一：硬过滤this space+global（LIMIT 100）
									const seg1 = db
										.prepare(
											ftsBase +
												` AND m.space IN (?, 'global') AND (m.valid_to IS NULL OR m.valid_to > ?) ORDER BY rank LIMIT 100`,
										).all(match, callerSpace, vtNow); // 落码车硬排：valid_to 已过期条不进 search 主路候选（占位符序 MATCH→space→valid_to·实参尾追）
									if (seg1.length >= Math.max(limit * 2, 8)) {
										ftsRows = seg1;
										recallMode = "segmented-" + callerSpace;
									} else {
										ftsRows = db
											.prepare(ftsBase + ` AND (m.valid_to IS NULL OR m.valid_to > ?) ORDER BY rank LIMIT 200`)
											.all(match, vtNow); // 段二：回退全库（硬排同治）
									}
								} else {
									ftsRows = db
										.prepare(ftsBase + ` AND (m.valid_to IS NULL OR m.valid_to > ?) ORDER BY rank LIMIT 200`)
										.all(match, vtNow);
								}
								// ── wave 车二-8e（10-17·落码规格 2.5）：检索软沉底补充池——原三处候选查询不动，另取「已过期仍 active」与 aged 条至多 30 条，
								//    按 id 去重后接在 ftsRows 尾（不挤原池前 100／200 名的候选位）；加权段过期 active ×0.2（⏦，原死码复活）、aged ×0.1（已老化）。
								//    只改 search 主路：注入面、向量路、as_of／spoken／scan 路照旧硬排。LEGION_LIFE_GUARD_OFF=1 或 LEGION_LIFE_SOFTSINK_OFF=1 时不取
								if (process.env.LEGION_LIFE_GUARD_OFF !== "1" && process.env.LEGION_LIFE_SOFTSINK_OFF !== "1") {
									try {
										const supSql =
											"SELECT m.*, bm25(memories_fts) AS rank FROM memories_fts JOIN memories m ON m.id = memories_fts.rowid WHERE memories_fts MATCH ? AND ((m.status = 'active' AND m.valid_to IS NOT NULL AND m.valid_to <= ?) OR m.status = 'aged')" +
											(callerSpace ? " AND m.space IN (?, 'global')" : "") +
											" ORDER BY rank LIMIT 30";
										const sup = db.prepare(supSql).all(...(callerSpace ? [match, vtNow, callerSpace] : [match, vtNow]));
										if (sup.length > 0) {
											const inPool = new Set(ftsRows.map((r) => r.id));
											let addN = 0;
											c2SupIds = new Set();
											for (const r of sup)
												if (!inPool.has(r.id)) {
													ftsRows.push(r);
													inPool.add(r.id);
													c2SupIds.add(r.id);
													addN++;
												}
											stats.c2SoftSinkAdded = (stats.c2SoftSinkAdded || 0) + addN;
										}
									} catch {
										stats.c2SoftSinkErrors = (stats.c2SoftSinkErrors || 0) + 1;
									}
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
										const _vtNowSp = nowIso(); // 车二-9 块一 1-c（10-25 落码）：一次提值，与主路同口径
										for (const row of spCands) {
											if (inRows.has(row.id)) continue;
											// 1-c：口语前缀路的候选里，凡 active 且已到点的一律不进——这类条只经补充池进来（带 ×0.2），
											//     与主路同一口径、也与桥说明对上；merged／done 两类照旧（as_of、已办查询要用）。
											if (process.env.LEGION_SEARCH_PATH_VT_OFF !== "1" && row.status === "active" && row.valid_to != null && String(row.valid_to) <= _vtNowSp) continue; // 规格 2.3 开关：设 1 回旧（桥不跟）
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
							const _vtNowScan = nowIso(); // 车二-9 块一 1-c：扫描兜底路同口径
							for (const row of scan) {
								// 1-c：扫描兜底路（FTS 降级时走）同滤——active 且已到点的不进；merged／done 照旧可回查。
								if (process.env.LEGION_SEARCH_PATH_VT_OFF !== "1" && row.status === "active" && row.valid_to != null && String(row.valid_to) <= _vtNowScan) continue; // 规格 2.3 开关：设 1 回旧
								const hay = (row.title + " " + row.content).toLowerCase();
								let score = 0;
								for (const w of words)
									if (hay.includes(w.toLowerCase())) score += 1;
								if (score > 0) rows.push({ row, base: score / words.length });
							}
						}
						// ── option·线上 rerank 精排应用（design-approved「按推荐」·论证稿落地路径第 2 步）──
						// ── 2step三·检索边过滤（design-approved：被 solved_by/duplicate_of 活边指向的条检索沉底 ×0.25）──
						//    边已在·只差过滤；沉底≠删除（merged/done 候选池放宽面仍可回查）；env LEGION_EDGE_FILTER_OFF 回退。
						if (!process.env.LEGION_EDGE_FILTER_OFF) {
							try {
								const _eIds = rows.map((r) => r.row.id);
								if (_eIds.length) {
									const _phE = _eIds.map(() => "?").join(",");
									const _sinkSet = new Set(
										db.prepare(
											"SELECT DISTINCT dst FROM memories_edges WHERE edge_type IN ('solved_by','duplicate_of') AND invalid_at IS NULL AND dst IN (" +
												_phE +
												")",
											).all(..._eIds).map((e) => e.dst),
										);
									if (_sinkSet.size) {
										let _sunk = 0;
										for (const r of rows)
											if (_sinkSet.has(r.row.id)) {
													r.base = r.base * 0.25;
												r.edgeSinked = 1;
												_sunk++;
											}
										stats.edgeSinked = (stats.edgeSinked || 0) + _sunk;
									}
								}
							} catch {}
						}
						//    挂点=融合候选封盘后/加权链前：候选帽 RERANK_CAND 按 base 取头 → rerankDocs 精排 →
						//    topN 以 relevance_score 为 base 独尺续链（未入列弃置·防量纲混排）；故障/无 key →
						//    rows 不动原链直通。仅 viaFts 融合路挂（scan 兜底路=FTS 异常态不叠外部依赖）。
						if (!process.env.LEGION_RERANK_OFF && viaFts && rows.length > 1) {
							try {
								// wave 车二-8e 盲审处置（must）：补充池条带原始 bm25 进 rows、与活条同尺，降权要到下方加权链才施加——取头时若同场按 base 抢席，
								//   过期／aged 条会把 bm25 第 15–20 名前后的活条挤出精排候选（未入列即弃置）。改为补充池条一律排在其余候选之后：
								//   活条（含向量独有、前缀保底行）≥ RERANK_CAND 时补充池条进不了候选，候选集与关态（LIFE_GUARD_OFF／SOFTSINK_OFF）相同；
								//   不足时才补位，精排分出来后照旧在加权链 ×0.1／×0.2。补充池为空时比较式与原式逐项相同
								const c2SupLast = (a, b) => (c2SupIds && c2SupIds.size > 0 ? (c2SupIds.has(a.row.id) ? 1 : 0) - (c2SupIds.has(b.row.id) ? 1 : 0) : 0);
								const cand = [...rows]
									.sort((a, b) => c2SupLast(a, b) || b.base - a.base)
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
									kept.sort((a, b) => c2SupLast(a, b) || b.base - a.base); // wave 车二-8e 盲审处置：RERANK_TOPN 小于候选数时（env 可调），补充池条不占活条的入链席
									rows = kept.slice(0, RERANK_TOPN);
									if (queryClass === "proper") core.rerankProperPicked += rows.length; // option车观测位：proper 类查询 rerank 后拾取条数（零权重变更·D 项同态趋势）
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
							//    seat 记下本条这一份加成（④-b 盲审 M1 修：回顾句里这一份照旧多乘一次 decay，见下方 段）
							let seat = 0;
							if (
								!process.env.LEGION_SPOKEN_BOOST_OFF &&
								qTokensShared &&
								!r.spokenBoost
							) {
								const ov = spokenOverlap(r.row.spoken_prefix, qTokensShared);
								if (ov > 0) {
									seat = Math.min(SPOKEN_BOOST.seatBonusCap, ov * 0.1);
									w += seat;
								}
							}
							return { ...r, decay: Math.round(dec * 1000) / 1000, w, seat };
						});
						// ── 意图条件权重（wave·17:45 approved·Cortex query_router 意融入）：时间类 query
						//    抬时序信号——论文实证 temporal 类受益最大；我方「上次/最近/之前」类 query 高频。
						//    实现=temporal 意图时对非 todo 行的 decay 再开方（半衰折半·旧条沉更快·新条浮）——
						//    非 temporal 零改动零回归；识别词面与 PANZ/轴提示同源（不新造轮子）。
						//    SelRoute：词表改引 SELROUTE_TEMPORAL_RE 单源（六类 temporal 类同词面）——防双份词表漂移
						// ④-b 回顾类(design note)：先判回顾、后判 temporal——
						//    问句含「之前」「上次」一类回顾词（SELROUTE_BACKWARD_RE，与 SELROUTE_TEMPORAL_RE 同源 core.cjs）时问的是旧办法，
						//    非 todo 行不再多乘一次 decay；其余 temporal 词（最近、昨天、上周等）照旧平方化。日期窗过滤本车不动。
						//    开关：LEGION_TEMPORAL_BACKWARD_OFF 置 1 只关回顾类跳过（回旧统一行为）；LEGION_TEMPORAL_OFF 照旧整段关二次衰减（含回顾类，行为同旧）
						//    N1（盲审可改）：新开关只认值 "1"（同注入瘦身车、板机制车与 LEGION_TRIPLES_KG_ON 惯例），写 0 或别的值不生效
						const isBackwardQ =
							process.env.LEGION_TEMPORAL_BACKWARD_OFF !== "1" &&
							SELROUTE_BACKWARD_RE.test(String(args.query || ""));
						const isTemporalQ = SELROUTE_TEMPORAL_RE.test(
							String(args.query || ""),
						);
						if (isTemporalQ && !isBackwardQ && !process.env.LEGION_TEMPORAL_OFF) {
							// E-3（09-12 approved）：temporal 意图 decay 调制（平方化）可关——原无开关
							for (const r of weighted) {
								if (r.row.type === "todo") continue; // todo 不衰减恒 1.0（现行待办不动）
								r.w = r.w * (r.decay === undefined ? 1 : r.decay); // decay 平方化：旧条（低 dec）沉更快·新条相对浮
							}
						} else if (isTemporalQ && isBackwardQ && !process.env.LEGION_TEMPORAL_OFF) {
							// ④-b 盲审 M1 修（10-24 新会话盲审「甲」·中枢定）：回顾句只让主分少乘一次 decay，席内前缀加成那一份照 75a81fc 仍乘一次 decay。
							//    病灶：席内加成是加法、帽 0.3、不看精排；回顾词「之前」「上次」本身就算前缀交叠锚，旧码里这份加成被二次衰减压住，
							//    只跳过平方化时它原样露出——精排 0.006 的旧条以约 0.20 进前 5、前 8，还顶掉「全低只回首条」。
							//    做法：旧码 w＝（主分＋seat）×decay；本支 w＝主分＋seat×decay，即 w −＝ seat×(1−decay)（decay 用上面同一个取三位的值，与旧码平方化同值）。
							//    todo 行与旧码一样不动；LEGION_TEMPORAL_BACKWARD_OFF=1 时 isBackwardQ 恒假，本支不走（整段回旧）
							for (const r of weighted) {
								if (r.row.type === "todo") continue;
								if (r.seat > 0)
									r.w -= r.seat * (1 - (r.decay === undefined ? 1 : r.decay));
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
						} catch (eVT) {
							stats.validToMapErrors = (stats.validToMapErrors || 0) + 1; // 批⑰ C13（09-21·J 路）：裸 catch 补出口——valid_to 沉底面读取失败可观测（原静默空 catch＝闸失效不可辨）
						}
						for (const r of weighted) {
							// wave 车二-8e：aged 条（只经软沉底补充池进来）×0.1 沉底、标「已老化」（与合并条同法），先于 valid_to 判断
							if (r.row.status === "aged") {
								r.w *= 0.1;
								r.stale = String(r.row.closed_at || "").slice(0, 10);
								r.staleKind = "aged";
								continue;
							}
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
						_pMark("p1");
						// ── step：共现伙伴召回加成——「想起这个必须想起那个」──
						//    命中集中已存在的伙伴：+0.1 排序加成并标注；未命中的伙伴：以 0.1×权重×衰减 注入（上限 5 条防灌水）
						let cooccurBoosted = 0;
						let cooccurInjected = 0;
						const COOCCUR_MULT_GATE = 0.02; // ④-a 门：相关度（精排分，或无精排时的 ftsBase）低于此值不吃伙伴加成（防零分条被倍率拉进榜）
						//    N2（盲审可改）：同一个 0.02 管两种尺度，实效不同——
						//      精排分 0–1、无关条约 0.006，这道门真挡得住；
						//      ftsBase＝rr/(1+rr)，词面命中条一般 0.8–0.95（10-24 快照 D2′ 实测 0.902–0.943），这道门几乎挡不到任何词面行。
						//      词面路（精排没跑）起保护作用的是倍率上限 1.1（1＋0.1·min(1, ftsBase)），不是这道门
						try {
							const hitIds = new Set(weighted.map((r) => r.row.id));
							// ── vec 种子进 PPR（wave·16:47 approved）：RRF 向量路命中并入种子集——
							//    治「半边图」（原只 FTS 命中进图·语义近邻不传播）。种子帽仍 20。
							const pprSeeds = [...hitIds];
							for (const vid of vecIds) {
								if (!hitIds.has(vid)) pprSeeds.push(vid);
							}
							// E-3（09-12 approved）：`LEGION_PPR_OFF` ⇒ PPR **本体**整体消融
							//   （空 Map ＝零加成·与「无图回落」同径）；原只有 `LEGION_PPR_QUERY_OFF` 管查询因子，
							//   本体无可关面 ⇒ 审计「留并删」清单里这一路无法单独验。
							//    ⚠ 独立审计 D4（09-12）**订正**：必须返回 **`null`** 而非空 Map——`pprScores` 真「无图」返 null
							//    （4535 空图/无种子 · 4562 max≤0 · 4567 catch），而下游三条判路用 **map 真值**（非内容）：
							//    空 Map 是 truthy ⇒ 9187 `pprMap && pp<=0.02` 会把 cooccur **伙伴注入**一并 continue 掉
							//    ⇒ 与「无图回落」不同径、消融实验把「PPR 贡献」与「cooccur 注入贡献」混同。改 null ⇒ 真同径。
							const pprMap = process.env.LEGION_PPR_OFF
								? null
								: pprScores(
										pprSeeds.slice(0, 20),
										tokenize(String(args.query || "")),
									); // +种子=FTS∪vec 命中·teleport 回种子——查询相关
							const partnerOf = new Map();
							for (const p of db
								.prepare(
									// 车三补件（09-14 13:0x·全点枚举第四处漏点）：weak 档不进**伙伴加成／伙伴注入**——
									//   原直读全表 ⇒ C 档 9.6 万 weak 对全进 partnerOf ⇒
									//   ①「命中集内弱伙伴 +0.1~0.3 加成」②「未命中弱伙伴注入（上限 5 条）」双双污染。
									//   分档线与镜像分流**同源常量**（COOCCUR_CORE_MIN·防两处硬编码漂移）。
									"SELECT id_a, id_b FROM memories_cooccur WHERE score >= ?",
								)
								.all(COOCCUR_CORE_MIN)) {
								if (!hitIds.has(p.id_a) && !hitIds.has(p.id_b)) continue;
								if (!partnerOf.has(p.id_a)) partnerOf.set(p.id_a, new Set());
								if (!partnerOf.has(p.id_b)) partnerOf.set(p.id_b, new Set());
								partnerOf.get(p.id_a).add(p.id_b);
								partnerOf.get(p.id_b).add(p.id_a);
							}
							if (process.env.LEGION_COOCCUR_OFF) partnerOf.clear(); // E-3（09-12 批 A）：cooccur **整路消融**（加成＋伙伴注入）——原无任何开关，「留并删」不可逐路验；建边段照跑＝图数据无害
							for (const r of weighted) {
								const ps = partnerOf.get(r.row.id);
								if (!ps) continue;
								const inSet = [...ps].filter((x) => hitIds.has(x));
								if (inSet.length > 0) {
									const pp = pprMap ? pprMap.get(r.row.id) || 0 : 0;
									// ④-a(design note)：加成改乘法加门控——
									//    病灶：旧式 r.w += 0.1~0.3 与本条 base、decay、空间权重全无关，精排 0.006 的无关旧条靠互为伙伴抬到 0.3，压过真答案。
									//    精排在跑（r.rerank 有值）：base（＝精排分）≥ COOCCUR_MULT_GATE 才加，r.w ×= 1 + min(0.3, 0.1 + 0.2·pp)（上限倍率 1.3）；
									//    精排没跑（纯词面路）：ftsBase ≥ 门才加，r.w ×= 1 + 0.1·min(1, ftsBase)；ftsBase 也没有（scan 兜底、向量独有、前缀保底行）不加。
									//    伙伴注入路（下方 missingPartners）不动。LEGION_COOCCUR_OFF 照旧整路关（加成＋注入）；
									//    LEGION_COOCCUR_MULT_OFF 置 1 回旧加法（回退与 test/order-drill.cjs 基线用；N1：只认值 "1"）。
									//    门控挡下的条照旧标 cooccur/ppr（counter），只是不加分、不计 boosted
									let boosted = false;
									if (process.env.LEGION_COOCCUR_MULT_OFF === "1") {
										r.w += Math.min(0.3, 0.1 + 0.2 * pp); // 原静态 +0.1 → PPR 查询相关（近种子伙伴高分·远伙伴保底 0.1）
										boosted = true;
									} else if (r.rerank !== undefined) {
										if (r.base >= COOCCUR_MULT_GATE) {
											r.w *= 1 + Math.min(0.3, 0.1 + 0.2 * pp);
											boosted = true;
										}
									} else {
										const fb = Number(r.ftsBase);
										if (fb >= COOCCUR_MULT_GATE) {
											r.w *= 1 + 0.1 * Math.min(1, fb);
											boosted = true;
										}
									}
									r.ppr = Math.round(pp * 1000) / 1000;
									r.cooccur = inSet;
									if (boosted) cooccurBoosted += 1;
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

						_pMark("p2");
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
						_pMark("p3");
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
						_pMark("p4");
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
							if (!globalThis.__legionHitMap) {
								globalThis.__legionHitMap = new Map();
								// ── D-1 修（批 C 候办审计车·只读子代理行为实证红）：首建时从落盘键恢复 ──
								//   治「重启→注入（先于nightly patrol）→快照覆盖写→重启前旧攒批永久丢」——恢复提前到 Map 首建
								//   （一进程一次·零热路径成本·审计推荐首选）；nightly patrol消费入口的恢复∪保留作belt-and-braces（幂等）。
								try {
									const _p1 = db
										.prepare(
											"SELECT v FROM organ_meta WHERE k='hit_pending'",
										)
										.get();
									if (_p1?.v) {
										const _po1 = JSON.parse(_p1.v);
										for (const [k, v] of Object.entries(_po1 || {})) {
											const n1 = Number(v) || 0;
											if (n1 > 0)
												globalThis.__legionHitMap.set(Number(k), n1);
										}
									}
								} catch (e1) {
									// ── 81d8eff 后补（09-16 brain急件·option·创造落码）：恢复失败观测接线＋写门 ──
									//   原 `catch {}` 空吞 ⇒ 异常不上行 ⇒ 外层 B-1④ 不触发 ⇒ 空壳 Map 照常下行 →
									//   INSERT OR REPLACE 用不完整 Map 覆盖 hit_pending（重启前旧攒批永久丢·零信号）——
									//   D-1 要治的病在「恢复自身失败」这条路上原样复发（批A 第9/10 案交汇点）。
									//   甲面：失败计数双口径（进程 hitMapErrorsStat ＋ organ_meta 累计·与外层 B-1④ 同形）；
									//   乙面：置 _restoreFailed 旗 ⇒ 批末写门阻写——恢复失败时不覆盖落盘键·保留旧值候nightly patrol ∪
									//   （∪ belt-and-braces仅对「键仍存在」有效——覆盖写即belt-and-braces同灭）。旗一进程一次·下次重启重试恢复。
									hitMapErrorsStat.n++;
									globalThis.__legionHitRestoreFailed = true;
									try {
										const _b1e1 = db
											.prepare(
												"SELECT v FROM organ_meta WHERE k='hit_map_errors'",
											)
											.get();
										db
											.prepare(
												"INSERT OR REPLACE INTO organ_meta (k, v) VALUES ('hit_map_errors', ?)",
											)
											.run(String((Number(_b1e1?.v) || 0) + 1));
									} catch {}
								}
							}
							// wave 车二-8f（10-17·落码规格 2.5）：原 `!s.row.stale_state` 把 ttl 条也挡在命中外（注释原意只排 review、retired），
							//   自动提炼的带期条全是 ttl、永远攒不到命中；改为 active 且非 review／retired 才计（merged、done、aged 不计）。LEGION_LIFE_GUARD_OFF=1 退回原判
							//   盲审处置（nit）：标题是状态句(design note)的条不计命中——缺口条被 search 选中几次不该抬权重、撑命中保护
							//   补笔②（10-18·侦察 K3-01）：写时盖戳的被取代条（superseded-auto）也不计——到点即过期、经补充池可被选中；基线 `!stale_state` 本就不计
							const c2LifeOn = process.env.LEGION_LIFE_GUARD_OFF !== "1";
							const c2StateT = (r) => !!(CRIT.lifeStateTitle && CRIT.lifeStateTitle.test(String(r.title || "")));
							for (const s of picked)
								if (c2LifeOn ? s.row.status === "active" && s.row.stale_state !== "review" && s.row.stale_state !== "retired" && s.row.stale_state !== "superseded-auto" && !c2StateT(s.row) : !s.row.stale_state)
									globalThis.__legionHitMap.set(
										s.row.id,
										(globalThis.__legionHitMap.get(s.row.id) || 0) + 1,
									);
							// ── B-1②③（批 C 候办·09-15）：攒批落盘 + 注入键 ──
							//   ②治「重启丢攒批」（批 2 回收判据基础）：批末 upsert organ_meta['hit_pending']——一次注入一批一写
							//     （「零阻塞零写库」口径更新为「批末一写」·nightly patrol消费入口恢复∪消费毕删键·闭环见 settle 段）
							//   ③last_inject_at 另立键：注入发生时刻（picked>0 即写·供「距上次注入多久」类判读）
								// ── option写门（09-16）：恢复失败 ⇒ 本进程不覆盖 hit_pending（保留旧值候nightly patrol ∪）──
							if (
								picked.length > 0 &&
								!globalThis.__legionHitRestoreFailed
							) {
								db
									.prepare(
										"INSERT OR REPLACE INTO organ_meta (k, v) VALUES ('hit_pending', ?)",
									)
									.run(
										JSON.stringify(
											Object.fromEntries(globalThis.__legionHitMap),
										),
									);
								db
									.prepare(
										"INSERT OR REPLACE INTO organ_meta (k, v) VALUES ('last_inject_at', ?)",
									)
									.run(nowIso());
							}
						} catch (e) {
							// ── B-1④（批 C 候办）：原空 catch 静默 → 失败可观测（organ_meta 计数键·render 透出「hit错」）──
							hitMapErrorsStat.n++; // 进程口径（组装段无 db 可达·return 直引）
							try {
								const _b1e = db
									.prepare("SELECT v FROM organ_meta WHERE k='hit_map_errors'")
									.get();
								db
									.prepare(
										"INSERT OR REPLACE INTO organ_meta (k, v) VALUES ('hit_map_errors', ?)",
									)
									.run(String((Number(_b1e?.v) || 0) + 1));
							} catch {}
						}
						// ── wave 车二-4（10-17·落码规格 2.4）：本次 search 选中了本会话本轮自动召回的被截条 ⇒ inject_recall_cut_search_total +1（每次 search 至多 +1）──
						//    DSH 内 memory 工具没有按 id 取全文，窗口取回「✂未显」指针条的正路是 search 标题词；W4 只数 read_episodic（inject_recall_cut_read_total）会漏数。
						//    本轮被截以 bd.injRc（注入结算点存）为准、只认本轮（跨轮不计）；LEGION_RECALL_PTR_OFF=1 时不计
						if (process.env.LEGION_RECALL_PTR_OFF !== "1") {
							try {
								const sidC = exec?.agent?.session?.id || exec?.session?.id || "";
								const bdC = sidC ? bindMap.get(String(sidC)) : null;
								const rcC = bdC && bdC.injRc;
								if (rcC && Array.isArray(rcC.cut) && rcC.cut.length > 0 && rcC.turn === bdC.injTurn && picked.some((s) => rcC.cut.includes(Number(s.row.id))))
									db.prepare(
										"INSERT INTO organ_meta(k, v) VALUES('inject_recall_cut_search_total', '1') ON CONFLICT(k) DO UPDATE SET v = CAST(CAST(v AS INTEGER) + 1 AS TEXT)",
									).run();
							} catch {
								stats.c2CutSearchErrors = (stats.c2CutSearchErrors || 0) + 1;
							}
						}
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
								// 盲审处置（nit）：软沉底补充池条（aged、过期 active，c2SupIds）不计——它们已标 ⏦／已老化沉底，计进来会诱导窗口调大 limit 去翻老化条；补充池为空时与原式同
								const sunkOld = weighted.filter(
									(x) => !inIds.has(x.row.id) && isAged(x) && !(c2SupIds && c2SupIds.has(x.row.id)),
								).length;
								if (sunkOld >= 2)
									axisHint = `另有 ${sunkOld} 条 >30d 旧态相关条沉底未展示（decay 压制），需要时 limit↑或 timeline 查`;
							}
						} catch {}
					_pMark("p5");
					if (_prof) {
						const _d = {
							fts: _prof.t.p1 - _prof.t.p0,
							ppr: _prof.t.p2 - _prof.t.p1,
							hop: _prof.t.p3 - _prof.t.p2,
							a02: _prof.t.p4 - _prof.t.p3,
							tail: _prof.t.p5 - _prof.t.p4,
							total: _prof.t.p5 - _prof.t.p0,
						};
						ctx.logger?.info?.(
							`[living-memory] search-prof ${JSON.stringify(_d)}`,
						);
					}
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
											staleMark: `🕐${String(s.row.stale_at || "").slice(5, 10)} 记·${
												s.row.stale_state === "review"
													? "待复核"
													: s.row.stale_state === "retired"
														? "已退役·可回任" // E 主车（09-12 审计判据项⑤）：新增取值 retired 中文化——原直出英文原文
														: s.row.stale_state
											}`,
										}
									: {}), // P2：过时条带 staleMark 头标（审计补②：独立字段名——旧 stale 字段归retrieval gate专用，防同名覆盖）
								...(s.cooccur ? { cooccur: s.cooccur } : {}),
								...(s.ppr !== undefined ? { ppr: s.ppr } : {}), // 查询相关图谱分透出（counter）
								...(/^(?:auto:|session:)?(session-[0-9a-f-]+)$/.test(String(s.row.source || ""))
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
														: s.staleKind === "aged" // wave 车二-8e：软沉底 aged 条
															? "已老化（aged）"
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
							...(picked.length === 0 || (picked[0].w < ABSTAIN_TOP1_MIN && !allLow)
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
								error: "read_episodic 需要 id 参数（带会话锚条目）",
							};
						const row = db
							.prepare(
								"SELECT id, ts, title, source, space, status, stale_state FROM memories WHERE id = ?", // wave 车二-8f：加取 status（成功回流 active 条计命中）；补笔②（K3-01）加取 stale_state
							)
							.get(id);
						if (!row)
							return { episodicContext: null, note: "条目不存在 #" + id };
						const src = String(row.source || "");
						// 09-17 回流面修（统筹实勘：B 手写 session:/C 裸形态 2192 条存量被 ^auto: 锚死挡——指针在文件在纯形态挡）：
						//   受理放宽为三形态（auto:session-/session:session-/session- 裸）——sid 捕获组同源·护栏（频控/脱敏/charCap）零变
						const mSid = src.match(/^(?:auto:|session:)?(session-[0-9a-f-]+)$/);
						if (!mSid)
							return {
								episodicContext: null,
								note: "该条目无会话锚（source=" +
									(src || "空") +
									"——手写未补锚/patrol 等形态无原文；auto:/session:/裸 session- 三带锚形态均可回流）",
							};
						// 频控：每会话 4 真用户轮 1 次（D2 修正 8-26 逐字审计：fcKey 取 exec 真身 session id——sessionOfLastTurn 是「最后活动会话」·多窗并发会串频控（bindMap 分账同型病 L876 教训））
						const fcKey =
							exec?.agent?.session?.id ||
							exec?.session?.id ||
							sessionOfLastTurn() ||
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
						// ── V2/V3 双格式兼容（2026-09-13 A 段·DSH 0.1.5 适配）──
						//  0.1.5 起会话日志按 generation 命名：v0=session.jsonl，vN=session.vN.jsonl（＋.zstd）；
						//  旧件保留不再写、迁移按需触发 ⇒ 同会话可能有 V2/V3 两份。宿主解析器明确拒带压缩
						//  后缀之名，故此处自建正则；按 generation 取最新（同代 zstd 优先），全无则明确报缺。
						const sessDir = path.join(
							os.homedir(),
							".dsh",
							"sessions",
							sessWorkspace,
							mSid[1],
						);
						const zst = (() => {
							try {
								const re = /^session(?:\.v([1-9][0-9]*))?\.jsonl(\.zstd)?$/;
								let best = null,
									bestGen = -1,
									bestZ = -1;
								for (const f of fs2.readdirSync(sessDir)) {
									const m = re.exec(f);
									if (!m) continue;
									const gen = m[1] === undefined ? 0 : Number(m[1]);
									const z = m[2] ? 1 : 0;
									if (gen > bestGen || (gen === bestGen && z > bestZ)) {
									best = path.join(sessDir, f);
									bestGen = gen;
									bestZ = z;
									}
								}
								return best;
							} catch (eSel) {
								return null;
							}
						})();
						if (!zst || !fs2.existsSync(zst))
							return {
								episodicContext: null,
								note:
									"原文文件已不在位（V2/V3 皆未找到）·指针：" +
									sessDir,
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
chunk_buf = []  # assistant/chunk 回退缓冲（2026-09-13 A 段·仅无 message 事件时启用）
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
            sysf = s0.startswith('Time sampled while preparing') or s0.startswith('当前运行时上下文。此快照取代') or s0.startswith('Current runtime context') or s0.startswith('<system-reminder>') or s0.startswith('<goal_round>') or s0.startswith('<goal_complete>') or s0.startswith('〔记忆闸') or (((o.get('data') or {}).get('source') or {}).get('kind') == 'legion-askgate')  # 盲审处置（nit）：车二-1 闸文按系统帧折叠，不当maintainer原话
            frames.append({'k': 'U', 'ms': o.get('time', 0), 't': txt, 'sys': sysf})
    elif t == 'assistant/message':
        # V2/V3 双格式兼容（2026-09-13 A 段·DSH 0.1.5）：authoritative = data.message.content[]
        # 实测 V2 的 assistant/message 亦带正文；V3 中 assistant/chunk 归零、正文落 message.content。
        _m = ((o.get('data') or {}).get('message') or {})
        _b = _m.get('content') or []
        _txt = ' '.join(x.get('text', '') for x in _b if isinstance(x, dict) and x.get('type') == 'text')
        if not _txt.strip():
            # 回退：嵌于 stream[] 的流式块（V3 canonical-envelope 形态）
            _parts = []
            for _e in (((o.get('data') or {}).get('stream')) or []):
                if not isinstance(_e, dict): continue
                _c = _e.get('chunk') or {}
                if _c.get('type') == 'text':
                    _parts.append(_c.get('text', ''))
                elif _c.get('type') == 'block-end':
                    _bk = (_c.get('block') or {})
                    if _bk.get('type') == 'text' and _bk.get('text'): _parts.append(_bk.get('text'))
            _txt = ' '.join(_parts)
        if _txt.strip(): frames.append({'k': 'A', 'ms': o.get('time', 0), 't': _txt})
    elif t == 'assistant/chunk':
        c = ((o.get('data') or {}).get('chunk') or {})
        if c.get('type') == 'text' and c.get('text'): chunk_buf.append(c.get('text', ''))
if chunk_buf and not any(f['k'] == 'A' for f in frames):
    # 兼容回退：全日志无 assistant/message 事件时的极老格式（避免与权威路重复成帧）
    frames.append({'k': 'A', 'ms': 0, 't': ''.join(chunk_buf)})
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
							if (process.env.LEGION_LIFE_GUARD_OFF !== "1" && row.status === "active" && row.stale_state !== "superseded-auto" && !(CRIT.lifeStateTitle && CRIT.lifeStateTitle.test(String(row.title || ""))) && c2HitBump(db, row.id)) stats.c2EpisodicHits = (stats.c2EpisodicHits || 0) + 1; // wave 车二-8f；盲审处置（nit）：状态句标题不计；补笔②（K3-01）：写时盖戳的被取代条不计
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
							.trim(); // 余件item二（09-09·三轮审计 §4.4-1）：剥缀集对齐——原 (active|aged|retired|resolved) 漏 merged/superseded →手写区带缀条目三级匹配全失恒落回落窗·取并集六态归一（批⑳ N-1 订正：原引「入库剥缀集 L7303 单源有之」已随 evo-mirror 退役〔29ef1c4〕删除——本行并集即唯一在位源）
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
						// ── 序3（09-15 执行令 §2.4·maintainer）：锚机制改「库为源优先」──
						//    根因：source of record每夜被投影器幂等重写 ⇒ 行号锚必然漂移（结构性冲突·第 12 漏点——brain实测 
						//    走回落窗·锚 L3213 在source of record已无此条）；mirror 条身份在库（投影化hard rule：库=正账）。
						//    mirror 条跳过source of record三级匹配直落库源窗（准确·省三次全文扫描）；降级可见化 sourceKind。
						const isMirrorSrc = String(row.source || "").startsWith("mirror:");
						let hi = isMirrorSrc ? -1 : heads.findIndex((h) => h + 1 === anchorLine);
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
									sessionOfLastTurn() ||
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
									file: isMirrorSrc
										? "living memory库源窗（序3·09-15：mirror 条以库为源——source of record每夜重写行号锚必漂·库=正账）"
										: "living memory in-store fallback window (absent from source of record; excluded by evo ingest, rebuild, and mirror filters)",
									sourceKind: "库源", // 序3：降级可见化（source of record窗=「source of record」·库窗=「库源」——「库窗也算成功」不再隐语）
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
							sessionOfLastTurn() ||
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
							sourceKind: "source of record", // 序3：降级可见化（source of record窗/库源窗双标——窗源一眼可辨）
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
						// ── option day 按天读（09-17 approved·brain 11:16 交办主责this space）：ts 日窗过滤·与 as_of 同构可叠加 ──
						//    窗=ts>=day ∧ ts<day+1（ISO 字典序与 +08:00 串天然兼容·与 as_of 三窗同边界判据家族·不另起炉灶）；
						//    day 模式时间正序（晨→夜·读史叙事序）；旋钮 LEGION_TIMELINE_DAY_OFF 非空即停（fail-closed 显式报停用·非静默）。
						//    次日边界用 Date.UTC 纯日期算术（无时区依赖）；ASC/DESC 为码面字面量二选一（非用户串拼接·无注入面）。
						const dayTl = String(args.day || "").trim();
						if (dayTl && process.env.LEGION_TIMELINE_DAY_OFF)
							return { error: "day 参数已由 LEGION_TIMELINE_DAY_OFF 停用" };
						if (
							dayTl &&
							!/^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])$/.test(dayTl)
						)
							return {
								error:
									"day 格式非法（YYYY-MM-DD·严格日粒度·同款闸）: " +
									dayTl.slice(0, 30),
							};
						// D-1 修（09-17 独立审计⚠中·真历日回读校验）：形状正则放行 02-30/04-31/平年02-29 等假日期，
						//    Date.UTC 溢出自动滚动（02-30→03-02）⇒ 日窗静默扩大两日（沙箱实捞 03-01+03-02 硬证据）。
						//    修法＝回读比对月/日（年不含 0-99 特例：输入恒四位·D-B 订正〔独立审计🟡低〕：四位串数值可 0-9999——
						//    proleptic 闰 0000-02-29 误拒为唯一边界实例·零实际流量〔库 ts 全 2025-2026〕·不另设闸）。
						if (dayTl) {
							const _dChk = new Date(
								Date.UTC(
									Number(dayTl.slice(0, 4)),
									Number(dayTl.slice(5, 7)) - 1,
									Number(dayTl.slice(8, 10)),
								),
							);
							if (
								_dChk.getUTCDate() !== Number(dayTl.slice(8, 10)) ||
								_dChk.getUTCMonth() + 1 !== Number(dayTl.slice(5, 7))
							)
								return {
									error:
										"day 非真历日（YYYY-MM-DD 须为真实存在日期）: " +
										dayTl.slice(0, 30),
								};
						}
						const dayNext = dayTl
							? new Date(
									Date.UTC(
										Number(dayTl.slice(0, 4)),
										Number(dayTl.slice(5, 7)) - 1,
										Number(dayTl.slice(8, 10)) + 1,
									),
								)
									.toISOString()
									.slice(0, 10)
							: "";
						const dayWin = dayTl ? " AND ts >= ? AND ts < ?" : "";
						const dayWinArgs = dayTl ? [dayTl, dayNext] : [];
						const tlOrder = dayTl
							? " ORDER BY COALESCE(event_at, ts) ASC, id ASC LIMIT ?"
							: " ORDER BY COALESCE(event_at, ts) DESC, id DESC LIMIT ?";
						let rows;
						if (args.space) {
							rows = db
								.prepare(
									"SELECT * FROM memories WHERE status = 'active' AND space = ?" +
										tlWin +
										dayWin +
										tlOrder,
								)
								.all(String(args.space), ...tlWinArgs, ...dayWinArgs, limit); // D3 审计修（18:57）：id 序被镜像 440 条霸屏——真时间序（镜像老 ts 沉底·事件钟优先）
						} else {
							rows = db
								.prepare(
									"SELECT * FROM memories WHERE status = 'active'" +
										tlWin +
										dayWin +
										tlOrder,
								)
								.all(...tlWinArgs, ...dayWinArgs, limit); // D3 同修
						}
						const total = db
							.prepare(
								"SELECT COUNT(*) AS c FROM memories WHERE status = 'active'" +
									tlWin +
									dayWin,
							)
							.get(...tlWinArgs, ...dayWinArgs);
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
							...(dayTl ? { day: dayTl } : {}),
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
							// ⑤ 三元组停建（10-24 13:51 maintainer准·fu-nao/order-fix）：计数旁加注——nightly patrol投影默认停，上面 triplesTotal 是存量
							// S2（盲审应改）：停没停以nightly patrol为准——优先读 patrol_last.triplesKgStopped（nightly patrol每轮记 true／false），
							//    读不到（还没在本车码上跑过nightly patrol、或 patrol_last 坏）再按本进程环境 LEGION_TRIPLES_KG_ON 判；note 里写明取自哪里。
							//    原因：副脑记忆桥启动时删光 LEGION_*，按读取进程环境判，DSH 侧设 KG_ON=1 重开后经桥看 stats 仍显示「已停建」。
							//    开态（上次nightly patrol照旧投影，或读不到时本进程设了 1）不出 triplesKgStopped、triplesKgNote 两键，与 75a81fc 同（N4）
							...(() => {
								let stopped, from;
								try {
									const r = db
										.prepare("SELECT v FROM organ_meta WHERE k='patrol_last'")
										.get();
									const pl = r ? JSON.parse(r.v) : null;
									if (pl && typeof pl.triplesKgStopped === "boolean") {
										stopped = pl.triplesKgStopped;
										from = "据上次nightly patrol记账 patrol_last";
									}
								} catch {}
								if (stopped === undefined) {
									stopped = process.env.LEGION_TRIPLES_KG_ON !== "1";
									from = "据本进程环境（patrol_last 未记停建标）";
								}
								return stopped
									? {
											triplesKgStopped: true,
											triplesKgNote: `三元组已停建（${from}）：上数是存量（不删不改），nightly patrol投影开关 LEGION_TRIPLES_KG_ON 默认停，置 1 重开`,
										}
									: {};
							})(),
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
							vecChannelErrors: core.vecChannelErrors || 0, // 8-31 锈面修③：vec 通道故障累计（F0-1 静默死遥测透出）
							vecSkippedDueToBudget: core.vecSkippedDueToBudget || 0, // 批㉙ option：vec 臂超帽降级计数
							vecCapNear: core.vecCapNear || 0, // 帽接近度哨（10-01）：chunks/cap ≥80% 未超帽预警计数（render「帽接近」同源）
							// 帽接近度哨族（10-01·approved·5 顶补齐）——每哨一键（四点接线：赋值／return／render／drill）
							familyCapNear: CAP_NEAR.family || 0,
							hisimCapNear: CAP_NEAR.hisim || 0,
							spokenCapNear: CAP_NEAR.spoken || 0,
							chunkCapNear: CAP_NEAR.chunk || 0,
							vecPoolCapNear: CAP_NEAR.vecpool || 0,
							vecPoolCapOver: CAP_NEAR.vecpoolOver || 0, // 超帽对偶观测（10-02·#28825）
							capNearKeyErrors: CAP_NEAR.keyErrors || 0,
							patrolLedgerErrors: stats.patrolLedgerErrors || 0, // R路车一⑤：patrol_last 主写失败计数（render「nightly patrol账错」同源）
							__ledgerFail: (() => {
								// 车三审即修（甲⚠1/乙🔴2·双路同发现）：W7「主写失败标」render 哑键根治——stats return 原无接线 ⇒ L924 恒 undefined 失败态也不显示。从 organ_meta patrol_last 透出（跨重启可读·字符串形态供 render 直显）。
								try {
									const _plRow = db
										.prepare(
											"SELECT v FROM organ_meta WHERE k='patrol_last'",
										)
										.get();
									const _lf =
										_plRow && JSON.parse(_plRow.v).__ledgerFail;
									// 10-01 即修（stats 全挂案·probe 实证）：无失败标时原返回 undefined ⇒ 属性值 undefined
									//   ⇒ harness snapshotJsonValue 拒收「value is not lossless JSON」⇒ stats action 全线不可用（车三 69c6cf3 引入·R16a/R17 验收未覆盖真返回面）。改 null（合法 JSON·render 层 falsy 不显示·与 linkSentinel/edgeCoveragePct 族同形）。
									return _lf
										? String(_lf.at || "") + " ×" + String(_lf.n || "")
										: null;
								} catch {
									return null;
								}
							})(),
							vecScanSkipKeyErrors: core.vecScanSkipKeyErrors || 0, // 车一补丁（补审 B-21）：落键失败计数（render「跳过键错」同源）
							familySkippedDueToCap: stats.familySkippedDueToCap || 0, // 车二件1a（render「fam帽跳」同源）
							hiSimSkippedDueToCap: stats.hiSimSkippedDueToCap || 0, // 车二件1b（render「hisim帽跳」同源）
							a19SkippedDueToCap: stats.a19SkippedDueToCap || 0, // 车二件2（render「跳闸」同源）
							soulExemptSuppressed: stats.soulExemptSuppressed || 0, // 车二件4（render「魂豁免压」同源）
							sentryAlarmSuppressed: stats.sentryAlarmSuppressed || 0, // 车二件3：light sentry同源已立被抑计数
							famRotCursorErrors: stats.famRotCursorErrors || 0, // 十九审即修①（render「fam游标错」同源）
							hiRotCursorErrors: stats.hiRotCursorErrors || 0, // 十九审即修①（render「hisim游标错」同源）
							sentryTtlHeal: stats.sentryTtlHeal || 0, // 漏点即修⑤（render「哨自愈」同源）
							sentryWriteErrors: stats.sentryWriteErrors || 0, // 漏点即修④（render「哨写错」同源）
							rerankErrors: core.rerankErrors || 0, // option：rerank 通道故障累计（vecChannelErrors 同族透出·降级直通可见）
							queryCapDyn: core.queryCapDyn || 0, // 帽分治（五件一item1）：动态放宽生效次数（>8 实词查询）
							queryCapKept: core.queryCapKept || 0, // 帽分治：≤8 保持原帽次数（预期占大头·中文短查询零害）
							rerankProperPicked: core.rerankProperPicked || 0, // option车观测位（D 项）：proper 类查询 rerank 拾取累计——同态趋势可比
							pprBuildMs: stats.pprBuildMs || 0, // PPR 性能车观测键①：最近一次建图耗时（物化+过滤·改前主 SQL 实测 9s 级）
							pprEdges: stats.pprEdges || 0, // 观测键②：最近一次入图边数（非零断言防假绿）
							pprQueryFactorMs: stats.pprQueryFactorMs || 0, // 观测键③：查询因子块耗时（限域后应大幅下降）
							pprShortCircuit: stats.pprShortCircuit || 0, // 观测键④：无图短路累计（沙箱/新库场景）
							edgeCoveragePct: stats.edgeCoverageMeasured ? stats.edgeCoveragePct : null, // 修车-D5 补尾刀（审计 P2）：null=未测保留——原 `|| 0` 抹回 0 致「首巡前/真0」不可辨
							edgeCoverageMeasured: stats.edgeCoverageMeasured || false, // D5 四点接线②（赋值 L8124→此 return→render→drill T0 动态扫）
							sentryErrors: stats.sentryErrors || 0,
							toolUsageErrors: core.toolUsageErrors, // 吸收item：遥测写库失败累计透出（⑬ 出口闭环·09-02 审计真缺陷 1 即修）
							usageWeeklyRuns: stats.usageWeeklyRuns || 0, // P0 同item：周报常驻段跑/败计数透出（09-03 自审补——⑬ 计数不可见=半合规同型病）
							ttlExpired: stats.ttlExpired || 0, // 批⑲ P6
							dreamerClusters: stats.dreamerClusters || 0, // 批⑲ P6
							deadVecCleaned: stats.deadVecCleaned || 0, // 批⑲ P6
							deadVecSkipErrors: stats.deadVecSkipErrors || 0, // 批⑲ P6
							commonsenseChanged: stats.commonsenseChanged || 0, // 批⑲ P6
							commonsenseFirstSnap: stats.commonsenseFirstSnap || 0, // 批⑲ P6
							commonsenseSkipErrors: stats.commonsenseSkipErrors || 0, // 批⑲ P6
							conflictPendingStale: stats.conflictPendingStale || 0, // 批⑲ P6
							conflictsPending: stats.conflictsPending || 0, // 批⑲ P6
							backlogErrors: stats.backlogErrors || 0, // 批⑲ P6
							ftsRebuildErrors: stats.ftsRebuildErrors || 0, // 批⑲ P6
							ftsRebuildRows: stats.ftsRebuildRows || 0, // 批⑲ P6
							episodicAnchorMiss: stats.episodicAnchorMiss || 0, // 批⑲ P6
							sweepErrors: stats.sweepErrors || 0, // 批⑲ P6
							singleBodyWarns: stats.singleBodyWarns || 0, // 批⑲ P6
							pendingMsgsCount: stats.pendingMsgsCount || 0, // 批⑲ P6
							watermarkAdvanced: stats.watermarkAdvanced || 0, // 批⑲ P6
							a25DroppedUnextracted: stats.a25DroppedUnextracted || 0, // 批⑲ P6
							noteIngestAuto: stats.noteIngestAuto || 0, // 批⑲ P6
							noteIngestFiles: stats.noteIngestFiles || 0, // 批⑲ P6
							edgeKeepAliveSkips: stats.edgeKeepAliveSkips || 0, // 批⑲ P6
							// 批⑲ P6 分档：时刻/布尔/诊断类不接注入面（bufferedChars/firehoseSeen/lastPatrolAt/lastPatrolDay/lastPatrolMerged/lastPatrolMergedIds·保留进程内诊断面）
							a10SkipErrors: stats.a10SkipErrors || 0, // 批⑲ P6 二档补（主 stats 面原缺·他对象有）
							backlogAccounts: stats.backlogAccounts || 0, // 批⑲ P6 二档补
							backlogSegments: stats.backlogSegments || 0, // 批⑲ P6 二档补
							promptUnsafe: stats.promptUnsafe || 0, // 批⑲ P6 二档补
							triplesIn: stats.triplesIn || 0, // 批⑲ P6 二档补
							usageWeeklyErrors: stats.usageWeeklyErrors || 0,
							usageQuality: usageQualityDigest(db, 7), // 检索质量仪表（09-12 第一步）：近 7 天 search 真命中成分（对象键·render 一行呈现·null=无样本）
							usageQualityErrors: core.usageQualityErrors, // 仪表聚合面异常累计（⑬ 四点接线：赋值+return+render+drill）
							writeTimeEnhanceErrors: core.writeTimeEnhanceErrors, // 09-12 主案：写时增强（两写入点）异常累计（⑬ 四点接线）
							writeTimeReflectLinked: stats.writeTimeReflectLinked || 0, // 提炼路反思建边命中数（观测位）
							vecBackfillRuns: core.vecBackfillRuns, // 向量补嵌可见化（09-12 第二步诊断·四点接线）
							vecBackfillFilled: core.vecBackfillFilled,
							vecBackfillNoCred: core.vecBackfillNoCred,
							vecBackfillErrors: core.vecBackfillErrors,
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
							autorecallSeatDedup: stats.autorecallSeatDedup || 0, // E-2 注入席互斥剔重数（2026-09-12：同条不入「待办席」与「召回区」两席）
							dupWriteBlocked: dupWriteStat.blocked || 0, // 梁1b 同文幂等闸：memory_write 同文重复拒写数（模块级对象·09-11 kimi 21 连发案治本件②·⑬ 四点接线齐）
							injectErrors: stats.injectErrors || 0, // 8-31 锈面修④：注入段兜底失败计数（F2-2 吞错出口透出）
							injectBlockErrors: stats.injectBlockErrors || 0, // 批⑳ J8-7：注入面十处块级降级计数（L 路实勘·块级失败原零痕迹）
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
							// ── ⑩ 任务账step/一闸·四键透出（E 路审计 I1：本仓「⑬ 计数不可见＝半合规」在批⑩ 自犯·补）──
							boardMirrored: stats.boardMirrored || 0, // ⑩A 板态镜像成功数
							boardMirrorSkipped: stats.boardMirrorSkipped || 0, // ⑩A 空/形状不符跳过数
							boardMirrorErrors: stats.boardMirrorErrors || 0, // ⑩A 镜像失败数
							// ── 架构 C（10-04 maintainer·双板镜像升双向）四键透出（⑬四点接线·return 面）──
							// option（10-04 maintainer·🟡-2 恒零根治）：三键改读 organ_meta（board_write 系 write 行注册·闭包 stats 跨 apply 不可达——唯一真相源 organ_meta·先例 patrol_last）。
							boardWriteBack: (() => { try { const r = db.prepare("SELECT v FROM organ_meta WHERE k='board_wb_counts'").get(); return r ? Number(JSON.parse(r.v).ok) || 0 : 0; } catch (eBWB) { ctx.logger?.warn?.("[living-memory] board_wb_counts 读失败(键ok): " + String(eBWB).slice(0, 60)); return 0; } })(), // 🟡B（2317 执行令·三十九审）：catch 静默→warn（假零真零可辨·⑬族）·board_write 回写 todo/write 会话事件成功数
							boardWriteBackSkip: (() => { try { const r = db.prepare("SELECT v FROM organ_meta WHERE k='board_wb_counts'").get(); return r ? Number(JSON.parse(r.v).skip) || 0 : 0; } catch (eBWB) { ctx.logger?.warn?.("[living-memory] board_wb_counts 读失败(键skip): " + String(eBWB).slice(0, 60)); return 0; } })(), // append 不可达跳过数
							boardWriteBackErrors: (() => { try { const r = db.prepare("SELECT v FROM organ_meta WHERE k='board_wb_counts'").get(); return r ? Number(JSON.parse(r.v).err) || 0 : 0; } catch (eBWB) { ctx.logger?.warn?.("[living-memory] board_wb_counts 读失败(键err): " + String(eBWB).slice(0, 60)); return 0; } })(), // 回写异常数
							boardMirrorSelfSrc: (() => { try { const r = db.prepare("SELECT v FROM organ_meta WHERE k='board_selfsrc_count'").get(); return r ? Number(r.v) || 0 : 0; } catch (eBWB) { ctx.logger?.warn?.("[living-memory] board_selfsrc_count 读失败: " + String(eBWB).slice(0, 60)); return 0; } })(), // ⑩A 自源事件分诊数（不镜像·防降格覆写）·option补移第四键：organ_meta 直读（同族三键同式·🟡B 同族全量补 warn——执行令字面三键·第四键 b3818ef 同式同病一并治·枚举面＝族特征词原则）
							mwYearClamped: (() => { try { const r = db.prepare("SELECT v FROM organ_meta WHERE k='mw_year_clamped'").get(); return r ? Number(r.v) || 0 : 0; } catch (eBWB) { ctx.logger?.warn?.("[living-memory] mw_year_clamped 读失败: " + String(eBWB).slice(0, 60)); return 0; } })(), // 年份钳制·手写面拒记计数（2317 §二·tools.cjs 写点 原子式·四点接线 return 面）
							mwYearRewritten: (() => { try { const r = db.prepare("SELECT v FROM organ_meta WHERE k='mw_year_rewritten'").get(); return r ? Number(r.v) || 0 : 0; } catch (eBWR) { ctx.logger?.warn?.("[living-memory] mw_year_rewritten 读失败: " + String(eBWR).slice(0, 60)); return 0; } })(), // 年份改写·手写面计数（10-06 1907 §一·tools.cjs 写点 原子式同构·四点接线 return 面）
							boardCloseWarn: (() => { try { const r = db.prepare("SELECT v FROM organ_meta WHERE k='board_close_warn'").get(); return r ? Number(r.v) || 0 : 0; } catch (eBWB) { ctx.logger?.warn?.("[living-memory] board_close_warn 读失败: " + String(eBWB).slice(0, 60)); return 0; } })(), // wrap-up断点提示计数（板 v2 余面⑤·tools.cjs board_write 写点 原子式·四点接线 return 面）
							ledgerSnapshots: stats.ledgerSnapshots || 0, // ⑩C 任务账快照入册数
							compactionSummarized: stats.compactionSummarized || 0, // 刀⑥ summary 段压缩总结入册+建边成功数
							organMetaPruned: stats.organMetaPruned || 0, // 三车：organ_meta 过期计数键清理数（四点接线·⑬）
							organMetaPruneErrors: stats.organMetaPruneErrors || 0, // AUDIT-FIX（A-D3）：错误侧补透出（原只自增+warn）
							// ── 09-03 P0 修复车十键透出（⑬ 纪律：计数不可见=半合规同型病·自立法不自犯）──
							entityExtractErrors: stats.entityExtractErrors || 0, // entity 段失败（原型链闸后仍兜底）
							g2ArchiveErrors: stats.g2ArchiveErrors || 0, // G2 归档失败（主卷不覆写保护在役）
							compactBridgeSecReject: stats.compactBridgeSecReject || 0, // 批⑰ 压缩直插路安检拒
							compactBridgeSensitiveSkip: stats.compactBridgeSensitiveSkip || 0, // 批⑰ 压缩直插路敏感闸跳
							mirrorWmErrors: stats.mirrorWmErrors || 0, // mirror 水线读写失败
							autorecallErrors: stats.autorecallErrors || 0,
							autorecallHookDead: stats.autorecallHookDead || 0, // 批⑲ P1
							sweepCallErrors: stats.sweepCallErrors || 0, // v4b（10-03）：sweep 调用期异常（⑬ 四点接线·core render 同键）
							sweepHookDead: stats.sweepHookDead || 0, // v4b（10-03）：sweep 挂载注册期异常
							patrolCrossSkip: stats.patrolCrossSkip || 0, // 批⑲ P2
							a12Truncated: stats.a12Truncated || 0, // 批⑲ P5：账 400 帽截断计数（批㉑ Y3 订正：原行误挂「召回整链异常」残注——批⑰ B5② counter族）
							pressureAlertErrors: stats.pressureAlertErrors || 0, // 换窗警报写库失败
							extractDupSkipped: stats.extractDupSkipped || 0, // 中断重抽闸拦截数
							signalErrors: stats.signalErrors || 0, // tool/result 信号段异常
							// ── 09-03 P2 修复车九键透出（⑬ 纪律同法：计数不可见=半合规）──
							bufferRestoreErrors: stats.bufferRestoreErrors || 0, // extract_buffer 启动恢复失败
							a25KeysWritten: stats.a25KeysWritten || 0, // 批⑳ J7-8：拆键后 flush 实写键数（写放大观测）
							a25TombCleared: stats.a25TombCleared || 0, // 2026-09-22 戊路R-2：墓碑清删数（原载荷缺席⇒死位）
							a25TombSkipped: stats.a25TombSkipped || 0, // 2026-09-22 戊路W-1：放弃/闸拒而跳过的墓碑数
							validatedErrors: stats.validatedErrors || 0, // 段异常
							semanticEdgeErrors: stats.semanticEdgeErrors || 0, // 段异常
							pressureReadErrors: stats.pressureReadErrors || 0, // 压力缓存读段失败（「无高压」假绿防线）
							spokenFillWmErrors: stats.spokenFillWmErrors || 0, // spoken 观测键写失败
							promptSafeHits: PROMPT_SAFE_STAT.hits, // 2026-09-13 注入面净化车：境外数据出境前「连续双花括号」破坏计数（>0 ⇒ 库内仍有雷被兜住·★0=干净）
							extractEventsErrors: stats.extractEventsErrors || 0, // extract events 增强段异常
							extractParseFail: stats.extractParseFail || 0, // 批⑰ 提炼解析失败可辨化计数（J3-1 修）
							extractParseFailGiveUp: stats.extractParseFailGiveUp || 0, // 批⑱ K1-3：解析连败弃段计数
							conflictsPreverdicted: stats.conflictsPreverdicted || 0, // AUDN 预裁counter（wave·pending 已裁数）
							assistantDistillLines: stats.assistantDistillLines || 0, // wave 蒸馏行计数
							entitiesResolved: stats.entitiesResolved || 0, // wave 消解计数
							conflictsPreclassifErrors: stats.conflictsPreclassifErrors || 0,
							preverdictApiFails: stats.preverdictApiFails || 0, // 10-24 加活②：预裁端点失败计数（index.cjs preclassifyConflict 两处 noteApiFail 旁自增，10-25 起 fetch 抛错末次也计·进程内）
							evoFallbackWindows: stats.evoFallbackWindows || 0, // 回落窗使用数——09-06 审计补接（乙批：赋值 L7143/透出读 L319 在而 return 漏=三点断一线永不显示·⑬ 自犯第三例根治；drill=render-stats-drill T0 动态断言新增键自动进面）
							closureCheckErrors: core.closureCheckErrors, // 四闸核心异常累计（模块级·同 toolUsageErrors 族）
							// ── 序2 观测键七枚（09-15 closure gate三案＋门控 P0·maintainer）——⑬四点接线：赋值（模块级/runExtract 内）↔ return ↔ render ↔ drill ──
							closureGateBlocked: core.closureGateBlocked, // closure gate拦截总数——B-2 订正口径（done证≥5,000 且仍 0 才判失效·换班窗未满=insufficient-data·原「归零即失效」已废）
							closureGateAnchorWeak: core.closureGateAnchorWeak, // ②门槛 1→2 副作用：单锚样本放行计数
							closureGateDoneEvidence: core.closureGateDoneEvidence, // 证据池 done todo 累计（④-a 豁免面基线）
							closureGateFreshDoneSkip: core.closureGateFreshDoneSkip, // ④-a 时效豁免命中（N=24 外推counter·标定数据源）
							closureGateSeqClash: core.closureGateSeqClash, // 判定精确化（#31515）：踵次/编号冲突放行计数（同单位不同数字⇒不同题）
							closureGateFreshDoneIds: closureGateFreshDoneIds.size, // B-2②（批 C 候办）：豁免去重**条**口径（当日 Set size·量纲=条·N 标定真数据源）
							hitMapErrors: hitMapErrorsStat.n, // B-1④（批 C 候办）：hitMap 攒批/落盘失败（进程口径·累计口径 organ_meta['hit_map_errors']）
							gateBlocked: stats.gateBlocked || 0, // 门控 salience 观测态（首期只计数不真拦）
							gateMerged: stats.gateMerged || 0, // 同题合并路命中（A 路＋D14 吸收）
							gateVcBumped: stats.gateVcBumped || 0, // 合并路 vc bump 次数
							gateEdgeErrors: stats.gateEdgeErrors || 0, // 审计 A③（09-15）：B 升级路回指边失败计数（原静默）
							gateSpaceFallback: stats.gateSpaceFallback || 0, // A① 必带（09-15）：sessionOrgan 判不出→词面/global 回落次数（门控共用三级值·「在多少条上走了回落」可回溯）
							autoTodoClosedSkip: stats.autoTodoClosedSkip || 0, // 批⑯(design note)：extraction gate todo→fact 降格实落计数（auto 路禁新鲜豁免面·实落才计）
							// ── 2step一四键 return 补接线（design-approved·⑬四点接线上半）：render 已画（core memoryRender「禁立降格/候并立/候转正/销叙跳」）而 return 缺＝「画了而 return 缺」缺陷形态——stats 观察窗（#31552②）恰要看本键·盲区根治 ──
							autoTodoV2Demote: stats.autoTodoV2Demote || 0, // 刀一①：auto todo 非候语义降 fact 实落计数（默认禁立面）
							autoWaitMerged: stats.autoWaitMerged || 0, // 刀一②：候面同题并而不立（vc+1）计数
							autoWaitPromoted: stats.autoWaitPromoted || 0, // 刀一②：候语义 fact→todo 装计数（晋位面）
							autoStateDemote: stats.autoStateDemote || 0, // 补丁车项2（五十审 🟡-3）：止血降格实落计数（四点接线·read return 位）
							patrolGateSkip: stats.patrolGateSkip || 0, // nightly patrol写入闸拦截计数（写入面修·R1 车·⑬四点接线②return；赋值在 patrol.cjs _patrolGateSkip）
							obsGroups: (() => { try { return core.obsLedgerFromRender().groups; } catch (eOG) { void eOG; return null; } })(), // obs 归一总账（10-09 maintainer·memory organ自办）：render 全键按类目归组计数（键自省·零维护·「子系统×状态」矩阵类目视图·零删键）
							obsUnclassified: (() => { try { return core.obsLedgerFromRender().unclassified; } catch (eOG2) { void eOG2; return null; } })(), // obs 未归类键数（0=规则全覆盖·非零=新键出现/规则待扩）
							autoWaitFuzzyDup: stats.autoWaitFuzzyDup || 0, // 丁项（10-07 maintainer·四十七审再检查）：同题查重升级词面级并而不立命中计数（⑬四点接线②return）
							autoWaitSeqClash: stats.autoWaitSeqClash || 0, // 丁项：序数对撞放行计数（踵|审|刀|案|批|件|车|令 同单位不同号⇒不并）
							saoNarrSkip: stats.saoNarrSkip || 0, // 刀一③：销叙事无疑义 skip 计数（闭环以 closed_at 为唯一凭）
							evYearClamped: stats.evYearClamped || 0, // 年份钳制（10-04 maintainer）：|event_at 年−ts 年|>1 拒记计数（提炼面·LLM 幻觉年）
						evYearRewritten: stats.evYearRewritten || 0, // 年份改写（10-06 1907 §一·maintainer按荐）：|=1 且月日同 ts ⇒ 改年计数（提炼面）
							edgeSinked: stats.edgeSinked || 0, // 刀三（2331）：被 solved_by/duplicate_of 活边指向条检索沉底计数——render 已画（「边沉底」）而 return 缺＝ 同族·render-stats T0 修提取面后当场抓出·本车补
							validToMapErrors: stats.validToMapErrors || 0, // 批⑰ C13：validToMap 裸 catch 补出口
							essentialSeatAgedOut: stats.essentialSeatAgedOut || 0, // C-1③（09-15 批 C）：锚点类超期退席计数（「永久占席」治理读数）
							sensitiveWarnCount: stats.sensitiveWarnCount || 0, // option第二步（09-15）：L2 魂红线词面 warn 计数（只 warn 不跳·入册留痕）
							// ── 序2 最小刀（guard 漂移/写路拒）──
							guardRulesDrift: core.guardRulesDriftCount, // ①②级分叉键数（0=无·>0=②级裁定未落地）
							guardRulesDriftKeys: core.guardRulesDriftKeys, // 分叉键名（诊断面）
							boardRowsPruned: core.boardRowsPruned, // 丁（09-27）：板行 GC 删除计数（nightly patrol超 30 天行）
							boardRowsPrunedSids: stats.boardRowsPrunedSids || [], // 甲1 订正：删前留痕出口（原写点无出口＝死写）
							// 板机制六缺口车·乙批（10-18）：设 LEGION_BOARD_GC_OPEN_DAYS 后nightly patrol板 GC 留下的超 30 天未完板数（organ_meta 键 board_gc_skip_rows·本轮值）——
							//   开关不认即不出、连库都不查（盲审中-1：撤开关回滚后库里留着旧行，也与本车前逐字同）；开关认而键不在亦不出
							...(() => {
								if (core.boardGcOpenDays() === null) return {};
								try {
									const r = db.prepare("SELECT v FROM organ_meta WHERE k='board_gc_skip_rows'").get();
									return r
										? {
												boardGcSkipRows: Number(r.v) || 0,
											}
										: {};
								} catch (eGS) {
									ctx.logger?.warn?.("[living-memory] board_gc_skip_rows 读失败: " + String(eGS).slice(0, 60));
									return {};
								}
							})(),
							boardSnapErrors: stats.boardSnapErrors || 0, // 二十四审⚠️1 修：⑩C 板态取数错计数出口（原仅自增＝假绿）
							wsSnapErrors: stats.wsSnapErrors || 0, // O2（09-30 压缩面优化车）：工作区快照失败计数出口（F5·原无此面＝写失败静默）
							boardLedgerOk: stats.boardLedgerOk || 0, // L2（10-01 落码·approved）：①一致 id 对数（最近一次注入时快照·观测）
							boardLedgerDrift: stats.boardLedgerDrift || 0, // L2：②真漂移（板 completed ∧ 账 active·非零即显·方向订正版）
							boardLedgerUnlinked: stats.boardLedgerUnlinked || 0, // L2：③未open item（无 id/非 todo·**禁入告警**＝回退红线·只计数）
							boardLedgerErrors: stats.boardLedgerErrors || 0, // L2：三桶计算异常计数（⑬出口）
							// 板机制六缺口车·甲批（10-18）：缺口 2、3 三键（最近一次注入时快照）——总闸 LEGION_BOARD_HINT_OFF=1 不出键（stats 键表与本车前同）
							...(core.boardHintOn()
								? {
										boardLedgerLag: stats.boardLedgerLag || 0, // 板 pending/in_progress ∧ 所open item非 active（按板项计·只计数不进三桶行）
										boardLedgerUnlinkedNoId: stats.boardLedgerUnlinkedNoId || 0, // 未open item之无号项
										boardLedgerUnlinkedBadRef: stats.boardLedgerUnlinkedBadRef || 0, // 未open item之有号而无一中 todo 账
									}
								: {}),
							dispatchStale: core.dispatchStale, // C 件：流转超时滞留数（48h+·本轮值）
							dispatchStale7d: core.dispatchStale7d, // C 件：超 7d 滞留(design note)
							globalDispatchCap: core.globalDispatchCap, // A 件：数量帽触发计数
							guardSource: (GUARD && GUARD.source) || "", // 在役判据来源（①级路径/②级路径/floor）
							guardSensitiveMd5: String(
								(GUARD && GUARD.sensitive && GUARD.sensitive.source) || "",
							).length
								? require("node:crypto")
										.createHash("md5")
										.update((GUARD.sensitive && GUARD.sensitive.source) || "")
										.digest("hex")
										.slice(0, 8)
								: "", // 在役 sensitive 串指纹（前 8 位·「哪级在役内容是什么」一眼可核）
							writeRejectedCount: core.writeRejectedCount, // 写路安检拒计数（原零计数零 warn·事后不可审计）
							sourceOverrideCount: core.sourceOverrideCount, // E 甲′：source 会话前缀保护覆盖计数（模块级·2026-09-24 落码车）
							softSameTopicWarn: core.softSameTopicWarn, // C：软同题观测计数（模块级·闸①同判据·2026-09-24 落码车）
							echoWarnCount: core.echoWarnCount, // 回声车（09-25 批③）：fact/lesson 已办回声观测计数
							surgerySkipVecEmbed: core.surgerySkipVecEmbed, // 挂牌期惰性补嵌跳过
							surgerySkipWrite: core.surgerySkipWrite, // 挂牌期写入冻结
							queryClassDist: { ...queryClassDist }, // SelRoute 六类分布（观测一周·本期零权重变更）
							// ── counter白名单（step·07:30 maintainer·夜审wave F1 清单）：赋值面 35 键 return 仅 23——14 counter补全 ──
							bufferRestored: stats.bufferRestored || 0,
							semanticConflictsFound: stats.semanticConflictsFound || 0,
							// ── P0-4（09-12 maintainer）帽治理观测位（四点接线：赋值nightly patrol段／落账 patrol_last／return 此处／render「池·采样·帽跳」）──
							a10PoolSize: stats.a10PoolSize || 0,
							a10Sampled: stats.a10Sampled || 0,
							a10SkippedDueToCap: stats.a10SkippedDueToCap || 0,
							a10RotSlot: stats.a10RotSlot || 0,
							// F2（二轮审计）：两键原「只接一点」（赋值在·三面缺）⇒ 补 return（render/patrol_last 另补）
							a10RotCursorErrors: stats.a10RotCursorErrors || 0,
							patrolLastPatchErrors: stats.patrolLastPatchErrors || 0,
						crossOrganErrors: stats.crossOrganErrors || 0,
						evoMetricErrors: stats.evoMetricErrors || 0,
						forelookLessons: stats.forelookLessons || 0,
						forelookErrors: stats.forelookErrors || 0,
							// E-4（09-12 批 A）evo-mirror 水线：本巡入/跳（0/0＝空转可辨·非静默）
							evoMirrorIns: stats.evoMirrorIns || 0,
							evoMirrorSkip: stats.evoMirrorSkip || 0,
							stalePoolNow: stats.stalePoolNow || 0,
							stalePoolT7: stats.stalePoolT7 || 0,
							stalePoolT14: stats.stalePoolT14 || 0,
							stalePoolT30: stats.stalePoolT30 || 0,
							stalePoolErrors: stats.stalePoolErrors || 0,
							// E 主车（09-12 maintainer·E 洪峰容量化·⑬ 四点接线③return）：三观测位＋带宽/错误面
							staleReviewPoolSize: stats.staleReviewPoolSize || 0,
							staleAutoRetired: stats.staleAutoRetired || 0,
							staleSinkSent: stats.staleSinkSent || 0,
							staleSinkBandwidth: stats.staleSinkBandwidth || 0,
							staleSinkErrors: stats.staleSinkErrors || 0,
							// E-7（09-12 批 A）：API 用量/成本计数（模块级累计·**重启清零**·原始为全仓零可见）
							apiUsageEmbed: apiUsage.embed,
							apiUsageExtract: apiUsage.extract, // D2 修正：**主提炼**（原误指 AUDN 预裁）
							apiUsagePreverdict: apiUsage.preverdict, // D2：预裁（分家后独立键）
							apiUsageSpoken: apiUsage.spoken,
							apiUsageRerank: apiUsage.rerank,
							apiUsageCalls: apiUsage.calls,
							apiUsageNoUsage: apiUsage.noUsage, // D2：成功但无 usage 字段（下界口径可解释性）
							apiUsageHttpErr: apiUsage.httpErr, // D2：失败/重试次数（token 无出口·下界口径）
							// E-1（09-12 批 A）：注入预算总账（实测字节·last/max/samples ＋ 超线计数）
							injectCtxBytes: injectMeter.ctxLast,
							injectCtxMax: injectMeter.ctxMax,
							injectWfBytes: injectMeter.wfLast,
							injectWfMax: injectMeter.wfMax,
							injectTotalMax: injectMeter.totalMax,
							injectSamples: injectMeter.samples,
							injectOverBudget: injectMeter.overBudget,
							// 注入瘦身车 W4（10-11·依据 设计/注入瘦身/落码task brief.md §3.10）：同轮账、冻结、帽与 D 案下沉两数（进程内·重启清零）＋持久四键（organ_meta·跨重启累计）
							injectTurnBytes: injectMeter.turnLast || 0,
							injectTurnMax: injectMeter.turnMax || 0,
							injectTurnP50: core.injectTurnPct(50),
							injectTurnP95: core.injectTurnPct(95),
							injectTurnN: injectMeter.turnN || 0,
							injectFreezeHits: injectMeter.freezeHits || 0,
							injectFreezeBreaks: injectMeter.freezeBreaks || 0,
							injectFreezeUnknown: injectMeter.freezeUnknown || 0,
							injectSettleLate: injectMeter.settleLate || 0,
							injectOverCap: injectMeter.overCap || 0,
							injectOverCapProtected: injectMeter.overCapProtected || 0,
							injectTrimmed: injectMeter.trimmed || 0,
							injectRecallDedup: injectMeter.recallDedup || 0,
							injectMeterErrors: injectMeter.errors || 0,
							injectSlim: process.env.LEGION_INJECT_SLIM_OFF === "1" ? 0 : 1,
							injectTurnLog: (injectMeter.turnLog || []).map((o) => ({ ...o, shown: [...o.shown], cut: [...o.cut], xd: [...o.xd], trim: [...o.trim] })),
							injAxisFresh48: stats.injAxisFresh48 || 0,
							injAxisAged30: stats.injAxisAged30 || 0,
							injectRecallCutTotal: readNudgeTotal("inject_recall_cut_total"),
							injectRecallCutReadTotal: readNudgeTotal("inject_recall_cut_read_total"),
							injectRecallShownReadTotal: readNudgeTotal("inject_recall_shown_read_total"),
							injectMidTurnUserClaimsTotal: readNudgeTotal("inject_mid_turn_user_claims_total"),
							injectRecallCutPtrTotal: readNudgeTotal("inject_recall_cut_ptr_total"), // wave 车二-4：发出的被截指针数（持久）
							injectRecallCutSearchTotal: readNudgeTotal("inject_recall_cut_search_total"), // wave 车二-4：search 选中本轮被截条的次数（持久）
							c2CutSearchErrors: stats.c2CutSearchErrors || 0, // wave 车二-4：cut_search 计数失败（进程内）
							mergeExecuted: stats.mergeExecuted || 0,
							c2EpisodicHits: stats.c2EpisodicHits || 0, // wave 车二-8f：read_episodic 成功回流计入命中的次数（进程内）
							...(core.memGetOn() ? { memGetCount: stats.memGetCount || 0, memGetAged: stats.memGetAged || 0, memGetHits: stats.memGetHits || 0 } : {}), // wave 复活口·阶段 1：get 取到条目的次数、其中已老化条、计入命中（进程内；get 关时三键不出）
							...(core.reviveOn()
								? {
										reviveRecorded: { ...core.reviveStat.recorded }, // wave 复活口·阶段 2：记下的复活候选按触发类别分(design note)
										reviveRecordSkipRO: core.reviveStat.skipRO, // 只读连接上跳过记候选的次数（进程内）
										reviveRecordErrors: core.reviveStat.errors, // 记候选出错（进程内）
										reviveRecallNoSpace: core.reviveStat.recallNoSpace, // maintainer重提：调用窗空间认不出、没比的消息数（进程内）
										reviveSkipAdmin: { ...core.reviveStat.skipAdmin }, // wave 阶段 5：管理窗（memory organ、brain）不记的候选，分 get、cite（按本该记下的 aged 条数；进程内）
										revivePending: (() => {
											try {
												const r = db.prepare("SELECT COUNT(*) AS c FROM revive_pending").get();
												return r ? Number(r.c) : 0;
											} catch {
												return 0;
											}
										})(), // 待nightly patrol复活段处理的候选数（库；表未建为 0）
										reviveLast: stats.reviveLast ?? null, // wave 复活口·阶段 3：本进程最近一次跑完（COMMIT）的nightly patrol复活段复活的条数（null＝本进程没跑完过，0＝跑完复活 0 条）
										reviveRevived: stats.reviveRevived || 0, // nightly patrol复活累计（进程内）
										reviveTotal: readNudgeTotal("life_revive_total"), // nightly patrol复活累计（持久，organ_meta life_revive_total）
										reviveByWhy: { ...(stats.reviveByWhy || {}) }, // 复活条按触发类别分（一条可占多类；进程内）
										reviveSkip: { ...(stats.reviveSkip || {}) }, // 复核不合跳过的候选按理由分（gone、notAged、todo、superseded、patchedDst、duplicateSrc、stateTitle、sentinel；进程内）
										reviveCapHit: stats.reviveCapHit || 0, // 超每夜帽次数（进程内）
										reviveErrors: stats.reviveErrors || 0, // nightly patrol复活段出错次数（整段回滚、当夜不复活；进程内）
										reviveManualTotal: readNudgeTotal("life_manual_total"), // wave 阶段 4（键名照复活开关一族以 revive 起头）：memory_write renewTargetId 手工改期／复活累计（持久，organ_meta life_manual_total）
									}
								: {}), // 复活关（LEGION_REVIVE_OFF=1）时这几键都不出
							edgesMigrated: stats.edgesMigrated || 0,
							c2SoftSinkAdded: stats.c2SoftSinkAdded || 0, // wave 车二-8e：软沉底补充池并入候选的条数（进程内）
							c2SoftSinkErrors: stats.c2SoftSinkErrors || 0, // wave 车二-8e：补充池查询失败（进程内）
							validatedUp: stats.validatedUp || 0,
							c2Life7: stats.c2Life7 || 0, // wave 车二-8d：本车改成 7 天并标 stale_by='car2-life:7d' 的自动条数（进程内）
							c2LifeTierErrors: stats.c2LifeTierErrors || 0, // wave 车二-8d：寿命分档失败（进程内）
							lifeRenew: { ...(stats.lifeRenew || {}) }, // wave 车二-8b：nightly patrol续期条数按理由分（进程内；笔 6 未接出口，本笔补）
							lifeRenewTotal: readNudgeTotal("life_renew_total"), // wave 车二-8b：续期累计（持久）
							lifeRenewCapHit: stats.lifeRenewCapHit || 0, // wave 车二-8b：续期超每夜帽次数（进程内）
							lifeRenewLast: stats.lifeRenewLast ?? null, // wave 补笔盲审处置（10-18·宿主契约 S1）：本进程最近一次跑完（COMMIT）的续期段续的条数（null＝本进程没跑完过续期段，0＝跑完续 0 条；出错回滚的那夜不改它，看 lifeRenewErrors；进程内）
							lifeRenewCapped: stats.lifeRenewCapped || 0, // wave 补笔④（10-18）：续期帽挡下的条数（续满 2 次、上次续期后无真命中，进程内）
							lifeRenewErrors: stats.lifeRenewErrors || 0, // wave 补笔④（10-18）：nightly patrol续期段出错次数（整段回滚、当夜不续，进程内）
							lifeRenewHitsErrors: stats.lifeRenewHitsErrors || 0, // wave 补笔盲审处置（10-18·宿主契约 S2）：命中结算写 life_renew.hits 失败次数（当夜真命中没记上，续期段照跑；进程内）
							todoAgeClosed: stats.todoAgeClosed || 0, // wave 车二-8c：待办老化前查得已闭的条数（进程内）
							todoAgeHeld: stats.todoAgeHeld || 0, // wave 车二-8c：待办满 14 天未闭而留的条数（进程内）
							semanticEdges: stats.semanticEdges || 0,
							askgateMode: stats.askgateMode || "", // wave 车二-1：开口前retrieval gate态（off＝总闸关；probe＝缺省只探针计数；armed＝LEGION_ASKGATE_ON=1 还没发过；steer＝真续步过；inject＝真走过降级路）
							askgateHooks: { ...(stats.askgateHooks || {}) }, // wave 车二-1：宿主挂点被调次数（stop、pre、post、sub、claim·进程内；首次派发时刻见 organ_meta askgate_probe）
							askgateErrors: stats.askgateErrors || 0, // wave 车二-1：闸内出错（进程内）
							askgateTotals: Object.fromEntries(["fired", "steer", "deny", "ctx", "pass", "nohit", "skip", "follow", "stash", "inject"].map((k) => [k, readNudgeTotal("askgate_" + k + "_total")])), // wave 车二-1：闸各项累计（持久，organ_meta askgate_*_total）
							semanticEdgeSkippedDueToCap: stats.semanticEdgeSkippedDueToCap || 0, // 批⑰ 降采样轮转未入窗条数（复活面观测）
							chunkBackfillDone: stats.chunkBackfillDone || 0, // 批⑰ B6：chunks nightly patrol回补入量
							chunkBackfillErrors: stats.chunkBackfillErrors || 0,
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
									const sid = sessionOfLastTurn() || null;
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
							jieba: !!core.jieba,
						};
					}
					// ── wave 复活口·阶段 1（10-18·草纲下篇一改点 1）：按 id 取全文 get（纯读）──
					//    回全文与状态（status、stale_state、stale_at、stale_by、valid_to、closed_at、aged、ttl 标等），已老化条也能取；单次至多 MEM_GET_BYTES 字节，
					//    按 nextOffset 续读（core.memGetChunk）。get 本体不直接写库：桥的 memory 工具 schema 是插件参数的深拷贝，get 会出现在桥上、
					//    在桥的只读连接上执行（草纲码 6）。「算不算用上」照wave 8-f：首段（offset=0）取到 active 条时走 read_episodic 同一条命中记账路 c2HitBump
					//    （进程 Map＋organ_meta hit_pending；桥装载时设了 __legionHitRestoreFailed，只进桥进程内 Map、不落库）；状态句标题、写时盖戳的被取代条、
					//    LIFE 总闸关时不计，与 read_episodic 同判；续读段不重计。复活候选不在这里记（阶段 2 在 index.cjs 的 tool/call 事件里记，桥没有该事件）。
					//    LEGION_MEM_GET_OFF=1 时本分支不受理，落到下面「未知 action」，与底座同
					if (action === "get" && core.memGetOn()) {
						const id = Number(args.id);
						if (!Number.isInteger(id) || id <= 0) return { kind: "memory-get", error: "get 需要 id 参数（条目 id，正整数）" };
						const row = db.prepare("SELECT * FROM memories WHERE id = ?").get(id);
						if (!row) return { kind: "memory-get", id, found: false, note: "条目不存在 #" + id };
						const ck = core.memGetChunk(row.content, args.offset);
						const out = { kind: "memory-get", found: true, id: row.id };
						for (const k of ["type", "space", "title", "ts", "event_at", "source", "status", "stale_state", "stale_at", "stale_by", "valid_to", "closed_at", "superseded_by", "essential", "validated_count", "last_hit_at", "confidence"])
							if (Object.prototype.hasOwnProperty.call(row, k)) out[k] = row[k];
						out.aged = row.status === "aged";
						out.ttl = row.stale_state === "ttl";
						Object.assign(out, ck);
						stats.memGetCount = (stats.memGetCount || 0) + 1;
						if (out.aged) stats.memGetAged = (stats.memGetAged || 0) + 1;
						if (ck.offset === 0 && process.env.LEGION_LIFE_GUARD_OFF !== "1" && row.status === "active" && row.stale_state !== "superseded-auto" && !(CRIT.lifeStateTitle && CRIT.lifeStateTitle.test(String(row.title || ""))) && c2HitBump(db, row.id))
							stats.memGetHits = (stats.memGetHits || 0) + 1;
						return out;
					}
					return {
						error:
							"未知 action：" +
							action +
							"（只读三 action：search/timeline/stats；写库用 memory_write）",
					};
	};
}
module.exports = { setupRead };
