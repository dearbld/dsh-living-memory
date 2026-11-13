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
// ══ ⑤-2 会话板工具（design-approved「继续开」·研判二 C 档＋四点吸收①②③）══
//   把「板」从对话历史搬出：board_write 整表替换（TodoWrite 意·last-write-wins·按 sid 一行）；


module.exports = {
	name: "dsh-living-memory",
	inject: ["tools", "credentials", "systemPrompt"],
	// ── step：写分权双角色。role='write'（preset 层挂载行专用）只注册 memory_write 工具；
	//    role='host'（默认，host 层挂载行）注册只读 memory 工具 + 内部管道（nightly patrol/提炼/注入/面板）。
	//    挂载矩阵：memory-zh / standard-zh 组合文件挂 write 行；其余space只继承 host 行=只读。──
	apply(ctx, config) {
		const role = (config && config.role) || "host";
		const tools = ctx.tools;
		// ── 状态与提炼缓冲（架构 C·10-04 上移：原在 host 路径 L1956——write 行 L1769 提前 return 恒不初始化
		//    ⇒ board_write 的 statsGet 惰性取值 TDZ 炸。上移至 role 分支前＝两行共用·纯字面量零副作用）──
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
			require("./tools.cjs").registerMemoryWrite(
				tools,
				dbW,
				() => writeSessionId,
				ctx.credentials?.resolve?.bind(ctx.credentials),
				ctx.logger,
			);
			// ⑤-2 会话板工具注册（design-approved「继续开」）——与 memory_write 同连接/sid 源
			try {
				require("./tools.cjs").registerBoardTools(tools, dbW, () => writeSessionId, ctx.logger); // option（10-04 maintainer）：statsGet 撤除——计数走 organ_meta（跨 apply 直读）·闭包 stats 方案证伪
			} catch (eBtr) {
				ctx.logger?.warn?.(
					"[living-memory] 板工具注册失败: " + String(eBtr).slice(0, 80),
				);
			}
			require("./tools.cjs").setupCredentialSet({ ctx, tools });
			return;
		}
		const credentials = ctx.credentials;
		const systemPrompt = ctx.systemPrompt;

		// ── 数据库（WAL：读不阻塞写，写串行——多窗口并发安全）──
		let db;
		try {
			db = core.openDb(DB_PATH, ctx); // [拆④] 建库段逐字归 core.openDb（行为等价：错时同 catch warn+return）
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
			// ── 车二 FTS 重建：实现点＝**nightly patrol入口**(design note)──
			//   ⚠ 首版曾误落**启动路径**（我方实现偏差·已按批字改正）：重建只在 nightPatrol() 开头执行。
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

		// [拆②] 段A（注入主块 L1954-2890）已抽 inject.cjs setupInject·调用后移 deps 定义后（v3·修 v2 段A漏删事故）
		// ── 状态与提炼缓冲：stats 定义已上移 role 分支前（架构 C·10-04·write 行提前 return 恒不达·TDZ 根治）──
		let buffer = "";
		// ── 消息级水位（wave首发·design-approved「补」·graph-memory getUnextracted/markExtracted 水位意融入）──
		//    治：buffer 纯字符流拼接=提炼失败后重抽全段（重放风险）；水位=turn/seq 粒度精确追踪抽到哪。
		//    审计 D2 修正（13:44·键族同型第 N 犯）：原插件级单例队列=多窗消息混队——甲窗成功推水位后乙窗
		//    消息被当已抽跳过（跨窗丢抽）。改 Map<sid, {msgs, watermark, seq}> 会话分账（bindMap 同款结构）。
		//    sid 键源=session.id（firehose 主键·L996 sessionOfLastTurn 同源）；env LEGION_A12_OFF 一键回退旧 buffer 路。
		const pendingBySid = new Map(); // sid -> { msgs: [{turn,role,text,seq}], watermark: number, seq: number, lastTs: number }
		const a25Tombstones = new Set(); // 批㉒ option（L2 路发现②·maintainer「实勘推理下提前修」）：本进程已删账墓碑——flush 只清墓碑键（跨进程零误删）
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
					if (a.msgs.length === 0) {
						pendingBySid.delete(k); // 空账顺手清（全已提炼·释放 Map 位·不占帽）
						a25Tombstones.add(k); // 批㉒：墓碑记档（flush 时清盘上键）
					}
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
					a25Tombstones.add(oldestK); // 批㉒：超帽淘汰亦记墓碑
				}
				// ── 批⑳ J7-8（L 路实勘·extract_buffer 1.15MB ×2.53 恶化）：单键整表序列化拆 per-session 键——
				//    只写 _dirty 账（写放大从 O(全缓冲) 降 O(增量)）·盘上过期键同步清·多进程 lost-update 面收窄到同 sid；
				//    旧单键 'extract_buffer' 由恢复段迁移（加载即标脏→本次 flush 落 per-sid 键后此处删除·不双读）。
				const stmt25 = db.prepare(
					"INSERT INTO organ_meta (k, v) VALUES (?, ?) ON CONFLICT(k) DO UPDATE SET v = excluded.v",
				);
				let wrote25 = 0;
				for (const [k, a] of pendingBySid) {
					if (!a._dirty) continue;
					stmt25.run(
						"extract_buffer." + k,
						JSON.stringify({
							msgs: a.msgs,
							watermark: a.watermark,
							seq: a.seq,
							lastTs: a.lastTs,
						}),
					);
					a._dirty = false;
					a25Tombstones.delete(k); // 批㉒b（自审抓洞）：写键成功即撤墓碑——防「删后重建」场景同 flush 先写后清自冲突（净删盘键·崩溃窗真丢账）
					wrote25++;
				}
				// 批㉒ option（maintainer「实勘推理下提前修」·L2 路发现②）：过期键清改 **tombstone 白名单**——
				//   原全表扫 `!pendingBySid.has(k25)` 即清（批㉑ W1 注释所述三删除面同步）：多进程形态下他进程内存
				//   持有的账键必被本进程误清（丢账交集＝清后彼进程崩溃且无新消息·L2 路实勘坐实）。
				//   修：只清本进程四删除点（空账/超帽/chase 毕/2h 清账）记档的墓碑键——语义收敛「只删本进程确信其盘上副本已无未抽段的账」
				//   ·跨进程零误删；_a25LegacyOK 闸保留（恢复失败态禁一切减法·批㉑ W1 语义不动）。
				if (stats._a25LegacyOK && a25Tombstones.size > 0) {
					const del25 = db.prepare("DELETE FROM organ_meta WHERE k = ?");
					for (const k25 of a25Tombstones) {
						del25.run("extract_buffer." + k25);
							stats.a25TombCleared = (stats.a25TombCleared || 0) + 1; // 2026-09-22 丁路F-4：墓碑清删观测（减法面必留痕）
						a25Tombstones.delete(k25);
					}
				}
				stats.a25KeysWritten = (stats.a25KeysWritten || 0) + wrote25; // 观测：每次 flush 实写键数（对照旧单键每触全账写）
				try {
					// 旧单键退役（幂等）——批㉑ W1（M 路审计）：闸改「本进程恢复段全轮成功」（stats._a25LegacyOK）∧（实写过 per-sid 键∨盘上已存在）
					//   ——原闸验的不是「迁移成功」：恢复 parse 失败态下旧单键（损坏但物理在盘可人工修复的唯一副本）会被后续任一 flush 物理消灭。
					if (
						stats._a25LegacyOK &&
						(wrote25 > 0 ||
							db.prepare(
								"SELECT 1 FROM organ_meta WHERE k LIKE 'extract_buffer.%' LIMIT 1",
							).get())
					)
						db.prepare("DELETE FROM organ_meta WHERE k = 'extract_buffer'").run();
				} catch {}
				// ── 批⑰ B5②（2026-09-21·J 路 B 级）：救援四计数落 organ_meta 持久化——
				//    进程态 stats 重启即失 ⇒ 「观察一周再议」永远无数据面（J10 兑现件）。随 落盘同车写（同节流窗·非热路径）；
				//    跨重启累计＝首 flush 读旧值作基线＋进程内增量（多进程并发末写赢·微损可容·注记备察）。
				if (!stats._rescueBase) {
					try {
						stats._rescueBase = JSON.parse(
							db.prepare("SELECT v FROM organ_meta WHERE k='rescue_counts'").get()
								?.v || "{}",
						);
					} catch {
						stats._rescueBase = {};
					}
				}
				const _rb = stats._rescueBase;
				db.prepare(
					"INSERT INTO organ_meta (k, v) VALUES ('rescue_counts', ?) ON CONFLICT(k) DO UPDATE SET v = excluded.v",
				).run(
					JSON.stringify({
						at: nowIso(),
						pnRescue: (_rb.pnRescue || 0) + (stats.autorecallPnRescue || 0),
						kwRescue: (_rb.kwRescue || 0) + (stats.autorecallKwRescue || 0),
						vecFallback: (_rb.vecFallback || 0) + (stats.autorecallVecFallback || 0),
						errors: (_rb.errors || 0) + (stats.autorecallErrors || 0),
					}),
				);
				a25LastFlush = Date.now(); // 写库成功才消耗节流窗（F3-4②）
			} catch (eF) {
				// 8-31 长尾甲档③（F3-4①）：吞错无出口修——计数+节流 warn（治本承诺静默失效=sentry无从发现）
				stats.a25FlushErrors = (stats.a25FlushErrors || 0) + 1;
				// 2026-09-22 丁路F-1（合车批㉔）：异常即放弃本趟全部减法——本进程「已删确信」在异常后不可靠；
				//   防残留墓碑于下一趟按旧确信删掉盘上（他进程/恢复段产生的）新键。**代价＝本趟正确墓碑亦被放弃**
				//   （盘键残留·由重启后空账 pass 重记并清除）——2026-09-22 戊路W-1 订正：原「不丢正确墓碑」按字面不成立。
				const _tombAbandoned = a25Tombstones.size; a25Tombstones.clear(); // 2026-09-22 创造审计修：先取 size 再 clear（原序 clear 先行 ⇒ 代价留痕恒 +0 死位）
				stats.a25TombSkipped = (stats.a25TombSkipped || 0) + _tombAbandoned; // 2026-09-22 创造审计修：用 clear 前快照（原 a25Tombstones.size 在 clear 后恒 0）
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
			// 批⑳ J7-8：拆 per-session 键恢复（extract_buffer.<sid>）——旧单键 'extract_buffer' 兼容读：
			// 旧单键账加载即标 _dirty ⇒ 下次 flush 落 per-sid 键后由落盘段删旧键（迁移一次完成·不双读）。
			if (!A25_OFF) {
				const rows25 = db
					.prepare(
						"SELECT k, v FROM organ_meta WHERE k = 'extract_buffer' OR k LIKE 'extract_buffer.%'",
					)
					.all();
				for (const row25 of rows25) {
					const legacySingle = row25.k === "extract_buffer";
					const dump25 = JSON.parse(row25.v);
					const entries25 = legacySingle
						? Object.entries(dump25 || {})
						: [[row25.k.slice("extract_buffer.".length), dump25]];
					for (const [k, a] of entries25) {
						if (a && Array.isArray(a.msgs) && a.msgs.length > 0)
							pendingBySid.set(k, {
								msgs: a.msgs,
								watermark: Number(a.watermark) || 0,
								seq: Number(a.seq) || 0,
								lastTs: Number(a.lastTs) || 0,
								_dirty: legacySingle, // 旧单键账标脏→迁移；per-sid 键在盘已是最新态
							});
					}
				}
				if (rows25.length > 0)
					stats.bufferRestored = [...pendingBySid.values()].reduce(
						(n, a) => n + a.msgs.filter((m) => m.seq > a.watermark).length,
						0,
					);
				stats._a25LegacyOK = true; // 批㉑ W1：全轮恢复成功（含旧单键 parse＋全部账载入）——旧单键删除闸前置条件（任一 row 失败即不置·catch 拦）
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
				clipSurrogateTail(String(entry.nTitle).slice(0, 120)) +
				"\ncontent=" +
				clipSurrogateTail(String(entry.nContent).slice(0, 600)) +
				"\n【旧条】type=" +
				entry.oType +
				" space=" +
				entry.oSpace +
				" title=" +
				clipSurrogateTail(String(entry.oTitle).slice(0, 120)) +
				"\ncontent=" +
				clipSurrogateTail(String(entry.oContent).slice(0, 600)) +
				"\n【立案依据】" +
				clipSurrogateTail(String(entry.basis || "").slice(0, 120)); // 2026-09-22 戊路W-4
			let resp = null;
			for (let attempt = 0; attempt <= 2; attempt++) {
				try {
					resp = await fetch(PREVERDICT_API, { // 09-13 切档车：预裁路**专属端点**（原与提炼共用 EXTRACT_API ⇒ 切 GLM 后必须分家·否则打到 deepseek 端点）
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
							max_tokens: llmBudget(8000), // 09-13 切档车：glm-5.3 属**推理形态**·思考链吃预算（实测小额必 content 空）⇒ 预裁路缺省提额 2000→8000（仅本路·他路不动）
							temperature: 0.1,
							stream: false,
							response_format: { type: "json_object" }, // AUDN 刀①（09-08 maintainer）：JSON 模式——治「非 JSON/解析失败」降级
						}),
					});
				} catch {
					if (attempt === 2) {
						stats.preverdictApiFails = (stats.preverdictApiFails || 0) + 1; // 10-25 中枢定第 7 条：fetch 抛错路（网络断、DNS 失败）到末次也计；只计本路，不调 noteApiFail（不动它的口径），不碰 preguard
						return null;
					}
					await new Promise((r2) => setTimeout(r2, 800 * 2 ** attempt));
					continue;
				}
				if (resp.ok) break;
				const st = resp.status;
				if ([401, 403, 404].includes(st)) {
					preguard.pausedUntil = Date.now() + 10 * 60_000;
					noteApiFail(); // F1（二轮审计）：预裁失败出口（**可达**）——前轮仅主提炼补计且落在死码
					stats.preverdictApiFails = (stats.preverdictApiFails || 0) + 1; // 10-24 加活②（#45875 停因三查第四节 2）：预裁失败本路计数透到 stats（noteApiFail 是全路合计·看不出是预裁）；进程内计、重启清零
					return null; // 熔断（件三车病灶1·09-09：预裁独立 preguard——AUDN 刀②模型已分家 v4-pro/flash·预裁 404（模型 id 特有故障）连坐提炼主链不成立·解连坐）
				}
				if (![429, 500, 502, 503, 529].includes(st) || attempt === 2) {
					noteApiFail(); // F1：预裁失败出口（非可重试 或 末次）
					stats.preverdictApiFails = (stats.preverdictApiFails || 0) + 1; // 10-24 加活②：同上
					return null;
				}
				await new Promise((r2) => setTimeout(r2, 800 * 2 ** attempt));
			}
			if (!resp || !resp.ok) return null; // F1：**不可达**（循环内三 return 封闭）——失败计数已移至循环内两处 return 前
			try {
				const data = await resp.json();
				addUsage("preverdict", data); // E-7/D2：**预裁**（AUDN preguard）token 用量——原误记入 extract，已分家
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
		// ── PPR 建图性能车（B＋C＋A 三档·design-approved「按建议」·brainkickoff §二合并设计）──
		//    根因（brain真库实测·tool_usage p50=7,320ms）：原主 SQL 双 IN (SELECT…memories…) 嵌套在
		//    零二级索引库上 CLI 实测 9.0s（子查询对每条边全表评估）＋ 全表 title tokenize（8,928 条
		//    每题全切·无图时纯浪费＝沙箱 3,051ms 真源）。B＝一次物化活跃集→内存过滤（语义等价：
		//    src/dst ∈ 活跃集 ⇔ actSet.has）；C＝titleTok 只对边端点构建且复用 B 的物化行（title 已在手·
		//    零额外查询·跨题复用由 pprGraphCache 30s TTL 整体承担）；A＝无图短路（跳过 pprScores
		//    下游 !adj.size→null 早备）。观测键四条（stats 透出·防假绿非零断言）。
		//    审计③修语义保持：valid_to 同口径（IS NULL OR > now·短串字典序 < 同日带时刻串 ⇒ 当日到点即排除）。
		const t0Build = Date.now();
		const actRows = db
			.prepare(
				"SELECT id, title FROM memories WHERE status='active' AND (valid_to IS NULL OR valid_to > ?)",
			)
			.all(nowIso()); // B：物化（一次全表 ≈10ms 级·替代原双 IN 嵌套 9s）——C 复用 title 列
		const actSet = new Set(actRows.map((r) => Number(r.id)));
		const rawEdges = db
			.prepare(`SELECT src, dst, weight, instruction FROM memories_edges WHERE invalid_at IS NULL
        AND edge_type != 'cooccur_weak' -- 车三分档：weak 边不进 PPR 主图（防指标达标而主图污染·哨①指标面不计滤）`)
			.all();
		const edges = rawEdges.filter(
			(e) => actSet.has(Number(e.src)) && actSet.has(Number(e.dst)),
		); // B：内存过滤（等价原 src/dst IN 活跃集）
		stats.pprBuildMs = Date.now() - t0Build; // 观测键①：建图耗时（物化+过滤全量）
		stats.pprEdges = edges.length; // 观测键②：入图边数
		// A：无图短路（置于 之前——无图时全表 tokenize 纯浪费）
		if (!edges.length) {
			pprGraphCache = { at: Date.now(), adj: new Map(), qh };
			stats.pprShortCircuit = (stats.pprShortCircuit || 0) + 1; // 观测键④
			return pprGraphCache.adj;
		}
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
				// C 档（PPR 性能车）：原每题全表取 8,928 条 title 逐条 tokenize——改**只对边端点**构建
				//    （端点 ⊂ 活跃集·title 已随 B 档物化在 actRows·零额外查询）；跨题复用由
				//    pprGraphCache 30s TTL 整体承担（qh 命中即整图含因子复用·不重入本块）。
				const t0Qf = Date.now();
				qSet = new Set(qTokens);
				const titleById = new Map(
					actRows.map((r) => [Number(r.id), String(r.title)]),
				);
				titleTok = new Map();
				const seen = new Set();
				for (const e of edges) {
					for (const id of [Number(e.src), Number(e.dst)]) {
						if (seen.has(id)) continue;
						seen.add(id);
						const t = titleById.get(id);
						if (t !== undefined) titleTok.set(id, tokenize(t));
					}
				}
				stats.pprQueryFactorMs = Date.now() - t0Qf; // 观测键③：块耗时
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
		const compactionMetaBySid = new Map(); // O3（09-30 approved·压缩面优化车）：sid -> {s,e,c}（summary 段暂存 seq 边界/摘要字数·end 段锚条取用）——治「锚条仅记 compactionId·质检须手工解档」；sweep 同 compactionBySid 闸（bind/pending 双无即清·防 B-I3 型无界）
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
		require("./inject.cjs").setupInject({ ctx, systemPrompt, stats, bindMap, db, getSessionOfLastTurn: () => sessionOfLastTurn, currentCompactionN }); // [拆②补丁 v4] +db（闭包连接传递·治 text() ReferenceError）
		// AUDIT-FIX-20260912（修复车·审计 C 路第 5 项·P0-2 主体）：**回落落键**。
		//   ⚠ **因果模型订正（审计 A 路 D1·2026-09-12）**：原注释称「锚条入册失败 ⇒ 序数丢失」**与实测
		//   不符**——`nextCompactionN` 在锚条 INSERT **之前**即已 UPSERT 写键并回写（v+1），故「仅锚条
		//   失败」时键仍在、重启后下一号与不落键**完全等价**（A/B 对照实测 key/n 全同）。真实增量面
		//   只有一处：**键写路径失败**（nextCompactionN 的 catch(eC)）——**且**随后
		//   ⚠ **A⁗ D4 订正**：原文写「catch(eC) 路径·**不落键**」**不实**——实测该路径下 `v+1` 自增**已成功**、
		//   仅**回写**（`v = excluded.v`）失败时键值**已被改写**（如 5→6），并非「不落键」；措辞改为
		//   「键写**路径**失败」以涵盖「自增成功/回写失败」两态（合取项本身不受影响·第三项「值 < n」已兜住）。
		//   锚条/建边路径也失败（即 catch(eB) 被触发）时，回落落键才补上事实源（实测 NEW key=1 保留
		//   vs LEGACY key 缺失·下一号 2 vs 1）；**仅键写失败而锚条成功时本函数不被调用**（审计 A′ 实证），
		//   此时第三源（锚条数）兜住、无损害。叠加 MAX 语义即**幂等冗余**。
		//   记账口径：本函数是**二次机会垫片**——两个面**不可混述**（A″D8 订正）：
		//   **调用面**＝`catch (eB)` 触发即被调用（含「键写成功 + 锚条失败」·实测 __FB_CALLS=1）；
		//   **增量面**（真正改变事实源的场景）＝键写失败 **∧** `catch (eB)` 触发 **∧ 该键当前值 < n
		//   （含键缺失）**——末项不可省（A‴ 审计 F2 实证：预置键值 9 而 n=1 时 MAX(9,1)=9，**写语句执行
		//   但值不变**，属**空写**；若照「前两项即增量面」写 drill 断言会假红/漏判）。
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
		// [拆⑥·2026-10-04] runExtract/parseExtractEntries 归 extract.cjs——装配于 signalQ 声明后（全 deps 就绪·firehose 首调前）。
		//    buffer/sessionOfLastTurn getter（#31831 let 重赋值型读点）＋两写点回调化（onExtracted/onSignalsDigested——闭包写 index 侧原变量）；extracting 互斥随迁（判定点走 isExtracting）。
		const _extractMod = require("./extract.cjs").setupExtract({ ctx, stats, db, credentials, pendingBySid, guard, buffer: () => buffer, sessionOfLastTurn: () => sessionOfLastTurn, a25Flush, onExtracted: () => { buffer = ""; }, onSignalsDigested: () => { signalQ = []; }, A12_OFF });
		const runExtract = _extractMod.runExtract;
		const isExtracting = _extractMod.isExtracting;
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
		// wave 车二-1：闸模块装配期取一次——会话事件与 setupAskGate 用同一模块实例（热载清缓存后不串到新实例）；载入失败只计错告警，退成空转桩、不挡插件
		let askgateMod = { askOnSessionEvent() {}, setupAskGate() {} };
		try {
			askgateMod = require("./askgate.cjs");
		} catch (eAGm) {
			stats.askgateErrors = (stats.askgateErrors || 0) + 1;
			ctx.logger?.warn?.(`[living-memory] wave 开口前retrieval gate载入失败: ${String(eAGm).slice(0, 80)}`);
		}
		ctx.on("session/event", (session, event) => {
			stats.firehoseSeen = true;
			stats.eventCount += 1;
			const t = String(event.type);
			// ── ⑩A 板态自动镜像（maintainer 09-20 批「处置方案（step＋一闸·发车」·实勘step A）──
			//    病灶(design note)：窗口板「seq 1295 后约 1300 事件零更新」——板被做成「需模型手动维护的第二副本」。
			//    治法：监听宿主 `todo/write` 事件（payload＝`{todos}`·`dsh-tool-todo` L173 整表快照）⇒ 自动 upsert
			//    `session_boards`（与 `board_write` 的整表替换语义天然同构）⇒ **窗口即使忘记维护板，板态仍自动留存**
			//    （治「机制不应依赖个人自律」·交办件 §五）。零模型负担·零宿主改动（同一 firehose）。
			try {
				if (t === "todo/write" && session && typeof session.id === "string") {
					// 架构 C（10-04）：board_write 自源回写事件（src=legion-board-write）不镜像——板已由 board_write 本体落库
					//   （含 note/id 全字段）·若再镜像将以降格形（仅 content/status）覆写·丢板账链接面（自源计数 boardMirrorSelfSrc）。
					const _selfSrcBw = !!(
						event.data &&
						event.data.src === "legion-board-write"
					);
					if (_selfSrcBw)
						// option补移（10-04 kickoff「四键全移」·第四键）：boardMirrorSelfSrc 闭包改 organ_meta 唯一真相源
						//   ——同款 v+1 型 SQL 原子自增 UPSERT（免读-改-写竞态·三十九审 🟡A 教训前置规避）·⑬ 出口在位。
						try {
							db.prepare(
								"INSERT INTO organ_meta (k, v) VALUES ('board_selfsrc_count', '1') ON CONFLICT(k) DO UPDATE SET v = CAST(v AS INTEGER) + 1",
							).run();
						} catch (eSS) {
							ctx.logger?.warn?.(
								"[living-memory] board_selfsrc_count 计数写失败: " +
									String(eSS).slice(0, 60),
							);
						}
					// F 路审计 M-1 修（09-20 23:2x·⚠中·本洞系批⑪ 修 E-真2 时引入）：**以形状分诊·非以长度**——
					//   原「空则跳过 upsert」无法区分「模型显式清板 `todos:[]`」与「形状不符 `data:{}`」（二者同产
					//   `_itemsA=[]`）⇒ 清板被静默丢弃、旧板残留并被注入面再回灌。契约：宿主 `todo/write` 载荷恒为
					//   `{todos:[…]}`（`dsh-tool-todo` L173）⇒ **形状合法即允许空数组清板**（与 `board_write`「空数组=清板」同语义）。
					const _shapeOk = !!(event.data && Array.isArray(event.data.todos));
					const _rawTW = _shapeOk ? event.data.todos : [];
					const _sidA = /^session-/.test(session.id)
						? session.id
						: "session-" + session.id;
					const _itemsA = [];
					for (const _it of _rawTW) {
						const _cA = String((_it && _it.content) || "")
							.trim()
							.slice(0, 300);
						if (!_cA) continue;
						const _sA = ["pending", "in_progress", "completed"].includes(
							String(_it && _it.status),
						)
							? String(_it.status)
							: "pending";
						_itemsA.push({ content: _cA, status: _sA });
						if (_itemsA.length >= 50) break;
					}
					// E 路审计 真2 修（09-20 23:1x·⚠中）：原 upsert **恒执行** ⇒ `event.data` 形状不符时静默**写空板**；
					//   改：空/形状不符 ⇒ 跳过 upsert（宁可无板不写空板）并计数；payload 加 `mir`（来源）与 `dropped`（未收录数）。
					if (!_shapeOk) {
						stats.boardMirrorSkipped = (stats.boardMirrorSkipped || 0) + 1;
					} else if (_selfSrcBw) {
						// 架构 C：自源事件形状合法但不镜像（计数已上前·belt-and-braces分支）
					} else {
						db.prepare(
							"INSERT INTO session_boards (sid, payload, updated_at) VALUES (?, ?, ?) ON CONFLICT(sid) DO UPDATE SET payload = excluded.payload, updated_at = excluded.updated_at",
						).run(
							_sidA,
							JSON.stringify({
								todos: _itemsA,
								mir: "host",
								dropped: _rawTW.length - _itemsA.length,
							}),
							new Date(Date.now() + 8 * 3600 * 1000)
								.toISOString()
								.slice(0, 16) + "+08:00",
						);
						stats.boardMirrored = (stats.boardMirrored || 0) + 1;
					}
				}
			} catch (eBM) {
				// ⑬ 出口纪律：错误不可见＝双重封闭——计数 + warn
				stats.boardMirrorErrors = (stats.boardMirrorErrors || 0) + 1;
				ctx.logger?.warn?.(
					"[living-memory] 板态镜像失败 (#" +
						stats.boardMirrorErrors +
						"): " +
						String(eBM).slice(0, 60),
				);
			}
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
					bd.turnSearchCalled = false; // wave 车二-2：本轮 search／timeline 戳清零（结算在 turn/end，见下）
					bd.lastOrderText = "";
					bd.at = Date.now(); // 车二件5（治 P3-新4·R2 册）：过期改按「活动时点」——原 bd.at 仅创建时写⇒活跃会话 2h 后绑定观测态被 sweep 重置（nudge 可复发·间距语义失真）；与 autoLatestPrompt v.at（随捕获刷新）语义统一
				} // option：新轮重置本 turn 已查戳+本轮directive留档（I-1）
				askgateMod.askOnSessionEvent(bindSid, t, event); // wave 车二-1：攒本轮助手正文与本轮 search／timeline 查询词（函数自带 try／catch；总闸关或未装配时空转）
				if (process.env.LEGION_INJECT_SLIM_OFF !== "1") require("./inject.cjs").slimOnSessionEvent(bd, t, event, db); // 注入瘦身车 W4（10-11·maintainer 1324 四案＋统筹审核 W4·落码task brief §3.4、§3.11 第 22–24 条）：按轮冻结三计数（turn/start、compaction/end、memory_write 白名单）与漏召观测——SLIM_OFF 时整行不走；第四参传 db（盲审 R7·10-12：漏召两键改走 inject 的 slimBump，失败计注计错，不再借 bumpNudgeTotal）
				if (core.reviveOn()) core.reviveOnEvent(db, session, t, event); // wave 复活口·阶段 2（10-18）：get 取 aged、maintainer重提、回复 #id 引用三类触发只记复活候选（revive_pending），nightly patrol复活段消费——LEGION_REVIVE_OFF=1 时整行不走（函数自带 try／catch）
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
					// wave 车二-2（10-17·落码规格 2.2）：催查提示 A/B——本轮调了 memory 的 search／timeline 记 bd.turnSearchCalled（LEGION_SEARCH_HINT_ON=1 才记）；
					//   arguments 可能是 JSON 串（先 parse）也可能是对象；read_episodic 等其余 action 不算
					if (process.env.LEGION_SEARCH_HINT_ON === "1" && name === "memory") {
						try {
							const a0 = event.data?.arguments ?? event.data?.args;
							const a1 = typeof a0 === "string" ? JSON.parse(a0) : a0;
							if (a1 && (a1.action === "search" || a1.action === "timeline")) bd.turnSearchCalled = true;
						} catch {}
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
				// ── wave 车二-2（10-17·落码规格 2.2）：真用户轮收轮时按臂结算 hint_ab_{a,b}_turns，本轮调过 search／timeline 再加 hint_ab_{a,b}_active。
				//    规格原写「turn/start 先结算上一轮」；但 _userMsgThisTurn 在下面option turn/end 处已清零，turn/start 时读不到，故挪到 turn/end、option清零之前（偏离见落码记录）
				if (t === "turn/end" && process.env.LEGION_SEARCH_HINT_ON === "1" && bd._userMsgThisTurn) {
					try {
						const armK = require("./inject.cjs").hintArm(bindSid) === "A" ? "a" : "b";
						const upH = db.prepare("INSERT INTO organ_meta (k, v) VALUES (?, '1') ON CONFLICT(k) DO UPDATE SET v = CAST(CAST(v AS INTEGER) + 1 AS TEXT)");
						upH.run("hint_ab_" + armK + "_turns");
						if (bd.turnSearchCalled) upH.run("hint_ab_" + armK + "_active");
					} catch {
						stats.c2HintErrors = (stats.c2HintErrors || 0) + 1;
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
			// wave 车二-1（规格 2.1「必须做」）：插件续步／追加的闸文（user/message，source.kind = legion-askgate）不进文本缓冲、不进 待抽账——
			//   否则闸文会被当成 USER 原话送去提炼、提成新条，反成新回环。不受 LEGION_ASKGATE_OFF 管（无害）
			const _askgateMsg = t === "user/message" && event?.data?.source?.kind === "legion-askgate";
			if ((t === "user/message" || t === "assistant/message") && !_askgateMsg) {
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
					buffer += (buffer ? "\n---\n" : "") + clipSurrogateTail(text.slice(0, 3000));
					if (buffer.length > 12000) buffer = clipSurrogateHead(buffer.slice(-12000));
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
						acct._dirty = true; // 批⑳ J7-8：入账即脏——拆键落盘只写脏账（O(增量)）
						acct.msgs.push({
							turn: stats.eventCount,
							role: t === "user/message" ? "USER" : "ASSISTANT",
							text: clipSurrogateTail(text.slice(0, 3000)),
							seq: acct.seq,
						});
						if (acct.msgs.length > 400) {
							// 批⑲ P5（J8-1）：截断计数出口——原静默丢账（对照 2000 帽有 a25DroppedUnextracted）
							stats.a12Truncated = (stats.a12Truncated || 0) + 1;
							ctx.logger?.warn?.(
								`[living-memory] account truncated (#${stats.a12Truncated}): 旧未抽段被截（每会话 400 帽）`,
							);
							acct.msgs = acct.msgs.slice(-400); // 无界保护（每会话 400 帽·与 buffer 12k 同精神）
						}
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
			if (t === "turn/end" && buffer.trim().length > 200 && !isExtracting()) {
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
						// ── ⑩C 压缩点账快照（step C·治「压缩总结里的待办清单靠模型从叙述重构」）──
						//    在压缩起点把**账态导出的三张清单**入册为独立 fact 条（与「压缩总结」条并存·互链可回溯）——
						//    压缩后新窗「一读即有」；幂等＝标题 `本窗任务账快照：<sid8>·<compactionId8>` 唯一（重放不重插）。
						try {
							const _cIdC = String(
								(event.data && event.data.compactionId) || "",
							).slice(0, 8);
							if (_cIdC) {
								const _sid8C = String(cSid)
									.replace(/^session-/, "")
									.slice(0, 8);
								const _spaceC = (() => {
									try {
										const _cw = session && session.header && session.header.cwd;
										const _byC = _cw ? organFromPath(_cw) : null;
										return (
											_byC ||
											(typeof sessionOrgan === "function"
												? sessionOrgan(String(cSid))
												: "") ||
											""
										);
									} catch {
										return "";
									}
								})();
								const _txtC = renderTaskLedger(
									exportTaskLedger(db, {
										space: _spaceC,
										sid: String(cSid),
										hours: 12,
										limit: 5, // O2 ③：候裁/在办指针各 ≤5 条——**export 侧 SQL LIMIT 同传**（原仅 render 侧帽·export 默认 3 ⇒ 实际恒 3·o2o3-drill U4 抓出）
									}),
									{ limit: 5 },
								);
								// ── 甲（maintainer 09-27「按建议」·治 L1 压缩↔板态联动缺失）：压缩点同步快照**板态** ──
								//    （临时规划项可回溯·与账态三清单同条入册·normSid 归一防 I-5 型落空）
								let _boardSnapC = "";
								try {
									const _browC = db.prepare("SELECT payload, updated_at FROM session_boards WHERE sid = ?").get(normSid(String(cSid)));
									if (_browC) {
										const _plC = JSON.parse(_browC.payload);
										const _tdsC = _plC && Array.isArray(_plC.todos) ? _plC.todos : [];
										if (_tdsC.length) {
											_boardSnapC = "\n\n[板态快照 " + String(_browC.updated_at).slice(5, 16) + "] 共 " + _tdsC.length + " 项：\n" +
												_tdsC.slice(0, 12).map((t2) => {
													// 板v2车余面④：快照条目带三要素（content 内 源＝/流转＝ 提取＋板项 id 账态联查真值——低频压缩路径·帽 12 条）
													const _c2 = String((t2 && t2.content) || "");
													const _fl = (_c2.match(/流转[＝=]\s*([^｜|；;\n]{1,16})/) || [])[1] || "";
													const _sr = (_c2.match(/源[＝=]\s*([^｜|；;\n]{1,16})/) || [])[1] || "";
													let _ld = "";
													if (t2 && Number.isInteger(t2.id)) {
														try {
															const _mr = db.prepare("SELECT status, closed_at FROM memories WHERE id = ? AND type = 'todo'").get(t2.id); // 写入面修（R1 车）：板项 id 只挂 todo 账，加 type 限定免把同号非 todo 条的状态误当账态
																if (_mr) _ld = "账:" + _mr.status + (_mr.closed_at ? "@" + String(_mr.closed_at).slice(5, 10) : "");
														} catch (eLd) {
															// 四十二审甲路 P2-2 修（🟡B 同族）：账态联查失败不再空吞——warn 留痕（不阻断快照生成·条目退化为无「账:」短注·外层 eSnapC 感知面不变）
															try { ctx.logger?.warn?.("[living-memory] ⑩C 账态联查失败(id=" + t2.id + "): " + String(eLd).slice(0, 60)); } catch {}
														}
													}
													const _tri = [_sr ? "源" + _sr.trim().slice(0, 10) : "", _fl ? "流转" + _fl.trim().slice(0, 12) : "", _ld].filter(Boolean).join("·");
													return "- [" + (t2 && t2.status === "completed" ? "x" : " ") + "] " + _c2.slice(0, 60) + (_tri ? "（" + _tri + "）" : "");
												}).join("\n") +
												(_tdsC.length > 12 ? "\n…（余 " + (_tdsC.length - 12) + " 项）" : "");
										}
									}
								} catch (eSnapC) {
									// 乙 O1（二十三审·真因不可回证的直接根）：板态取数异常必须留痕——甲7 同族漏网点
									try {
										stats.boardSnapErrors = (stats.boardSnapErrors || 0) + 1;
										ctx.logger?.warn?.("[living-memory] ⑩C 板态取数失败: " + String(eSnapC).slice(0, 80));
									} catch {}
								}
								// ── O2（09-30 approved·压缩面优化车）：压缩点**工作区快照**——工程仓 porcelain＋未入库报告清单 ──
								//    治 N6「未落盘不可见」（本窗实证：31 件补提＋抢救五件皆因缺此面）；cwd 推工程目录（cwd 即工程 或 space根/工程）；
								//    git 先证管在（#27417：顶层空读假报 porcelain 0）→ untracked 报告型（YYYY-MM-DD-*）前 10 件入快照（验收＝drill 造未入库件→快照含其名）。
								let _wsSnapC = "";
								try {
									const _cwdW = session && session.header && session.header.cwd ? String(session.header.cwd) : "";
									const _engW = _cwdW
										? [_cwdW.endsWith("/工程") ? _cwdW : _cwdW + "/工程"].find((p) => {
												try {
													return fs2.statSync(p).isDirectory();
												} catch {
													return false;
												}
											}) || null
										: null;
									if (_engW) {
										let _gitOkW = false;
										try {
											_gitOkW = String(execFileSync("git", ["-C", _engW, "rev-parse", "--is-inside-work-tree"], { timeout: 3000 })).trim() === "true";
										} catch {} // 豁免报备（⑬同族）：非仓目录 git exit 128 抛错＝**设计内分支**（下文显式记「非 git 仓」·非缺陷）
										if (_gitOkW) {
											// O2 即修二（o2o3-drill U3b 行为实证）：git porcelain 默认 core.quotePath=true ⇒ 中文文件名
											//   输出带引号八进制转义（"2099-01-01-\344…"）⇒ /^\d{4}-/ 全不匹配 ⇒ 「未入库报告」恒 0（生产报告名皆中文）。
											//   修：`-c core.quotePath=false` 输出原始 UTF-8 名（git 标准解法·零副作用）。
											const _porW = String(execFileSync("git", ["-C", _engW, "-c", "core.quotePath=false", "status", "--porcelain"], { timeout: 5000 })).trim();
											if (_porW) {
												const _unW = _porW
													.split("\n")
													.map((l) => l.replace(/^\?\?\s+/, ""))
													.filter((n) => /^\d{4}-\d{2}-\d{2}/.test(path.basename(n)))
													.slice(0, 10);
												_wsSnapC = "\n\n[工作区快照·工程仓 porcelain " + _porW.split("\n").length + " 行｜未入库报告 " + _unW.length + " 件]" + (_unW.length ? "\n" + _unW.map((n) => "- " + n).join("\n") : "");
											} else {
												_wsSnapC = "\n\n[工作区快照·工程仓 porcelain 0（净）]";
											}
										} else {
											_wsSnapC = "\n\n[工作区快照·工程目录非 git 仓（porcelain 面跳过·N8：模块内唯memory organ工程为 git 仓）]";
										}
									}
								} catch (eWs) {
									// F5（八维·09-30）：快照**写失败面禁静默**——计数＋warn＋render 透出（否则快照形同虚设·与「零静默」族同守）
									try {
										stats.wsSnapErrors = (stats.wsSnapErrors || 0) + 1;
										ctx.logger?.warn?.("[living-memory] O2 工作区快照失败 (#" + stats.wsSnapErrors + "): " + String(eWs).slice(0, 80));
									} catch {}
								}
								const _titleC = "本窗任务账快照：" + _sid8C + "·" + _cIdC;
								if (
									_txtC &&
									_spaceC &&
									!db
										.prepare(
											"SELECT id FROM memories WHERE title = ? LIMIT 1",
										)
										.get(_titleC)
								) {
									db.prepare(
										"INSERT INTO memories (ts, type, title, content, space, source, checksum) VALUES (?, 'fact', ?, ?, ?, ?, ?)",
									).run(
										nowIso(),
										_titleC,
										(String(_txtC) + _boardSnapC + _wsSnapC)
											.replace(/[{}]/g, (m) => (m === "{" ? "｛" : "｝"))
											.slice(0, 2400), // 甲（09-27 maintainer）：含板态快照·帽 1500→2400；E 路审计 I7 净化同纪律
										_spaceC,
										"auto:" + String(cSid),
										// F 路审计 M-3 订正（09-20 23:2x·🟡）：本路径**真闸＝标题精确匹配**（`WHERE title = ?`·标题不含 space）
										//   ⇒ 批⑪ 加的这个「checksum 并入 space 防相撞」对生产幂等**不起作用**（checksum 在该入库路径零消费·
										//   仅留痕）；库内 `checksum LIKE 'taskledger:%'` 实测 0 条 ⇒ 对既有条零影响。**幂等实由标题键保证**。
										"taskledger:" + _spaceC + ":" + _sid8C + ":" + _cIdC,
									);
									stats.ledgerSnapshots = (stats.ledgerSnapshots || 0) + 1;
								}
							}
						} catch (eLC) {
							stats.compactionBridgeErrors =
								(stats.compactionBridgeErrors || 0) + 1;
							ctx.logger?.warn?.(
								"[living-memory] 任务账快照失败 (#" +
									stats.compactionBridgeErrors +
									"): " +
									String(eLC).slice(0, 60),
							);
						}
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
			// ── 压缩↔living memory联动桥 **刀⑥**（design-approved「先 P0」·压缩漏点复查  pending ruling落地）──
			//    治漏点：①「摘要正文不入living memory」（ 判定·09-11 四查候裁至今未落）＋⑤「摘要 preamble 无元数据」。
			//    病灶实勘：宿主 `compaction/summary` 事件 data 带 `summary` 正文与
			//      `shadowedRange/shadowedSeqs/shadowedTokenCount/provider/model/maxTokens/usage` 十类字段，
			//      而原桥只监听 `compaction/end`（data 仅 `{compactionId, turn}`）⇒ 6.9KB 浓缩成果与全部元数据在桥上被丢弃。
			//    落法：summary 段独立入册「压缩总结」fact 条（source=auto:<sid> 满足 episodic 通道）——
			//      title 带 sid8 + compactionId8（**免计数耦合**·天然唯一·不扰动 end 段次数编号）；
			//      content = 元数据头 + 摘要正文全文（直插路径无长度帽·嵌入面已有 1500 字截断帽）；
			//      幂等：checksum 先查后插（事件重放不重复入库）；
			//      建边：链回同源近 3 条（sg=compact-bridge·UPSERT 幂等）。
			//    **顺序红利**：summary 先于 end ⇒ end 段锚条的 nearRows 自动把本条链入 ⇒ 锚条↔总结互链零额外代码。
			//    零宿主改动（同刀①②依据：session.append 无差别过 firehose）·回退 LEGION_COMPACT_BRIDGE_OFF（同闸）。
			if (!process.env.LEGION_COMPACT_BRIDGE_OFF && t === "compaction/summary") {
				try {
					const sSid =
						session && typeof session.id === "string" ? session.id : null;
					if (sSid) {
						const sd = event.data || {};
						const blocks = Array.isArray(sd.summary) ? sd.summary : [];
						const sBody = blocks
							.map((b) => (b && typeof b.text === "string" ? b.text : ""))
							.join("\n")
							.trim();
						if (sBody) {
							const cid = String(sd.compactionId || "?").slice(0, 36);
							const sTitle =
								"压缩总结：" +
								String(sSid).replace(/^session-/, "").slice(0, 8) +
								"·" +
								cid.slice(0, 8);
							const ckSum = sha1(sSid + cid + sTitle);
							// ── 批⑰ ＋ 批⑱ K 路修车（K1-1·2026-09-21 K 路独立审计 🔴）：直插路补闸——
							//    批⑰ 版曾把 SENSITIVE_RE 判为**硬拦**，实测误伤面 13/14（93%）：①级词表实为「凭据词面表」
							//    （api_key|password|secret|token|bearer…·:247 强制 i），压缩总结正文常含 token/Bearer/apiKey
							//    技术词 ⇒ 压缩桥几乎全灭。判据来源与 意图（directive原文族·②级表）错配。
							//    批⑱ 修：**敏感层降 warn 不拦**（对齐魂红线 L2「只 warn 不跳·入册留痕」分层）——
							//    硬拦只留 L1 形态层 securityCheck（真凭据形态 sk- 族）。
							const _cbSec = securityCheck(promptSafe(sTitle), promptSafe(sBody));
							const _cbSens =
								_cbSec.ok &&
								(SENSITIVE_RE.test(sTitle) || SENSITIVE_RE.test(sBody));
							// 批⑲-K1 并入（K1-6·10-07 maintainer归口自办）：幂等闸前置＋计数移入 !dupRow 分支——
							//   原序「计数先于 dupRow 查」⇒ 同 cid 事件重放（dupRow 命中不插入）仍重复计数，
							//   与批⑯④「实落才计」口径不一（⑬ 计数可信性面）；warn 日志保留原位（重放亦留痕·事件面非统计面）。
							const dupRow = db
								.prepare("SELECT id FROM memories WHERE checksum = ?")
								.get(ckSum);
							if (!_cbSec.ok) {
								if (!dupRow)
									stats.compactBridgeSecReject = (stats.compactBridgeSecReject || 0) + 1;
								ctx.logger?.warn?.(
									`[living-memory] compact-bridge security reject (#${stats.compactBridgeSecReject}): ${_cbSec.reason} · ${sTitle}`,
								);
							} else if (_cbSens) {
								if (!dupRow)
									stats.compactBridgeSensitiveSkip = (stats.compactBridgeSensitiveSkip || 0) + 1; // 口径＝敏感命中**告警**计数（批⑱ 起非拦截·批⑲-K1 起实落才计）
								ctx.logger?.warn?.(
									`[living-memory] compact-bridge sensitive-warn（只警不拦·批⑱） (#${stats.compactBridgeSensitiveSkip}): ${sTitle}`,
								);
							}
							// O3（09-30 压缩面优化车·approved）：summary 元数据暂存——供 end 段锚条增记三字段（顺序红利：summary 先于 end）；缺值记 "?"（end 段据此判降级形态）
							const _srM = sd.shadowedRange || {};
							compactionMetaBySid.set(sSid, {
								s: _srM.start ?? "?",
								e: _srM.end ?? "?",
								c: sBody.length,
							});
							if (!dupRow && _cbSec.ok) { // 批⑱ K 路修车（K1-1）：硬拦只留 L1 安检（敏感层降 warn 不拦·误伤面 93% 治本）；批⑲-K1-6：dupRow 查询已前置（计数序修）
								const sr = sd.shadowedRange || {};
								const usg = sd.usage || {};
								const sHead =
									"【" + nowIso() + " 压缩总结】compactionId=" + cid +
									"｜被压 seq " + (sr.start ?? "?") + "→" + (sr.end ?? "?") +
									"｜被压 " + (Number(sd.shadowedTokenCount) || 0) + " tokens → 摘要 " + sBody.length + " 字" +
									"｜模型 " + String(sd.provider || "?") + "/" + String(sd.model || "?") +
									"·maxTokens " + (sd.maxTokens ?? "?") +
									"｜用量 in " + (usg.inputTokens ?? "?") + " / out " + (usg.outputTokens ?? "?") +
									"｜本条即「被压段浓缩成果」source of record；被压段原文在 session.jsonl.zstd 留盘，姊妹锚条见「上下文压缩锚」同 sid。";
								const sInfo = db
									.prepare(
										"INSERT INTO memories (ts, type, title, content, space, source, checksum) VALUES (?, 'fact', ?, ?, ?, ?, ?)",
									)
									.run(
										nowIso(),
										promptSafe(sTitle), // 审计处置车（09-13·D1c/）：刀⑥ 直插路原未过净化 ⇒ 被压段含完整花括号组时摘要正文原样入库（与事故凶器同形态）
										promptSafe(sHead + "\n\n" + sBody),
										sessionOrgan(sSid) || "global",
										"auto:" + sSid,
										ckSum,
									);
								try {
									const nearS = db
										.prepare(
											"SELECT id FROM memories WHERE source = ? AND id != ? ORDER BY id DESC LIMIT 3",
										)
										.all("auto:" + sSid, Number(sInfo.lastInsertRowid));
									const sEdge = db.prepare(`INSERT INTO memories_edges (src, dst, edge_type, weight, valid_at, source_group, last_seen, instruction)
                VALUES (?, ?, 'explicit', 0.5, ?, 'compact-bridge', ?, '压缩总结 ↔ 同源条目')
                ON CONFLICT(src, dst, edge_type) DO UPDATE SET last_seen = excluded.last_seen`);
									for (const ns of nearS)
										sEdge.run(
											Number(sInfo.lastInsertRowid),
											ns.id,
											nowIso(),
											nowIso(),
										);
									stats.compactionSummarized =
										(stats.compactionSummarized || 0) + 1;
								} catch (eSumEdge) {
									// ⑬ 出口（同刀②建边内层·归因靠 warn 文案前缀）
									stats.compactionBridgeErrors =
										(stats.compactionBridgeErrors || 0) + 1;
									ctx.logger?.warn?.(
										"[living-memory] compaction summary edge failed (#" +
											stats.compactionBridgeErrors +
											"): " +
											String(eSumEdge).slice(0, 60),
									);
								}
							}
						}
					}
				} catch (eSum) {
					// ⑬ 出口（同刀①外层法）
					stats.compactionBridgeErrors =
						(stats.compactionBridgeErrors || 0) + 1;
					ctx.logger?.warn?.(
						"[living-memory] compaction/summary bridge failed (#" +
							stats.compactionBridgeErrors +
							"): " +
							String(eSum).slice(0, 60),
					);
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
						// O3（09-30 压缩面优化车·approved）：锚条增记三字段（被压 seq 边界/摘要字数/触发 turn）——
						//    压缩质检与「丢了没」判定**免解档**（设计件 §四：三值事件里全有·零成本）；幂等不受影响（checksum 含标题不变）。
						const _m3 = compactionMetaBySid.get(cSid2);
						const _turn3 = event.data?.turn ?? "?";
						const cContent =
							"compaction/end 捕获（compactionId=" +
							String(event.data?.compactionId || "?").slice(0, 36) +
							"）——被压段原文在 session.jsonl.zstd 全量留盘，read_episodic 本条 id 可取压缩前窗口原文。" +
							(_m3 && _m3.s !== "?"
								? "｜O3 增记：被压 seq " + _m3.s + "→" + _m3.e + "·摘要 " + _m3.c + " 字·触发 turn " + _turn3
								: "｜O3 增记：触发 turn " + _turn3 + "（summary 元数据缺——seq 边界/摘要字数见姊妹「压缩总结」条）");
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
			// [拆②补丁 v4b] 原 `if (t === "turn/end") sweepExpiredAccounts();` 悬空调用已移——函数随拆②段B 归 inject.cjs
			//   setupAutoRecall 作用域·挂载一并收回该域（inject.cjs sweep 定义后·治每轮 turn/end ReferenceError）。
		});

		// ── 吸收项·每轮自动召回（wave·13:07 maintainer融入令；源=graph-memory dsh.ts L472-556 三件套逐块融入）──
		// [拆②] 段B（高精门 L5080-5739）已抽 inject.cjs setupAutoRecall
		require("./inject.cjs").setupAutoRecall({ ctx, stats, bindMap, db, runExtract, a25Flush, a25Tombstones, compactionBySid, compactionMetaBySid, pendingBySid }); // [拆②补丁 v4] +db
		// ── wave 车二-1（10-17·落码规格 2.1）：开口前retrieval gate——收轮前（agent/turn-stopping）、问maintainer前（tools/pre-execute）、登记后（tools/post-execute）三挂点 ──
		//    只在 host 角色挂（write 角色在上方早退）；LEGION_ASKGATE_OFF=1 时一个挂点都不注册。装配失败只计错告警、不挡插件其余部分
		try {
			askgateMod.setupAskGate({ ctx, stats, db });
		} catch (eAG) {
			stats.askgateErrors = (stats.askgateErrors || 0) + 1;
			ctx.logger?.warn?.(`[living-memory] wave 开口前retrieval gate装配失败: ${String(eAG).slice(0, 80)}`);
		}
		// ── wave 补笔①（10-18·侦察 K3-08）：车二-8 LIFE 四开关只认 "1"——值非空又不是 "1"（true、yes、0 等）时载入打一次 warn、按未设处理
		//    （同 askgate.cjs setupAskGate 开头同式；四个开关各处现读、都按 !== "1" 判，本段只告警、不改判法）。只在 host 角色（write 角色在上方早退）
		try {
			for (const k of ["LEGION_LIFE_GUARD_OFF", "LEGION_LIFE_SOFTSINK_OFF", "LEGION_LIFE_PTR_OFF", "LEGION_LIFE_SKILL30_ON"]) {
				const v = process.env[k];
				if (v && v !== "1") ctx.logger?.warn?.(`[living-memory] wave ${k}=${String(v).slice(0, 8)} 不是 1，按未设处理`);
			}
		} catch {}
		// ── wave 复活口（10-18）：新开关只认 "1"——值非空又不是 "1" 时载入打一次 warn、按未设处理（同上段 LIFE 四开关；各处现读、都按 !== "1" 判）。只在 host 角色
		try {
			for (const k of ["LEGION_MEM_GET_OFF", "LEGION_REVIVE_OFF"]) {
				const v = process.env[k];
				if (v && v !== "1") ctx.logger?.warn?.(`[living-memory] wave ${k}=${String(v).slice(0, 8)} 不是 1，按未设处理`);
			}
		} catch {}
		// ── nightly patrol(design note)──
		// [拆⑥·2026-10-04] nightPatrol 归 patrol.cjs（去重/建边/投影/handover note/todo 衰减/sentry）——deps 九名全 const/fn 直传零体内改动；
		//    TDZ 绿（preclassifyConflict/runExtract/pruneOrganMeta/db 全定义于前）·setInterval 调用面零改动（同名 const 装配）。
		//    10-24 加活①：deps 加 preguard（预裁段改判预裁自己的熔断态·见 patrol.cjs 预裁段）
		const { nightPatrol } = require("./patrol.cjs").setupPatrol({ ctx, stats, db, credentials, pendingBySid, guard, preguard, preclassifyConflict, runExtract, pruneOrganMeta });
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
		// [106踵即修] _nullifyDeep：工具返回 undefined 属性清洗 null（10-01 stats 全挂案同款根因族——harness
		//   snapshotJsonValue 拒收 undefined ⇒「value is not lossless JSON」⇒工具全线不可用）。首清 warn 留根因观测
		//   （清洗键名进日志·定位动态 undefined 源后根治——防御层与观测层一体）。
		let __nullifyWarned = 0;
		const _nullifyDeep = (v, tag) => {
			try {
				if (v === undefined) return null;
				if (v === null) return v;
				if (typeof v === "function" || typeof v === "symbol") return null; // [再检查补] harness 亦拒收函数/Symbol（lossless 严格）
				if (typeof v !== "object") return v;
				if (Array.isArray(v)) return v.map((x) => _nullifyDeep(x, tag));
				const o = {};
				for (const k of Object.keys(v)) {
					o[k] = _nullifyDeep(v[k], tag);
					if (v[k] === undefined && __nullifyWarned < 3) {
						__nullifyWarned++;
						try {
							ctx.logger?.warn?.(
								"[living-memory] nullify undefined key #" + __nullifyWarned + " (" + String(tag) + "." + k + ")——动态 undefined 源候根因观测",
							);
						} catch {}
					}
				}
				return o;
			} catch {
				return null;
			}
		};
		// [拆③] read 域抽出（v1·10-04）：五 action 实现归 read.cjs setupRead——装配点在十 deps 全定义后（db L1733/credentials L1729/runExtract hoisting/pprScores L3131 hoisting·TDZ 绿）
		const readExec = require("./read.cjs").setupRead({ ctx, db, stats, bindMap, sessionOfLastTurn: () => sessionOfLastTurn, runExtract, credentials, episodicRate, readNudgeTotal, pprScores });
		require("./tools.cjs").setupMemoryTool({ ctx, tools, readExec, _nullifyDeep, db }); // [再检查10-04] db deps 传参（usageWrap getConn 静默断供·第六起）
		require("./tools.cjs").setupMemoryExtract({ ctx, tools, runExtract, stats, db, buffer: () => buffer }); // [再检查10-04] stats/db 直传＋buffer getter（重赋值型·#31831）——步⑤b 漏传第四/五/六起
	},
};
