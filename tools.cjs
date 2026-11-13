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
// [拆⑦·2026-10-04] 闸归一轻接：手写路（memory_write）安检段统一走 pipeline 模块（判据 core 原语零新·宽面 extra 语义保持·行为零变）
const { securityGate: _pipelineSec } = require("./pipeline.cjs");
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

//   board_read 读板（可选 sid 读他窗＝父子链·四点③）；三级回退的 ①层（②层＝注入面账态摘要 ④B·③层＝对话历史）。
//   sid 取法＝exec 真身优先＋归一（与 memory_write 同款·09-17 串窗修：多窗并发不得错锚他窗）；
//   output 必带 render（ hard rule）；零板返回 null（零打扰）。
function registerBoardTools(tools, dbConn, getSessionId, logger) {
	const STATUSES = ["pending", "in_progress", "completed"];
	// 审计 I-5 修正（09-20 22:2x）：sid 归一唯一入口——原 board_write 走归一而 board_read 显式 sid 分支/注入面父 sid **不归一**
	//   ⇒ 传裸 uuid 写得到·读不到（仓内 ZD-3 注释自证「子代理 exec 真身可能为裸 uuid」·非纯理论）。三处同径归一。
	//   车五即修（09-30）：normSid 已提升**模块级**（⑩C firehose 作用域可达·原局部定义致压缩点板态快照全败）——本函数内同名引用不改。
	const sidOf = (exec) => {
		const raw =
			(exec && exec.agent && exec.agent.session && exec.agent.session.id) ||
			(typeof getSessionId === "function" && getSessionId()) ||
			"";
		return normSid(raw);
	};
	const renderTxt = (v) => [{ type: "text", text: v && v.text ? v.text : "" }];
	tools.register({
		name: "board_write",
		description:
			"会话板写入（整表替换·last-write-wins）：把本窗任务板持久化到独立存储（板态）——治「几轮后忘/换窗忘」（板不再随上下文压缩或换窗失）。**每次必须发送整份列表**（无增量更新·发送即替换）；空数组=清板。status: pending(未开始)|in_progress(在办)|completed(已完成)。",
		parameters: {
			type: "object",
			required: ["todos"],
			properties: {
				// 审计 D-1 修正（09-20 22:2x·A 路码面审计·致命）：原此处 `required: true` 逐属性＝`defineTool` 的 **spec 授权式**方言，
				//   写在 raw JSON Schema 面会被宿主 `assertSupportedJsonSchema`（dsh-tools L144-159）判 violation ⇒ `tools.register` 注册即抛
				//   ⇒ 板工具双缺（静默）。raw 面唯一合法形态＝**对象级 `required: [字符串数组]`**（阳性对照实测 PASS·仓内 memory/memory_extract 同款）。
				todos: {
					type: "array",
					description: "完整任务列表（整表替换）",
					items: {
						type: "object",
						additionalProperties: false,
						required: ["content", "status"],
						properties: {
							content: {
								type: "string",
								description: "任务内容（短祈使句）",
							},
							status: {
								type: "string",
								enum: STATUSES,
								description: "pending|in_progress|completed",
							},
							note: {
								type: "string",
								description: "可选备注",
							},
							// ── 2403 板 v2（design-approved）：板项可选 id＝open item态 memories.id（板账链接·机器对平）
							//    老调用不带 id＝自由项（对平跳过）·兼容性验证环见 drill。
							id: {
								type: "integer",
								description: "可选·open item态 memories.id（板账链接·wrap-up对平用）",
							},
						},
					},
				},
			},
		},
		output: {
			// 审计 D-1 修正（09-20 22:2x）：逐属性 `required: true` → 对象级数组式（raw JSON Schema 面唯一合法形态·宿主 assertSupportedJsonSchema 硬闸）
			schema: {
				type: "object",
				additionalProperties: false,
				required: ["ok", "sid", "counts", "text"],
				properties: {
					ok: { type: "boolean" },
					sid: { type: "string" },
					// 审计 I-6：>50 帽/空内容未收录数——原静默丢弃（receipt counts 仍自洽·看不出丢），现如实回报
					truncated: { type: "integer" },
					counts: {
						type: "object",
						additionalProperties: false,
						required: ["pending", "inProgress", "completed"],
						properties: {
							pending: { type: "integer" },
							inProgress: { type: "integer" },
							completed: { type: "integer" },
						},
					},
					text: { type: "string" },
				},
			},
			render: (_a, v) => renderTxt(v),
		},
		execute(args, exec) {
			try {
				const sid = sidOf(exec);
				if (!sid)
					return Promise.resolve({
						ok: false,
						sid: "",
						counts: { pending: 0, inProgress: 0, completed: 0 },
						text: "board_write 失败：无法解析本窗 sid（exec.agent.session.id 与兜底单例皆空）",
					});
				const raw = Array.isArray(args.todos) ? args.todos : [];
				const todos = [];
				let truncated = 0; // 审计 I-6：未收录计数（空内容 + 超 50 帽）
				for (const t of raw) {
					const content = String((t && t.content) || "").trim().slice(0, 300);
					if (!content) {
						truncated++;
						continue;
					}
					const status = STATUSES.includes(String(t && t.status))
						? String(t.status)
						: "pending";
					const item = { content, status };
					if (t && t.note) item.note = String(t.note).slice(0, 200);
					// ── 2403 板 v2：id 字段透传（open item态链接·对平用·非法值丢弃防脏数据）──
					if (t && Number.isInteger(t.id) && t.id > 0 && t.id < 1e9) item.id = t.id;
					if (todos.length < 50) todos.push(item);
					else truncated++;
				}
				const nowIsoB = new Date(Date.now() + 8 * 3600 * 1000)
					.toISOString()
					.slice(0, 16) + "+08:00";
				dbConn
					.prepare(
						"INSERT INTO session_boards (sid, payload, updated_at) VALUES (?, ?, ?) ON CONFLICT(sid) DO UPDATE SET payload = excluded.payload, updated_at = excluded.updated_at",
					)
					.run(sid, JSON.stringify({ todos }), nowIsoB);
				// ── 架构 C（design-approved「按建议」·双板镜像升双向）：board_write 写回 todo/write 会话事件 ──
				//    同源 API＝dsh-tool-todo L173（exec.agent.session.append("todo/write",{todos})）——治「板变更不进会话事件流」（⑩A 只单向 todo→board·压缩/换窗后宿主侧板迹断）。
				//    回灌防环：payload 带 src 标记·⑩A 监听段见自源事件跳过镜像（防 note/id 字段降格覆写——⑩A _itemsA 只收 content/status）。
				//    防御：append 不可达（执行环境差异）⇒ 计数 skip（⑬ 计数不可见=半合规·板态写主路径零影响）。
				//    ⚠ 作用域（本函数签名仅 tools/dbConn/getSessionId/logger——无 ctx/stats 全局）
				// ── option（design-approved·🟡-2 四键恒零根治）：三计数落 organ_meta 单键 JSON 持久化 ──
				//   病灶：board_write 系 write 行注册·闭包 stats 与 host 行 stats action 跨 apply 实例物理隔离 ⇒ 四键恒零假观测（ 族）。
				//   治法：计数唯一真相源＝organ_meta（跨 apply 可读）·host 行 stats return 直读——先例同构：rescue_counts（批⑰ B5②）/ patrol_last（车三审 W7）。
				const _wbBump = (k) => {
					try {
						// 🟡A（三十九审·2026-10-05 2317 执行令）：RMW（SELECT→JS加一→UPSERT）改 SQL 原子自增——免并发丢更新（参照 先例 board_selfsrc_count）。
						//   k 系代码内固定三值 ok/skip/err（非用户输入·拼接无注入面）；首 bump 三键全量语义保持（json_object 先立 0 底再 set 目标键=1·与原 {ok:0,skip:0,err:0} 后加一同构）。
						dbConn
							.prepare(
								"INSERT INTO organ_meta (k, v) VALUES ('board_wb_counts', json_set(json_object('ok',0,'skip',0,'err',0), '$.' || ?, 1))" +
									" ON CONFLICT(k) DO UPDATE SET v = json_set(v, '$.' || ?, coalesce(json_extract(v, '$.' || ?), 0) + 1)",
							)
							.run(k, k, k);
					} catch (eWbB) {
						// ⑬ 出口纪律（检查轮抓出·原空吞违例）：计数写失败不可见＝双重封闭——warn 出口（低频·不限流）
						logger?.warn?.(
							"[living-memory] board_wb_counts 计数写失败: " +
								String(eWbB).slice(0, 60),
						);
					}
				};
				try {
					if (
						exec &&
						exec.agent &&
						exec.agent.session &&
						typeof exec.agent.session.append === "function"
					) {
						exec.agent.session.append("todo/write", {
							todos,
							src: "legion-board-write",
						});
						_wbBump("ok");
					} else {
						_wbBump("skip");
					}
				} catch (eWB) {
					_wbBump("err");
					logger?.warn?.(
						"[living-memory] board_write 回写 todo/write 失败: " +
							String(eWB).slice(0, 60),
					);
				}
				const c = (s) => todos.filter((x) => x.status === s).length;
				// ── ⑤wrap-up断点机制化（板 v2 余面⑤·2326 排期·蓝图 §3.3-5「wrap-up前板账对平」机制面）──
				//   触发：新板全部 completed（wrap-up信号）→ 三桶判据（content 内 #id 提取〔正则与 inject.cjs L2 L584 逐字同 /#(\d+)/g〕∪ 板项 id 字段 × 库 type='todo'）
				//   提取源语义差异披露（四十二审甲路 P2-1 修·#27098「同构」须逐字核）：L2 为「id 字段优先·词面退路」（有 id 字段时 content #id 不参与该项判定）；本面为双源∪全查——断点场景更严（content 提及的 active 账也报漂移）·语义取向不同非逐字同构。
				//   漂移（板 completed ∧ 账 active）>0 ⇒ text 附醒目断点提示（不硬拦——板写必须成功·提示随结果走·模型见提示先对账）＋观测键 board_close_warn（原子式·四点接线）。
				let _closeWarn = "";
				try {
					if (todos.length > 0 && c("completed") === todos.length) {
						const _ids = [];
						for (const t of todos) {
							const _m = String(t.content).match(/#(\d+)/g);
							if (_m) for (const s of _m) _ids.push(Number(s.slice(1)));
							if (Number.isInteger(t.id)) _ids.push(t.id);
						}
						if (_ids.length) {
							const _ph = _ids.map(() => "?").join(",");
							const _rows = dbConn
								.prepare("SELECT id, status, space FROM memories WHERE id IN (" + _ph + ") AND type='todo'") // space：缺口 1 销号样例印真实空间（盲审建议-3）
								.all(..._ids);
							const _driftIds = _rows.filter((r) => r.status === "active").map((r) => r.id);
							if (_driftIds.length > 0) {
								// 板机制六缺口车·甲批·缺口 1（10-18）：列号 5→20、超出写「…共 N」，另起一行销号样例（规范·写回第 6、7 条正路：
								//   closeTargetId 双发合一＋solved_by 边＋relatedIds，正文带外部实勘依据）；只提示不动库。总闸 LEGION_BOARD_HINT_OFF=1 回原样
								const _hintOn = core.boardHintOn();
								const _capN = _hintOn ? 20 : 5;
								_closeWarn =
									"\n⚠ wrap-up断点：板账漂移 " +
									_driftIds.length +
									" 项（板已毕∧账 active：" +
									_driftIds.slice(0, _capN).map((x) => "#" + x).join(" ") +
									(_driftIds.length > _capN ? (_hintOn ? " …共 " + _driftIds.length : " …") : "") +
									"）——wrap-up前必先板账对平归账（总纲 §六）" +
									(_hintOn
										? "\n销号样例（每号各发一次）：memory_write(type=fact, space=" +
											((_rows.find((r) => r.id === _driftIds[0]) || {}).space || "<账所在空间>") +
											", title=<销句>, content=<外部实勘依据：文件、SQL 值或提交号>, closeTargetId=" +
											_driftIds[0] +
											", edgeKind=solved_by, relatedIds=[" +
											_driftIds[0] +
											"])"
										: "");
								try {
									dbConn
										.prepare(
											"INSERT INTO organ_meta (k, v) VALUES ('board_close_warn', '1') ON CONFLICT(k) DO UPDATE SET v = CAST(v AS INTEGER) + 1",
										)
										.run();
								} catch (eCW) {
									logger?.warn?.(
										"[living-memory] board_close_warn 计数写失败: " +
											String(eCW).slice(0, 60),
									);
								}
							}
						}
					}
				} catch (eCB) {
					// ⑬ 出口纪律：检测面故障放行不阻断（板写主路径已成功）——warn 留痕
					logger?.warn?.(
						"[living-memory] wrap-up断点检测失败（放行不阻断·⑬）: " +
							String(eCB).slice(0, 60),
					);
				}
				return Promise.resolve({
					ok: true,
					sid,
					truncated,
					counts: {
						pending: c("pending"),
						inProgress: c("in_progress"),
						completed: c("completed"),
					},
					text:
						"板已更新（" +
						sid.slice(0, 24) +
						"）：" +
						c("pending") +
						" 待办 / " +
						c("in_progress") +
						" 在办 / " +
						c("completed") +
						" 已完成" +
						(truncated > 0
							? "（未收录 " +
								truncated +
								" 项：空内容或超 50 项帽——整表替换语义下请压缩列表后重发）"
							: "") +
						_closeWarn,
				});
			} catch (eBw) {
				logger?.warn?.(
					"[living-memory] board_write 失败: " + String(eBw).slice(0, 80),
				);
				return Promise.resolve({
					ok: false,
					sid: "",
					counts: { pending: 0, inProgress: 0, completed: 0 },
					text: "board_write 失败：" + String(eBw).slice(0, 80),
				});
			}
		},
	});
	// 1558＋2301 A①：注册时同臂；只认 "1"。OFF 的定义、render 和返回值均走原路。
	const fullBoardRead = process.env.LEGION_BOARD_READ_FULL_OFF !== "1";
	tools.register({
		name: "board_read",
		description:
			"会话板读取：读本窗持久化的任务板（板态）。可选 sid 参数读指定会话板（父子链：子窗读父窗板）。返回 todos 列表；无板返回 null。" +
			(fullBoardRead ? "默认为摘要（写明总数/显示数/省略数）；完整读板传 full:true，返回可原样写回的 todos（id、状态、全文、note）。禁止据摘要整表回写。" : ""),
		parameters: {
			type: "object",
			properties: {
				sid: {
					type: "string",
					description: "可选：指定会话 id 读他窗板（缺省＝本窗）",
				},
				...(fullBoardRead ? { full: { type: "boolean", description: "true：结构化全量及完整文本（含id、note），可往返整表；缺省只读摘要，不可据摘要回写。" } } : {}),
			},
		},
		output: {
			// 审计 D-1 修正（09-20 22:2x）：逐属性 `required: true` → 对象级数组式（同 board_write）
			schema: {
				type: "object",
				additionalProperties: false,
				required: ["ok", "sid", "total", "text"],
				properties: {
					ok: { type: "boolean" },
					sid: { type: "string" },
					updatedAt: { type: "string" },
					total: { type: "integer" },
					text: { type: "string" },
					...(fullBoardRead ? {
						shown: { type: "integer" },
						omitted: { type: "integer" },
						todos: { type: "array", items: {
							type: "object", additionalProperties: false, required: ["content", "status"],
							properties: { content: { type: "string" }, status: { type: "string", enum: STATUSES }, id: { type: "integer" }, note: { type: "string" } },
						} },
					} : {}),
				},
			},
			render: (_a, v) => renderTxt(v),
		},
		execute(args, exec) {
			try {
				// 审计 I-5 修正（09-20 22:2x）：显式 sid 分支同径归一（原直用裸值 ⇒ 写得到读不到）
				const sid =
					(args && args.sid ? normSid(args.sid) : "") || sidOf(exec);
				if (!sid)
					return Promise.resolve({
						ok: false,
						sid: "",
						total: 0,
						text: "board_read 失败：无法解析 sid",
					});
				const row = dbConn
					.prepare(
						"SELECT sid, payload, updated_at FROM session_boards WHERE sid = ?",
					)
					.get(sid);
				if (!row)
					return Promise.resolve({
						ok: true,
						sid,
						total: 0,
						text: "本窗暂无持久化板（板态为空）" + (fullBoardRead ? "；共 0 项，列 0 项，省略 0 项。全量传 full:true。" + (args?.full === true ? "\n{\"todos\":[]}" : "") : ""),
						...(fullBoardRead ? { shown: 0, omitted: 0, ...(args?.full === true ? { todos: [] } : {}) } : {}),
					});
				let todos = [];
				try {
					const p = JSON.parse(row.payload);
					if (p && Array.isArray(p.todos)) todos = p.todos;
				} catch {}
				const marks = { completed: "☒", in_progress: "■", pending: "☐" };
				const lines = todos
					.slice(0, 20)
					.map(
						(t) =>
							(marks[t.status] || "☐") + " " + String(t.content).slice(0, 60),
					);
				if (fullBoardRead) {
					const full = args?.full === true;
					const shown = full ? todos.length : lines.length;
					return Promise.resolve({
						ok: true, sid, updatedAt: row.updated_at, total: todos.length,
						shown, omitted: todos.length - shown,
						...(full ? { todos } : {}),
						text: "板（" + sid.slice(0, 24) + "·" + row.updated_at + "）：共 " + todos.length + " 项，列 " + shown + " 项，省略 " + (todos.length - shown) + " 项。" +
							(full ? "全量 todos（可原样传给 board_write）：\n" + JSON.stringify({ todos }, null, 2) : "摘要不可整表回写；取全量请 board_read 传 full:true（读他窗时保留同一 sid）。\n" + (lines.join("\n") || "（空）")),
					});
				}
				return Promise.resolve({
					ok: true,
					sid,
					updatedAt: row.updated_at,
					total: todos.length,
					text:
						"板（" +
						sid.slice(0, 24) +
						"·" +
						row.updated_at +
						"·" +
						todos.length +
						" 项）：\n" +
						(lines.join("\n") || "（空）"),
				});
			} catch (eBr) {
				logger?.warn?.(
					"[living-memory] board_read 失败: " + String(eBr).slice(0, 80),
				);
				return Promise.resolve({
					ok: false,
					sid: "",
					total: 0,
					text: "board_read 失败：" + String(eBr).slice(0, 80),
				});
			}
		},
	});
}


// ── wave 复活口·阶段 4（10-18·草纲下篇一改点 5；侦察 K3 T3-9）：memory_write 手工改期／复活（renewTargetId）──
//    携带时双发合一（closeTargetId 同法）：本条入库作改期说明，另按 id 改期或复活一条目标。走写闸：空间闸、手术旗、安检等在前照判本条，这里再限
//    ①只受理memory organ、brain两窗（双特权；其他窗回明示投件）②目标须在本条 space（桥白名单只放 brain，故经桥只能动 brain 条）
//    ③每窗每北京日至多 LIFE_MANUAL_DAY_CAP 条（organ_meta life_manual_day）④待办、被取代条、active／aged 以外的状态不受理，active 条没有期限（valid_to 空）也不受理。
//    两步：lifeManualPlan 先核、不写库——不受理时整条拒写（说明条不入库）；说明条入库后 lifeManualDo 再写；说明条被同文闸拦时改期也不办（同参重发不会连改两回）。
//    active 条改期只改 valid_to；aged 条复活：status 回 active、closed_at 清空、valid_to 改新期（stale_state 照旧）。新期＝renewTo（YYYY-MM-DD，晚于今天、一年内），
//    或 max(今天, 原期)＋renewDays（1–365，缺省 30；形态随原值，同nightly patrol复活段）。记 life_renew（reason manual:<本窗空间>，已有行 n+1、hits 清零），
//    life_revive 记一行原值（与nightly patrol复活同一复原口径），revive_pending 里有就删。一个 SAVEPOINT，出错整段回滚、回 ok:false。
//    LEGION_REVIVE_OFF=1 时 schema 三参不出、传了也不理（与底座逐字同）
const LIFE_MANUAL_DAY_CAP = 30;
function lifeManualPatch(def) {
	if (!core.reviveOn()) return def;
	const p = def.parameters.properties;
	p.renewTargetId = {
		type: "number",
		description:
			"可选·wave复活口手工兜底：按 id 改期或复活一条——目标须在本次写入 space；active 条改期，aged（已老化）条复活回在役。限memory organ、brain两窗（其他窗请投 模块/memory organ/inbox/ 代办），每窗每北京日至多 " +
			LIFE_MANUAL_DAY_CAP +
			" 条；待办、被取代条不受理。携带时双发合一：本条照常入库作改期说明（title 写改期句与理由），记 life_renew（reason manual:<本窗空间>）",
	};
	p.renewDays = { type: "number", description: "renewTargetId 用：新期限＝max(今天, 原期)＋renewDays 天（1–365，缺省 30）" };
	p.renewTo = { type: "string", description: "renewTargetId 用：直接给新期限 YYYY-MM-DD（须晚于今天、一年以内；与 renewDays 同给时以 renewTo 为准）" };
	def.description += "按 id 改期／复活用 renewTargetId（限memory organ、brain窗·每日帽 " + LIFE_MANUAL_DAY_CAP + "）。";
	return def;
}
function lifeManualPlan(dbConn, args, caller, priv, ts) {
	const id = Number(args.renewTargetId);
	if (!Number.isInteger(id) || id <= 0) return { ok: false, reason: "renewTargetId 非法（正整数）: " + String(args.renewTargetId).slice(0, 20) };
	if (!caller) return { ok: false, id, reason: "本窗归属解析失败——改期／复活不受理" };
	if (!priv) return { ok: false, id, reason: "改期／复活限memory organ、brain两窗——本窗「" + caller + "」请投 模块/memory organ/inbox/ 代办" };
	const today = String(ts).slice(0, 10);
	const plusD = (d10, n) => new Date(Date.parse(d10 + "T00:00:00Z") + n * 86400e3).toISOString().slice(0, 10);
	const bjMin = (ms) => {
		const d = new Date(ms + 8 * 3600e3);
		const p2 = (n) => String(n).padStart(2, "0");
		return `${d.getUTCFullYear()}-${p2(d.getUTCMonth() + 1)}-${p2(d.getUTCDate())}T${p2(d.getUTCHours())}:${p2(d.getUTCMinutes())}+08:00`;
	};
	let day = { d: today, n: {} };
	try {
		const r0 = dbConn.prepare("SELECT v FROM organ_meta WHERE k = 'life_manual_day'").get();
		const o = r0 ? JSON.parse(r0.v) : null;
		if (o && o.d === today && o.n && typeof o.n === "object") day = { d: today, n: o.n };
	} catch {}
	const used = Number(day.n[caller]) || 0;
	if (used >= LIFE_MANUAL_DAY_CAP)
		return { ok: false, id, reason: "每日帽 " + LIFE_MANUAL_DAY_CAP + " 已满（本窗「" + caller + "」北京 " + today + " 已改期／复活 " + used + " 条）——明日再办或投memory organ" };
	let supCol = false;
	try {
		supCol = dbConn.prepare("PRAGMA table_info(memories)").all().some((c) => c && c.name === "superseded_by");
	} catch {}
	const t = dbConn.prepare("SELECT id, type, space, status, stale_state, valid_to, closed_at" + (supCol ? ", superseded_by" : "") + " FROM memories WHERE id = ?").get(id);
	if (!t) return { ok: false, id, reason: "目标 #" + id + " 不存在" };
	if (t.space !== String(args.space))
		return { ok: false, id, reason: "目标 #" + id + " 在「" + t.space + "」，本条写入 space「" + String(args.space) + "」——改期／复活限本 space 的条目（说明条与目标同 space）" };
	if (t.type === "todo") return { ok: false, id, reason: "目标 #" + id + " 是待办——待办走 closeTargetId 闭环或「重开：」，不改期／复活" };
	if (t.status !== "active" && t.status !== "aged") return { ok: false, id, reason: "目标 #" + id + " status=" + t.status + "——只受理 active（改期）、aged（复活）" };
	if (t.stale_state === "superseded-auto" || t.stale_state === "retired" || (supCol && t.superseded_by != null))
		return { ok: false, id, reason: "目标 #" + id + " 已被取代——不改期／复活" };
	const v0 = t.valid_to == null ? null : String(t.valid_to);
	if (t.status === "active" && !v0) return { ok: false, id, reason: "目标 #" + id + " 没有期限（valid_to 空）——不用改期" };
	let v1;
	if (args.renewTo !== undefined && args.renewTo !== null && String(args.renewTo).trim() !== "") {
		const s = String(args.renewTo).trim();
		const real = /^\d{4}-\d{2}-\d{2}$/.test(s) && Number.isFinite(Date.parse(s + "T00:00:00Z")) && new Date(Date.parse(s + "T00:00:00Z")).toISOString().slice(0, 10) === s;
		if (!real) return { ok: false, id, reason: "renewTo 须是 YYYY-MM-DD 的真实日期: " + s.slice(0, 20) };
		if (s <= today) return { ok: false, id, reason: "renewTo 须晚于今天（北京 " + today + "）" };
		if (s > plusD(today, 365)) return { ok: false, id, reason: "renewTo 至多一年内（≤ " + plusD(today, 365) + "）" };
		v1 = s;
	} else {
		const days = args.renewDays === undefined || args.renewDays === null ? 30 : Number(args.renewDays);
		if (!Number.isInteger(days) || days < 1 || days > 365) return { ok: false, id, reason: "renewDays 须是 1–365 的整数" };
		const s = v0 || "";
		if (/^\d{4}-\d{2}-\d{2}$/.test(s) && Number.isFinite(Date.parse(s + "T00:00:00Z"))) v1 = plusD(s > today ? s : today, days);
		else {
			const t0 = s ? Date.parse(s.replace(" ", "T")) : NaN;
			v1 = s.length > 10 && Number.isFinite(t0) ? bjMin(Math.max(t0, Date.now()) + days * 86400e3) : plusD(today, days);
		}
	}
	return { ok: true, id, t, v0, v1, reason: "manual:" + caller, caller, used };
}
// 第二步（说明条入库之后）：照计划写库。一个 SAVEPOINT，出错整段回滚、回 ok:false（说明条已入库，receipt带 ⚠）
function lifeManualDo(dbConn, plan, ts, logger) {
	const { id, t, v0, v1, reason, caller } = plan;
	const P = require("./patrol.cjs"); // 两表建表句只在nightly patrol件一处（ensureLifeRenew、ensureLifeRevive），这里借用
	let sp = false;
	let used = plan.used;
	try {
		dbConn.exec("SAVEPOINT c3_life");
		sp = true;
		// 每日帽在事务里再核一次（先核到入库之间别的窗可能也办了）
		let day = { d: String(ts).slice(0, 10), n: {} };
		try {
			const r0 = dbConn.prepare("SELECT v FROM organ_meta WHERE k = 'life_manual_day'").get();
			const o = r0 ? JSON.parse(r0.v) : null;
			if (o && o.d === day.d && o.n && typeof o.n === "object") day = { d: day.d, n: o.n };
		} catch {}
		used = Number(day.n[caller]) || 0;
		if (used >= LIFE_MANUAL_DAY_CAP) throw new Error("每日帽 " + LIFE_MANUAL_DAY_CAP + " 已满");
		P.ensureLifeRenew(dbConn);
		P.ensureLifeRevive(dbConn);
		const lr0 = dbConn.prepare("SELECT n, reason, last_at, hits FROM life_renew WHERE id = ?").get(id);
		const up =
			t.status === "aged"
				? dbConn.prepare("UPDATE memories SET status = 'active', closed_at = NULL, valid_to = ? WHERE id = ? AND status = 'aged'").run(v1, id)
				: dbConn.prepare("UPDATE memories SET valid_to = ? WHERE id = ? AND status = 'active'").run(v1, id);
		if (Number(up.changes) !== 1) throw new Error("目标状态已变");
		dbConn
			.prepare(
				"INSERT INTO life_renew (id, n, first_at, last_at, old_valid_to, reason, hits) VALUES (?, 1, ?, ?, ?, ?, 0) ON CONFLICT(id) DO UPDATE SET n = n + 1, last_at = excluded.last_at, reason = excluded.reason, hits = 0",
			)
			.run(id, ts, ts, v0, reason);
		dbConn
			.prepare(
				"INSERT INTO life_revive (id, at, why, old_status, old_closed_at, old_valid_to, new_valid_to, lr_had, lr_n, lr_reason, lr_last_at, lr_hits) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
			)
			.run(id, ts, reason, t.status, t.closed_at == null ? null : String(t.closed_at), v0, v1, lr0 ? 1 : 0, lr0 ? lr0.n : null, lr0 ? lr0.reason : null, lr0 ? lr0.last_at : null, lr0 ? lr0.hits : null);
		if (dbConn.prepare("SELECT 1 AS x FROM sqlite_master WHERE type = 'table' AND name = 'revive_pending'").get()) dbConn.prepare("DELETE FROM revive_pending WHERE id = ?").run(id);
		day.n[caller] = used + 1;
		dbConn.prepare("INSERT INTO organ_meta (k, v) VALUES ('life_manual_day', ?) ON CONFLICT(k) DO UPDATE SET v = excluded.v").run(JSON.stringify(day));
		dbConn.prepare("INSERT INTO organ_meta (k, v) VALUES ('life_manual_total', '1') ON CONFLICT(k) DO UPDATE SET v = CAST(CAST(v AS INTEGER) + 1 AS TEXT)").run();
		dbConn.exec("RELEASE c3_life");
		sp = false;
	} catch (e) {
		if (sp) {
			try {
				dbConn.exec("ROLLBACK TO c3_life");
				dbConn.exec("RELEASE c3_life");
			} catch {}
		}
		try {
			logger?.warn?.("[living-memory] wave 手工改期／复活失败（已回滚、目标未动）: " + String((e && e.message) || e).slice(0, 60));
		} catch {}
		return { ok: false, id, reason: "改期／复活执行异常（已回滚、目标未动）: " + String((e && e.message) || e).slice(0, 40) };
	}
	return { ok: true, id, act: t.status === "aged" ? "revive" : "renew", from: { status: t.status, valid_to: v0 }, to: { status: "active", valid_to: v1 }, reason, today: used + 1, cap: LIFE_MANUAL_DAY_CAP };
}

function registerMemoryWrite(tools, dbConn, getSessionId, credResolve, logger) {
	// 审计 D3 修正（14:42）：+logger 参数——模块级函数无 ctx，原两处 ctx.logger 引用系作用域幻引用
	tools.register(lifeManualPatch({ // wave 复活口·阶段 4：复活开关开时补 renewTargetId 三参（关时原样）
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
						"subtask_of",
						"depends_on",
						"part_of_campaign",
						"duplicate_of",
					],
					description:
						"可选·语义边型（wave·graph-memory 五语义边意）：显式边关系类型·缺省 explicit；**闸④（09-20 maintainer）扩四型＝任务结构边**：subtask_of 子步→父任务／depends_on 前置依赖／part_of_campaign 归属战役／duplicate_of 同题族（思维导图图基座）",
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
				const spaceGatePriv = core.privSpace(spaceGateCaller); // wave 阶段 5：双特权两窗名单提成 core.PRIV_SPACES，与复活口管理窗判法共用（原内联两等式，判法不变）
				// ── A-G 架构 A 件（09-27 approved）：cross-space流转账放行型 ──
				//    global＋todo＋title 前缀（交办：/pending ruling：）——各窗可dispatch（B 三通道之 global 席触达）；
				//    数量帽 30 防注入席污染（超帽拒并计数）；fact/lesson 等仍拦（防cross-space记忆污染·option原意保住）。
				const _dispatchOk =
					args.space === "global" &&
					String(args.type) === "todo" &&
					/^(交办：|呈裁：)/.test(String(args.title || ""));
				if (_dispatchOk) {
					try {
						const _n = dbConn
							.prepare(
								"SELECT COUNT(*) c FROM memories WHERE space='global' AND type='todo' AND status='active'",
							)
							.get().c;
						if (_n >= 30) {
							core.globalDispatchCap += 1;
							return {
								error:
									"流转账数量帽 30 已满（global 活跃交办/pending ruling todo " +
									_n +
									" 条）——请先闭环旧件（销账/呈maintainer）再派新",
							};
						}
					} catch (eCap) {
						// fail-visible（drill T4 实抓：db 变量名错曾致静默失效）：帽查询异常必须可见·不放行静默
						try { logger?.warn?.("[living-memory] 流转账帽查询失败（放行但不计数）: " + String(eCap).slice(0, 60)); } catch {}
					}
				}
				if (!SPACE_GATE_CONFIGURED) spaceGateCounts.unconfigured += 1; // 降级放行面可观测（⑬：计数不可见＝半合规）
				if (
					!(
						!SPACE_GATE_CONFIGURED || // 闸未配置（无任何目录映射）→ 放行·见上「空间闸未配置降级」段
						(spaceGateCaller &&
							(spaceGatePriv || spaceGateCaller === args.space || _dispatchOk))
					)
				) {
					spaceGateCounts.blocked += 1;
					if (Date.now() - core.spaceGateLastWarnAt > 60000) {
						core.spaceGateLastWarnAt = Date.now();
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
					core.surgerySkipWrite += 1;
					return {
						error: "maintenance window挂牌中——写入冻结（摘牌恢复·SURGERY_FLAG 在位）",
						surgery: true,
					};
				}
				// [拆⑦·2026-10-04] 闸归一轻接：pipeline.securityGate 宽面形态（extra=source+relatedNotes 并入扫描——拼接序逐字同：content\nsource\nrelatedNotes；判据 core 原语零新·行为零变）
				const sec = _pipelineSec(
					stripUrls(String(args.title)),
					stripUrls(String(args.content)),
					String(args.source || "") + "\n" + String(args.relatedNotes || ""),
				); // 8-31 长尾乙档（F0-3）：source/relatedNotes 并入扫描面——原两列模型可控输入裸奔（凭据/指令载荷可经 source/边语义落盘并回流注入面）；件三车病灶3（09-09）：title/content 改检 stripUrls 后形态（与落库面一致——原检原文致长 URL query 含 sk- 族形态误伤·提炼路先 strip 后检同序对齐）·source/relatedNotes 保持原样（独立落库列无 strip·真载荷面）
				if (!sec.pass) {
					core.writeRejectedCount++; // 序2 最小刀④（09-15 brain 00:59 件）：写路安检拒计数——原零计数零 warn（安检拒只盖提炼路·写路拒条事后不可审计·⑬计数不可见族）
					console.warn?.(
						`[living-memory] write-security reject (#${core.writeRejectedCount}): ${String(args.title || "").slice(0, 50)}`,
					);
					return {
						error:
							"rejected: " + sec.reason + "（记忆安检：凭据/指令载荷不入库）",
					};
				}
				// ── 四闸·closure gate（2026-08-22）：todo 写入前验真——主题与近期闭环面条目重叠≥3 → 拒绝入库 ──
				//    先 UPDATE status=done 再写销账 fact（闭环正路）；「重开：」前缀=显式重开专项，越过闸门。
				if (String(args.type) === "todo") {
					const t0 = String(args.title || "");
					// D 路审计 D-真3 修（09-20 22:4x·低）：原 `!t0.startsWith("重开：")`（仅全角冒号·零空白容忍）与
					//   闸① 的 `/^\s*重开\s*[:：]/` **不同款** ⇒ 非标准形态（`重开:X`／前导空格）闸①跳查重而本闸不认
					//   ⇒ 出现「用户已带重开却被劝『请用重开：』」的绕圈。两闸统一为宽松形态（显式重开语义优先）。
					//   ⚠ E 路审计 真3 补注（09-20 23:1x）：统一后的新闸**严格于旧闸**——旧 `startsWith("重开：")` 曾把
					//   `重开X：`／裸 `重开`／`X重开：` 也当逃生口；新式 `/^\s*重开\s*[:：]/` 要求「重开」在行首且后随冒号
					//   ⇒ 上述三形态**不再**视为显式重开（语义更严谨·登记在案）。
					if (!/^\s*重开\s*[:：]/.test(t0)) {
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
						// ── C 档open item词表软提示：原口径（08-24「open item型建议 fact 装」）已被 #30540「候面一律 todo」取代（ superseded），
						//    原提示反劝回旧口径（10-03 起 75 次）。改为提醒写清流转、立账后记得销（写入面修·R1 车）──
						if (CRIT.stashHint.test(t0)) { // C 档open item词表已外置（criteria-rules.default.json·2026-09-12 一车）
							cGateHint =
								"C档提示：todo「" +
								t0.slice(0, 30) +
								"」属open item型——候面照 #30540 用 todo 装；正文写清「流转＝」等谁、办结后用 closeTargetId 销";
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
				const title = promptSafe(stripUrls(String(args.title))); // 注入面净化（09-13·源头治本）：写入即破坏「连续双花括号」——库内零雷
				const content = promptSafe(stripUrls(String(args.content)));
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
				// ── 闸① 立条查重·同题提示（design-approved「同意 按建议」·治「重复提起」）──
				//    定位＝梁 1b（同文幂等闸·title+content 全同＋24h 窗·**硬拦**）的**同族补充**：
				//      梁 1b 治「同文连发」，本闸治「同题不同文·跨时间重复open item」（SZXL-001 同题五连立实证 /8765/9955/10221/10230）
				//    判据：type=todo ∧ 同 space ∧ status='active' ∧ title 归一化（剥括注/待办前缀/空白标点）后 ①完全同串 或 ②长句互含（≥10 字）
				//    动作：**softWarn 提示·不阻断**（同题跨时间二次open item可能合理——如再次候令；硬拦留给梁 1b 全同场景·宁漏勿误）
				//    依据：#12280 lesson「立条前必先检索查重」的机器化 + 闸①方案（maintainer）
				try {
					// I-2 修（09-20 审计·改进）：与closure gate「重开：」逃生口**对齐**——显式重开＝用户明示另起，
					//   闸①不再提示（原仅closure gate认该前缀 ⇒ `重开：SZXL-001 …` 与在办条判近似、提示语却劝「勿另立」＝语义相反）。
					if (
						String(args.type) === "todo" &&
						!/^\s*重开\s*[:：]/.test(String(title || ""))
					) {
						const _stripPrefix = (s) =>
							String(s || "").replace(
								/^\s*(?:待办|todo)\s*[:：]?\s*/i,
								"",
							);
						const _stripParen = (s) =>
							String(s || "").replace(/[（(][^)）]{0,40}[)）]/g, "");
						const _normT = (s) =>
							_stripParen(_stripPrefix(s))
								.replace(/[\s·、,，。:：;；\-—_]+/g, "")
								.toLowerCase();
						// I-1 修（09-20 审计·改进）：括注内**判别符**（批次/日期）被无差别抹平 ⇒ 该族必误判「同题」
						//   （实测构造：`四靶①event_at补填（9-12）` vs `（9-15）` → 判同题）。治法＝同时算一份**不剥括注**的
						//   归一形：仅当两形皆同才称「同题」；仅括注不同 → 降级标「疑似同题（仅括注/判别符不同）」——
						//   不丢提示面（宁漏勿误的反面亦然），但禁假断言。
						const _normRaw = (s) =>
							_stripPrefix(s)
								.replace(/[\s·、,，。:：;；\-—_]+/g, "")
								.toLowerCase();
						const _nt = _normT(title);
						const _nrt = _normRaw(title);
						if (_nt.length >= 6) {
							const _cands = dbConn
								.prepare(
									"SELECT id, title, status, ts FROM memories WHERE type='todo' AND space=? AND status='active' ORDER BY id DESC LIMIT 60",
								)
								.all(String(args.space || ""));
							const _dup = [];
							for (const _c of _cands) {
								const _ct = _normT(_c.title);
								if (_ct.length < 6) continue;
								if (_ct === _nt) {
									_dup.push(
										"#" +
											_c.id +
											"「" +
											String(_c.title).slice(0, 34) +
											"」" +
											(_normRaw(_c.title) === _nrt
												? "同题"
												: "疑似同题（仅括注/判别符不同）"),
									);
									continue;
								}
								if (
									(_nt.length >= 10 && _ct.includes(_nt)) ||
									(_ct.length >= 10 && _nt.includes(_ct))
								)
									_dup.push(
										"#" +
											_c.id +
											"「" +
											String(_c.title).slice(0, 34) +
											"」近似",
									);
								if (_dup.length >= 3) break;
							}
							if (_dup.length > 0) {
								const dHint =
									"⚠闸①同题查重（09-20）：本 space 已有 active todo 疑似同题——" +
									_dup.join("；") +
									"——若确为同题请带 relatedIds=[该id]+edgeKind=duplicate_of 重提（勿另立），或改标题确为新任务";
								cGateHint = cGateHint
									? cGateHint + "；" + dHint
									: dHint;
							}
						}
					}
				} catch (_e1) {
					// ⑬ 出口纪律：错误不可见＝双重封闭——本闸失败出声不吞
					logger?.warn?.(
						"[living-memory] 闸①同题查重失败: " +
							String(_e1).slice(0, 60),
					);
				}
				// ── C 落码车（2026-09-24）：手写路软同题闸（fact/lesson 观测级）——复用闸① 同一判据族·不阻断只计数 ──
				//    判据（禁另起炉灶）：gateTitleTokens／gateTokenInter／同题交叠阈键（_ovMin·来源 criteria 外置）
				//    ⚠ **召回边界（10-01 标注·#22820 承接·maintainer「发车」）**：本闸＝字面 token 判据，**召回有限**——
				//    十四审量化：auto 回声与手写条同款判据交叠中位 0.115／最大 0.384／人工认定真回声对低至 0.020–0.075，
				//    复刻在役判据得**召回上限 9.7%**（乙路）。⇒ **不得宣称本闸「治提炼回声」**；补 content 级／向量级同题判据＝另案评估。
				try {
					if (String(args.type) === "fact" || String(args.type) === "lesson") { // R1①修（十八审甲路·原裸 type 零绑定 ReferenceError·闸死两日）
						const _ovMin = CRIT.sameTopicOverlapMin; // V13 取用归一（0.2.10 发布链）
						const _cToks = gateTitleTokens(title);
						if (_cToks.size >= 2) { // 写入面修（R1 车）：gateTitleTokens 返回 Set，原 .length 恒 undefined ⇒ 条件恒假、本闸 09-24 起从未生效
							for (const _p of dbConn.prepare(
								"SELECT id, title FROM memories WHERE status='active' AND type IN ('fact','lesson') AND space = ? ORDER BY id DESC LIMIT 50",
							).all(String(args.space || "memory-organ"))) {
								if (gateTokenInter(_cToks, gateTitleTokens(_p.title)) >= _ovMin) {
									core.softSameTopicWarn += 1; // 模块级计数（L14180 区接线·闸① 同判据·观测不阻断）
									logger?.warn?.(
										`[living-memory] 软同题观测（#${core.softSameTopicWarn}）：新条与 #${_p.id} title 交叠 ≥${_ovMin} 词（soft 不阻断·请用 relatedIds+duplicate_of 显式关联；⚠ 字面判据召回上限 9.7%·勿当回声治理闸）`,
									);
									break;
								}
							}
						}
						// ── 回声车（09-25 maintainer③·净候裁③深勘第三修正）：fact/lesson 已办回声观测 ──
						//    缺口settled：主路 todo 型closure gate完整（L3775 阻断级）；fact/lesson 零 closureCheck ⇒ #21105 型回声漏网。
						//    观测不阻断（正当复盘重录合法）——closureCheck 复用（禁另起炉灶）。
						//    Y2（09-26 maintainer·十八审候裁四小项）：收进本 type 门内——消 decision 超设计面与 todo 白跑扫描（实现回归settled面）。
						const _cc2 = closureCheck(dbConn, title, content, null);
						if (_cc2.closed) {
							core.echoWarnCount += 1;
							logger?.warn?.(
								`[living-memory] 已办回声观测（#${core.echoWarnCount}）：与闭环条 #${_cc2.by}「${_cc2.byTitle}」重叠 ${_cc2.overlap} 词——已办事项再录为 ${String(args.type)}（观测不阻断·复盘请引 #id·勿重述）` // R1②修（原 ${type} 第二颗雷）,
							);
						}
					}
				} catch (_eC) {
					logger?.warn?.("[living-memory] C 软同题闸失败: " + String(_eC).slice(0, 60)); // 照闸① ⑬ 惯例：失败出声不吞
				}
				// wave 复活口·阶段 4：renewTargetId 先核（不写库，放 closeTargetId 之前）——不受理时整条拒写（说明条不入库、别的也不动）；
				//   受理的等说明条入库后再改期／复活（lifeManualDo）；说明条被同文闸拦时改期也不办（同参重发不会连改两回）
				let renewPlan = null;
				let renewTarget = null;
				if (core.reviveOn() && args.renewTargetId !== undefined && args.renewTargetId !== null) {
					renewPlan = lifeManualPlan(dbConn, args, spaceGateCaller, spaceGatePriv, ts);
					if (!renewPlan.ok) return { error: "改期／复活不受理：" + renewPlan.reason + "（本条未入库）" };
				}
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
									"SELECT id, status, closed_at, space, type, title FROM memories WHERE id = ?", // ⑩B：+title（办结刷板同题匹配）
								)
								.get(closeTarget);
							if (
								!t3 ||
								t3.type !== "todo" ||
								(t3.space !== String(args.space) &&
									// 甲4 订正（二十二审·解冻后）：global 交办/pending ruling账承办窗可销——原硬判 space 相等
									// ⇒ receipt销句（fact）被闸拦·dispatch账永久占帽（蓄死账）。
									// 边界（09-27 深夜maintainer「甲路推荐·案 a」）：豁免追加**承办校验**——
									// title 须含本窗 caller space名（「交办：<承办space>｜」惯例）·特权窗（统筹）不受限。
									!(t3.space === "global" &&
										/^(交办：|呈裁：)/.test(String(t3.title || "")) &&
										(spaceGatePriv ||
											String(t3.title || "").includes(String(spaceGateCaller || "\u0000")) ||
											// 形态补（T7 实抓）：title 惯例用中文space名（「交办：创造｜」）·caller 是英文 space 名——
											// 二十四审⚠️2 修：反查改用完整 ORGAN_DIR_MAP（含自注册·与 caller 解析同表）＋全段遍历（hand 双目录名「PTC/代码」皆认）
											ORGAN_DIR_MAP.filter((m2) => m2[1] === spaceGateCaller).some((m2) =>
												String(t3.title || "").includes(m2[0])))))
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
									.run(ts, closeTarget, t3.space); // WHERE belt-and-braces收窄 status='active'（防 SELECT→UPDATE 间竞态·三案修补案1）
								closedTodo = {
									ok: true,
									id: t3.id,
									closedAt: ts,
									prior: t3.status,
								};
								// ⑩B 办结即刷板（step B·「办毕销回账」变机器动作·治「机制不该依赖自律」）
								//   办结成功后：本窗板内同题项标 completed（归一化互含匹配·匹配不到静默不硬造·零明文零网络）。
								try {
									const _rawSidB =
										(exec &&
											exec.agent &&
											exec.agent.session &&
											exec.agent.session.id) ||
										(typeof getSessionId === "function" && getSessionId()) ||
										"";
									const _sidBd = _rawSidB
										? /^session-/.test(String(_rawSidB))
											? String(_rawSidB)
											: "session-" + String(_rawSidB)
										: "";
									if (_sidBd) {
										const _rowB = dbConn
											.prepare(
												"SELECT payload FROM session_boards WHERE sid = ?",
											)
											.get(_sidBd);
										if (_rowB) {
											let _plB = null;
											try {
												_plB = JSON.parse(_rowB.payload);
											} catch {}
											const _arrB =
												_plB && Array.isArray(_plB.todos)
													? _plB.todos
													: null;
											if (_arrB) {
												const _nzB = (s) =>
													String(s == null ? "" : s)
														.replace(/[\s·、,，。:：;；\-—_]+/g, "")
														.toLowerCase();
												const _tgtB = _nzB(t3.title); // 词面兜底仍用原目标标题，OFF 不改旧语义
												let _idRowsB = null;
												if (process.env.LEGION_BOARD_CLOSE_ID_OFF !== "1") {
													// 1558：全表分级优先，已命中 closeTargetId 不再旁刷标题号或词面。
													const pendingB = _arrB.filter((it) => it && it.status !== "completed");
													let matchedB = pendingB.filter((it) => it.id === closeTarget);
													if (!matchedB.length) {
														const titleIdsB = String(title || "").match(/#[1-9]\d*\b/g) || [];
														if (titleIdsB.length === 1) matchedB = pendingB.filter((it) => it.id === Number(titleIdsB[0].slice(1)));
													}
													if (matchedB.length) _idRowsB = new Set(matchedB); // 只对 id 字段，不搜正文 #id
												}
												let _hitB = 0;
												const _hitsB = []; // E 路审计 真4：命中留痕（原仅计数·误标后不可回溯）
												for (const _itB of _arrB) {
													if (!_itB || _itB.status === "completed")
														continue;
													const _ncB = _nzB(_itB.content);
													if (_idRowsB ? _idRowsB.has(_itB) : (
														_ncB && _tgtB.length >= 6 && (_ncB === _tgtB ||
															(_tgtB.length >= 8 && (_ncB.includes(_tgtB) || _tgtB.includes(_ncB))))
													)) {
														_itB.status = "completed";
														_hitB++;
														_hitsB.push(_arrB.indexOf(_itB));
													}
												}
												if (!_hitB) closedTodo.boardMiss = true; // E 路审计 真4：无命中留痕（便于观测「板内无同题」频度）
												if (_hitB > 0) {
													dbConn
														.prepare(
															"UPDATE session_boards SET payload = ?, updated_at = ? WHERE sid = ?",
														)
														.run(
															JSON.stringify({ todos: _arrB }),
															new Date(Date.now() + 8 * 3600 * 1000)
																.toISOString()
																.slice(0, 16) + "+08:00",
															_sidBd,
														);
													closedTodo.boardRows = _hitB;
													closedTodo.boardHits = _hitsB;
												}
											}
										}
									}
								} catch (eBC) {
									try {
										logger?.warn?.(
											"[living-memory] 办结刷板失败: " +
												String(eBC).slice(0, 60),
										);
									} catch {}
								}
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
										(closedTodo.boardRows
											? "·刷板 " + closedTodo.boardRows + " 行"
											: "") +
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
									_closeNote +
									(renewPlan ? "｜改期／复活未办（本条同文被拦，目标 #" + renewPlan.id + " 未动）" : ""), // wave 阶段 4（未带 renewTargetId 或复活开关关时为空串，与底座逐字同）
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
				// 年份钳制·手写面（design-approved）：与提炼链同一判据（extract.cjs 47e6a98——|event_at 年−当前年|>1 ⇒ 拒记 null；
				//   |差|=1 保留·真历史深补记兼容〔#12902/#27135 先例〕——执行令括号「差1年改年/差2年拒记」系荐裁概括·以在产闸判据为准·差异随receipt escalate）。
				//   观测键 mw_year_clamped（原子自增·跨 apply 唯一真相源 organ_meta——🟡A 同病根预防）＋warn 出口（⑬）。
				if (eventAt !== null) {
					// D1 钟基统一（四十一审甲路 P2 修）：new Date().getFullYear() 系宿主机本地年（EDT）≠提炼链 nowIso() 北京年（UTC+8·core.cjs L1178）——跨年 12-13h 窗两闸 |年差|=1 边界会双向分歧；统一取 nowIso() 年（本文件 L243 同款先例 nowIsoB）。
					const _yGap = Math.abs(Number(eventAt.slice(0, 4)) - Number(String(nowIso()).slice(0, 4)));
					if (_yGap > 1) {
						logger?.warn?.(
							`[living-memory] event_at 年份钳制拒记（手写面）: "${eventAt}"（|年差|=${_yGap}＞1）`,
						);
						eventAt = null;
						try {
							dbConn
								.prepare(
									"INSERT INTO organ_meta (k, v) VALUES ('mw_year_clamped', '1') ON CONFLICT(k) DO UPDATE SET v = CAST(v AS INTEGER) + 1",
								)
								.run();
						} catch (eYClamp) {
							logger?.warn?.(
								"[living-memory] mw_year_clamped 计数写失败: " +
									String(eYClamp).slice(0, 60),
							);
						}
					} else if (
						// ── 1907 §一加严(design note)：|=1 且月日同 ts ⇒ 改年为 ts 年；月日不同＝真补记保留。
						//    观测键 mw_year_rewritten（organ_meta 原子自增·同 mw_year_clamped 构）。
						_yGap === 1 &&
						eventAt.slice(5, 10) === String(nowIso()).slice(5, 10)
					) {
						const _fixed = String(nowIso()).slice(0, 4) + eventAt.slice(4);
						logger?.warn?.(
							`[living-memory] event_at 年份改写（手写面·|=1 月日同）: "${eventAt}"→"${_fixed}"`,
						);
						eventAt = _fixed;
						try {
							dbConn
								.prepare(
									"INSERT INTO organ_meta (k, v) VALUES ('mw_year_rewritten', '1') ON CONFLICT(k) DO UPDATE SET v = CAST(v AS INTEGER) + 1",
								)
								.run();
						} catch (eYRw) {
							logger?.warn?.(
								"[living-memory] mw_year_rewritten 计数写失败: " +
									String(eYRw).slice(0, 60),
							);
						}
					}
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
				// ── F09 寿命契约（10-24 中枢定；修文草稿第六节第 2 小节）：有效显式 valid_to ＞ 合法显式 ttl_days ＞ 缺省分级 ──
				//    合法 TTL＝1–365 的整数，按入册北京日换算；没给有效 valid_to 时先由 TTL 填期，下面的缺省 +30 天就不再抢先（原顺序缺省先填、TTL 只打标）。
				//    directive锚词只免缺省分级、不抵消显式 TTL（原码已如此，本车不改）。同给有效 valid_to 与合法 TTL：沿用 valid_to 日期、仍打 ttl 标（原码已如此，
				//    「同给两参」的硬软性质待审定，本车不改）。非法 TTL 照旧按没传处理（warn 一行、不打标）。
				//    回退：LEGION_F09_TTL_OFF=1 回到原顺序（与 patrol.cjs 续期准入式同一开关）
				const ttlDays = Number(args.ttl_days);
				const ttlOk = Number.isInteger(ttlDays) && ttlDays >= 1 && ttlDays <= 365;
				const ttlIso = ttlOk
					? new Date(Date.now() + 8 * 3600 * 1000 + ttlDays * 86400000).toISOString().slice(0, 10) // 8-31 时区修复（审计 P0-7）：+8h 后取 UTC 分量=上海本地日——原裸 toISOString=UTC 日·本地 0-8 点写入到期日早一天硬退出（全库 +08:00 轴·对照 时态列口径）
					: null;
				if (ttlOk && validTo === null && process.env.LEGION_F09_TTL_OFF !== "1") validTo = ttlIso;
				// ── 2step二·工具面生命周期分级（fact 默认 +30d 软沉底·directive锚词永久·显式入参优先·design-approved）──
				if (
					validTo === null &&
					String(args.type) === "fact" &&
					!process.env.LEGION_LIFECYCLE_OFF &&
					!/军令|命令|指令|批示|裁决/.test(
						String(args.title || "") +
							" " +
							String(args.content || "").slice(0, 200),
					)
				) {
					validTo = new Date(
						Date.now() + 8 * 3600 * 1000 + 30 * 86400000,
					).toISOString()
						.slice(0, 10); // 不写 ttl 标·与 硬 aged 区分。wave 8-g 订正：「软沉底」只在 LIFE_GUARD 开时成立（过期后 search ×0.2 带 ⏦ 可见）；关（LEGION_LIFE_GUARD_OFF=1）时过期即被 search 硬排
					logger?.warn?.(
						"[living-memory] 生命周期分级：fact 默认 +30d valid_to（显式入参优先·LEGION_LIFECYCLE_OFF 回退）: " +
							String(args.title || "").slice(0, 40),
					);
				}
				// 在库 TTL：声明寿命→valid_to 换算+ttl 标（nightly patrol硬退出 aged·与 显式补记软沉底区分）
				//    F09：换算已提到缺省分级之前（见上），这里照旧打 ttl 标；补期一句只在回退态（LEGION_F09_TTL_OFF=1）才会真补
				let ttlMark = false;
				if (ttlOk) {
					if (validTo === null) validTo = ttlIso; // 显式 valid_to 优先·TTL 仅补（新顺序下合法 TTL 到这里 validTo 已非空）
					ttlMark = true;
				} else if (args.ttl_days !== undefined) {
					logger?.warn?.(
						`[living-memory] ttl_days 非法忽略（1-365 整数）: "${String(args.ttl_days).slice(0, 20)}"`,
					);
				}
				// ── P1② 写闸 source 自动带（design-approved·契约审计 C4/C12 治单）：手写路缺 source 时自动补会话锚，回链不断、写权可审计 ──
				//    09-17 串窗修（统筹实勘）：getSessionId 实参=writeSessionId（「最后活动会话」单例）——多窗并发时错锚他窗 sid
				//    (ops note)——改 exec 真身优先（与频控 fcKey 同解法·单例仅兜底）
				const _rawSid =
					(exec && exec.agent && exec.agent.session && exec.agent.session.id) ||
					(typeof getSessionId === "function" && getSessionId()) ||
					"";
				// ZD-3（独立审计）：子代理 exec 真身可能为裸 uuid（无 session- 前缀·sessionOrgan 注释实证）——归一补前缀
				// 成读侧可受理形态（写侧归一优于读侧再放宽：受理面不越开越宽）
				const sidAnchor = _rawSid
					? /^session-/.test(_rawSid) ? _rawSid : "session-" + _rawSid
					: "";
				const _rawSrc = String(args.source || "").trim(); // E 甲′ 落码车：source 归属前缀保护（白名单＝方案件 L84 原文正则·模块级 SRC_OK_RE）
				const _sidSrc = sidAnchor ? "session:" + sidAnchor : "handwrite";
				const finalSource = _rawSrc && SRC_OK_RE.test(_rawSrc) ? _rawSrc : _sidSrc;
				if (_rawSrc && finalSource !== _rawSrc) {
					core.sourceOverrideCount += 1; // 模块级计数（L14180 区接线·十五审甲路修订版）
					logger?.warn?.(
						`[living-memory] source 归属保护（#${core.sourceOverrideCount}）：入参 "${_rawSrc.slice(0, 60)}" 非系统前缀形态 → 改 "${finalSource}"；文件出处请写进 content`,
					); // 形参 logger（registerMemoryWrite L3566 形参·ctx 幻引用修订）
				}
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
				core.noteFreshEpisodicWrite(Number(info.lastInsertRowid), Date.now(), String(args.space || "memory-organ"), finalSource, dbConn);
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
						// 闸④（09-20 approved）：任务结构边四型——思维导图图基座
						"subtask_of",
						"depends_on",
						"part_of_campaign",
						"duplicate_of",
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
							if (core.jieba)
								for (const w of core.jieba.cut(str, true))
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
						// ⚠ 开关语义订正（批⑳ J2-3·L 路实锤原注释反向失实）：`LEGION_REFLECT_LINK_OFF` 实际只关 **建边 INSERT**——
						//    扫描段（best19 产出）无 env 判照常跑·盖戳门是 `LEGION_STAMP_OFF`（不含本开关）⇒ OFF 态盖戳对象照常存在。
						//    原注（09-12 三轮审计 D4）「关整段·连带盖戳·无建边即无盖戳对象」前提为假——应急回退即「只停 link」（盖戳另用 STAMP_OFF 单独关）。
						if (!process.env.LEGION_REFLECT_LINK_OFF && best19 && bestJ19 >= 0.2 && bestI19 >= 2) {
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
											// D-2 修（批 C 候办审计车）：created_at 与nightly patrol路同形（nowIso 全长）——
											//   原 slice(0,10) date-only 会使 C 挂哨 MIN(created_at) 解析 NaN→oldestH=-1 退化
											ts,
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
				// ── 写时即时嵌入（wave主件·**09-12 主案抽取为公共件 writeTimeEmbed**）──
				//    本段原为内联实现，09-12 approved主案：抽模块级 `writeTimeEmbed(db,id,title,content,credResolve)`
				//    供 ①本处（手工写路）②提炼器 runExtract（auto 条路径·此前不覆盖）复用——治「旁路写入点两侧写时增强同时失能」。
				//    语义不变：fire-and-forget（void）·不阻塞返回·失败静默（nightly patrol backfill 仍兜底）。
				try {
					if (credResolve && !process.env.LEGION_A14_OFF) {
						void writeTimeEmbed(
							dbConn,
							Number(info.lastInsertRowid),
							title,
							content,
							credResolve,
						);
					}
				} catch {}
				// ── item 写时前缀生成（design-approved）：fire-and-forget 异步（同法·不阻塞返回）──
				//    生成口语前缀 UPDATE spoken_prefix——au 触发器自动重嵌 FTS（监听列已含）。失败静默=nightly patrol回填段兜底。
				//    双特权/8 窗挂载全走此路（写权空间闸在前已判）。env LEGION_SPOKEN_OFF 回退。
				try {
					if (credResolve && !process.env.LEGION_SPOKEN_OFF) {
						void (async () => {
							try {
								if (!core.spCredCache.v || Date.now() - core.spCredCache.t > 60000) {
									const c = await credResolve(LLM_KEY_REF); // 09-13 自审补：第四处漏改（本题名 credResolve≠credentials.resolve ⇒ 前轮替换未覆盖）
									if (!c || !c.value) return;
									core.spCredCache = { v: c.value, t: Date.now() };
								}
								const sp = await llmSpokenPrefix(title, content);
								if (sp)
									dbConn
										.prepare(
											"UPDATE memories SET spoken_prefix = ? WHERE id = ?",
										)
										.run(promptSafe(sp), Number(info.lastInsertRowid)); // 审计处置车（09-13·D1c）：spoken 生成路纳净化面（原裸写）
							} catch {}
						})();
					}
				} catch {}
				// wave 复活口·阶段 4：说明条已入库，照计划改期／复活（失败整段回滚，receipt带 ⚠）
				if (renewPlan) renewTarget = lifeManualDo(dbConn, renewPlan, ts, logger);
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
					...(renewTarget ? { renewTarget } : {}), // wave 阶段 4：改期／复活结果透出（ok、act、from、to、reason、today/cap）
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
	}));
}

function setupCredentialSet({ ctx, tools }) {
			// ══ credential_set（B案·design-approved）：凭据一跳入库·schema 零明文位 ══
			// 哲学：明文只存在于maintainer的 user/message 原文（会话档既有事实）——参数/返回/日志三面零明文。
			// 提取：snapshotEvents 倒序取真用户消息（source.kind==='user' 判法＝firehose 权威范式，
			//       插件注入的 user/message 不算）；hint 关键词命中行优先 → 无 hint 走凭据形态正则；
			//       多候选/零候选一律失败返回（不猜）——宁漏勿错存。
			// 护栏：REF_PATTERN 硬校验＋新建-only（describe configured 即拒·覆写走生殖腺专窗）＋
			//       明文长度下限 8＋日志只记 ref 名与结果码（绝不记值）。
			{
				const CRED_REF_RE = /^[A-Za-z_][A-Za-z0-9_]*$/; // 官方 REF_PATTERN 同源（dsh-credentials 契约）
				// 形态正则组（宁漏勿错：未命中形态的怪 token → 模型补 hint 重试，不兜底乱抓）
				const CRED_SHAPES = [
					[/socks5[hr]?:\/\/[^\s"'<>]+/g, "socks5-url"],
					[/\bsk-[A-Za-z0-9_-]{16,}/g, "sk-型"],
					[/\bAKIA[0-9A-Z]{16}\b/g, "AKIA型"],
					[/\bgh[pousr]_[A-Za-z0-9]{20,}\b/g, "github型"],
					[/\bshp(?:at|ca|pa|ss)_[a-f0-9]{24,}\b/g, "shopify型"],
					[/\bnpm_[A-Za-z0-9]{30,}\b/g, "npm型"],
					[/\bxoxb-[A-Za-z0-9-]{20,}\b/g, "slack型"],
				];
				const extractFromText = (text, hint) => {
					if (!text || typeof text !== "string") return { ok: false, reason: "空文本" };
					const scan = (s) => {
						const hits = [];
						for (const [re, label] of CRED_SHAPES) {
							for (const m of s.matchAll(re)) {
								const v = m[0];
								if (v.length >= 8 && !/(?:\.|@)example\.[a-z]+/i.test(v)) hits.push({ v, label }); // 排文档示例域（单测用例3 抓出://example 覆盖不足）
							}
						}
						return hits;
					};
					if (hint && typeof hint === "string" && hint.trim()) {
						// hint 路：只在包含 hint 关键词的行内扫（收窄面·防跨行误抓）
						const kw = hint.trim().slice(0, 40);
						const lines = text.split(/\n/).filter((l) => l.includes(kw));
						if (!lines.length) return { ok: false, reason: "hint 关键词未在maintainer最近消息中命中" }; // 审计BUG2修：不回显 kw 原文（防 hint 误填明文时 reason 回显违护-1）
						const hits = lines.flatMap(scan);
						if (!hits.length) return { ok: false, reason: `hint 命中 ${lines.length} 行但行内无已知凭据形态（可换更短关键词重试）` };
						const uniq = [...new Map(hits.map((h) => [h.v, h])).values()];
						if (uniq.length > 1) return { ok: false, candidates: true, reason: `hint 行内检出 ${uniq.length} 段候选（${[...new Set(uniq.map((u) => u.label))].join("/")}）——请maintainer示取哪段或换更精 hint` }; // 批⑰ D14③：多候选打标——执行侧 `if (r.candidates) break` 原死分支激活（歧义即停·不回落旧消息错存）
						return { ok: true, value: uniq[0].v, label: uniq[0].label };
					}
					// 无 hint 路：全消息形态扫（多候选即拒·不猜）
					const hits = [...new Map(scan(text).map((h) => [h.v, h])).values()];
					if (!hits.length) return { ok: false, reason: "maintainer最近消息中未检出已知凭据形态——若为特殊格式请带 hint（形态关键词，非明文）重试" };
					if (hits.length > 1) return { ok: false, candidates: true, reason: `检出 ${hits.length} 段候选（${[...new Set(hits.map((u) => u.label))].join("/")}）——请带 hint（形态关键词，非明文）指明取哪段` }; // 批⑰ D14③：多候选打标（歧义即停·死分支激活）
					return { ok: true, value: hits[0].v, label: hits[0].label };
				};
				const userTextsOf = (exec, K = 5) => {
					// 审计BUG1修：倒序收集最近 K 条真用户消息（原只取最近一条——「msg1发key→msg2说入库」双轮场景必失败）。
					// K=5 近期窗界定防旧消息误存；source.kind==='user' 真人判法不变（插件注入不算）。
					const out = [];
					try {
						const events =
							exec?.agent?.session?.snapshotEvents?.() ??
							exec?.agent?.session?.events; // 双源兼容（0.1.2+ 按需 API／≤0.1.1 回落·同 extract 增强段）
						if (!Array.isArray(events)) return out;
						for (let i = events.length - 1; i >= 0 && out.length < K; i--) {
							const e = events[i];
							if (e?.type !== "user/message") continue;
							if (e?.data?.source?.kind !== "user") continue; // 插件注入不算（同 isDshUserTurn 判法）
							const c = e.data.content;
							const t = Array.isArray(c)
								? c.map((b) => (b && b.text) || "").join("")
								: String(c ?? "");
							if (t.trim()) out.push(t);
						}
					} catch {} // 读面失败按无料报（下方提取自然失败·不静默错存）
					return out; // [最近, 次近, …]
				};
				try {
					tools.register({
						name: "credential_set",
						description:
							"凭据一跳入库(design note)：把maintainer在本窗对话中刚发的凭据存入官方凭证库（~/.dsh/.credentials.yaml·热生效）。只传 ref（大写蛇形，如 IPFOXY_US_SOCKS5）；明文由服务端从maintainer最近消息内存提取——【参数绝不携带明文】（明文进参数＝落会话档＝红线）。新建-only：REF 已存在即拒（覆写/换绑走生殖腺专窗）。提取失败会返回原因，按提示带 hint（凭据形态关键词如 socks5、sk-，非明文）重试。读凭据不走本工具（插件内部 resolve·每操作解析）。",
						parameters: {
							type: "object",
							required: ["ref"],
							properties: {
								ref: {
									type: "string",
									description:
										"凭据引用名：^[A-Za-z_][A-Za-z0-9_]*$·大写蛇形惯例 <用途>_<供应商>_<后缀>（如 ADSPOWER_LOCAL_API_KEY）",
								},
								hint: {
									type: "string",
									description:
										"可选·提取提示：明文在消息中的形态关键词（如 socks5://、sk-、AKIA、ghp_）——多候选/特殊格式时收窄用；禁填明文本身",
								},
							},
						},
						output: {
							// D 路审计 D-真1 修（09-20 22:4x·🔴高）：**原缺 render** ⇒ 宿主 `Tools.register()` 首步硬性要求
							//   `typeof output.render === "function"`（dsh-tools L2775-2784·09-13 已在）⇒ **注册即抛 TypeError** ⇒
							//   被本块外层 catch（L4389）吞成一行 warn ⇒ 本工具自 09-18 引入（`b601523`）起**从未在任何窗口注册成功**
							//   ——maintainer「凭据一跳入库」通道空转两天；/ lesson「工具 output 必填 render」**复犯**。
							//   渲染只含 ref／形态／长度／下一步，**零明文**（与 execute 的零明文日志同纪律）。
							schema: {
								type: "object",
								additionalProperties: false,
								properties: {
									stored: { type: "boolean" },
									ref: { type: "string" },
									length: { type: "number" },
									shape: { type: "string" },
									reason: { type: "string" },
									next: { type: "string" },
								},
								required: ["stored", "ref"],
							},
							render: (_a, v) => [
								{
									type: "text",
									text:
										v && v.stored
											? "✅ 凭据已入库 ref=" +
												String(v.ref) +
												"（形态 " +
												String(v.shape || "?") +
												"·长度 " +
												String(v.length || 0) +
												"）" +
												(v.next ? "\n" + String(v.next) : "")
											: "❌ 凭据未入库 ref=" +
												String((v && v.ref) || "") +
												"：" +
												String(
													(v && (v.reason || v.next)) || "未知原因",
												),
								},
							],
						},
						execute: async (args, exec) => {
							const ref = String(args?.ref ?? "").trim();
							const fail = (reason) => ({ stored: false, ref, reason });
							if (!CRED_REF_RE.test(ref))
								return fail("ref 不合规范：须 ^[A-Za-z_][A-Za-z0-9_]*$（大写蛇形惯例）");
							const cred = ctx.credentials;
							if (!cred?.set || !cred?.describe) return fail("credentials 服务不可用（set/describe 缺）");
							try {
								const info = await cred.describe(ref);
								if (info?.configured) return fail(`REF 已存在（source=${info.source}）——新建-only：覆写/换绑走生殖腺专窗（四步 SOP）`);
							} catch (eD) {
								return fail("describe 探测失败：" + String(eD).slice(0, 80));
							}
							const texts = userTextsOf(exec); // 审计BUG1修配套：最近优先逐条提取，首条出唯一候选者胜出
							if (!texts.length) return fail("未能读取maintainer最近消息（会话事件快照空）——可改走生殖腺四步 SOP");
							let got = null;
							let lastReason = "";
							for (const t of texts) {
								const r = extractFromText(t, args?.hint);
								if (r.ok) { got = r; break; }
								lastReason = r.reason;
								if (r.candidates) break; // 多候选歧义：更旧消息救不了，交模型带 hint 重试
							}
							if (!got) return fail(lastReason || "maintainer最近 5 条消息中未检出凭据形态——可带 hint 重试或改走四步 SOP");
							if (!got.value || got.value.length < 8) return fail("提取值长度异常（<8）——拒存");
							try {
								await cred.set(ref, got.value); // 进程内直达库（原子写·chokidar 热载·轮换即生效）
							} catch (eS) {
								ctx.logger?.warn?.(`[credential_set] set 失败 ref=${ref}: ${String(eS).slice(0, 60)}`); // 只记 ref 名
								return fail("入库失败：" + String(eS).slice(0, 80));
							}
							const today = new Date(Date.now() + 8 * 3600 * 1000).toISOString().slice(0, 10); // +8h 上海日（同 口径）
							ctx.logger?.info?.(`[credential_set] 入库 ref=${ref} 形态=${got.label} 长度=${got.value.length}`); // 零明文日志
							return {
								stored: true,
								ref,
								length: got.value.length,
								shape: got.label,
								next: `已入库（热生效）。请立即将登记行并入 source of record/凭证索引.md：「${ref} | ${got.label}·用途待补 | ${today}」（只记 REF/用途/日期·禁写明文）`,
							};
						},
					});
				} catch (eReg) {
					ctx.logger?.warn?.(`[living-memory/write] credential_set 注册失败: ${String(eReg).slice(0, 60)}`);
				}
			}

}

// ── wave 复活口·阶段 1（10-18·草纲下篇一改点 1）：memory 工具定义按 get 开关补 get（LEGION_MEM_GET_OFF=1 时原样返回，与底座逐字同）──
//    补三处：action 枚举加 get（并补说明）；参数加 offset、id 说明补 get；工具说明「五选一」改「六选一」并补 get 一句。
//    桥的 memory 工具 schema 深拷贝插件参数（additionalProperties:false），get 与 offset 随之上桥；桥自己的工具说明串不随插件，要桥另改（见落码记录）
function memGetPatch(def) {
	if (!core.memGetOn()) return def;
	const p = def.parameters.properties;
	p.action.enum.push("get");
	p.action.description += " get=按 id 取全文与状态（含已老化条·单次至多 " + core.MEM_GET_BYTES + " 字节·按 nextOffset 续读）";
	p.id.description = "read_episodic／get 用：条目 id";
	p.offset = { type: "number", description: "get 用：续读起点（字节·取上次返回的 nextOffset；缺省 0）" };
	def.description = def.description
		.replace("（action 五选一）", "（action 六选一）")
		.replace("read_evolution 进化史回流（mirror 条目·source of record行号窗）。", "read_evolution 进化史回流（mirror 条目·source of record行号窗）/ get 按 id 取全文与状态（含已老化条·单次至多 " + core.MEM_GET_BYTES + " 字节，按 nextOffset 续读）。");
	return def;
}
function setupMemoryTool({ ctx, tools, readExec, _nullifyDeep, db }) {
		tools.register(memGetPatch({
			name: "memory",
			description:
				"Read-only access to the memory store. When past records, decisions or lessons are relevant, search before answering (memory-first), and cite entries as [#id] with a timestamp so quotes stay checkable. Actions: search (hybrid keyword + vector recall) | timeline (reverse-chronological browse) | stats (self-check, incl. extraction and nightly-patrol counters) | read_episodic (replay the source-conversation window of any session-anchored entry) | read_evolution (replay the source-document window of a mirror entry). Writes go through memory_write (role-mounted).",
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
					day: {
						type: "string",
						description:
							"可选·option按天读（timeline 用·09-17 maintainer）：YYYY-MM-DD 严格日粒度——只看 ts 落在该日 00:00–24:00（+08:00）窗的条目·可与 as_of 叠加；day 模式时间正序（晨→夜）；env LEGION_TIMELINE_DAY_OFF 非空即停用",
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
					// [拆③] 五 action 实现已抽 read.cjs execRead（deps 十参·sessionOfLastTurn getter 化·#31294 hard rule②）——段体 L8255-10363 原样归位
					// [106踵即修] .then(_nullifyDeep)：工具返回 undefined 属性清洗为 null——10-01 stats 全挂案同款（L1834 注释族·
					//   harness snapshotJsonValue 拒收 undefined ⇒「value is not lossless JSON」⇒工具全线不可用）·首清 warn 留根因观测。
					const __r = await readExec(action, args, exec);
					return _nullifyDeep(__r, "memory/" + action);
				},
			),
		}));

		// ── 工具二：memory_extract（手动触发提炼）──

}

function setupMemoryExtract({ ctx, tools, runExtract, stats, db, buffer }) {
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
									if (t.trim()) parts.unshift(clipSurrogateTail(t.slice(0, 1500)));
								}
							}
							historyText = clipSurrogateHead(parts.join("\n---\n").slice(-10000));
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
					stats.bufferedChars = buffer().length; // [再检查10-04] buffer 重赋值型裸变量→getter 传参（#31831 hard rule·装配点 () => buffer）
					// 审计 D1 配套（13:44）：手写显式提炼传 source——防被 水位路劫持（本窗历史必须用本调用源）
					//  修复（09-12 brain实勘根因·**同族第四犯**）：原 `sid: args.sid` 在**无参调用**下＝undefined
					//   ⇒ runExtract 回落 `sessionOfLastTurn`（firehose 全局变量·每 turn 被他窗覆写）⇒ **多窗并发取他窗账**。
					//   治本＝本窗身份**显式化**：args.sid（他窗特权用法）优先，否则取 exec 真身会话 id（同文件 L3436 既有正确范式）。
					const result = await runExtract({
						auto: false,
						sid: args.sid || exec?.agent?.session?.id,
						fallback:
							historyText && historyText.trim() ? historyText : undefined,
					}); // P0修：fallback 回落（水位账优先消化照step语义） // step-2c（10:35）：解除 source:buffer 旧路劫持——原显式传旧路全局 buffer·水位路（step-1 滚动消化）从未接管手动路径；sid 透传他窗特权（ 后＝显式 sid ‖ exec 真身）
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

}

module.exports = { registerBoardTools, registerMemoryWrite, setupCredentialSet, setupMemoryTool, setupMemoryExtract };
