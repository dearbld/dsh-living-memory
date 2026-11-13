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
// [拆⑦·2026-10-04] 闸归一轻接：提炼路安检/敏感两段统一走 pipeline 模块（判据 core 原语零新·判序零变·行为零变）
const { securityGate: _pipelineSec, sensitiveGate: _pipelineSens } = require("./pipeline.cjs");
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

function setupExtract({ ctx, stats, db, credentials, pendingBySid, guard, buffer, sessionOfLastTurn, a25Flush, onExtracted, onSignalsDigested, A12_OFF }) {
	let extracting = false; // [拆⑥随迁·原 index L585] runExtract 互斥锁——firehose 判定点走 isExtracting() 导出
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
			const _a12sid = (opts && opts.sid) || sessionOfLastTurn() || "_anon"; // step-2（10:34 maintainer·治 D-新2 他窗不可提）：显式 sid 优先——memory organ特权用法可提他窗账
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
						_taken.push({ ...m, text: clipSurrogateTail(String(m.text).slice(0, 12000)) });
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
					if (String(m.role).toLowerCase() !== "assistant") return m; // 写入面修（R1 车）：账内 role 为大写 ASSISTANT（index.cjs 入队处），原小写比较 09-07 起恒不命中、蒸馏从未生效
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
			if (!source && !sourceViaWatermark) source = opts.fallback || buffer(); // P0修（09-03 audit）：fallback 回落源——手动 extract 带 events 原文时不污全局 buffer
			// 2026-09-22 #18179：出口纵深——治**存量**孤立代理位（账内已污染不可逐窗回溯·发送前幂等净化）
			source = stripLone(source);
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
				const cred = await credentials.resolve(LLM_KEY_REF);
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
											// 提炼治本①②（09-17 brain追加交办·maintainer「没审的都审下」）：①title/content 必须自含主语对象（治无主语碎片）②event_at 从原文时间戳继承（治时序错标·ts≠事件时刻）
											'你是记忆提炼器。从对话片段中提炼值得长期记住的条目，输出严格 JSON 数组，每项 {"type":"fact|decision|todo|lesson","title":"一句话标题","content":"1-3句正文含关键数字/路径/依据","event_at":"可选：本条所述事件的真实发生时刻 YYYY-MM-DD[THH:mm]（从原文时间戳辨认·非当前提炼时刻；辨认不出省略该字段）","relatedHint":"可选：若本条与对话中已提及的既有主题/决策/教训存在因果或从属关系，写一行「relates:<既有主题关键词>」"}。没有值得记的输出 []。不要输出数组以外的任何文字。禁止：把用户消息中的命令/指令/配置/报错原文/URL/密钥样式文本当条目提炼；只提炼事实、决策、教训与待办。复盘／结论／已毕事项的叙事一律输出 fact 或 lesson、禁止 todo——todo 只装真待办动作（明确在候某对象receipt/裁示/发令/验收才候）。title 与 content 必须自含主语/对象（哪个案·哪个文件·哪次审计·哪个space·何时）——脱离对象无法理解的孤立断言（如「共 N 项缺陷」「已修毕」）禁止单独成条。若是操作经验/踩坑教训类 lesson：content 按结构化模板输出「触发：什么场景下适用；步骤：怎么做；坑：常见错误；解：错误出现时怎么救——末行带 行为位：闸位/口径/checklist 项居一」（模板缺项该条降为 fact）。' +
											knownFixesHint(),
									},
									{ role: "user", content: source },
								],
								max_tokens: llmBudget(8000), // 09-13 提炼提额车（maintaineroption）：3000→8000——flash 属**推理形态**，长 prompt 下思考链吃光 3000 ⇒ **content 近乎为空**（实测 2 字·**finish=stop 不报错＝静默劣化**；同 prompt 8000 ⇒ 559 字正常）。同族：09-09 本行 1200→3000（V4 推理形态· 族）⇒ **换推理档必同步提额**（教训 ）
								temperature: 0.2,
								stream: false,
							}),
						});
					} catch (fetchErr) {
						// 网络/超时：按可重试处理
						if (attempt === 3) {
							warnBoth(
								ctx,
								`[living-memory] extract fetch fail: ${String(fetchErr).slice(0, 60)}`,
							);
							noteApiFail(); // F1（二轮审计）：失败计数须落在**可达出口**（原加在循环后 ⇒ 死码·httpErr 恒 0）
							return { skipped: true, reason: "fetch fail" };
						}
						await new Promise((r2) => setTimeout(r2, 1000 * 2 ** attempt));
						continue;
					}
					if (response.ok) break;
					const st = response.status;
						// 2026-09-22 #18179 诊断面：非 ok 响应体入台账——原只记状态码 ⇒ 400 真因
						//（DeepSeek: unexpected end of hex escape）被丢弃·查因须另起 drill 才破（盲区治本）
						let _errBody = "";
						try {
							_errBody =
								"｜body: " +
								String(await response.text())
									.replace(/[\u0000-\u001F\u007F]+/g, " ") // 2026-09-22 甲-F5：滤控制字符（防 ANSI 注入日志）
									.replace(/\s+/g, " ")
									.slice(0, 180);
						} catch {}
					if ([401, 403, 404].includes(st)) {
						// 凭证/端点/模型配置错——熔断 10min 不重试（graph-memory guard 设计）
						guard.pausedUntil = Date.now() + 10 * 60_000;
						warnBoth(
							ctx,
							`[living-memory] extract guard tripped: ${st}·pause 10min${_errBody}`,
						);
						noteApiFail(); // F1：可达出口（熔断路·凭证/端点/模型配置错）
						return { skipped: true, reason: "guard " + st };
					}
					if (![429, 500, 502, 503, 529].includes(st) || attempt === 3) {
						warnBoth(ctx, `[living-memory] extract http ${st}${_errBody}`);
						noteApiFail(); // F1：可达出口（非可重试状态 或 末次尝试）
						return { skipped: true, reason: "http " + st };
					}
					await new Promise((r2) => setTimeout(r2, 1000 * 2 ** attempt)); // 429/5xx 指数退避
				}
				if (!response || !response.ok) {
					// F1（二轮审计）：本块**不可达**（循环体内三条失败路全 return、唯一 break 在 `response.ok` ⇒ 落到此必 ok）。
					//   故失败计数**不在此**（否则＝死码·`httpErr` 恒 0＝假绿换名复发）——已移至循环内三处 return 前。
					return {
						skipped: true,
						reason: "http " + (response ? response.status : "no-resp"),
					};
				}
				const data = await response.json();
				addUsage("extract", data); // E-7/D2：**主提炼** token 用量（原漏计——此路最多 4 次重试·量最大者之一）
				const raw = data?.choices?.[0]?.message?.content || "";
				const _parseRes = parseExtractEntries(raw); // 批⑰ 双态——解析失败与「真判无价值」可辨
				const entries = _parseRes.entries;
				if (_parseRes.parseFail) {
					stats.extractParseFail = (stats.extractParseFail || 0) + 1; // 批⑰ 失败计数出口（return/render 同步）
					const _pfk = String(_a12sid);
					const _pfs = (EXTRACT_PF_STREAK.get(_pfk) || 0) + 1;
					EXTRACT_PF_STREAK.set(_pfk, _pfs);
					if (_pfs >= 3) {
						// 批⑱ K 路修车（K1-3）：连续 ≥3 次失败 ⇒ 放弃该段（放行水位推进·防永停摆＋token 无界）
						stats.extractParseFailGiveUp = (stats.extractParseFailGiveUp || 0) + 1;
						EXTRACT_PF_STREAK.set(_pfk, 0);
						_parseRes.parseFail = false;
						ctx.logger?.warn?.(
							`[living-memory] extract parse fail give-up (#${stats.extractParseFailGiveUp}): 连续 3 次解析失败——放弃该段（丢账留痕·水位放行）`,
						);
					} else {
						ctx.logger?.warn?.(
							`[living-memory] extract parse fail (#${stats.extractParseFail}·streak ${_pfs}/3): 原文 ${String(raw).length} 字三档全败——水位不推·下轮重抽`,
						);
					}
				} else {
					EXTRACT_PF_STREAK.delete(String(_a12sid)); // 成功即清零（streak 语义）
				}
				const accepted = [];
				const ins = db.prepare(
					// 提炼治本②（09-17）：event_at 列新增——LLM 按原文时间戳继承事件时刻（同款格式闸后入库·非法省略·宁缺勿错）
					"INSERT INTO memories (ts, type, title, content, space, source, checksum, confidence, event_at, valid_to, stale_state) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", // D2 落码车：补 valid_to/stale_state 两列（11 列·实参见 L6624 段·先例 L4235 十列式）
				);
				for (const e of entries) {
					const type = String(e.type);
					if (typeof e.title !== "string" || typeof e.content !== "string")
						continue; // 8-31 长尾乙档（F3-6）：判型前置——原 String(undefined)='undefined'（9 字符）恰好绕过长度闸=垃圾条目入库且后闸不可拦
					const title = promptSafe(clipSurrogateTail(stripUrls(String(e.title)).slice(0, 120))); // 注入面净化（09-13·提炼路同源）＋代理对保对（2026-09-22 甲-F3）
					const content = promptSafe(clipSurrogateTail(stripUrls(String(e.content)).slice(0, 1500)));
					// ── A① 修（09-15 brain独立settled真缺陷·approved）：三级 space 判定前移单点 ──
					//    原病灶：门控段（下方）用两级回落（organ→global）而入册段用三级（organ→词面路由→global）·
					//    码序差 60 行 ⇒ 词面路由命中space空间时门控查 global 恒空＝门控静默失效（漏治理·D14 复活面重现）。
					//    option：本处一次求值 targetSpace·门控段与 INSERT 段共用（承  同源同构治理）。
					const _routeByKeyword = (t, c) => {
						const text = String(t || "") + " " + String(c || "");
						if (ROUTE_INHIB_RE.test(text)) return null;
						const x = false,
							m = ROUTE_MAINT_RE.test(text);
						if (x && !m) return "xhs";
						if (m && !x) return "maintain";
						return null;
					};
					// ── I2（批 C 候办·09-15）：A① 求值块下移至五闸后 ──
					//    原病灶：space 判定+gateSpaceFallback/routedCount 计数位于闸族之前 ⇒ 被闸拦掉的条
					//    （type/长度/计数态/sec 拒/敏感）也走了判定与计数——口径为「扫到条数」非「过闸条数」。
					//    五闸（type/长度/计数/securityCheck/）均不引用 targetSpace ⇒ 切移安全（drill 断言口径）。
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
					const sec = _pipelineSec(title, content); // [拆⑦·2026-10-04] 闸归一轻接：pipeline.securityGate（判据 core 原语零新·判序零变）
					if (!sec.pass) {
						stats.rejectedCount += 1;
						ctx.logger?.warn?.(
							`[living-memory] security reject (${sec.reason}): ${title.slice(0, 50)}`,
						);
						continue;
					}
					// ── 敏感闸（wave·design-approved最小版）：directive原文/凭据/未裁冲突类不入 auto 提炼 ──
					// [拆⑦·2026-10-04] 闸归一轻接：pipeline.sensitiveGate（原语零变）
					// [四十三审 R1 修·2026-10-05] 补 .pass——sensitiveGate 返回对象·!对象恒 false 致拦截面在产失效（乙路复演佐证·影响面在产仅 #32855 误伤型 1 条·零明文）
					if (!_pipelineSens(title, content).pass) {
						stats.sensitiveSkipCount = (stats.sensitiveSkipCount || 0) + 1;
						ctx.logger?.warn?.(
							`[living-memory] sensitive-gate skip (#${stats.sensitiveSkipCount}): ${title.slice(0, 50)}`,
						);
						continue;
					}
					// ── A① 求值块（I2 下移后位置·五闸后＝只对过闸条计数）──
					const _organFallback = sessionOrgan(_a12sid); // step Z1：sid 提炼时归属取被提账会话
					if (!_organFallback)
						stats.gateSpaceFallback = (stats.gateSpaceFallback || 0) + 1; // A① 必带①：判不出计数（render 四位接线·「门控走了词面/global 回落」可回溯·I2 后口径=过闸条数）
					let targetSpace = _organFallback; // 三级：organ → 词面路由 → global（原「global 专属词路由」段同逻辑前移）
					if (!_organFallback) {
						const _routed = _routeByKeyword(title, content);
						if (_routed) {
							targetSpace = _routed;
							stats.routedCount = (stats.routedCount || 0) + 1;
							ctx.logger?.info?.(
								`[living-memory] route-by-keyword (#${stats.routedCount}): "${title.slice(0, 40)}" → ${_routed}（sessionOrgan 未中·词面路由）`,
							);
						}
					}
					// ── option第二步（09-15 maintainer丙）L2 魂红线词面层：只 warn 不跳（入册留痕）──
					//    治「09-08  魂红线意图落②级而生产读①级从未生效」且「不砍正当工程主题」双面：
					//    L1 形态层（reject 硬拦）不动·本层 6 词面 warn·L3 hard rule不入（directive体系正当高频词·误伤评估在案）。
					if (GUARD.sensitiveWarn) {
						try {
							if (
								GUARD.sensitiveWarn.test(title) ||
								GUARD.sensitiveWarn.test(content)
							) {
								stats.sensitiveWarnCount = (stats.sensitiveWarnCount || 0) + 1;
								ctx.logger?.warn?.(
									`[living-memory] sensitive-warn（L2 魂红线词面·入册留痕不跳） #${stats.sensitiveWarnCount}: ${title.slice(0, 50)}`,
								);
							}
						} catch {}
					}
					// ── 序2 门控·同题合并路（design-approved·门控 P0＋closure gate合车）——先合并不新建·不适用才走下方extraction gate降级 ──
					//    同题判据＝gateTitleTokens 交叠 ≥ 同题交叠阈键（与 pinboard-float fam 同源外置·drill 断言同值）；
					//    候选＝同 space active ∪ 近 72h done（含 D14 僵尸复活面——重写已办结同文 todo 必被吸收不得新建）；
					//    space 用 organ 兜底（词面路由面保守近似：查不到即放行原路·安全方向）；
					//    A 同题同 type：同文⇒吸收不新建；异文且目标 active⇒vc+1＋last_hit_at（复犯链回机器化·vc 消费面=PPR 边权/essential 席）
					//    B 同题异 type（fact→lesson 升级）：允许新建＋explicit 边回指（见 INSERT 后段）
					let sameTopicNewEdgeTo = null;
					{
						const overlapMin = Number(CRIT.sameTopicOverlapMin) || 0;
						if (overlapMin > 0 && title.length >= 4) {
							const gateSpace = targetSpace || "global"; // 写入面修（R1 车）：targetSpace 为空时原值 null ⇒ space = NULL 恒不命中、门控静默失效；与 INSERT 段同兜底。A① 修：共用三级判定值（原 sessionOrgan||"global" 两级⇒词面路由空间门控恒空·真缺陷已治）
							const iso = (msAgo) =>
								new Date(Date.now() + 8 * 3600e3 - msAgo)
									.toISOString()
									.replace("T", " ")
									.slice(0, 16);
							const cands = db
								.prepare(
									"SELECT id, type, title, content FROM memories WHERE space = ? AND (status = 'active' OR (status = 'done' AND closed_at >= ?)) AND ts >= ? ORDER BY id DESC LIMIT 2000", // 审计 A② 修＋修（09-15·批 C 审计真缺陷）：最新优先＋补 content 列——原缺列致 同文判据左侧比对库内 content 不可能（自反比较假象）
								)
								.all(gateSpace, iso(72 * 3600e3), iso(120 * 86400e3));
							const tT = gateTitleTokens(title);
							if (tT.size >= overlapMin) {
								let absorbed = false;
								for (const cr of cands) {
									const cT = gateTitleTokens(cr.title);
									const inter = gateTokenInter(tT, cT); // 批⑯b option：精确交∪长 token 子串互含（原纯精确交·双挂根因）
									if (inter < overlapMin) continue;
									if (cr.type !== type) {
										// 跨路同文吸收（09-15 批 C·maintainer）：同题异 type 且**同文**（title 全同＋content 去空白同）⇒ 吸收不新建
										//    审计 修（批 C 审计真缺陷）：原左侧误用 e.content（LLM 原始形态·与库内净化态不同源⇒自反比较）——
										//    改两侧同净化域：cr.content（库内已净化态）vs content（本轮已净化变量）·去空白比较；
										//    治跨 type 同文零闸覆盖（/#10409 活体）·真升级（异文）走 B 路新建＋边。
										const _crossText =
											String(cr.title).trim() === title.trim() &&
											String(cr.content || "").replace(/\s+/g, "") ===
												content.replace(/\s+/g, "");
										if (_crossText) {
											stats.gateMerged = (stats.gateMerged || 0) + 1;
											ctx.logger?.warn?.(
												`[living-memory] gate-merge: 跨type同文吸收（#${cr.id}·${cr.type}）: ${title.slice(0, 50)}`,
											);
											absorbed = true;
											break;
										}
										sameTopicNewEdgeTo = cr.id; // B 升级路：新建后建边回指（真升级·非同文）
										break;
									}
									stats.gateMerged = (stats.gateMerged || 0) + 1;
									const sameText =
										String(cr.title).trim() === title.trim();
									let _vcChanged = 0;
									// 写入面修（R1 车·2311 receipt gate-merge 前置面）：止血开时不给 todo 候选加 vc、不刷 last_hit_at——auto todo 已不立，
									//   给存量 todo 续命会把僵尸撑活。开关判据与止血段同写法（LEGION_AUTO_STATE_ON 非空即回退）。
									const _vcSkipStopgap = !process.env.LEGION_AUTO_STATE_ON && cr.type === "todo";
									if (!sameText && cr.id && !_vcSkipStopgap) {
										try {
											const _r = db.prepare(
												"UPDATE memories SET validated_count = validated_count + 1, last_hit_at = ? WHERE id = ? AND status = 'active'",
											).run(nowIso(), cr.id);
											_vcChanged = Number(_r && _r.changes) || 0;
											if (_vcChanged > 0)
												stats.gateVcBumped =
													(stats.gateVcBumped || 0) + 1;
										} catch {}
									}
									// 日志按实改行写（写入面修：原 UPDATE 命中 0 行——目标已非 active——也照写「vc+1」）
									ctx.logger?.warn?.(
										`[living-memory] gate-merge: 同题同型${sameText ? "同文吸收" : _vcChanged > 0 ? "合并 vc+1" : _vcSkipStopgap ? "合并（止血期不加 vc）" : "合并（目标非 active·vc 未加）"}（#${cr.id}·${cr.type}${sameText ? "·D14 面" : ""}）: ${title.slice(0, 50)}`,
									);
									absorbed = true;
									break;
								}
								if (absorbed) continue; // 不 INSERT（含 D14 例：同文重写必吸收）
							}
						}
					}
					// ── 序2 salience 观测态（首期只计数不真拦·跑一段统计「若有门控会挡多少」再定拦截·设计段 1.5）──
					// 审计 A④ 修（09-15·独立审计真缺陷）：删 `content.length<30` 拍脑阈——生产分布 MIN=32 致恒 0 命中
					// （近 7 日 auto fact 2,083 条 0 中·观测面落码即死·⑬假观测族）；三结构条件已足「低显著」语义。
					if (type === "fact" && !/→/.test(title) && !String(content || "").includes("行为位："))
						stats.gateBlocked = (stats.gateBlocked || 0) + 1;
					// ── 四闸·extraction gate（2026-08-22）：auto-extract 生成 todo 前与闭环面核对——同题冲突降为 fact 防复活 ──
					//    批⑯(design note)：①auto 路禁新鲜销号豁免（closureFreshDoneHours=24 本意=
					//    手工路「销号后同专项下step」放行；auto 再造=LLM 见「已办结」叙述误挂——#17185/#17186 活证·销号 36min 即再造）
					//    ②补「重开：」逃生口对齐工具面closure gate（同款正则）③降格正文注记闭环条 id（P1-2 A 档口径·时点＋指向）
					//    ④观测键 autoTodoClosedSkip＝实落才计（checksum 幂等跳过不计）。回退：LEGION_AUTO_TODO_GATE_OFF=1 复旧行为全形态。
					let entryType = type;
					// ── 补丁车项1/3/5（五十审 🟡-1/🟡-4/🟡-5·design-approved「发」）：_mil directive锚词外提（止血面与 2331 分级面共用——directive词条不降 decision 面＋不 7d 硬退）＋晋位 flag（实落才计移位）──
					const _mil =
						/军令|命令|指令|批示|裁决/.test(title) ||
						/军令|命令|指令|批示|裁决/.test(String(content || "").slice(0, 200));
					let _promotedTodo = false; // 项5：晋位 flag——checksum 幂等闸后实落才计（止血降格者不计入 promoted）
					let _fuzzyCounted = false; // R1 车（补丁车项2/项5 同族）：糊同降 flag——checksum 幂等闸后实落才计
					let contentOut = content; // 批⑯③：降格注记走 contentOut·checksum 仍用 raw content（保重跑幂等）
					let demoteBy = 0;
					const autoGateOff = !!process.env.LEGION_AUTO_TODO_GATE_OFF;
					if (
						type === "todo" &&
						!(!autoGateOff && /^\s*重开\s*[:：]/.test(title)) // 批⑯②：逃生口（env 回退时连逃生口一并复旧＝旧码本无此口）
					) {
						const cc = closureCheck(db, title, content, undefined, !autoGateOff); // 批⑯①：auto 路禁新鲜豁免（step C案：ts 参退役）
						if (cc.closed) {
							entryType = "fact";
							demoteBy = cc.by;
							if (!autoGateOff)
								contentOut =
									content +
									"\n（extraction gate降格：" +
									nowIso() +
									"·原拟 todo·同题已有办结条 #" +
									cc.by +
									" ⇒ fact 装）"; // 批⑯③：A 档注记——措辞避开 销账/收官/闭环 三词（ closureCheck 证据池 LIKE 面·防降格条自污成证据误拦后续手工同题 todo·drill T1e 抓出）
							ctx.logger?.warn?.(
								`[living-memory] extract-gate: todo→fact（与闭环条 #${cc.by} 重叠 ${cc.overlap} 词）: ${title.slice(0, 50)}`,
							);
						}
					}
					// ── 2step一·extraction gate v2（design-approved「同意」四层治本）──
					//    ①auto 提炼默认禁立 todo（只许 fact/lesson）：非候语义一律降 fact（与批⑯ closureCheck 降格互补——closure 查办结面·本闸管默认面）
					//    ②候面/等待型一律 todo 装：候语义 fact→todo·todo 保持 todo·两者必过同题 active 查重闸（同题已有 active todo ⇒ 并而不立 vc+1）
					//    ③「销 」叙事 fact 疑义才写：销前缀叙事无疑义标记 ⇒ skip（闭环以 closed_at 为唯一凭）
					//    回退：LEGION_EXTRACT_GATE_V2_OFF=1（与批⑯ autoGateOff 独立）。
					let _waitHit = false;
					if (!process.env.LEGION_EXTRACT_GATE_V2_OFF) {
						// 2011 正则修（kickoff·真库对拍定式）：宽式保 todo 面（不限位·用法排除）／严式晋位面（行首·title 面）
						// 用法排除＝名词化(design note)＋引号引用(design note)——行首锚定会误杀真候(design note)
						// 裸候支独立排 agent 首字：候maintainer项被 agent 支排除后禁回塌到裸候再中（支列回塌·#31223 案族）
						const _cls = "(?!(?:动作|事项|receipt|open item)?[项条面类])";
						const _enum = "(?!(?:maintainer|ops|脑|外部|裁|回|办结|验收|令|批|receipt|示)?[①-⑳/／、·])(?!前缀清账批次|清账批次)";
						const _looseRe = new RegExp(
							"(?<![「『\"'])(?:候maintainer|候ops|候脑|候外部|待maintainer)" +
								_enum +
								_cls +
								"(?![」』])" +
								"|(?<![「『\"'])候(?!(?:将|maintainer|ops|脑|外|部))" +
								_enum +
								_cls +
								"(?![」』])" +
								"|(?<![「『\"'])(?:等待|待回|待裁|待办结|待验收|待令|awaiting approval)" +
								_enum +
								_cls +
								"(?![」』])",
						);
						// 2011v6（三十七审丙路 ⚠A）：strict 裸候支补 agent 首字排除——治「候maintainer项三件已列呈」类名词化经交替回退裸候支穿透晋位（loose 已封·strict 漏封·回塌族第二例）
						const _strictRe = new RegExp(
							"^\\s*(?:(?:候maintainer|候ops|候脑|候外部|待maintainer)|候(?!(?:将|maintainer|ops|脑|外|部))|等待|待回|待裁|待办结|待验收|待令|awaiting approval)" +
								_enum +
								_cls +
								"(?![」』])",
						);
						_waitHit =
							_looseRe.test(title) ||
							_looseRe.test(String(content || "").slice(0, 40));
						if (entryType === "todo" && !_waitHit && !/^\s*重开\s*[:：]/.test(title)) {
							// ①默认禁立：auto todo（非候语义）降 fact
							entryType = "fact";
							contentOut =
								contentOut +
								"\n（extraction gate v2：auto 禁立 todo ⇒ fact 装·" +
								nowIso() +
								"）";
							stats.autoTodoV2Demote = (stats.autoTodoV2Demote || 0) + 1;
							ctx.logger?.warn?.(
								"[living-memory] extract-gate v2: auto todo→fact（默认禁立·非候语义）: " +
									title.slice(0, 50),
							);
						} else if (_waitHit && !demoteBy && (entryType === "todo" || _strictRe.test(title))) {
							// ②候面 todo 装＋同题 active 查重闸（并而不立）·晋位（fact→todo）须严式行首命中（2011 正则修：loose 命中的 fact 不晋位·按原 type 落库——治复盘叙事假晋位）
							// 2011v5 补（再检查漏点 L1·#31483 病根另一半）：`!demoteBy`——closureCheck 已降格条不得被本支晋位打回
							// （否则降格注记「fact 装」× 实落 todo 的矛盾复现·#31505 活证；降格=结论·恒优先）
							// ── 丁项（design-approved「同意 按推荐」·四十七审再检查）：同题查重 title 精确相等 → 闸①同族判据 ──
							//    治 #33596/#33605 双写案：复述换措辞穿透精确匹配（「候maintainer示…（保守/激进口径未定）」vs「待办：注入收敛案候maintainer示」）
							//    判据（禁另起炉灶·闸①同族）：归一化（剥括注/待办前缀/空白标点）完全同串 OR 长句互含（≥10 字）
							//    ＋词面交叠兜底（C 软同题判据族·sameTopicOverlapMin≥3 词）
							//    ＋字级 bigram Jaccard 兜底（autoWaitBigramJaccardMin=0.5·中文无空格长句 token 全穿时兜）——语序颠倒型互含抓不住（#33596/#33605 实案「候maintainer示注入面收敛案」vs「注入收敛案候maintainer示」）；
							//    序数对撞防护（closureCheck #31515 同族）：同单位（踵|审|刀|案|批|件|车|令）数字不同 ⇒ 不并（防「115 踵候」并「114 踵候」）
							//    观测键：autoWaitFuzzyDup＝fuzzy 同题→fact 降格命中（render「糊同降」·无损不并）·autoWaitSeqClash＝序数对撞放行（render「候序撞」）——⑬四点接线（赋值/return/render/drill）
							let _dupW = db
								.prepare(
									"SELECT id FROM memories WHERE space = ? AND type = 'todo' AND status = 'active' AND title = ? LIMIT 1",
								)
								.get(targetSpace || "global", String(title).trim());
							let _fuzzyDup = null; // 丁项：fuzzy 同题候选（降格支消费·不走并而不立）
							if (!_dupW) {
								const _stripPrefixD = (s) =>
									String(s || "").replace(/^\s*(?:待办|todo)\s*[:：]?\s*/i, "");
								const _stripParenD = (s) =>
									String(s || "").replace(/[（(][^)）]{0,40}[)）]/g, "");
								const _normTD = (s) =>
									_stripParenD(_stripPrefixD(s))
										.replace(/[\s·、,，。:：;；\-—_]+/g, "")
										.toLowerCase();
								const _seqOfD = (s) => {
									const m = new Map();
									const re = /(\d+)\s*(踵|审|刀|案|批|件|车|令)/g;
									let mm;
									while ((mm = re.exec(String(s || "")))) m.set(mm[2], mm[1]);
									return m;
								};
								const _ntD = _normTD(title);
								if (_ntD.length >= 6) {
									const _tSeqD = _seqOfD(title);
									for (const _p of db
										.prepare(
											"SELECT id, title FROM memories WHERE space = ? AND type = 'todo' AND status = 'active' ORDER BY id DESC LIMIT 60",
										)
										.all(targetSpace || "global")) {
										const _ctD = _normTD(_p.title);
										if (_ctD.length < 6) continue;
										const _containD =
											_ctD === _ntD ||
											(_ntD.length >= 10 && _ctD.includes(_ntD)) ||
											(_ctD.length >= 10 && _ntD.includes(_ctD));
										// 词面交叠兜底（C 软同题判据族）：语序颠倒型同题互含抓不住——#33596/#33605 实案
										const _overlapD =
											!_containD &&
											gateTokenInter(
												gateTitleTokens(title),
												gateTitleTokens(_p.title),
											) >= (Number(CRIT.sameTopicOverlapMin) || 3);
										// 字级 bigram Jaccard 兜底（bigrams/jaccard 同族复用·阈值三级链 autoWaitBigramJaccardMin）：
										//   中文无空格长句 gateTitleTokens 产单 token——纯语序颠倒型互含/交叠双穿（T2 drill 红实证）
										const _bigramD =
											!_containD &&
											!_overlapD &&
											jaccard(bigrams(_ntD), bigrams(_ctD)) >=
												(Number(CRIT.autoWaitBigramJaccardMin) || 0.5);
										if (!_containD && !_overlapD && !_bigramD) continue;
										let _clashD = false;
										const _pSeqD = _seqOfD(_p.title);
										for (const [u, n] of _tSeqD) {
											const _pn = _pSeqD.get(u);
											if (_pn !== undefined && _pn !== n) {
												_clashD = true;
												break;
											}
										}
										if (_clashD) {
											stats.autoWaitSeqClash = (stats.autoWaitSeqClash || 0) + 1;
											continue;
										}
										_fuzzyDup = _p; // 丁项：fuzzy 命中不并（降格支处理·无损）
										break;
									}
								}
							}
							if (_dupW) {
								try {
									db.prepare(
										"UPDATE memories SET validated_count = validated_count + 1, last_hit_at = ? WHERE id = ? AND status = 'active'",
									).run(nowIso(), _dupW.id);
								} catch {}
								stats.autoWaitMerged = (stats.autoWaitMerged || 0) + 1;
								ctx.logger?.warn?.(
									"[living-memory] extract-gate v2: 候面同题并而不立（→#" +
										_dupW.id +
										" vc+1）: " +
										title.slice(0, 50),
								);
								continue;
							}
							// ── 丁项（design-approved「同意 按推荐」·四十七审再检查）：fuzzy 同题→降格 fact（无损·宁漏勿误对齐闸①传统——并而不立仅精确同串）──
							if (_fuzzyDup && !_dupW) {
								entryType = "fact";
								contentOut =
									contentOut +
									"\n（extraction gate丁项：疑似同题已有 active todo #" +
									_fuzzyDup.id +
									"「" +
									String(_fuzzyDup.title).slice(0, 30) +
									"」·降 fact 防双立·人工并账请引（" +
									nowIso() +
									"））";
								_fuzzyCounted = true; // R1 车：原在此即计，注释称「实落才计」而实在 checksum 幂等闸之前——计数移到闸后
								ctx.logger?.warn?.(
									"[living-memory] extract-gate 丁项: 候面 fuzzy 同题→fact（防双立·→#" +
										_fuzzyDup.id +
										"）: " +
										title.slice(0, 50),
								);
							}
							if (entryType !== "todo" && !_fuzzyDup) { // 丁项：fuzzy 降格=结论·恒优先（2011v5 `!demoteBy` 同型防护——否则降格注记×实落 todo 矛盾复现）
								entryType = "todo";
								_promotedTodo = true; // 补丁车项5：flag 替代即计（实落才计移至 checksum 闸后）
								ctx.logger?.warn?.(
									"[living-memory] extract-gate v2: 候面 fact→todo 装: " +
										title.slice(0, 50),
								);
							}
						} else if (
							entryType === "fact" &&
							/^\s*销[\s\u3000]/.test(title) &&
							!/疑义|存疑|待核|有疑|\?|？/.test(String(content || ""))
						) {
							// ③销叙事疑义才写（2step一③）
							stats.saoNarrSkip = (stats.saoNarrSkip || 0) + 1;
							ctx.logger?.warn?.(
								"[living-memory] extract-gate v2: 销叙事 skip（疑义才写·闭环以 closed_at 为唯一凭）: " +
									title.slice(0, 50),
							);
							continue;
						}
					}
					// ── 止血·auto 不立待办（design-approved「测好就上线」）──
					//    病灶：候面/等待型时点态被立成 todo（②支保持或晋位），等待在他处了结后无感 ⇒ 僵尸待办
					//    （创造 10-07 案件档·#33853/#33855/#33856/#33867 四条实证）。
					//    做法：auto 落库前 todo 一律降 fact；本窗计划类 decision（本窗／下step／执行序／下一步）同降；
					//    正文加注「源对话时点·非现况」，7 天到期。任务状态只认窗口与副脑显式记账（工具面 memory_write）。
					//    注记措辞避开三词（closureCheck 证据池 LIKE 面·批⑯③ 同纪律）。
					//    回退：LEGION_AUTO_STATE_ON=1 复旧（②支候面 todo 照旧落）。观测键 autoStateDemote（warn 出口同记）。
					let _autoStateDemoted = false;
					if (
						!process.env.LEGION_AUTO_STATE_ON &&
						(entryType === "todo" ||
							(entryType === "decision" &&
								!_mil &&
								(/本窗|next step|执行序|下一步/.test(title) ||
									/本窗|next step|执行序|下一步/.test(String(content || "").slice(0, 200))))) // 补丁车项3（五十审 🟡-4）：判据扩 content 面＋directive锚词排除（消 190 漏检/37 误伤）
					) {
						const _wasType = entryType;
						entryType = "fact";
						_autoStateDemoted = true;
						contentOut =
							contentOut +
							"\n（自动提炼止血：原拟 " +
							_wasType +
							"·源对话时点的状态，非现况·不立待办·" +
							nowIso() +
							"）";
						ctx.logger?.warn?.(
							"[living-memory] extract 止血: auto " + _wasType + "→fact（源对话时点·非现况）: " + title.slice(0, 50),
						);
					}
					// ── 2step二·auto 面生命周期分级（fact 默认 30d·话术语面 7d·directive锚词永久·env LEGION_LIFECYCLE_OFF 回退）──
					let _lcVt = null;
					let _lcTtl = null;
					if (
						entryType === "fact" &&
						!process.env.LEGION_LIFECYCLE_OFF
					) {
						// _mil 已外提至循环头（补丁车项1·五十审 🟡-1：与止血面共用——降格条directive锚词不 7d 硬退）——本块直用
						if (!_mil) {
							const _skill =
								/话术|技巧|措辞|口径句|说法|prompt|sop/i.test(title);
							_lcVt = isoAhead((_skill ? 7 : 30) * 86400e3);
							_lcTtl = "ttl";
						}
					}
					// ── A① 修（09-15）：本段三级判定已前移至循环头（targetSpace 单点求值）——原「global 专属词路由」段至此退役为指针 ──
					//    （step Z1 organFallback＋词面路由＋routedCount 逻辑全在循环头·本段直用 targetSpace）
					// P0修（09-03 audit）：中断重抽闸——水位在循环后推，中途异常已 COMMIT 条目下轮重抽防重放（checksum 提炼路同式 sha1(entryType+title+content)·轻闸挡逐字重·近重复由nightly patrol去重兜）
					// ── 跨路同文吸收（09-15 批 C 同车·approved）：并入合并路 B 分支（见下）──
					//    治跨路不等价（跨 type 同文零闸覆盖·/#10409 活体案）——同文跨 type 在合并路吸收·checksum 保留幂等键不作同文判据。
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
					if (demoteBy) stats.autoTodoClosedSkip = (stats.autoTodoClosedSkip || 0) + 1; // 批⑯④：实落才计（过 checksum 幂等闸后才计·重跑不重计）
					if (_autoStateDemoted) stats.autoStateDemote = (stats.autoStateDemote || 0) + 1; // 补丁车项2（五十审 🟡-3）：实落才计（原在闸内·幂等跳过者误计）
					if (_promotedTodo && entryType === "todo") stats.autoWaitPromoted = (stats.autoWaitPromoted || 0) + 1; // 补丁车项5（五十审 🟡-5）：晋位且实落 todo 才计（止血降格者不计）
					if (_fuzzyCounted) stats.autoWaitFuzzyDup = (stats.autoWaitFuzzyDup || 0) + 1; // R1 车：糊同降实落才计（同上两行同族）
					db.exec("BEGIN"); // step B-5：主+边同事务（防崩窗孤儿边）——try 内 ·catch 回滚
					// ── external 信任分级（投毒防线同款·论文 2606.24322）：批次级判定——提炼源（source 文本）URL≥3 = 外部粘贴为主 → 本批条目 confidence 0.5（内容可总结·来源属性保留防洗白·检索面外显）
					const extBatch =
						(String(source).match(/https?:\/\//g) || []).length >= 3;
					// 提炼治本②（09-17）：event_at 继承——同款格式闸（日期或日期+时分）；LLM 未给/非法 ⇒ NULL（ts 仍提炼时刻·时序轴不劣于现状）
					// 年份钳制（design-approved·event_at 2025 错位族 626 条治本）：|event_at 年 − 提炼时刻年|>1 ⇒ 拒记 NULL
					//   ——错位族 100% auto 面（LLM 年份幻觉）；|差|=1 保留（真 2025 历史事件如 #12902/#27135 兼容）·手写面 主闸不钳（深度补记合法）。
					//   观测键 evYearClamped＝⑬ 四点接线（赋值/return/render/drill·拒记可数·计数不可见=半合规）。
					const _evRaw = String(e.event_at || "").trim();
					const _evFmtOk = /^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])(T(0[0-9]|1\d|2[0-3]):[0-5]\d)?$/.test(
						_evRaw,
					);
					const _evYearOk =
						_evFmtOk &&
						Math.abs(
							Number(_evRaw.slice(0, 4)) -
								Number(String(nowIso()).slice(0, 4)),
						) <= 1;
					// ── 1907 §一加严(design note)：|=1 且 event_at 月日 == ts 月日 ⇒ 改年为 ts 年（模型抄 ts 月日只错年的高发形态）；
					//    月日不同者＝真补记保留不动。观测键 evYearRewritten＝⑬ 四点接线同族（赋值本行/return read.cjs/render core.cjs「钳年改」/drill）。
					const _evRewrite =
						_evFmtOk &&
						_evYearOk &&
						_evRaw.slice(0, 4) !== String(nowIso()).slice(0, 4) &&
						_evRaw.slice(5, 10) === String(nowIso()).slice(5, 10);
					if (_evRewrite) stats.evYearRewritten = (stats.evYearRewritten || 0) + 1;
					if (_evFmtOk && !_evYearOk)
						stats.evYearClamped = (stats.evYearClamped || 0) + 1;
					const evAt = _evYearOk
						? _evRewrite
							? String(nowIso()).slice(0, 4) + _evRaw.slice(4)
							: _evRaw
						: null;
					const _c2Forced7 = !process.env.LEGION_AUTO_STATE_ON && !_mil && (_autoStateDemoted || (type === "todo" && entryType !== "todo")); // wave 车二-8d：止血 7 天判据（同下行原式，提名供下方两段用）
					if (!process.env.LEGION_AUTO_STATE_ON && !_mil && (_autoStateDemoted || (type === "todo" && entryType !== "todo"))) { _lcVt = isoAhead(7 * 86400e3); _lcTtl = "ttl"; } // 止血：降格的时点态 7 天到期（补丁车项1·五十审 🟡-1/🟡-2：补 env 回退判据＋_mil directive锚词排除——directive锚词条走 2331 面永久·不 7d 硬退；回退态=旧码 30d 分级）
					// ── wave 车二-8d（10-17·落码规格 2.5）：寿命分档——自动 fact、decision 中标题是状态句(design note)，
					//    且不以「pending ruling：」「销 #」开头、不含directive锚词的 → 7 天 ttl；本车真改了寿命的（原 30 天或永久）入库后另标 stale_by='car2-life:7d'（数据面可筛；
					//    ttl 条 P2 段不碰，无副作用）。止血降格、话术类原本已是 7 天的不重复标。LEGION_LIFE_GUARD_OFF=1 或 LEGION_LIFECYCLE_OFF 时不动。
					//    LEGION_LIFE_SKILL30_ON=1(design note)：话术类 7 天改 30 天（状态句、止血降格者除外）
					let _c2Life7 = false;
					if (process.env.LEGION_LIFE_GUARD_OFF !== "1" && !process.env.LEGION_LIFECYCLE_OFF && !_mil && (entryType === "fact" || entryType === "decision")) {
						try {
							const _stRe = CRIT.lifeStateTitle;
							if (_stRe && _stRe.test(title) && !/^(?:呈裁：|销 #)/.test(title)) {
								const _vt7 = isoAhead(7 * 86400e3);
								if (_lcVt !== _vt7 || _lcTtl !== "ttl") {
									_lcVt = _vt7;
									_lcTtl = "ttl";
									_c2Life7 = true;
								}
							} else if (
								process.env.LEGION_LIFE_SKILL30_ON === "1" &&
								entryType === "fact" &&
								!_c2Forced7 &&
								/话术|技巧|措辞|口径句|说法|prompt|sop/i.test(title)
							) {
								_lcVt = isoAhead(30 * 86400e3);
								_lcTtl = "ttl";
							}
						} catch {
							stats.c2LifeTierErrors = (stats.c2LifeTierErrors || 0) + 1;
						}
					}
					const insInfo = ins.run(
						nowIso(),
						entryType,
						title,
						contentOut, // 批⑯③：落库用注记后形态（降格条带「原拟 todo·与已闭环条 #id」）；checksum（下行）仍用 raw content 保幂等
						targetSpace || "global",
						"auto:" + _a12sid,
						sha1(entryType + title + content),
						extBatch ? 0.5 : 1.0,
						evAt,
						entryType === "todo" ? isoAhead(7 * 86400e3) : _lcVt, // D2 todo 7d＋2step二：auto fact 分级带龄（30d·话术语面 7d·directive锚词永久=null）
						entryType === "todo" ? "ttl" : _lcTtl, // D2 ttl 标＋2step二（_lcVt 落值时同步 ttl 标）
					); // step Z1②：source 归因同源 _a12sid；8-31 移植族修复①：run 返回值存 insInfo——node:sqlite 的 lastInsertRowid 在 run 结果上（statement 本体无·原 引用幻属性 NaN→NULL 致产边整路死·正库 0 条铁证）
					if (_c2Life7 && insInfo && insInfo.lastInsertRowid) {
						try {
							db.prepare("UPDATE memories SET stale_by = 'car2-life:7d' WHERE id = ?").run(Number(insInfo.lastInsertRowid)); // wave 车二-8d：数据面可筛标记
							stats.c2Life7 = (stats.c2Life7 || 0) + 1;
						} catch {
							stats.c2LifeTierErrors = (stats.c2LifeTierErrors || 0) + 1;
						}
					}
					// ── 序2 B 升级路边（门控·同题异 type）：新建后建 explicit 边回指同题条（升级链留痕·fact→lesson 族）──
					if (sameTopicNewEdgeTo && insInfo && insInfo.lastInsertRowid) {
						try {
							db.prepare(
								"INSERT INTO memories_edges (src, dst, edge_type, weight, valid_at, instruction) VALUES (?, ?, 'explicit', 0.5, ?, ?) ON CONFLICT(src, dst, edge_type) DO UPDATE SET last_seen = excluded.valid_at",
							).run(
								Number(insInfo.lastInsertRowid),
								sameTopicNewEdgeTo,
								nowIso(),
								"门控同题升级路回指（序2·09-15）",
							);
						} catch {
							stats.gateEdgeErrors = (stats.gateEdgeErrors || 0) + 1; // 审计 A③ 修（09-15）：边失败原静默仍 COMMIT（无回指边新条·事后不可审·⑬计数不可见族）
						}
					}
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
                  WHERE memories_fts MATCH ? AND m.status='active' AND m.space = ? AND m.id != ? AND (m.valid_to IS NULL OR m.valid_to > ?)
                  ORDER BY bm25(memories_fts) LIMIT 1`)
									.get(
										match2,
										targetSpace || "global",
										Number(insInfo.lastInsertRowid),
										nowIso(), // 十七审候裁⑦：建边目标路硬排（过期条不作建边靶）
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
					// ── **09-12 主案（approved）：提炼条补齐写时增强——与手工写路同源** ──
					//    根治实测缺口：auto 条缺向量 568（09-10 起断崖）·无活边覆盖仅 37.1%；根因＝提炼器自带 INSERT 不走统一写入口。
					//    本处新增 ①写时嵌入（writeTimeEmbed·fire-and-forget）②反思建边（writeTimeReflectLink·同判据 j≥0.20∧inter≥2 ⇒ auto-link 0.4）。
					//    盖戳（stale_state/conflicts）**不随之**——涉他条状态变更，不随提炼批量放大。
					try {
						const _newId = Number(insInfo.lastInsertRowid);
						void writeTimeEmbed(
							db,
							_newId,
							title,
							content,
							(k) => ctx.credentials.resolve(k),
						);
						// 审计 D2（2026-09-12）：建边为**同步重扫**（jieba 分词 ≤2000 候选）——原在提炼循环内同步调用，
						// 批量提炼时逐条阻塞循环 ⇒ 改 fire-and-forget（不阻塞·失败计数走 writeTimeEnhanceErrors）。
						// 审计 D3：建边受 `LEGION_REFLECT_LINK_OFF` 控（与手工路内联段同开关·该行为可应急回退）。
						void (async () => {
							try {
								if (process.env.LEGION_REFLECT_LINK_OFF) return;
								const _rl = writeTimeReflectLink(db, {
									id: _newId,
									title,
									space: targetSpace || "global",
									type: entryType,
									ts: nowIso(),
								});
								if (_rl) stats.writeTimeReflectLinked = (stats.writeTimeReflectLinked || 0) + 1;
							} catch (eRl) {
								core.writeTimeEnhanceErrors += 1;
								ctx.logger?.warn?.(
									`[living-memory] extract reflect-link fail: ${String(eRl).slice(0, 80)}`,
								);
							}
						})();
					} catch (eWte) {
						core.writeTimeEnhanceErrors += 1;
						ctx.logger?.warn?.(
							`[living-memory] extract write-enhance fail: ${String(eWte).slice(0, 80)}`,
						);
					}
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
				// 批⑰ parseFail（三档全败/截断）⇒ **水位不推**——该段留下轮重抽（治「解析失败≡判无价值·水位照推段永丢」不可逆丢账）。
				if (
					!_parseRes.parseFail &&
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
					onExtracted(); // 提炼成功后清空缓冲（旧路）
					// 配套（审计 D7）：信号在成功路径消化（与 水位同生命周期）——失败留队下轮再试
					if (stats.signalArmed) {
						onSignalsDigested();
						stats.signalArmed = false;
					}
				}
				stats.bufferedChars =
					buffer().length +
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
			// 批⑰ （design-approved·J 路 🔴J3-1）：返回双态 {entries, parseFail}——
			//   原三档全败静默返回 [] ⇒ 「LLM 真判无价值 []」与「解析失败」零区分·水位照推段永丢（不可逆丢账）；
			//   修：非空原文三档全败 ⇒ parseFail:true（调用面水位不推＋计数出口）；真「[]」空数组 parseFail:false 照推不误伤。
			const _ok = (arr) => ({
				entries: arr.filter((x) => x && typeof x === "object"),
				parseFail: false,
			});
			try {
				const parsed = JSON.parse(raw);
				if (Array.isArray(parsed)) return _ok(parsed);
			} catch {}
			const fence = raw.match(/```(?:json)?\s*([\s\S]*?)```/);
			if (fence) {
				try {
					const parsed = JSON.parse(fence[1]);
					if (Array.isArray(parsed)) return _ok(parsed);
				} catch {}
			}
			const bracket = raw.match(/\[[\s\S]*\]/);
			if (bracket) {
				try {
					const parsed = JSON.parse(bracket[0]);
					if (Array.isArray(parsed)) return _ok(parsed);
				} catch {}
			}
			return { entries: [], parseFail: true }; // 批⑰ 可辨失败（含空原文/截断形态）
		}
	return { runExtract, parseExtractEntries, isExtracting: () => extracting };
}

module.exports = { setupExtract };
