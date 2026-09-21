window.__ModuleLoader__.load({
	id: "dsh-living-memory",
	factory: (require) => {
		var module = { exports: {} };
		var exports = module.exports;
		const react = require("react");
		// 09-06 rc.1：client 侧 inject 必须作为模块静态导出声明（对照官方 conversation 的 exports.inject）
		exports.inject = ["slots"];
		exports.apply = function apply(ctx) {
			const slots = ctx.slots; // inject 已声明，Proxy 放行 ctx.slots
			if (slots === undefined) return;
			const stop = slots.inject("conversation.view", () =>
				slots.register(
					{
						name: "conversation.view",
						id: "legion-memory",
						order: 50,
						label: "记忆",
					},
					() => {
						const tab = react.useState(0);
						const setTab = tab[1];
						return react.createElement(
							"div",
							{
								style: {
									padding: "16px",
									fontFamily: "monospace",
									fontSize: "13px",
								},
							},
							react.createElement(
								"div",
								{
									style: {
										display: "flex",
										gap: "12px",
										marginBottom: "10px",
										borderBottom: "1px solid rgba(128,128,128,.3)",
										paddingBottom: "8px",
									},
								},
								react.createElement(
									"span",
									{
										onClick: () => {
											setTab(0);
										},
										style: {
											cursor: "pointer",
											fontWeight: tab[0] === 0 ? "bold" : "normal",
											opacity: tab[0] === 0 ? 1 : 0.6,
										},
									},
									"🧠 记忆时间线",
								),
								react.createElement(
									"span",
									{
										onClick: () => {
											setTab(1);
										},
										style: {
											cursor: "pointer",
											fontWeight: tab[0] === 1 ? "bold" : "normal",
											opacity: tab[0] === 1 ? 1 : 0.6,
										},
									},
									"💰 token spend",
								),
								react.createElement(
									"span",
									{
										onClick: () => {
											setTab(2);
										},
										style: {
											cursor: "pointer",
											fontWeight: tab[0] === 2 ? "bold" : "normal",
											opacity: tab[0] === 2 ? 1 : 0.6,
										},
									},
									"🛡 sentry",
								),
							),
							tab[0] === 0
								? react.createElement(MemoryView)
								: tab[0] === 1
									? react.createElement(TokenView)
									: react.createElement(SentryView),
						);
					},
				),
			);
			ctx.effect(() => stop);
		};
		function MemoryView() {
			const data = react.useState(null);
			const setData = data[1];
			react.useEffect(() => {
				const load = () => {
					fetch("/living-memory/memory-snapshot")
						.then((r) => r.json())
						.then(setData)
						.catch((e) => {
							setData({ error: String(e) });
						});
				};
				load();
				const timer = window.setInterval(load, 30000);
				return () => {
					window.clearInterval(timer);
				};
			}, []);
			const items = (data[0] && data[0].entries) || [];
			const total = data[0] && data[0].total;
			const err = data[0] && data[0].error;
			return react.createElement(
				"div",
				null,
				react.createElement(
					"h3",
					null,
					"living memory · timeline" +
						(total !== undefined ? "（共 " + total + " 条）" : ""),
				),
				err ? react.createElement("p", null, "错误: " + err) : null,
				!data[0] ? react.createElement("p", null, "加载中…") : null,
				items.map((e, i) =>
					react.createElement(
						"div",
						{
							key: i,
							style: {
								padding: "6px 0",
								borderBottom: "1px solid rgba(128,128,128,.25)",
							},
						},
						"[" + e.ts + "] [" + e.type + "] " + e.title,
					),
				),
			);
		}
		function TokenView() {
			const data = react.useState(null);
			const setData = data[1];
			react.useEffect(() => {
				const load = () => {
					fetch("/living-memory/token-snapshot")
						.then((r) => r.json())
						.then(setData)
						.catch((e) => {
							setData({ error: String(e) });
						});
				};
				load();
				const timer = window.setInterval(load, 30000);
				return () => {
					window.clearInterval(timer);
				};
			}, []);
			const err = data[0] && data[0].error;
			if (err) return react.createElement("p", null, "错误: " + err);
			if (!data[0]) return react.createElement("p", null, "加载中…");
			const g = data[0].grand;
			const top = data[0].top || [];
			const fmt = (n) =>
				n >= 1000000
					? (n / 1000000).toFixed(1) + "M"
					: n >= 1000
						? (n / 1000).toFixed(1) + "K"
						: String(n);
			const tfmt = (ms) => {
				if (!ms) return "—";
				const d = new Date(ms);
				const pad = (n) => (n < 10 ? "0" + n : "" + n);
				const hm = pad(d.getHours()) + ":" + pad(d.getMinutes());
				const today = new Date();
				return d.toDateString() === today.toDateString()
					? hm
					: d.getMonth() + 1 + "/" + d.getDate() + " " + hm;
			};
			return react.createElement(
				"div",
				null,
				react.createElement(
					"h3",
					null,
					"token spend panel · consumption meter (" + g.sessions + " 会话）",
				),
				react.createElement(
					"div",
					{
						style: {
							display: "flex",
							gap: "16px",
							padding: "8px 0 12px",
							borderBottom: "1px solid rgba(128,128,128,.3)",
						},
					},
					react.createElement(
						"span",
						null,
						"总: ",
						react.createElement("b", null, fmt(g.total)),
					),
					react.createElement("span", null, "输入: " + fmt(g.uncachedIn)),
					react.createElement("span", null, "缓存读: " + fmt(g.cacheRead)),
					react.createElement("span", null, "缓存写: " + fmt(g.cacheWrite)),
					react.createElement("span", null, "输出: " + fmt(g.out)),
				),
				react.createElement(
					"table",
					{
						style: {
							borderCollapse: "collapse",
							width: "100%",
							marginTop: "6px",
						},
					},
					react.createElement(
						"thead",
						null,
						react.createElement(
							"tr",
							{
								style: {
									borderBottom: "1px solid rgba(128,128,128,.4)",
									textAlign: "left",
								},
							},
							react.createElement(
								"th",
								{ style: { padding: "4px 8px 4px 0" } },
								"会话",
							),
							react.createElement("th", null, "标题"),
							react.createElement("th", null, "轮"),
							react.createElement("th", null, "输入"),
							react.createElement("th", null, "输出"),
							react.createElement("th", null, "总"),
							react.createElement("th", null, "活跃"),
							react.createElement("th", null, "压力"),
						),
					),
					react.createElement(
						"tbody",
						null,
						top.map((r, i) =>
							react.createElement(
								"tr",
								{
									key: i,
									style: { borderBottom: "1px solid rgba(128,128,128,.15)" },
								},
								react.createElement(
									"td",
									{ style: { padding: "4px 8px 4px 0", opacity: 0.6 } },
									r.sid,
								),
								react.createElement("td", null, r.title || "—"),
								react.createElement("td", null, r.turns),
								react.createElement("td", null, fmt(r.uncachedIn)),
								react.createElement("td", null, fmt(r.out)),
								react.createElement(
									"td",
									null,
									react.createElement("b", null, fmt(r.total)),
								),
								react.createElement(
									"td",
									{ style: { opacity: r.lastAt ? 1 : 0.4 } },
									tfmt(r.lastAt),
								),
								react.createElement(
									"td",
									{
										style: {
											color:
												r.pressurePct > 70
													? "#e74c3c"
													: r.pressurePct > 50
														? "#e67e22"
														: "inherit",
										},
									},
									r.pressurePct === null
										? "—"
										: r.pressurePct +
												"%" +
												(r.pressureFloor ? "⌊" : "") +
												(r.legacyWindow ? "†" : ""), // 批⑰ B8b（09-21·J5-1）：两信号位消费——⌊=下限（pressure 缺失·假零兜底）†=老分母（262144 型·与现役 1M 不可比）
								),
							),
						),
					),
				),
				react.createElement(
					"p",
					{ style: { opacity: 0.5, marginTop: "8px" } },
					"压力>70% 红·>50% 橙（组织学：>70% 建议换窗）·⌊=下限值（pressure 缺失）·†=老分母窗（与现役 1M 不可比）· 30s 自动刷新 · 按最近活跃排序（新窗口跑完首轮即上镜）",
				),
			);
		}
		function SentryView() {
			const data = react.useState(null);
			const setData = data[1];
			react.useEffect(() => {
				const load = () => {
					fetch("/living-memory/sentry-snapshot")
						.then((r) => {
							if (!r.ok)
								throw new Error(
									"HTTP " + r.status + "（端点未生效·待重启加载）",
								);
							return r.json();
						})
						.then(setData)
						.catch((e) => {
							setData({ error: String(e) });
						});
				};
				load();
				const timer = window.setInterval(load, 300000);
				return () => {
					window.clearInterval(timer);
				};
			}, []);
			const err = data[0] && data[0].error;
			if (err) return react.createElement("p", null, "错误: " + err);
			if (!data[0]) return react.createElement("p", null, "加载中…");
			if (!data[0].date)
				return react.createElement(
					"p",
					{ style: { opacity: 0.6 } },
					"no report for today yet (generated daily at 07:05)",
				);
			const lines = String(
				data[0].content ||
					"(report body empty — content is null/undefined)",
			).split("\n"); // 十三刀乙⑤ N-c1：content null 兜底（原 String(null/undefined) 显 "null"/"undefined" 单行）
			return react.createElement(
				"div",
				null,
				react.createElement(
					"h3",
					null,
					"sentry · morning check (" + data[0].date + "）",
				),
				lines.map((l, i) => {
					const color = /FAIL|红灯|✗/.test(l)
						? "#e74c3c"
						: /PASS|绿/.test(l)
							? "#2ecc71"
							: "inherit";
					return react.createElement(
						"div",
						{
							key: i,
							style: { padding: "2px 0", whiteSpace: "pre-wrap", color: color },
						},
						l,
					);
				}),
				react.createElement(
					"p",
					{ style: { opacity: 0.5, marginTop: "8px" } },
					"one report per day (07:05) · refreshes every 300s · FAIL red / PASS green",
				),
			);
		}
		return module.exports;
	},
});
