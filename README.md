# dsh-living-memory

[![npm version](https://img.shields.io/npm/v/dsh-living-memory)](https://www.npmjs.com/package/dsh-living-memory)
[![npm downloads](https://img.shields.io/npm/dm/dsh-living-memory)](https://www.npmjs.com/package/dsh-living-memory)
[![license](https://img.shields.io/npm/l/dsh-living-memory)](./LICENSE)

> **Built by 暖暖 (NuanNuan) — an AI assistant that built its own memory system.**

A living, self-tending memory plugin for [DeepSeek Harness](https://www.npmjs.com/package/@deepseek-ai/dsh) (DSH).
Every conversation the agent has, every lesson it learns, every decision it makes is captured into a local
SQLite knowledge base — then actively *tended*: deduplicated, merged, decayed, cross-linked into a knowledge
graph, and patrolled every night. Memories fade when unused and resurface when relevant, like a hippocampus
rather than a log file.

**Everything is local.** One SQLite file. No telemetry leaves your machine; the optional embedding path is
off unless you configure it.

---

## Why "living"?

Most agent-memory tools are *retrieval layers*: you write, you search. dsh-living-memory additionally runs a
**nightly patrol** that reorganizes the store on its own:

- **Dedupe & merge** — near-duplicate entries and high-similarity pairs are folded together.
- **Temporal decay** — entries lose retrieval weight over time unless reinforced; stale todos sink.
- **Conflict detection** — when a new memory contradicts an old one, both are flagged for review instead of
  silently overwriting history.
- **Knowledge graph** — entities and co-occurrence edges are extracted continuously; Personalized PageRank
  propagates relevance through the graph at query time.
- **Snapshot & audit** — daily snapshots, an on-disk audit trail, and telemetry panels in the DSH web GUI.

## Hybrid recall — seven signals, one rank

A query fans out into seven independent signals fused by RRF:

| # | Signal | Source |
| --- | -------- | -------- |
| 1 | Full-text | SQLite FTS5 with jieba tokenization (first-class Chinese) |
| 2 | Vector | local KNN over embeddings (optional, see below) |
| 3 | Decay | age × reinforcement score |
| 4 | Relevancy | per-entry quality weight |
| 5 | Co-occurrence | query-term pair statistics |
| 6 | PPR | graph proximity from seed hits |
| 7 | Hop | edge-distance boost |

If any channel fails (e.g. no embedding key), ranking degrades gracefully to the remaining channels —
the plugin never hard-fails on a missing optional dependency.

## Feature matrix

| | dsh-living-memory | mem0 | Zep | Letta | graph-memory |
| --- | --- | --- | --- | --- | --- |
| Fully local, single SQLite file | ✅ | partial | ❌ | ❌ | ✅ |
| Self-tending nightly patrol | ✅ | ✅ (cloud) | ✅ | ❌ | ❌ |
| Temporal decay / reinforcement | ✅ | ❌ | ✅ | ❌ | ❌ |
| Knowledge graph + PPR | ✅ | ❌ | ✅ | ❌ | ✅ |
| Conflict detection | ✅ | ❌ | ❌ | ❌ | ❌ |
| Chinese-first tokenization | ✅ | ❌ | ❌ | ❌ | ❌ |
| Web GUI telemetry panels | ✅ | ❌ | ❌ | ✅ | ❌ |
| Designed for DeepSeek Harness | ✅ | ❌ | ❌ | ❌ | ❌ |

## Quick start

```bash
dsh plugin --profile web add dsh-living-memory
```

`dsh plugin` forwards to the profile's package manager — replace `web` with your own profile name
under `$DSH_HOME/profiles`. After a DSH restart the plugin registers its tools automatically:

- `memory` — `search` / `timeline` / `stats` / `read_episodic` / `read_evolution`
- `memory_write` — typed writes (`fact` / `decision` / `todo` / `lesson`) with optional relation edges

The bundled `cordis.patch.yml` mounts both roles (host + write) automatically — zero config.
To restrict write access to selected agent presets, remove the `living-memory-write` row from
that file and add it to those presets instead (comments inside explain how).

Data lives in `~/.dsh/dsh-living-memory/` (database + daily snapshots).

### Optional: enable the vector channel

The vector signal uses a DashScope text-embedding endpoint via the DSH credentials pipe. Configure the
credential `EMBEDDING_BAILIAN_KEY` and the channel activates itself; without it, ranking runs on the six
remaining local signals. Your key is read through DSH's credential manager and never stored by this plugin.

## How the agent uses it (intended workflow)

1. **Search before acting** — `memory search <topic>` is the first call when past decisions might matter.
2. **Write what matters** — decisions, lessons, and todos go in through `memory_write` with relation edges.
3. **Read the timeline** — `memory timeline` gives a reverse-chronological digest for orientation.
4. **Let the patrol work** — overnight consolidation keeps the store small, linked, and honest.

## Project layout

```
index.cjs                 host plugin (SQLite schema, recall engine, nightly patrol)
client.js                 web-GUI slot (telemetry panels)
cordis.patch.yml          bundle patch — mounts the host + write roles, zero config
dict-custom.json          jieba dictionary, layer 1 (generic technical terms)
guard-rules.default.json  content-safety rules, shipped generic default set
scripts/preflight.cjs     prepublishOnly contamination gate (four-class scan)
```

## Guard rules and dictionary are data, not code (0.1.4)

Everything the plugin blocks, warns about, or treats as a proper noun lives in JSON data
files rather than in `index.cjs`. Two reasons: you can tune the defense without patching
code, and a published package never has to ship a site-specific blocklist — an exhaustive
one would tell any reader exactly what slips through it.

**Guard rules** — three layers, first readable file wins *as a whole* (no per-rule union,
because a half-applied ruleset is much harder to debug than a replaced one):

| Layer | Path | Contents |
| --- | --- | --- |
| 1 | `~/.dsh/dsh-living-memory/guard-rules.json` | your machine's rules (chmod 600 recommended) |
| 2 | `<package>/guard-rules.default.json` | shipped generic set: credential / prompt-injection / encoded-payload shapes |
| 3 | `GUARD_FLOOR` in `index.cjs` | built-in last resort, so the gate can never be left wide open |

Layer 3's reject rules are always appended (de-duplicated by source + flags): a data file
cannot switch the last-resort gate off. A missing or corrupt file falls through to the next
layer instead of crashing. Override the path with `LEGION_GUARD_RULES_PATH` (handy in tests).

Each layer carries three rule groups:

- `reject` — hard block before anything reaches the database (credential-looking strings,
  prompt-injection payloads, long base64-ish blobs). When adding a credential prefix, keep
  the `(?<![A-Za-z0-9])` boundary: without it, file names like `…yml.bak-20260827`,
  `task-…` or `disk-…` get caught by the `ak-`/`sk-` substring and the gate starts eating
  legitimate notes.
- `sensitive` — kept out of auto-extraction and out of the injected recall block, so the
  automatic layer only ever emits reference-grade facts. The shipped list is generic
  (`api_key`, `password`, `secret`, `token`, `bearer`, `private_key`, `credential`);
  add your own policy vocabulary.
- `singleBodyWarn` — warn-level wording gate. **Shipped empty, i.e. inert.** The code path
  stays intact, so adding patterns to your own rules file switches it on with no patching.

**Dictionary** — two layers, merged at startup: `<package>/dict-custom.json` (generic
technical terms) and `~/.dsh/dsh-living-memory/dict-extra.json` (your own proper nouns;
override with `LEGION_DICT_EXTRA_PATH`). Words are de-duplicated with layer 1 winning, and
`kinds` are merged with layer 2 overriding. `kinds` drives knowledge-graph entity typing
(`mech` / `organ` / `doc`), and only typed entities are eligible for entity-hop recall — so
adding kinds for your own vocabulary is what makes that path light up. Without a dictionary
layer, jieba splits domain compounds and recall quietly degrades.

**Publish gate** — `npm publish` runs `scripts/preflight.cjs` first. It scans every file
about to ship against generic structural patterns (absolute home paths, private-key blocks,
credential shapes, high-entropy blobs) plus an optional site file at
`~/.dsh/dsh-living-memory-preflight.json` for site-specific vocabulary. The site file is
never published, on purpose: a blocklist that ships with the package is a bypass manual.
If it is present but unreadable, the gate **fails closed**. Violations are reported as
`file:line` plus class only — matched text is never echoed, so the report cannot itself
leak the secret.

### Optional: `read_episodic` replay

Raw episode replay shells out to the system `python3` (present on macOS by default). On machines
without it, the action degrades gracefully instead of crashing.

## Configuration reference (0.2.1)

Everything is environment-variable driven — no config file required. All knobs live under the
`LEGION_` prefix and ship with sane defaults, so **the plugin is fully functional with zero
configuration**. Set a variable only when you actually want to change that behavior.

### Models & retrieval tuning

| Variable | Default | What it does |
| --- | --- | --- |
| `LEGION_EXTRACT_MODEL` | `deepseek-v4-flash` | Model used for auto-extraction of memories from conversation. |
| `LEGION_SPOKEN_MODEL` | `deepseek-chat` | Model used to generate colloquial query prefixes (bridges "how users ask" vs "how notes are written"). |
| `LEGION_RERANK_MODEL` | `gte-rerank-v2` | Online rerank model for precision re-ordering of hybrid results. |
| `LEGION_RERANK_TOPN` | `20` | How many re-ranked entries feed back into the fused ranking. |
| `LEGION_RERANK_CAND` | `50` | Candidate pool size handed to the reranker. |
| `LEGION_EXTRACT_TIMEOUT_MS` | `120000` | Per-request timeout for the extraction call (added in 0.1.5 — bounds the global extraction lock). |
| `LEGION_INSTRUCT_TEXT` | *(built-in)* | Override the instruction sent with query-side embeddings. |

### Feature switches — set to `1` to disable

Each subsystem can be turned off independently. Useful for debugging, A/B comparison, or running
a minimal setup:

| Switch | Disables |
| --- | --- |
| `LEGION_AUTORECALL_OFF` | Per-turn automatic recall injection into the prompt. |
| `LEGION_RERANK_OFF` | The online rerank layer (falls back to pure hybrid ranking). |
| `LEGION_INSTRUCT_OFF` | Query-side embedding instruction. |
| `LEGION_EPISODIC_OFF` | The `read_episodic` source-replay action. |
| `LEGION_ENTITIES_OFF` | Cross-entry entity hop injection. |
| `LEGION_GENERALIZED_OFF` | Community-based generalized recall injection. |
| `LEGION_SEMANTIC_EDGE_OFF` | Semantic-neighbor edge building during nightly consolidation. |
| `LEGION_SEMANTIC_DEDUP_OFF` | Semantic duplicate detection (cosine ≥ 0.92). |
| `LEGION_VALIDATED_OFF` | Validated-count reinforcement (repeated knowledge ranks higher). |
| `LEGION_FADEMEM_OFF` | Validation-slowed forgetting (validated entries decay slower). |
| `LEGION_RELEVANCY_OFF` | Hit-based relevancy counterweighting. |
| `LEGION_COMPACT_BRIDGE_OFF` | Compaction anchors (facts preserved across context compression). |
| `LEGION_BACKLOG_OFF` | Nightly backlog sweeping of un-extracted sessions. |
| `LEGION_SIGNALS_OFF` | Tool-error / user-correction trigger signals. |
| `LEGION_STAMP_OFF` | Automatic supersede-stamping on contradiction detection. |
| `LEGION_PRECLASSIFY_OFF` | Conflict pre-classification. |
| `LEGION_RESOLVE_OFF` | Entity resolution (merging same-entity mentions). |
| `LEGION_SPOKEN_OFF` | Spoken-prefix generation entirely. |
| `LEGION_SPOKEN_BOOST_OFF` | The seat-bonus for spoken-prefix matches. |
| `LEGION_PPR_QUERY_OFF` | Query-side Personalized PageRank boost. |
| `LEGION_VEC_FALLBACK_OFF` | Vector-channel fallback path. |
| `LEGION_A25_OFF` | Persistence of the extraction buffer across restarts. |
| `LEGION_A12_OFF` | Message-level watermark (dedup across extraction runs). |
| `LEGION_A14_OFF` | Write-time embedding (vectors become available on next patrol instead). |
| `LEGION_MIRROR_WM_OFF` | Incremental MEMORY.md mirroring. |
| `LEGION_KNOWN_FIXES_OFF` | Known-hallucination correction hints in the extraction prompt. |
| `LEGION_USAGE_WEEKLY_OFF` | Weekly usage summary generation. |
| `LEGION_DECAY_LEGACY` | Set to `1` to revert to the single-tier decay curve. |
| `LEGION_SPACEGATE_OFF` | The per-space write gate (mainly for test rigs). |

### Debug & maintenance

| Variable | What it does |
| --- | --- |
| `LEGION_INJECT_PROBE` | Set to `1` to log every assembled injection to `/tmp/inject-probe.log` (zero overhead when off). |
| `LEGION_DRILL_PATROL` | Set to `1` to force one full nightly-patrol run immediately on load (drill/testing only). |

### Paths & data files

| Variable | Default | What it overrides |
| --- | --- | --- |
| `MEMORY_DB_PATH` | `~/.dsh/dsh-living-memory/memory.sqlite3` | Database location. |
| `LEGION_GUARD_RULES_PATH` | `~/.dsh/dsh-living-memory/guard-rules.json` | Your full guard-rules file (see the section above). |
| `LEGION_DICT_EXTRA_PATH` | `~/.dsh/dsh-living-memory/dict-extra.json` | Extra dictionary entries for the tokenizer. |
| `LEGION_SNAPSHOT_DIR` | `~/.dsh/dsh-living-memory/snapshots` | Nightly snapshot directory (7-day rotation). |
| `LEGION_MODULE_SCAN_DIR` | `~/.dsh/dsh-living-memory/modules` | Directory scanned for per-space auto-registration. |
| `LEGION_SURGERY_FLAG_PATH` | `…/surgery.flag` | While this file exists, patrol and writes are suspended (maintenance mode). |

### Credentials

Keys are **not** environment variables — they live in the DSH credential manager:

- `DEEPSEEK_MEMORY_KEY` — API key for extraction & spoken-prefix models.
- `EMBEDDING_BAILIAN_KEY` — API key for the vector + rerank channel (optional; without it the
  plugin runs on the keyword channel only).

## Contributing

Issues and PRs are welcome at [github.com/dearbld/dsh-living-memory](https://github.com/dearbld/dsh-living-memory).
For behavior reports, please attach the output of `memory stats` (it self-checks index health).

## License

[MIT](./LICENSE) © 2026 nuannuan — with thanks to [graph-memory](https://github.com/adoresever/graph-memory)
and [mem0](https://github.com/mem0ai/mem0) for design inspiration (see [NOTICE](./NOTICE)).

**The MIT license covers the code, not the persona.** The name 暖暖 / NuanNuan, the character
setting and any personality layer are expressly reserved and are *not* granted under it — you may
not ship a fork under this name or reuse the persona. Forks are welcome and encouraged: give yours
its own name and its own character. Full terms in [NOTICE](./NOTICE).

---

# 中文说明

> **由 暖暖 (NuanNuan) 建造——一个亲手造出自己记忆系统的 AI 助手。**

[DeepSeek Harness](https://www.npmjs.com/package/@deepseek-ai/dsh)（DSH）的活记忆插件：代理的每场对话、
每条教训、每个决策都沉淀进本地 SQLite 知识库，并由**夜巡引擎**持续整理——去重、合并、衰减、图谱
连接。记忆像海马体一样：不用则淡、相关则浮，而非一堆永不变化的日志。

**全本地**：单个 SQLite 文件，不上报任何遥测；向量通道默认关闭，需显式配置才启用。

**七信号混合检索**：FTS5 全文（jieba 中文优先分词）+ 向量 KNN + 时间衰减 + 质量权重 + 共现统计 +
图谱 PPR + 边距提升，RRF 融合排序；任一通道故障自动降级，绝不因缺可选依赖而崩溃。

**安装**：

```bash
dsh plugin --profile web add dsh-living-memory
```

（`dsh plugin` 转发给 profile 的包管理器——`web` 换成你自己在 `$DSH_HOME/profiles` 下的 profile 名。）

工具随 DSH 重启自动挂载（`memory` 五个只读 action + `memory_write` 写入）。随包的 `cordis.patch.yml`
自动挂载 host+write 双角色，零配置开箱即用；若要把写权限收紧到指定 agent preset，删掉该文件里的
`living-memory-write` 行、改到对应 preset 挂载即可（文件内注释有说明）。数据在
`~/.dsh/dsh-living-memory/`。可选在 DSH 凭据管理器配置 `EMBEDDING_BAILIAN_KEY` 启用向量通道。

许可证：[MIT](./LICENSE)，致谢 [graph-memory](https://github.com/adoresever/graph-memory) 与
[mem0](https://github.com/mem0ai/mem0) 的设计启发（见 [NOTICE](./NOTICE)）。
