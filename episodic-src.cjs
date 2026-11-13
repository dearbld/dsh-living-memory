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

const fs = require("node:fs");
const path = require("node:path");
const os = require("node:os");
const { createHash } = require("node:crypto");

const CODEX_KIND = "codex-client-window";
const CODEX_SOURCE = /^(auto|manual):(intl-social|us-ops)-client\|chat:([a-zA-Z0-9-]{8,100})$/;
const FU_NAO_SOURCE = /^manual:fu-nao(?::\d{8})?$/;
const CHAT_ID = /^[a-zA-Z0-9-]{8,100}$/;
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const SHA256 = /^[0-9a-f]{64}$/;
const MAX_PREFIX = 256 * 1024 * 1024;
const MAX_ENTRIES = 100_000;
const MAX_DEPTH = 8;
const CHUNK_BYTES = 1024 * 1024;
const profiles = {};

const sha256 = (value) => createHash("sha256").update(value).digest("hex");
const matches = (pattern, value) => typeof value === "string" && pattern.test(value);
const record = (value) => value && typeof value === "object" && !Array.isArray(value);
const keysAre = (value, required, optional = []) =>
	record(value) &&
	required.every((key) => Object.hasOwn(value, key)) &&
	Object.keys(value).every((key) => required.includes(key) || optional.includes(key));

function canonicalAnchor(anchor) {
	if (anchor?.kind === "fu-nao-window") {
		if (!keysAre(anchor, ["kind", "sid"], ["msg", "sidechain"]) ||
			!matches(UUID, anchor.sid) ||
			(Object.hasOwn(anchor, "msg") && !matches(UUID, anchor.msg)) ||
			(Object.hasOwn(anchor, "sidechain") && typeof anchor.sidechain !== "boolean")) return null;
		return { kind: anchor.kind, sid: anchor.sid, msg: anchor.msg ?? null, sidechain: anchor.sidechain === true };
	}
	if (!keysAre(anchor, ["schemaVersion", "kind", "profile", "chatId", "projectRoot", "scope", "transcript", "messages"]) ||
		anchor.schemaVersion !== 1 || anchor.kind !== CODEX_KIND ||
		typeof anchor.profile !== "string" || !Object.hasOwn(profiles, anchor.profile) || !matches(CHAT_ID, anchor.chatId) ||
		typeof anchor.projectRoot !== "string" || !path.isAbsolute(anchor.projectRoot) || anchor.projectRoot.includes("\0") ||
		!["extraction-window", "single-message"].includes(anchor.scope) || !Array.isArray(anchor.messages) ||
		anchor.messages.length === 0 || (anchor.scope === "single-message" && anchor.messages.length !== 1)) return null;
	const { transcript } = anchor;
	if (!keysAre(transcript, ["relativePath", "prefixBytes", "prefixSha256"]) ||
		typeof transcript.relativePath !== "string" || !transcript.relativePath || path.isAbsolute(transcript.relativePath) ||
		transcript.relativePath.includes("\0") || transcript.relativePath.split(/[\\/]/).includes("..") ||
		!Number.isSafeInteger(transcript.prefixBytes) || transcript.prefixBytes < 1 || transcript.prefixBytes > MAX_PREFIX ||
		!matches(SHA256, transcript.prefixSha256)) return null;

	const messages = [];
	const seen = new Set();
	for (const message of anchor.messages) {
		if (!keysAre(message, ["ordinal", "role", "timestamp", "rowSha256", "textSha256"]) ||
			!Number.isSafeInteger(message.ordinal) || message.ordinal < 0 || seen.has(message.ordinal) ||
			!["user", "assistant"].includes(message.role) || typeof message.timestamp !== "string" || !message.timestamp ||
			!matches(SHA256, message.rowSha256) || !matches(SHA256, message.textSha256)) return null;
		const { ordinal, role, timestamp, rowSha256, textSha256 } = message;
		seen.add(ordinal);
		messages.push({ ordinal, role, timestamp, rowSha256, textSha256 });
	}
	const { schemaVersion, kind, profile, chatId, projectRoot, scope } = anchor;
	const { relativePath, prefixBytes, prefixSha256 } = transcript;
	return { schemaVersion, kind, profile, chatId, projectRoot, scope, transcript: { relativePath, prefixBytes, prefixSha256 }, messages };
}

function sourceMatches(row, anchor) {
	if (anchor.kind === "fu-nao-window") return row.space === "brain" && FU_NAO_SOURCE.test(row.source);
	const mode = anchor.scope === "extraction-window" ? "auto" : "manual";
	return row.space === anchor.profile && row.source === mode + ":" + anchor.profile + "-client|chat:" + anchor.chatId;
}

class TranscriptError extends Error {
	constructor(code) {
		super(code);
		this.code = code;
	}
}

function guardDeadline(deadline) {
	if (Date.now() >= deadline) throw new TranscriptError("read-error");
}

function transcriptName(name, chatId) {
	if (name.endsWith("-" + chatId + ".jsonl")) return true;
	const at = name.lastIndexOf("-" + chatId + "_");
	return at !== -1 && name.endsWith(".jsonl") &&
		matches(UUID, name.slice(at + chatId.length + 2, -6).toLowerCase());
}

function scanTranscripts(roots, chatId, deadline) {
	const candidates = [];
	let entries = 0;
	function walk(root, directory, depth) {
		guardDeadline(deadline);
		if (!fs.lstatSync(directory).isDirectory() || fs.realpathSync(directory) !== directory) return;
		for (const item of fs.readdirSync(directory, { withFileTypes: true })) {
			guardDeadline(deadline);
			if (++entries > MAX_ENTRIES) throw new TranscriptError("too-large");
			if (item.isSymbolicLink()) continue;
			const file = path.join(directory, item.name);
			if (item.isDirectory() && depth < MAX_DEPTH) walk(root, file, depth + 1);
			if (item.isFile() && transcriptName(item.name, chatId)) candidates.push({ root, file });
		}
	}
	for (const directory of roots) {
		let stat;
		try {
			stat = fs.lstatSync(directory);
		} catch (error) {
			if (error.code === "ENOENT") continue;
			throw error;
		}
		if (!stat.isDirectory() || stat.isSymbolicLink()) continue;
		const root = { name: path.basename(directory), path: fs.realpathSync(directory) };
		walk(root, root.path, 0);
	}
	return candidates;
}

function sameFile(left, right) {
	return left.isFile() && right.isFile() && left.dev === right.dev && left.ino === right.ino;
}

function readPrefix(candidate, bytes, deadline, consume) {
	guardDeadline(deadline);
	const { file } = candidate;
	const before = fs.lstatSync(file);
	// Refuse links in the directory chain as well as at the file itself.
	if (!before.isFile() || fs.realpathSync(file) !== file) return null;
	const fd = fs.openSync(file, fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW | fs.constants.O_NONBLOCK);
	try {
		const current = fs.fstatSync(fd);
		if (!sameFile(before, current) || current.size < bytes) return null;
		const hash = createHash("sha256");
		let lastByte;
		for (let offset = 0; offset < bytes;) {
			guardDeadline(deadline);
			const block = Buffer.allocUnsafe(Math.min(CHUNK_BYTES, bytes - offset));
			const length = fs.readSync(fd, block, 0, block.length, offset);
			if (!length) return null;
			const chunk = block.subarray(0, length);
			hash.update(chunk);
			consume?.(chunk);
			lastByte = chunk[length - 1];
			offset += length;
		}
		if (lastByte !== 10 || !sameFile(before, fs.lstatSync(file)) || fs.realpathSync(file) !== file) return null;
		return hash.digest("hex");
	} finally {
		fs.closeSync(fd);
	}
}

function isMainChat(meta, chatId) {
	if (!record(meta) || meta.id !== chatId ||
		["parent_thread_id", "agent_path", "subagent_history_start_ordinal"].some((key) => Object.hasOwn(meta, key))) return false;
	if (meta.thread_source === "user") return typeof meta.source === "string" && !/subagent|review/i.test(meta.source);
	return meta.thread_source === "agent_created_thread" && meta.source === "vscode" && meta.originator === "codex_work_desktop";
}

function getFrame(row) {
	const message = row.payload;
	if (row.type !== "response_item" || message?.type !== "message" ||
		!(message.role === "user" || (message.role === "assistant" && message.phase === "final_answer"))) return null;
	if (!Array.isArray(message.content)) return null;
	const text = [];
	for (const block of message.content) {
		if (!record(block)) return null;
		if (block.type !== "input_text" && block.type !== "output_text") continue;
		if (typeof block.text !== "string") return null;
		text.push(block.text);
	}
	const joined = text.join("\n");
	return joined.trim() ? { role: message.role, text: joined } : null;
}

function readFrames(candidate, anchor, project, prompt, window, deadline) {
	const decoder = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true });
	const requested = new Map(anchor.messages.map((message) => [message.ordinal, message]));
	const matched = new Map();
	const single = anchor.scope === "single-message";
	const before = [];
	const selected = [];
	let meta, anchorIndex = null, frames = 0, sysFolded = 0;

	function line(bytes) {
		guardDeadline(deadline);
		if (!bytes.length) return;
		const row = JSON.parse(decoder.decode(bytes));
		if (!record(row)) throw new TranscriptError("read-error");
		if (!meta) {
			if (row.type !== "session_meta" || !isMainChat(row.payload, anchor.chatId)) throw new TranscriptError("not-main-chat");
			meta = row.payload;
			if (typeof meta.cwd !== "string" || !path.isAbsolute(meta.cwd) || realProject(meta.cwd) !== project)
				throw new TranscriptError("project-mismatch");
			return;
		}
		if (row.type === "session_meta") throw new TranscriptError("not-main-chat");

		const wanted = requested.get(row.ordinal);
		const frame = getFrame(row);
		const isAgents = frame?.role === "user" && /^\s*# AGENTS\.md instructions\b/.test(frame.text);
		const isFolded = frame?.role === "user" && (isAgents || /^\s*<environment_context\b/.test(frame.text) || frame.text === prompt);
		if (wanted) {
			if (matched.has(row.ordinal) || !frame || isAgents || (single && isFolded) ||
				wanted.role !== frame.role || wanted.timestamp !== row.timestamp ||
				wanted.rowSha256 !== sha256(JSON.stringify(row)) || wanted.textSha256 !== sha256(frame.text)) {
				throw new TranscriptError("message-mismatch");
			}
			matched.set(row.ordinal, frame);
		}
		if (!single || !frame) return;
		if (isFolded) {
			sysFolded++;
			return;
		}
		if (wanted) {
			anchorIndex = frames;
			selected.push(...before, frame);
		} else if (anchorIndex === null) {
			before.push(frame);
			if (before.length > window) before.shift();
		} else if (frames - anchorIndex <= window) selected.push(frame);
		frames++;
	}

	let pending = [];
	const digest = readPrefix(candidate, anchor.transcript.prefixBytes, deadline, (chunk) => {
		let start = 0;
		for (let end = chunk.indexOf(10); end !== -1; end = chunk.indexOf(10, start)) {
			const tail = chunk.subarray(start, end);
			if (pending.length) {
				pending.push(tail);
				line(Buffer.concat(pending));
				pending = [];
			} else line(tail);
			start = end + 1;
		}
		if (start < chunk.length) pending.push(chunk.subarray(start));
	});
	if (digest !== anchor.transcript.prefixSha256) throw new TranscriptError("prefix-mismatch");
	if (!meta) throw new TranscriptError("not-main-chat");
	if (matched.size !== requested.size) throw new TranscriptError("message-mismatch");
	const file = path.relative(candidate.root.path, candidate.file);
	const pieces = single ? selected : [...matched.keys()].sort((a, b) => a - b).map((ordinal) => matched.get(ordinal));
	return {
		pieces,
		attribution: single ? "单条锚" : "提炼输入窗，非逐条归因",
		scope: anchor.scope,
		session: anchor.chatId,
		root: candidate.root.name,
		file,
		segment: Object.hasOwn(meta, "history_base") ? "continuation" : "base",
		migrated: candidate.root.name === "archived_sessions" || file !== anchor.transcript.relativePath,
		frames: single ? frames : pieces.length,
		windowRange: single ? [Math.max(0, anchorIndex - window), Math.min(frames, anchorIndex + window + 1)] : [0, pieces.length],
		sysFolded,
		anchorIndex,
	};
}

function realProject(project) {
	try { return fs.realpathSync(project); } catch { return null; }
}

function resolveCodexWindow(anchor, {
	roots = ["sessions", "archived_sessions"].map((name) => path.join(os.homedir(), ".codex", name)),
	projectRoots,
	window = 6,
	deadline = Date.now() + 30_000,
} = {}) {
	const canonical = canonicalAnchor(anchor);
	if (!canonical || canonical.kind !== CODEX_KIND) return { ok: false, code: "kind-mismatch" };
	try {
		const expected = realProject(projectRoots?.[canonical.profile] ?? profiles[canonical.profile].projectRoot);
		if (!expected || realProject(canonical.projectRoot) !== expected) return { ok: false, code: "project-mismatch" };
		const candidates = scanTranscripts(roots, canonical.chatId, deadline);
		if (!candidates.length) return { ok: false, code: "file-missing" };
		const { prefixBytes, prefixSha256 } = canonical.transcript;
		let found;
		for (const candidate of candidates) {
			if (readPrefix(candidate, prefixBytes, deadline) !== prefixSha256) continue;
			if (found) return { ok: false, code: "file-ambiguous" };
			found = candidate;
		}
		if (!found) return { ok: false, code: "prefix-mismatch" };
		return { ok: true, ...readFrames(found, canonical, expected, profiles[canonical.profile].prompt, window, deadline) };
	} catch (error) {
		return { ok: false, code: error instanceof TranscriptError ? error.code : "read-error" };
	}
}

module.exports = { CODEX_KIND, CODEX_SOURCE, FU_NAO_SOURCE, sha256, canonicalAnchor, sourceMatches, resolveCodexWindow };
