#!/usr/bin/env node
'use strict'
// prepublishOnly contamination gate -- four-class scan of everything about to ship.
//
// Design note (and the reason this file exists): the previous release passed a
// hand-run drill that checked only two of the four classes, and shipped internal
// governance vocabulary anyway. A gate that only runs when someone remembers to
// run it is not a gate. This one is wired to `npm publish` and fails the publish.
//
// Two pattern layers:
//   - BUILT_IN below: GENERIC structural patterns, safe to publish (absolute home
//     paths, internal package scope, private-key blocks, credential shapes,
//     high-entropy blobs, this project's own internal directory layout).
//   - an optional site file at ~/.dsh/dsh-living-memory-preflight.json for
//     site-specific vocabulary (governance terms, internal case numbers, org
//     jargon). It is NEVER published -- that is the whole point: a blocklist that
//     ships with the package tells an attacker exactly what slips through it.
//     If the file is present but unreadable/corrupt, the gate FAILS CLOSED.
//
// Exit 0 = clean. Exit 1 = violations listed (file:line + class), publish blocked.
// Only file names, line numbers, classes and pattern labels are printed -- matched
// text is never echoed, so a violation report cannot itself leak the secret.
const fs = require('node:fs')
const path = require('node:path')
const os = require('node:os')

const ROOT = path.resolve(__dirname, '..')
const RULES_PATH = process.env.DSH_LM_PREFLIGHT_RULES ||
  path.join(os.homedir(), '.dsh', 'dsh-living-memory-preflight.json')

const BUILT_IN = {
  'internal-path': ['/Users/[^/\\s"\')]+/', '/home/[\\w.-]+/', 'C:\\\\Users\\\\',
    '\\.dsh/(profiles|plugins-src|sessions|storages)', 'plugins-src', 'harness/'],
  'private-key': ['-----BEGIN [A-Z ]*PRIVATE KEY-----'],
  'credential-shape': ['(?<![A-Za-z0-9])(?:sk|pk|ak|ark)-[A-Za-z0-9._\\-]{16,}',
    '(?<![A-Za-z0-9])(?:sk|pk|ak|ark)-[A-Za-z0-9]{1,8}(?:[.\\-_][A-Za-z0-9]{4,}){2,}',
    '[a-f0-9]{28,}\\.[A-Za-z0-9]{10,}', 'MEYCIQ[A-Za-z0-9+/=]{20,}', 'MEUCI[A-Za-z0-9+/=]{20,}',
    '[A-Za-z0-9+/=]{120,}'],
}   // __BUILTIN_END__ single-source block terminator; every block-range scanner keys off this line

const DEFAULT_FILES = ['index.cjs', 'askgate.cjs', 'core.cjs', 'episodic-anchor.cjs',
  'episodic-src.cjs', 'extract.cjs', 'inject.cjs', 'patrol.cjs',
  'pipeline.cjs', 'read.cjs', 'tools.cjs', 'client.js',
  'cordis.patch.yml', 'dict-custom.json', 'guard-rules.default.json', 'criteria-rules.default.json',
  'package.json', 'README.md', 'NOTICE', 'LICENSE',
  'scripts/preflight.cjs']

function loadSite() {
  if (!fs.existsSync(RULES_PATH)) {
    return { classes: {}, files: null, warn: 'site rules file absent -- running BUILT_IN classes only (' + RULES_PATH + ')' }
  }
  try {
    const d = JSON.parse(fs.readFileSync(RULES_PATH, 'utf8'))
    if (!d || typeof d !== 'object' || !d.classes) throw new Error('missing .classes')
    return { classes: d.classes, files: Array.isArray(d.files) ? d.files : null,
      soulLiteral: Object.prototype.hasOwnProperty.call(d, 'soul_literal') ? d.soul_literal : undefined, warn: null }
  } catch (e) {
    return { classes: null, files: null, fatal: 'site rules file present but unreadable or invalid' }
  }
}

const site = loadSite()
if (site.fatal) { console.error('[x] preflight FAILED CLOSED -- ' + site.fatal); process.exit(1) }

const classes = Object.assign({}, BUILT_IN, site.classes || {})
const files = site.files || DEFAULT_FILES
if (!files.length) { console.error('[x] preflight FAILED CLOSED -- empty file list'); process.exit(1) }
// Self-exemption: this script's own BUILT_IN literal necessarily contains the very
// shapes it looks for (a credential regex has to spell the credential shape). Skip
// exactly that block's line range when scanning this file -- nothing else is exempt.
// 2026-09-12 (audit follow-up): decode \\uXXXX / \\u{...} before testing. An escaped payload
// (e.g. a forbidden word written as \\u519B\\u4EE4) used to sail through this gate untouched --
// the build-side checker decoded, this shipped gate did not. Same regex family both sides.
const U4DEC = (s) => s.replace(/\\u\{([0-9a-fA-F]+)\}|\\u([0-9a-fA-F]{4})/g, (m, a, b) => {
  const cp = parseInt(a || b, 16)
  return cp > 0x10FFFF ? m : String.fromCodePoint(cp)
})

const WS_CP = new Set([
  0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x20, 0x85, 0xA0, 0x1680,
  0x2000, 0x2001, 0x2002, 0x2003, 0x2004, 0x2005, 0x2006, 0x2007, 0x2008, 0x2009, 0x200A,
  0x2028, 0x2029, 0x202F, 0x205F, 0x3000, 0xFEFF, 0x1C, 0x1D, 0x1E, 0x1F,
])
const normSecret = (s) => Array.from(U4DEC(s).normalize('NFKC').replace(/\\[nrt]/g, ' '))
  .filter((c) => !WS_CP.has(c.codePointAt(0))).join('')

let soulSet = null
if (site.soulLiteral !== undefined) {
  try {
    const cfg = site.soulLiteral
    if (!cfg || cfg.k !== 12 || cfg.normalize !== 'U4DEC_NFKC_EXPLICIT_WS_CP' ||
        typeof cfg.source !== 'string' || !cfg.source) throw new Error('invalid soul config')
    let source = cfg.source
    if (source.startsWith('~/')) source = path.join(os.homedir(), source.slice(2))
    if (!path.isAbsolute(source)) throw new Error('soul source must be absolute')
    const soulChars = Array.from(normSecret(fs.readFileSync(source, 'utf8')))
    if (soulChars.length < 12) throw new Error('soul source too short')
    const grams = new Set()
    for (let i = 0; i + 12 <= soulChars.length; i++) grams.add(soulChars.slice(i, i + 12).join(''))
    if (!grams.size) throw new Error('soul ngram set empty')
    soulSet = grams
  } catch {
    console.error('[x] preflight FAILED CLOSED -- soul source missing, unreadable, or invalid')
    process.exit(1)
  }
}

const SELF_SKIP = (() => {
  try {
    const s = fs.readFileSync(__filename, 'utf8').split('\n')
    const a = s.findIndex((l) => l.startsWith('const BUILT_IN = {'))
    if (a < 0) return null
    // Brace counting -- NOT "first line that trims to '}'". The closing brace may legally sit on the
    // last entry's line, and that formatting-only variant used to widen the exemption window enough to
    // swallow a real finding elsewhere in this file (fail-open, reproduced 2026-09-11). Unbalanced
    // => null = no exemption, i.e. fail-closed (report more, never less). Aligned with the build-side
    // V8b/V8g checks so both faces of the gate behave the same way.
    // 2026-09-12 (audit D3): key off an explicit terminator marker instead of brace counting --
    // brace counting could be stretched by a stray '{' inside the literal and swallow real findings
    // outside the block (reproduced: 2 injected lines were enough). The marker is single-source.
    const e = s.findIndex((l) => l.includes('__BUILTIN_END__'))
    if (e < 0 || e <= a) return null   // fail-closed: no exemption when the marker is missing
    return [a + 1, e + 1]
  } catch { return null }
})()

const violations = []
const missing = []
let scanned = 0, compiled = 0
for (const rel of files) {
  const abs = path.join(ROOT, rel)
  if (!fs.existsSync(abs)) { missing.push(rel); continue }
  scanned++
  const lines = fs.readFileSync(abs, 'utf8').split('\n')
  const isSelf = abs === path.resolve(__filename)
  for (const [cls, pats] of Object.entries(classes)) {
    for (const p of pats) {
      let re
      try { re = new RegExp(p) } catch { violations.push({ rel, line: 0, cls, note: 'UNCOMPILABLE PATTERN (gate cannot run)' }); continue }
      compiled++
      lines.forEach((text, i) => {
        if (isSelf && SELF_SKIP && i + 1 >= SELF_SKIP[0] && i + 1 <= SELF_SKIP[1]) return
        const dec = U4DEC(text).normalize('NFKC')
        if (re.test(text) || (dec !== text && re.test(dec))) violations.push({ rel, line: i + 1, cls })
      })
    }
  }
  const whole = lines.join('\n')
  const decodedWhole = U4DEC(whole).normalize('NFKC')
  // Site-root split paths only; generic DSH data layouts are not private machine instances.
  const crossPathRules = [
    /path\.join\s*\(\s*os\.homedir\s*\(\s*\)\s*,\s*['"]harness['"]/,
  ]
  for (const re of crossPathRules) {
    const m = whole.match(re) || decodedWhole.match(re)
    if (!m) continue
    const line = whole.slice(0, m.index).split('\n').length
    if (!violations.some((v) => v.rel === rel && v.line === line && v.cls === 'internal-path')) {
      violations.push({ rel, line, cls: 'internal-path' })
    }
  }
  if (soulSet) {
    const chars = Array.from(normSecret(lines.join('\n')))
    let hits = 0
    for (let i = 0; i + 12 <= chars.length; i++) {
      if (soulSet.has(chars.slice(i, i + 12).join(''))) hits++
    }
    if (hits) violations.push({ rel, soul: hits, cls: String.fromCodePoint(83, 79, 85, 76) + '12' })
  }
}
if (missing.length) {
  console.error(`[x] preflight BLOCKED publish -- listed file(s) missing: ${missing.join(', ')}`)
  process.exit(1)
}
console.log(`preflight: scanned ${scanned} file(s), ${Object.keys(classes).length} class(es), ${compiled} pattern application(s)`)
if (site.warn) console.log('[warn] ' + site.warn)
if (violations.length) {
  console.error(`\n[x] preflight BLOCKED publish -- ${violations.length} hit(s):`)
  const byClass = {}
  for (const v of violations) (byClass[v.cls] = byClass[v.cls] || []).push(v)
  for (const [cls, vs] of Object.entries(byClass)) {
    console.error(`  [${cls}] ${vs.length} hit(s)`)
    for (const v of vs.slice(0, 12)) console.error(v.soul ? `    ${v.rel}:${v.cls} (${v.soul})` : `    ${v.rel}:${v.line}`)
    if (vs.length > 12) console.error(`    ... +${vs.length - 12} more`)
  }
  console.error('\n(matched text is never printed; soul scan reports file-level hits only)')
  process.exit(1)
}
console.log('[ok] preflight clean -- four-class scan found nothing shippable-blocking')
process.exit(0)
