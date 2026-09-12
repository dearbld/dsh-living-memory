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
  // -- 2026-09-12 (batch 2): internal-id / knife-series / case-number families now ship --
  //   Why: these three groups used to live only on the build side (consumed by the build's own
  //   version gate), so editing a published artifact and re-publishing did NOT trip the gate
  //   -- a one-legged release gate. Scope: generic shapes only, no site-specific words.
  //   Deliberately NOT shipped: gm / wb (common abbreviations -- would false-positive).
  'internal-code-id': ['\\bA-\\d{1,2}\\b', '(?<![A-Za-z0-9_])A\\d{1,2}(?![\\-\\dA-Za-z_])'],
  // Knife-series and case-number families spell their non-ASCII terms as \\uXXXX escapes on
  // purpose: this shipped gate script must stay pure ASCII (a build-side assertion forbids CJK in it).
  'knife-series': ['(?:[\\u7532\\u4e59\\u4e19\\u4e01\\u620a\\u5df1\\u5e9a\\u8f9b\\u58ec\\u7678]|\\u5438\\u6536|\\u5fae\\u578b|\\u9996|\\u672b|\\u8865)\\u5200'],
  'case-number': ['#\\d{3,4}(?![\\dA-Za-z_])', '\\u7b2c\\s*[0-9\\u4e00\\u4e8c\\u4e09\\u56db\\u4e94\\u516d\\u4e03\\u516b\\u4e5d\\u5341]+\\s*\\u5200'],
  'credential-shape': ['(?<![A-Za-z0-9])(?:sk|pk|ak|ark)-[A-Za-z0-9._\\-]{16,}',
    '(?<![A-Za-z0-9])(?:sk|pk|ak|ark)-[A-Za-z0-9]{1,8}(?:[.\\-_][A-Za-z0-9]{4,}){2,}',
    '[a-f0-9]{28,}\\.[A-Za-z0-9]{10,}', 'MEYCIQ[A-Za-z0-9+/=]{20,}', 'MEUCI[A-Za-z0-9+/=]{20,}',
    '[A-Za-z0-9+/=]{120,}'],
}

const DEFAULT_FILES = ['index.cjs', 'client.js', 'cordis.patch.yml', 'dict-custom.json',
  'guard-rules.default.json', 'criteria-rules.default.json', 'package.json', 'README.md',
  'NOTICE', 'LICENSE', 'scripts/preflight.cjs']

function loadSite() {
  if (!fs.existsSync(RULES_PATH)) {
    return { classes: {}, files: null, warn: 'site rules file absent -- running BUILT_IN classes only (' + RULES_PATH + ')' }
  }
  try {
    const d = JSON.parse(fs.readFileSync(RULES_PATH, 'utf8'))
    if (!d || typeof d !== 'object' || !d.classes) throw new Error('missing .classes')
    return { classes: d.classes, files: Array.isArray(d.files) ? d.files : null, warn: null }
  } catch (e) {
    return { classes: null, files: null, fatal: 'site rules file present but unusable: ' + String(e).slice(0, 120) }
  }
}

const site = loadSite()
if (site.fatal) { console.error('[x] preflight FAILED CLOSED -- ' + site.fatal); process.exit(1) }

const classes = Object.assign({}, BUILT_IN, site.classes || {})
const files = site.files || DEFAULT_FILES
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
    let depth = 0, opened = false
    for (let i = a; i < Math.min(s.length, a + 48); i++) {   // window cap: a stray '{' used to stretch the block range over real findings
      for (const ch of s[i]) {
        if (ch === '{') { depth++; opened = true }
        else if (ch === '}') depth--
      }
      if (opened && depth <= 0) return [a + 1, i + 1]
    }
    return null
  } catch { return null }
})()

const violations = []
let scanned = 0, compiled = 0
for (const rel of files) {
  const abs = path.join(ROOT, rel)
  if (!fs.existsSync(abs)) continue
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
        const dec = U4DEC(text)
        if (re.test(text) || (dec !== text && re.test(dec))) violations.push({ rel, line: i + 1, cls })
      })
    }
  }
}
console.log(`preflight: scanned ${scanned} file(s), ${Object.keys(classes).length} class(es), ${compiled} pattern application(s)`)
if (site.warn) console.log('[warn] ' + site.warn)
if (violations.length) {
  console.error(`\n[x] preflight BLOCKED publish -- ${violations.length} hit(s):`)
  const byClass = {}
  for (const v of violations) (byClass[v.cls] = byClass[v.cls] || []).push(v)
  for (const [cls, vs] of Object.entries(byClass)) {
    console.error(`  [${cls}] ${vs.length} hit(s)`)
    for (const v of vs.slice(0, 12)) console.error(`    ${v.rel}:${v.line}`)
    if (vs.length > 12) console.error(`    ... +${vs.length - 12} more`)
  }
  console.error('\n(matched text is never printed -- open the file at the reported line)')
  process.exit(1)
}
console.log('[ok] preflight clean -- four-class scan found nothing shippable-blocking')
process.exit(0)
