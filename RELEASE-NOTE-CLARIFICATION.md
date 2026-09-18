# 0.2.7 clarification (post-release audit)

The 0.2.7 release note line "mirror day headers with entry counts" was **inaccurate**:
that feature belongs to the site-internal MEMORY.md mirroring pipeline, which the public
build deliberately strips (build stage C4b — see the maintenance ledger). The published
package therefore does **not** contain it. The note has been corrected at the source.

What 0.2.7 actually ships: `timeline day` parameter (strict real-calendar gate), auto-distill
`event_at` inheritance, distill subject constraint, the search profiling probe
(`LEGION_SEARCH_PROF_ON`), and the fake-date audit fix — all verified present in the tarball.

The npm tarball's bundled README also predates this release (pack happened before the README
refresh commit); the current README lives on GitHub and ships with the next version.
