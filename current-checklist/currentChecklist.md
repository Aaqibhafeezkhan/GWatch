# Current Checklist

The organisational checklist for the current working period. It is not a scope gate, task tracker, roadmap, or completion record — the issues are the tasks, and `development-docs/productRoadmap.md` is the scope. Move a finished checklist into `current-checklist/archives/` when a new working period begins if the history is worth keeping.

## Working period: towards 1.0 — Stage 1 (Trust)

- [ ] #202 Require sign-in on this computer once an account exists, with #288 (create the first administrator / reset a password from the command line)
- [ ] #200 Harden the agent's self-written systemd unit
- [ ] #247, #248, #207 Signing: retire the migration workflow, pin a spare offline key
- [ ] #203–#209, #249–#252 Low-severity security findings
- [ ] #253 Supply-chain scanning
- [ ] #201 `toolchain` directive
- [ ] #222 Publish a release that matches `VERSION` (0.8.12)
- [ ] #272 Tag the MCP module
- [ ] #10, #286 Licence decision and launch plan (maintainers)
- [ ] #120 Authenticode identity (maintainers)

## Handover notes

Agents record notes for the next session here (see `AGENTS.md` §10), newest first, and remove them when they no longer apply.

- 2026-11-14 — Claude Code: 0.8.12 / agent 0.8.8 on `claude/nice-shannon-qo6fq6` (alert outbox, re-check interval, flap detection, short-lived certificates, DNS-lookup hint, rollup percentiles, #210, #211, `testing/synctest`). Version is a patch bump by default; the maintainer may prefer 0.9.0 for this much. Not verified locally: PostgreSQL/MySQL (the new `alert_outbox` table and `rollups.latency_hist` column rely on CI's database jobs), Windows/macOS. Left for later: #211's suggested free-disk-space check in the retention loop; `evaluateDownLocked`'s acknowledged-incident read under the engine lock (mentioned in #210); per-check overrides for the re-check interval; CSV history export does not include percentiles. #210 and #211 can be closed once this is merged.
- 2026-11-10 — Claude Code: Spec-Driven Docs set up. Items marked "Inferred — maintainer to confirm" in `development-docs/` need a maintainer's review. #225 (refresh the roadmap) is addressed by the new `productRoadmap.md` and can be closed once this change is merged.
