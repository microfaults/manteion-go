@AGENTS.md

## Claude Code only

<!-- Keep this file to the import plus Claude-specific notes; repo knowledge belongs in AGENTS.md. Last verified 2026-11-16. -->
- Claude Code loads this file only for sessions whose working directory is this tree. A session or subagent started in the default checkout (`faults-lab/manteion-go`, an older `develop`) or in another repo gets that place's instructions instead. When you dispatch a subagent, give it this tree's absolute path (`faults-lab/worktrees/manteion-atropos-daemon`) and tell it to read `AGENTS.md` here first.
- Auto memory under `~/.claude/projects/` is machine-local and keyed by the launch directory. It is not a substitute for `AGENTS.md`: put durable repo facts there.
