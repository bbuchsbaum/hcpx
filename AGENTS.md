# Agent Coordination Instructions for hcpx

## Beads: Distributed Task Tracking

This project uses **beads** (`bd`) - a git-backed graph issue tracker designed for AI agent coordination.

### What is Beads?

Beads provides persistent, structured memory for coding agents. Instead of unstructured markdown plans, it uses a dependency-aware task graph that:

- Stores issues as JSONL in `.beads/` directory
- Uses hash-based IDs (like `bd-a1b2`) to prevent merge conflicts
- Tracks task dependencies and blockers
- Auto-syncs with git for multi-agent coordination
- Supports hierarchical tasks: `bd-a3f8` → `bd-a3f8.1` → `bd-a3f8.1.1`

### Installation

```bash
# npm
npm install -g @beads/bd

# Homebrew
brew install steveyegge/beads/bd

# Go
go install github.com/steveyegge/beads/cmd/bd@latest
```

### Initialize in Project

```bash
bd init                    # Standard mode - commits .beads/ to repo
bd init --stealth          # Local-only tracking (not committed)
bd hooks install           # Install git hooks (recommended)
```

## Essential Commands

| Command | Purpose |
|---------|---------|
| `bd ready` | Show unblocked tasks ready to work on |
| `bd create "Title" -p 0` | Create priority-0 task |
| `bd show <id>` | View full task details |
| `bd update <id> --status done` | Mark task complete |
| `bd dep add <child> <parent>` | Link task dependencies |
| `bd list` | List all tasks |
| `bd sync` | Export, commit, pull, import, push |

## Agent Protocol

### DO NOT

- **Never use `bd edit`** - opens interactive editors agents cannot use
- **Never use browser/playwright for GitHub** - use `gh` CLI instead
- **Never stop before pushing** - unpushed work causes rebase conflicts

### ALWAYS

- **Use `bd update` with flags** for title, description, design, notes, acceptance criteria
- **Include issue IDs in commit messages**: `"Fix auth bug (bd-abc)"`
- **Run `bd sync` after making issue changes**
- **Push to remote** - the task isn't done until `git push` succeeds

### Commit Message Format

```
Implement feature X (bd-abc)
```

This enables `bd doctor` to detect orphaned issues (work committed but not closed).

## Landing the Plane Protocol

When a user says "land the plane", you MUST complete ALL steps including pushing to remote. The plane hasn't landed until `git push` succeeds.

### Complete Checklist

1. **File remaining work** as beads issues for future sessions
2. **Run quality gates** (linting, tests) if code was changed
3. **Update issue statuses** - mark completed tasks as done
4. **Execute push sequence**:
   ```bash
   git pull --rebase
   # Resolve .beads/issues.jsonl conflicts if present
   bd sync
   git push                    # MANDATORY - don't stop here
   git status                  # Verify "up to date with origin"
   ```
5. **Clean git state**: `git stash clear` and `git remote prune origin`
6. **Verify** everything is committed and pushed
7. **Suggest next issue** with context for continuation

## Multi-Agent Coordination

### Why Beads Works for Multiple Agents

- **Hash-based IDs** prevent merge collisions between agents
- **JSONL format** makes conflicts rare and resolvable
- **Auto-sync daemon** can commit/push every 5 seconds
- **30-second debounce** batches multiple changes into single commits

### Resolving Merge Conflicts

If conflicts occur in `.beads/issues.jsonl`:

```bash
git checkout --theirs .beads/issues.jsonl
bd import
bd sync
```

### Git Hooks (Strongly Recommended)

Run `bd hooks install` once per workspace:

| Hook | Purpose |
|------|---------|
| pre-commit | Flushes pending changes immediately |
| post-merge | Imports updated JSONL after pulls |
| pre-push | Exports database before pushing |
| post-checkout | Imports JSONL after branch switching |

## Task Hierarchy for hcpx

Suggested task structure for this project:

```
bd-xxxx (Epic: Phase 1 - Catalog + Task Discovery)
├── bd-xxxx.1 (Task: Implement path parser)
│   ├── bd-xxxx.1.1 (Subtask: Parse task names)
│   ├── bd-xxxx.1.2 (Subtask: Parse directions)
│   └── bd-xxxx.1.3 (Subtask: Unit tests)
├── bd-xxxx.2 (Task: Implement task dictionary)
└── bd-xxxx.3 (Task: Implement bundles registry)
```

## Session Handoff

When ending a session, ensure the next agent can continue seamlessly:

1. All work is committed and pushed
2. Open tasks have clear descriptions and acceptance criteria
3. `bd ready` shows actionable next steps
4. Any blockers are documented in task notes

## Reference

- Beads repo: https://github.com/steveyegge/beads
- Agent instructions: https://github.com/steveyegge/beads/blob/main/AGENT_INSTRUCTIONS.md
