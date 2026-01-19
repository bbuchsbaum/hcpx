# hcpx Task Overview

This document provides a high-level view of all tasks, their dependencies, and parallel execution opportunities.

## Quick Start: Ready Tasks

Run `bd ready` to see tasks with no blockers. Currently ready:

```
bd ready
```

## Task Dependency Graph

### Phase 1: Catalog + Task Discovery (P0)

```
hcpx-b7q: Package Infrastructure Setup
    ├── hcpx-0ft: hcpx_ya() constructor ─────────────────┐
    ├── hcpx-8v4: catalog_connect.R ─────────────────────┼──► hcpx-qfw: subjects()
    ├── hcpx-hic: schema.R ──────────────────────────────┘         │
    │       │                                                       │
    │       ├──► hcpx-h58: bundles registry ──────────────┐        │
    │       │                                              ├──► hcpx-ii2: assets()
    │       └──► hcpx-slj: demo seed data                 │        │
    │                │                                     │        │
    ├── hcpx-ams: path parser ───────────────────────────┘        │
    │       │                                                       │
    │       └──► hcpx-2g3: path parser tests                       │
    │                                                               │
    ├── hcpx-2uh: task dictionary ────────────────────────────► hcpx-qha: tasks()
    │                                                               │
    └── hcpx-4gm: errors.R                                         │
                                                                    │
    ┌───────────────────────────────────────────────────────────────┘
    │
    ├──► hcpx-5to: overview()
    ├──► hcpx-zks: premium print methods
    └──► hcpx-why: query function tests
```

### Phase 2: Plan-First + Cache (P0)

```
hcpx-ii2 (assets) ──► hcpx-csz: plan_download() ──► hcpx-1c0: manifest I/O
                                    │
hcpx-hic + hcpx-8v4 ──► hcpx-z26: cache ledger
                              │
                              ├──► hcpx-5k9: cache_status()
                              ├──► hcpx-2nh: cache_prune()
                              └──► hcpx-5jn: cache_pin/unpin
```

### Phase 3: AWS Backend (P1)

```
hcpx-0ft ──► hcpx-5lg: backend interface ──► hcpx-16x: AWS backend
    │                                               │
    └──► hcpx-wkh: hcpx_auth()                     │
                                                    │
hcpx-csz + hcpx-z26 + hcpx-16x ──────────────► hcpx-71q: download()
```

### Phase 4: REST + Local Backends (P2)

```
hcpx-5lg ──► hcpx-c9h: REST backend
    │               │
    │               └── (also needs hcpx-wkh)
    │
    └──► hcpx-jev: local backend
```

### Derived Products Extension (P1)

```
hcpx-h58 ──► hcpx-rzb: register_recipe()
                  │
hcpx-ii2 + hcpx-qha + hcpx-rzb ──► hcpx-cre: derive()
                                        │
hcpx-71q + hcpx-z26 + hcpx-cre ──► hcpx-bv2: materialize()

hcpx-hic ──► hcpx-mon: derived() query
```

---

## Parallel Execution Opportunities

### Wave 1: Foundation (Can all run in parallel)
These tasks have no dependencies except the infrastructure setup:

| Task ID | Name | Priority |
|---------|------|----------|
| hcpx-b7q | Package Infrastructure Setup | P0 |

### Wave 2: Core Components (After Infrastructure)
These can all run in parallel once infrastructure is done:

| Task ID | Name | Priority |
|---------|------|----------|
| hcpx-0ft | hcpx_ya() constructor | P0 |
| hcpx-8v4 | catalog_connect.R | P0 |
| hcpx-hic | schema.R | P0 |
| hcpx-ams | path parser | P0 |
| hcpx-2uh | task dictionary | P0 |
| hcpx-4gm | errors.R | P0 |

### Wave 3: Query Layer + Supporting (After Wave 2)
Mixed parallelization - see dependencies:

| Task ID | Name | Dependencies | Can Parallel With |
|---------|------|--------------|-------------------|
| hcpx-qfw | subjects() | 0ft, 8v4, hic | bundles, demo seed |
| hcpx-h58 | bundles registry | hic | subjects, demo seed |
| hcpx-slj | demo seed | hic, ams | subjects, bundles |
| hcpx-2g3 | path parser tests | ams | subjects, bundles, demo |
| hcpx-z26 | cache ledger | hic, 8v4 | subjects, bundles |
| hcpx-5lg | backend interface | 0ft | query layer |
| hcpx-wkh | hcpx_auth | 0ft | query layer |

### Wave 4: Advanced Query + Cache (After Wave 3)

| Task ID | Name | Dependencies | Can Parallel With |
|---------|------|--------------|-------------------|
| hcpx-qha | tasks() | qfw, 2uh | assets, cache_* |
| hcpx-ii2 | assets() | qfw, h58 | tasks, cache_* |
| hcpx-5k9 | cache_status() | z26 | query functions |
| hcpx-2nh | cache_prune() | z26 | query functions |
| hcpx-5jn | cache_pin/unpin | z26 | query functions |
| hcpx-16x | AWS backend | 5lg | query functions |

### Wave 5: Integration (After Wave 4)

| Task ID | Name | Dependencies | Can Parallel With |
|---------|------|--------------|-------------------|
| hcpx-csz | plan_download() | ii2 | overview, prints |
| hcpx-5to | overview() | qfw, qha, ii2 | plan_download |
| hcpx-zks | print methods | 0ft, qfw, qha, ii2 | plan_download |
| hcpx-why | query tests | slj, qfw, qha, ii2 | plan_download |
| hcpx-rzb | register_recipe() | h58 | plan_download |

### Wave 6: Download + Derived (After Wave 5)

| Task ID | Name | Dependencies | Can Parallel With |
|---------|------|--------------|-------------------|
| hcpx-71q | download() | 16x, csz, z26 | derive, manifest |
| hcpx-1c0 | manifest I/O | csz | download, derive |
| hcpx-cre | derive() | rzb, ii2, qha | download, manifest |

### Wave 7: Final Integration

| Task ID | Name | Dependencies |
|---------|------|--------------|
| hcpx-bv2 | materialize() | cre, 71q, z26 |
| hcpx-7p2 | quickstart vignette | qfw, qha, ii2, 5to, csz |
| hcpx-c9h | REST backend | 5lg, wkh |
| hcpx-jev | local backend | 5lg |

---

## Task Summary by Priority

### P0 - Critical Path (Must complete for Phase 1 "WOW")

| ID | Task | Status |
|----|------|--------|
| hcpx-b7q | Package Infrastructure Setup | open |
| hcpx-6fe | Phase 1 Epic | open |
| hcpx-0ft | hcpx_ya() constructor | open |
| hcpx-8v4 | catalog_connect.R | open |
| hcpx-hic | schema.R | open |
| hcpx-ams | path parser | open |
| hcpx-2uh | task dictionary | open |
| hcpx-h58 | bundles registry | open |
| hcpx-qfw | subjects() | open |
| hcpx-qha | tasks() | open |
| hcpx-ii2 | assets() | open |
| hcpx-5to | overview() | open |
| hcpx-zks | print methods | open |
| hcpx-slj | demo seed | open |
| hcpx-4gm | errors.R | open |
| hcpx-9ei | Phase 2 Epic | open |
| hcpx-csz | plan_download() | open |
| hcpx-1c0 | manifest I/O | open |
| hcpx-z26 | cache ledger | open |
| hcpx-5k9 | cache_status() | open |
| hcpx-2nh | cache_prune() | open |
| hcpx-5jn | cache_pin/unpin | open |
| hcpx-2g3 | path parser tests | open |
| hcpx-why | query tests | open |

### P1 - Important (Phase 3 + Derived Products)

| ID | Task | Status |
|----|------|--------|
| hcpx-a3p | Phase 3 Epic | open |
| hcpx-5lg | backend interface | open |
| hcpx-16x | AWS backend | open |
| hcpx-71q | download() | open |
| hcpx-wkh | hcpx_auth() | open |
| hcpx-ddz | Derived Products Epic | open |
| hcpx-rzb | register_recipe() | open |
| hcpx-cre | derive() | open |
| hcpx-bv2 | materialize() | open |
| hcpx-mon | derived() query | open |
| hcpx-dz1 | catalog_build() | open |
| hcpx-4hd | policy.R | open |
| hcpx-9o5 | utils_hash.R | open |
| hcpx-7p2 | quickstart vignette | open |

### P2 - Nice to Have (Phase 4)

| ID | Task | Status |
|----|------|--------|
| hcpx-29s | Phase 4 Epic | open |
| hcpx-c9h | REST backend | open |
| hcpx-jev | local backend | open |

---

## Commands Reference

```bash
# See what's ready to work on
bd ready

# View a specific task
bd show hcpx-b7q

# Start working on a task
bd update hcpx-b7q --status in_progress

# Complete a task
bd update hcpx-b7q --status done

# List all tasks
bd list

# See task with dependencies
bd show hcpx-qfw  # Shows what blocks this task
```

---

## Recommended Implementation Order

1. **Start**: `hcpx-b7q` (Package Infrastructure)
2. **Parallel Wave**: `hcpx-0ft`, `hcpx-8v4`, `hcpx-hic`, `hcpx-ams`, `hcpx-2uh`, `hcpx-4gm`
3. **Query Layer**: `hcpx-qfw` → `hcpx-qha` + `hcpx-ii2`
4. **Demo + UX**: `hcpx-slj`, `hcpx-5to`, `hcpx-zks`
5. **Phase 2**: `hcpx-z26`, `hcpx-csz` → cache functions
6. **Phase 3**: Backend interface → AWS → download()
7. **Derived**: Recipe system → derive → materialize
8. **Phase 4**: REST + Local backends
