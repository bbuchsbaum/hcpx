# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

hcpx (HCP eXplorer) is an R package that turns HCP (Human Connectome Project) data into a local, queryable "asset catalog" with task/run awareness, plan-first downloads, reproducible manifests, and policy-aware metadata.

**This is not "a downloader". It's a dataset interface.**

## Key Documentation

- `PRD.md` - Complete architecture, data model, API signatures, and implementation phases
- `TASKS.md` - Dependency graph and parallel execution opportunities
- `AGENTS.md` - Multi-agent coordination protocol with beads

## Development Commands

```bash
# R package development
devtools::load_all()           # Load package for interactive development
devtools::test()               # Run all tests
devtools::test_active_file()   # Run tests for current file
devtools::check()              # Full R CMD check (run before committing)
devtools::document()           # Generate documentation from roxygen2

# Task management (beads)
bd ready                       # Show unblocked tasks
bd show <id>                   # View task details
bd update <id> --status done   # Mark complete
bd list                        # List all tasks
```

## Architecture

### Core Data Flow

```
hcpx_ya() → subjects() → tasks() → assets() → plan_download() → download()
     │           │           │          │              │             │
     └───────────┴───────────┴──────────┴──────────────┘             │
                    All return hcpx_tbl with attached context        │
                                                                     ↓
                                              load_asset() / load_run() / load_task()
                                                                     │
                                                                     ↓
                                                        neuroim2 objects (NeuroVec, etc.)
```

### Key Principles

1. **Lazy until materialize** - Nothing downloads until `download()` is called
2. **Table-first** - Everything is queryable via dplyr/dbplyr on DuckDB
3. **Context propagation** - `hcpx_tbl` objects carry their `hcpx` handle as `attr(x, "hcpx")`
4. **Everything is a query** - Tasks are grouped assets, bundles are filter rules, plans are asset selections

### S3 Object Model

- `hcpx` - Main handle (release, engine, cache, backend, connection, policy)
- `hcpx_tbl` - dbplyr tbl with `attr(x, "hcpx")` and `attr(x, "kind")`
- `hcpx_plan` - Frozen manifest (asset_id, remote_path, local_path, metadata)

### Backend Interface (all backends implement)

```r
backend_list(prefix)                    # → tibble(remote_path, size_bytes?, last_modified?)
backend_head(remote_path)               # → tibble(size_bytes, etag?, last_modified?)
backend_presign(remote_path, expires)   # → url
backend_download(url, dest, resume)     # → tibble(status, bytes)
```

### Database Tables

Primary: `assets` (the heart - every downloadable file is a row), `subjects`, `task_runs`
Support: `bundles`, `ledger` (cache tracking), `derived_ledger`, `dictionary`, `cohorts`

### neuroim2 Data Loading Layer

When users want to *work with* neuroimaging data (not just paths), hcpx returns neuroim2 objects:

| File Type | neuroim2 Class |
|-----------|----------------|
| `.dtseries.nii` | `DenseNeuroVec` or `SparseNeuroVec` |
| `.dscalar.nii` | `DenseNeuroVec` |
| `.dlabel.nii` | `ClusteredNeuroVol` |
| `.nii.gz` (3D) | `DenseNeuroVol` |
| `.nii.gz` (4D) | `DenseNeuroVec` |

**Key functions:**
- `load_asset(h, asset_id, mode)` - Load single asset
- `load_run(h, run_id, bundle)` - Load complete task run (dtseries + confounds + EVs)
- `load_task(x, bundle, concat)` - Load multiple runs
- `parcellate(vec, parcellation)` - Project to parcel space → `ClusteredNeuroVec`
- `extract_series(vec, coords)` - MNI coordinates → time series
- `zscore(vec, run_boundaries)` - Per-run aware normalization

**Loading modes:** `"normal"` (in-memory), `"mmap"` (memory-mapped), `"filebacked"`, `"sparse"`

See PRD Section 15 for full specification.

## Critical Implementation Notes

- **Path parser is foundational** - Must heavily unit-test `utils_paths.R`; derives task, direction, run, file_type, space from HCP paths
- **Never ship restricted data** - Use synthetic/demo data in tests and seeds
- **Canonical task names** - WM, MOTOR, GAMBLING, LANGUAGE, SOCIAL, RELATIONAL, EMOTION (task dictionary resolves synonyms)
- **Default bundles** - tfmri_cifti_min, tfmri_cifti_full, rfmri_cifti_min, evs_only, qc_min

## Beads Task Management

```bash
# Critical rules
# - Never use `bd edit` (opens interactive editor)
# - Include issue IDs in commits: "Implement subjects() (hcpx-abc)"
# - Run `bd sync` after changes
```

### Landing the Plane

When told "land the plane": file remaining work as issues → run tests → update statuses → `git pull --rebase && bd sync && git push` → verify with `git status`
