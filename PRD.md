# hcpx (HCP eXplorer) - Product Requirements Document

## Task Management

This project uses **beads** (`bd`) for task tracking - a git-backed graph issue tracker for AI agents. See `AGENTS.md` for full protocol and `CLAUDE.md` for development instructions.

---

## Executive Summary

**One-sentence differentiator:** hcpx turns HCP into a local, queryable "asset catalog" with task/run awareness, plan-first downloads, reproducible manifests, and policy-aware metadata — all lazy until `download()`.

This is not "a downloader". It's a dataset interface.

---

## 1. System Architecture

### 1.1 Components

| Component | Description |
|-----------|-------------|
| **Catalog Engine** | Local DB (DuckDB/SQLite) holding subjects, measures, tasks/runs, assets, dictionary, cohorts, bundle definitions, cache ledger. Queryable with dplyr/dbplyr. Persistent across sessions. |
| **Backend Adapters** | `aws`: S3 via presigned URLs + curl (fast + resumable). `rest`: XNAT/ConnectomeDB file URLs (authenticated). `local`: existing directory mirror. |
| **Cache + Ledger** | Filesystem store + DB ledger mapping remote assets → local paths + checksums + last_access. LRU pruning + pinning + "dry-run plan size". |
| **Query Layer** | `subjects()`, `tasks()`, `assets()` return "lazy tables" (dbplyr) with attached hcpx context. Designed for pipe chaining with dplyr. |
| **Plan + Manifest** | `plan_download()` freezes selection into portable manifest (JSON/YAML). `download(plan)` materializes with cache awareness. |

---

## 2. Package Structure

```
hcpx/
  DESCRIPTION
  NAMESPACE
  R/
    zzz.R
    hcpx.R                 # main constructor, print, options
    catalog_connect.R      # connect/create DB
    catalog_seed.R         # ship small seed tables, load/update
    catalog_build.R        # import behavioral CSV, session summary, etc.
    schema.R               # schema definitions + migrations
    policy.R               # access tier tagging & enforcement
    subjects.R             # subjects(), cohort helpers
    tasks.R                # tasks(), task_dictionary(), parsing rules
    assets.R               # assets(), bundles(), asset typing
    bundles.R              # bundle registry, validation, expansion
    plan.R                 # plan_download(), print plan, manifest IO
    download.R             # download(), resume, concurrency, progress
    cache.R                # cache_status(), prune, pin/unpin
    backends_aws.R         # AWS backend implementation
    backends_rest.R        # REST backend implementation
    backends_local.R       # local backend implementation
    query_dsl.R            # optional query("task:WM file:dtseries ...")
    utils_cli.R            # beautiful printing (cli/pillar)
    utils_paths.R          # path parsing to task/run/space/file_type
    utils_hash.R           # checksum helpers
    errors.R               # structured conditions
    derived.R              # register_recipe(), derive(), materialize_derivation()
  inst/
    extdata/
      bundles.yml          # bundle definitions (human-editable)
      tasks.yml            # task canonical names + synonyms + flags
      schema_version.txt
      demo_subjects.csv    # demo seed data
      demo_assets.csv      # demo seed data
  data/
    dictionary.rda         # variable dictionary (safe to ship)
    task_dictionary.rda    # canonical task map (safe to ship)
    seed_subjects.rda      # optional minimal open subject columns
    seed_scanning_info.rda # optional small snapshot
  vignettes/
    01_quickstart.Rmd
    02_task_workflows.Rmd
    03_restricted_data.Rmd
    04_manifests_reproducibility.Rmd
  tests/
    testthat/...
```

**Key design principle:** Everything complex is an internal table transform. The API stays small; power comes from composition.

---

## 3. Data Model

### 3.1 Assets Table (The Heart)

Everything downloadable is a row in `assets`.

| Column | Type | Description |
|--------|------|-------------|
| `asset_id` | string | Deterministic hash of release + remote_path |
| `release` | string | e.g., "HCP_1200" |
| `subject_id` | string | Subject identifier |
| `session` | string | 3T/7T/MEG/etc |
| `kind` | string | struct, diff, rfmri, tfmri, meg, ... |
| `task` | string (nullable) | Canonical: WM, MOTOR, GAMBLING, etc; NA if not a task |
| `run` | int (nullable) | Run number |
| `direction` | string (nullable) | LR, RL, NA |
| `space` | string | MNINonLinear, T1w, unprocessed, ... |
| `derivative_level` | string | unprocessed, preprocessed, MNINonLinear, ... |
| `file_type` | string | dtseries, nii.gz, ev, confounds, sbref, qc, ... |
| `remote_path` | string | Backend-agnostic locator |
| `size_bytes` | bigint (nullable) | Filled when backend supports HEAD |
| `access_tier` | string | open, tier1, tier2 |

**Why this design wins:** Once assets are normalized, everything else becomes a query:
- Tasks are grouped assets
- Bundles are filtering rules over assets
- Download plans are selected asset sets

### 3.2 Task Runs Table

Derived from assets + completion metadata.

| Column | Type | Description |
|--------|------|-------------|
| `run_id` | string | subject_id + task + direction + run |
| `subject_id` | string | Subject identifier |
| `task` | string | Canonical task name |
| `session` | string | 3T/7T |
| `direction` | string | LR/RL |
| `run` | int | Run number |
| `complete_flag` | bool | From subject completion metadata |
| `runs_present` | int | # runs discovered via assets |
| `expected_runs` | int | From task dictionary |
| `complete` | bool (nullable) | Computed from flag or assets |

### 3.3 Subjects Table

Open, safe-to-ship fields only by default:

| Column | Type |
|--------|------|
| `subject_id` | string (PK) |
| `release` | string |
| `gender` | string |
| `age_range` | string |
| `release_quarter` | string |
| completion flags | (open only) |

**Restricted data is never shipped; it's imported and tagged.**

### 3.4 Dictionary Table (Policy Engine)

| Column | Type |
|--------|------|
| `var_name` | string (PK) |
| `access_tier` | string |
| `category` | string |
| `instrument` | string |
| `description` | string |
| `values_enum` | string (nullable) |

Drives: `describe_variable()` and policy-aware print/export rules.

---

## 4. Storage Engine

**Default:** DuckDB + dbplyr
- Fast queries
- Works with dplyr
- Persists in cache folder (`catalog.duckdb`)
- No need to load huge tables into memory

**Fallback:** SQLite (if duckdb not installed)

---

## 5. Object Model (S3)

### 5.1 hcpx Object (Main Handle)

```r
list(
  release = "HCP_1200",
  engine = "duckdb",
  cache = hcpx_cache(...),
  backend = hcpx_backend_aws(...),
  con = <DBI connection or lazy connector>,
  policy = list(enforce = TRUE, allow_export_restricted = FALSE),
  versions = list(schema = 3, catalog = "2026-01-18")
)
```

**Invariants:**
- `hcpx$con` is valid or lazily reconnectable
- `hcpx$cache$root` exists
- Prints with friendly summary: release, backend, cache size, catalog status

### 5.2 hcpx_tbl (Lazy Query Result with Context)

A dbplyr tbl (or tibble) with attributes:
- `attr(x, "hcpx")` <- `<hcpx>`
- `attr(x, "kind")` <- "subjects"|"tasks"|"assets"

Enables: `download(plan_download(assets_tbl))` without re-passing handle.

### 5.3 hcpx_plan

```r
list(
  manifest = data.frame(asset_id, remote_path, expected_local_path, metadata),
  summary = list(counts, bytes, cache_hits),
  created_at,
  release,
  backend_snapshot  # type only; not credentials
)
```

**Plan is shareable without exposing secrets or restricted columns.**

---

## 6. Backend Interface (Pluggable)

All backends implement:

```r
backend_list(prefix, ...)        # -> tibble(remote_path, size_bytes?, last_modified?)
backend_head(remote_path, ...)   # -> tibble(size_bytes, etag?, last_modified?)
backend_presign(remote_path, expires_sec)  # -> url
backend_download(url_or_path, dest, resume=TRUE, ...)  # -> tibble(status, bytes)
```

### AWS Backend (Fast Lane)
- Uses presigned URLs
- Downloads via curl multi handles for concurrency
- Supports resume (HTTP Range), retries, exponential backoff

### REST Backend
- Uses ConnectomeDB credentials
- Downloads via curl with cookies/session ID
- Resume via Range (if supported)

### Local Backend
- `remote_path` becomes relative path inside user-provided dataset root
- "download" becomes "link/copy" (configurable)

---

## 7. Bundles (Killer UX Feature)

Named "profiles" for common analysis needs.

### 7.1 Bundle Definition Format (`inst/extdata/bundles.yml`)

```yaml
tfmri_cifti_min:
  description: "Task fMRI minimal CIFTI: Atlas dtseries only"
  where:
    kind: "tfmri"
    space: "MNINonLinear"
    file_type: ["dtseries"]

tfmri_cifti_full:
  description: "Task fMRI CIFTI + EVs + motion/confounds"
  where:
    kind: "tfmri"
    space: "MNINonLinear"
    file_type: ["dtseries", "ev", "confounds", "qc"]

evs_only:
  description: "Event timing files only"
  where:
    kind: "tfmri"
    file_type: ["ev"]
```

### 7.2 API Requirement

`assets(bundle="tfmri_cifti_min")` must "just work".

### Default Bundle Set
- `tfmri_cifti_min`
- `tfmri_cifti_full`
- `rfmri_cifti_min`
- `evs_only`
- `qc_min`

---

## 8. Task Ergonomics

### 8.1 Task Dictionary (`inst/extdata/tasks.yml`)

Maps synonyms + completion flags + expected runs:

```yaml
WM:
  synonyms: ["WORKING_MEMORY", "working memory", "tfMRI_WM"]
  completion_flag: "fMRI_WM_Compl"
  expected_runs: 2
  directions: ["LR", "RL"]

MOTOR:
  synonyms: ["MOTOR", "tfMRI_MOTOR"]
  completion_flag: "fMRI_Mot_Compl"
  expected_runs: 2
  directions: ["LR", "RL"]
```

### Default Canonical Task Names
WM, MOTOR, GAMBLING, LANGUAGE, SOCIAL, RELATIONAL, EMOTION

### 8.2 Path Parsing Rules

A single parser derives task, direction, run, file_type, space from remote paths.

**Must be heavily unit-tested because it's foundational.**

---

## 9. Public API Functions

### 9.1 Setup / Entry

```r
#' Create an HCP-YA catalog handle
#' @param release Character. Default "HCP_1200".
#' @param backend One of "aws", "rest", "local".
#' @param cache Path to cache root directory.
#' @param engine One of "duckdb", "sqlite".
#' @param catalog_version Optional. A named snapshot or "latest".
#' @return An object of class `hcpx`.
#' @export
hcpx_ya <- function(release = "HCP_1200",
                    backend = c("aws","rest","local"),
                    cache = hcpx_cache_default(),
                    engine = c("duckdb","sqlite"),
                    catalog_version = "seed") {}

#' Configure cache for an hcpx handle
#' @export
hcpx_cache <- function(h, root, max_size_gb = 200, strategy = c("lru","manual")) {}

#' Authenticate for a backend using secure storage
#' @export
hcpx_auth <- function(h, aws_profile = NULL, keyring_service = "hcpx") {}
```

### 9.2 Catalog Build / Import

```r
#' Build or update the local catalog
#' @param h hcpx handle
#' @param behavioral_csv Path to hcp1200 behavioral CSV (optional)
#' @param session_summary_zip Path to session summary zip (optional)
#' @param restricted_csv Optional restricted measures file (user-provided)
#' @param force Rebuild from scratch
#' @export
catalog_build <- function(h,
                          behavioral_csv = NULL,
                          session_summary_zip = NULL,
                          restricted_csv = NULL,
                          force = FALSE) {}
```

### 9.3 Query Layer

```r
#' Query subjects
#' @param h hcpx handle
#' @param ... dplyr-like filter expressions
#' @return A lazy table with class `hcpx_tbl`
#' @export
subjects <- function(h, ...) {}

#' Query task runs
#' @param x hcpx handle or subjects table
#' @param task Task selector (e.g., "WM", "working memory")
#' @param ... Filters (complete == TRUE, direction == "LR", etc)
#' @export
tasks <- function(x, task = NULL, ...) {}

#' Query assets
#' @param x hcpx handle, subjects table, or tasks table
#' @param bundle Named bundle (recommended)
#' @param ... Additional filters (file_type == "dtseries")
#' @export
assets <- function(x, bundle = NULL, ...) {}

#' Show what is available for a subject or cohort
#' @export
overview <- function(x) {}
```

### 9.4 Bundles and Dictionaries

```r
#' List available bundles
#' @export
bundles <- function(h) {}

#' Describe a behavioral variable and its access tier
#' @export
describe_variable <- function(h, var_name) {}

#' Task dictionary (canonical names + synonyms)
#' @export
task_dictionary <- function(h) {}
```

### 9.5 Plan + Manifest

```r
#' Create a download plan from an assets table
#' @param x assets table or tasks table
#' @param name Optional name for the plan
#' @param dry_run If TRUE, do not compute HEAD sizes
#' @export
plan_download <- function(x, name = NULL, dry_run = TRUE) {}

#' Download a plan (materialize)
#' @param plan hcpx_plan
#' @param parallel Whether to use concurrent downloads
#' @param workers Number of concurrent transfers
#' @param resume Resume partial downloads if possible
#' @export
download <- function(plan, parallel = TRUE, workers = 8, resume = TRUE) {}

#' Write a plan manifest to disk (JSON)
#' @export
write_manifest <- function(plan, path) {}

#' @export
read_manifest <- function(path) {}
```

### 9.6 Cache Management

```r
#' Cache status summary
#' @export
cache_status <- function(h) {}

#' Prune cache by LRU
#' @export
cache_prune <- function(h, max_size_gb = NULL) {}

#' Pin assets so they are not evicted
#' @export
cache_pin <- function(h, asset_ids) {}

#' @export
cache_unpin <- function(h, asset_ids) {}
```

---

## 10. Database Schema (v0.1)

### Tables

#### schema_version
| Column | Type |
|--------|------|
| version | int |
| applied_at | timestamp |

#### subjects
| Column | Type |
|--------|------|
| subject_id | text (PK) |
| release | text |
| gender | text |
| age_range | text |
| release_quarter | text |
| completion flags | (open only) |

#### dictionary
| Column | Type |
|--------|------|
| var_name | text (PK) |
| access_tier | text |
| category | text |
| instrument | text |
| description | text |
| values_enum | text |

#### task_runs
| Column | Type | Indexes |
|--------|------|---------|
| run_id | text (PK) | |
| subject_id | text | (subject_id) |
| task | text | (task), (task, complete) |
| session | text | |
| direction | text | |
| run | int | |
| complete | bool | |
| complete_source | text | flag\|derived |

#### assets
| Column | Type | Indexes |
|--------|------|---------|
| asset_id | text (PK) | |
| release | text | |
| subject_id | text | (subject_id) |
| session | text | |
| kind | text | (kind) |
| task | text (nullable) | (task) |
| direction | text (nullable) | |
| run | int (nullable) | |
| space | text | |
| derivative_level | text | |
| file_type | text | (file_type) |
| remote_path | text | (remote_path) |
| size_bytes | bigint (nullable) | |

#### bundles
| Column | Type |
|--------|------|
| bundle | text (PK) |
| description | text |
| definition_json | text |

#### ledger
| Column | Type |
|--------|------|
| asset_id | text (PK) |
| local_path | text |
| downloaded_at | timestamp |
| last_accessed | timestamp |
| size_bytes | bigint |
| checksum_md5 | text (nullable) |
| etag | text (nullable) |
| backend | text |
| pinned | bool (default false) |

#### derived_ledger
| Column | Type |
|--------|------|
| derived_id | text (PK) |
| recipe | text |
| source_id | text |
| local_path | text |
| created_at | timestamp |
| params_json | text |

#### cohorts (optional)
| Column | Type |
|--------|------|
| cohort_name | text (PK) |
| created_at | timestamp |
| query_sql | text |
| note | text |

---

## 11. Printing Requirements (Non-Negotiable UX)

### Printing `hcpx`:
- Release, backend, engine
- Cache size, pinned count
- Catalog status (built? last updated?)

### Printing `tasks()` result:
- Top tasks by count
- Completion rate
- Hint: `assets(bundle="tfmri_cifti_min") |> plan_download() |> download()`

### Printing `hcpx_plan`:
- Subjects, runs, files
- Total GB (known/unknown)
- Cache hit rate estimate
- Next steps shown as runnable code

**This makes the package feel premium.**

---

## 12. Derived Products Extension Mechanism

First-class support for storing derived data locally (low-rank compression, parcel-space betas, etc.).

### Concept
- Raw assets tracked in `assets`, materialized into cache with `ledger` table
- Derived outputs tracked in `derived_ledger`, stored under:
  ```
  <cache_root>/derived/<recipe>/<derived_id>.<ext>
  ```

### API

```r
register_recipe()    # Define a derivation
derive(x, recipe)    # Create lazy derivation plan
materialize(deriv)   # Execute: ensures inputs, runs compute, registers outputs
derived(h)           # Query derived products like any catalog table
```

### Two Recipe Modes

#### 1. Assets-Based Recipe
Good for "compress each dtseries file":

```r
register_recipe(
  name = "lowrank_dtseries",
  description = "Low-rank compressed representation of each dtseries",
  input = "assets",
  compute = function(h, sources_tbl, params, outputs_manifest) {
    src <- dplyr::collect(sources_tbl)   # includes local_path
    # for each source asset, write derived output at outputs_manifest$local_path[i]
  }
)
```

#### 2. Tasks-Based Recipe
Good for "parcel-space betas per task run":

```r
register_recipe(
  name = "parcel_betas",
  description = "Per task-run beta maps in parcel space",
  input = "tasks",
  requires_bundle = "tfmri_cifti_min",   # ensures dtseries present as inputs
  compute = function(h, sources_tbl, params, outputs_manifest) {
    src <- dplyr::collect(sources_tbl)   # all dtseries needed for all runs
    # group by run_id, compute one output per outputs_manifest row
  }
)
```

### Extension Ecosystem
Labs can ship derivations in separate packages:
- `hcpx.recipes.lowrank`
- `hcpx.recipes.parcelbetas`

Each extension calls `hcpx::register_recipe()` in `.onLoad()`.

---

## 13. Implementation Phases

### Phase 1: Catalog + Task Discovery (WOW Fast)
**No downloads needed**
- DuckDB catalog + `subjects()`, `tasks()`, `assets()`
- Robust path parser + task dictionary
- Bundles registry
- `overview()`

**Deliverable:** People can explore HCP-YA beautifully without downloading anything.

### Phase 2: Plan-First + Cache Ledger + Manifest
- `plan_download()`, `write_manifest()`, `read_manifest()`
- Cache DB ledger + LRU prune + pinning

**Deliverable:** Reproducible dataset slices.

### Phase 3: AWS Backend with Presigned URLs
- Concurrency + retries + resume
- HEAD size collection (optional)

**Deliverable:** Fast and robust "download only what you asked for".

### Phase 4: REST + Local Backends
- REST cookie/session handling
- Local mirror support

---

## 14. Implementation Milestones (Post-Skeleton)

### Milestone A: Real Catalog Build (Highest Leverage)

Implement `catalog_build()` to ingest:
- Scanning info (`hcp_1200_scanning_info`)
- Session summary CSVs
- Behavioral CSV (open)
- Dictionary (open)

**Output:** Real `assets` table with correct task/run/space/file_type fields.

### Milestone B: Path Parser + Task/Run Canon (Critical Foundation)

Write robust parser mapping HCP paths to:
- `kind`, `task`, `direction`, `run`
- `space`, `derivative_level`
- `file_type`

**This enables "task discovery that feels like magic."**

### Milestone C: AWS Backend (Fast Lane)

Implement:
- Presign URLs
- HEAD for sizes
- Parallel + resumable download
- Ledger updates and cache hit reporting

### Milestone D: Derived-Product Integration Polish
- Allow `derived()` outputs to behave like "assets" in plans
- Allow bundles to target derived products (`bundle = "wm_lowrank_rank50"`)
- Optional: content-addressed storage

### Milestone E: neuroim2 Data Loading Layer

Implement the neuroimaging data layer using neuroim2:

**Core Loading Functions:**
- `load_asset()` - Load single asset as neuroim2 object (NeuroVol/NeuroVec)
- `load_run()` - Load complete task run (dtseries + confounds + EVs)
- `load_task()` - Load multiple runs with optional concatenation

**File Type Dispatch:**
- CIFTI (.dtseries.nii, .dscalar.nii) → `neuroim2::read_vec()`
- CIFTI (.dlabel.nii) → `ClusteredNeuroVol` for parcellations
- NIfTI 3D → `neuroim2::read_vol()`
- NIfTI 4D → `neuroim2::read_vec()`
- Confounds (.tsv) → `read.delim()`
- EVs (.txt) → `read.table()`

**Memory Mode Selection:**
- Auto-detect file size and select mode (normal/mmap/filebacked)
- Expose mode parameter for explicit control

**Parcellation Support:**
- `load_parcellation()` - Load CIFTI dlabel atlases
- `parcellate()` - Project NeuroVec to parcel space → ClusteredNeuroVec

**Convenience Wrappers:**
- `extract_series()` - MNI coordinate → time series
- `zscore()` - Per-run aware normalization
- `roi_sphere()` - Spherical ROI creation with hcpx context

**This enables "download → load → analyze" in a single workflow.**

---

## 15. Neuroimaging Data Layer (neuroim2 Integration)

**Critical design principle:** When users want to actually *work with* neuroimaging data (not just download paths), hcpx returns `neuroim2` objects. This provides a rich, analysis-ready data representation with spatial metadata, coordinate transforms, and efficient memory handling.

### 15.1 Why neuroim2?

| Capability | Benefit for hcpx |
|------------|------------------|
| S4 class hierarchy | Type-safe, self-documenting data structures |
| NeuroSpace | Spatial metadata travels with data (no separate tracking) |
| Multiple backends | Dense, sparse, memory-mapped, file-backed for different scales |
| CIFTI support | NIfTI extension handling for HCP's primary format |
| ROI classes | First-class parcellation and region extraction |
| Time series methods | `series()`, `scale_series()` for fMRI analysis |

### 15.2 File Type → neuroim2 Class Mapping

| HCP File Type | Extension | neuroim2 Class | Notes |
|---------------|-----------|----------------|-------|
| CIFTI dtseries | `.dtseries.nii` | `DenseNeuroVec` or `SparseNeuroVec` | 4D grayordinate time series |
| CIFTI dscalar | `.dscalar.nii` | `DenseNeuroVec` | Single or multi-map scalars |
| CIFTI dlabel | `.dlabel.nii` | `ClusteredNeuroVol` | Parcellation/atlas labels |
| NIfTI volume | `.nii.gz` | `DenseNeuroVol` | T1w, T2w structural |
| NIfTI 4D | `.nii.gz` (4D) | `DenseNeuroVec` | Volume-space fMRI |
| Confounds | `.tsv` | `data.frame` | Motion parameters, etc. |
| EVs | `.txt` | `data.frame` | Event timing files |

### 15.3 Data Loading API

```r
#' Load a single asset as a neuroim2 object
#' @param h hcpx handle
#' @param asset_id Asset identifier (from assets table)
#' @param mode Loading mode: "normal", "mmap", "filebacked", "sparse"
#' @return neuroim2 object (NeuroVol, NeuroVec, etc.) or data.frame for non-image files
#' @export
load_asset <- function(h, asset_id, mode = c("normal", "mmap", "filebacked", "sparse")) {}

#' Load all assets for a task run as a named list
#' @param h hcpx handle
#' @param run_id Run identifier (from task_runs table)
#' @param bundle Which bundle to load (determines file set)
#' @param mode Loading mode for neuroimaging files
#' @return Named list: $dtseries (NeuroVec), $confounds (data.frame), $evs (list of data.frames)
#' @export
load_run <- function(h, run_id, bundle = "tfmri_cifti_full", mode = "normal") {}

#' Load assets for multiple runs, optionally concatenated
#' @param x tasks table (from tasks())
#' @param bundle Bundle name
#' @param concat If TRUE, concatenate time series across runs
#' @param mode Loading mode
#' @return List of run data, or single concatenated NeuroVec if concat=TRUE
#' @export
load_task <- function(x, bundle = "tfmri_cifti_min", concat = FALSE, mode = "normal") {}
```

### 15.4 Loading Modes (Memory Strategy)

| Mode | neuroim2 Class | Use Case | Memory |
|------|----------------|----------|--------|
| `"normal"` | `DenseNeuroVec` | Small datasets, frequent access | Full in RAM |
| `"mmap"` | `MappedNeuroVec` | Large files, sequential access | Minimal (pages on demand) |
| `"filebacked"` | `FileBackedNeuroVec` | Very large, sparse access | On-disk with caching |
| `"sparse"` | `SparseNeuroVec` | Brain-masked data | ~2-5% of dense (mask only) |

**Automatic mode selection:** For files > 2GB, default to `"mmap"` unless explicitly overridden.

### 15.5 Spatial Metadata Preservation

All loaded data carries `NeuroSpace` with:
- Dimensions (91×109×91 for MNINonLinear)
- Voxel spacing (2mm isotropic typical)
- Origin coordinates
- Affine transformation matrix
- Axis orientation (RAS/LPI conventions)

```r
# Spatial queries work automatically
vol <- load_asset(h, asset_id)
neuroim2::spacing(vol)     # c(2, 2, 2)
neuroim2::dim(vol)         # c(91, 109, 91, 1200)
neuroim2::origin(vol)      # World coordinates
neuroim2::trans(vol)       # 4x4 affine matrix
```

### 15.6 ROI and Parcellation Support

```r
#' Load a CIFTI dlabel as a ClusteredNeuroVol for parcellation
#' @export
load_parcellation <- function(h, parcellation = "Schaefer400") {}

#' Extract parcel time series from a loaded NeuroVec
#' @param vec NeuroVec object (from load_asset or load_run)
#' @param parcellation ClusteredNeuroVol or parcellation name
#' @return ClusteredNeuroVec with one time series per parcel
#' @export
parcellate <- function(vec, parcellation) {}

#' Create ROI from coordinates (MNI mm or voxel)
#' @export
roi_sphere <- function(h, center, radius_mm = 6, space = "MNINonLinear") {

  # Wraps neuroim2::spherical_roi() with hcpx context
}
```

### 15.7 Integration with Derived Products

The derived product system (Section 12) uses neuroim2 for both inputs and outputs:

#### Input Loading
```r
# Recipe receives local_path, loads via neuroim2
register_recipe(

  name = "lowrank_dtseries",
  compute = function(h, sources_tbl, params, outputs_manifest) {
    for (i in seq_len(nrow(sources_tbl))) {
      # Load source asset as neuroim2 object
      vec <- neuroim2::read_vec(sources_tbl$local_path[i], mode = "mmap")

      # Compute low-rank approximation
      result <- compute_lowrank(vec, rank = params$rank)

      # Write output (still neuroim2)
      neuroim2::write_vec(result, outputs_manifest$local_path[i])
    }
  }
)
```

#### Parcel-Space Derivations
```r
register_recipe(
  name = "parcel_betas",
  input = "tasks",
  requires_bundle = "tfmri_cifti_min",
  compute = function(h, sources_tbl, params, outputs_manifest) {
    # Load parcellation atlas
    atlas <- load_parcellation(h, params$parcellation)

    for (run_id in unique(sources_tbl$run_id)) {
      run_data <- load_run(h, run_id, bundle = "tfmri_cifti_min")

      # Convert to parcel space (ClusteredNeuroVec)
      parcel_ts <- parcellate(run_data$dtseries, atlas)

      # Compute betas per parcel
      betas <- compute_glm_betas(parcel_ts, run_data$evs)

      # Output is a ClusteredNeuroVol (one value per parcel)
      neuroim2::write_vol(betas, outputs_manifest$local_path[match(run_id, ...)])
    }
  }
)
```

### 15.8 Convenience Wrappers

```r
#' Extract time series at coordinates
#' @param vec NeuroVec from load_asset/load_run
#' @param coords Matrix of MNI coordinates (Nx3) or single coordinate vector
#' @return Matrix (timepoints x coordinates) or vector
#' @export
extract_series <- function(vec, coords) {

  # Converts MNI to voxel, calls neuroim2::series()
}

#' Z-score normalize time series (per-run aware)
#' @param vec NeuroVec

#' @param run_boundaries Optional vector of run lengths for split normalization
#' @return Normalized NeuroVec
#' @export
zscore <- function(vec, run_boundaries = NULL) {
  if (is.null(run_boundaries)) {
    neuroim2::scale_series(vec, center = TRUE, scale = TRUE)
  } else {
    run_factor <- rep(seq_along(run_boundaries), run_boundaries)
    neuroim2::split_scale(vec, run_factor, center = TRUE, scale = TRUE)
  }
}
```

### 15.9 Package Structure Addition

```
hcpx/
  R/
    ...
    neuroim2_load.R       # load_asset(), load_run(), load_task()
    neuroim2_parcellate.R # parcellate(), load_parcellation()
    neuroim2_utils.R      # extract_series(), zscore(), roi helpers
```

### 15.10 Dependency Declaration

In `DESCRIPTION`:
```
Imports:
    neuroim2 (>= 0.8.0),
    ...
```

**Note:** neuroim2 is on CRAN and actively maintained. It provides the foundational data structures; hcpx provides the HCP-specific catalog, planning, and workflow layer.

---

## 16. Configuration Defaults

| Setting | Default |
|---------|---------|
| Engine | DuckDB |
| Cache layout | `cache_root/files/<release>/<subject>/...` |
| Derived layout | `cache_root/derived/<recipe>/<derived_id>.<ext>` |
| neuroim2 load mode | `"normal"` (auto-upgrades to `"mmap"` for files > 2GB) |

---

## 17. Example Usage

### 17.1 Basic Workflow (Query → Download → Paths)

```r
library(hcpx)

# Initialize
h <- hcpx_ya(catalog_version = "demo", engine = "sqlite", backend = "local")
overview(h)

# Query and plan
wm_assets <- subjects(h, gender == "F") |>
  tasks("WM") |>
  assets(bundle = "tfmri_cifti_min")

plan <- plan_download(wm_assets, name = "wm_female_dtseries")
plan

# Materialize (returns paths)
paths <- download(plan)
```

### 17.2 Loading Data with neuroim2

```r
# Load a single asset as neuroim2 object
asset_id <- wm_assets |> head(1) |> pull(asset_id)
vec <- load_asset(h, asset_id)
class(vec)  # "DenseNeuroVec"
dim(vec)    # c(91, 109, 91, 405) for typical HCP tfMRI

# Load a complete task run (dtseries + confounds + EVs)
run_id <- tasks(h, "WM") |> head(1) |> pull(run_id)
run_data <- load_run(h, run_id, bundle = "tfmri_cifti_full")
names(run_data)  # c("dtseries", "confounds", "evs")

class(run_data$dtseries)  # "DenseNeuroVec"
class(run_data$confounds) # "data.frame"
class(run_data$evs)       # list of data.frames (one per condition)

# Memory-efficient loading for large datasets
vec_mmap <- load_asset(h, asset_id, mode = "mmap")
class(vec_mmap)  # "MappedNeuroVec"
```

### 17.3 Working with Loaded Data

```r
# Extract time series at MNI coordinates
motor_coord <- c(-38, -22, 56)  # Left motor cortex
ts <- extract_series(run_data$dtseries, motor_coord)
plot(ts, type = "l", xlab = "TR", ylab = "Signal")

# Z-score normalize
vec_z <- zscore(run_data$dtseries)

# Parcellation workflow
atlas <- load_parcellation(h, "Schaefer400")
parcel_ts <- parcellate(run_data$dtseries, atlas)
class(parcel_ts)  # "ClusteredNeuroVec"
dim(parcel_ts)    # c(400, 405) - 400 parcels x 405 timepoints

# Create spherical ROI
dlpfc_roi <- roi_sphere(h, center = c(-46, 10, 30), radius_mm = 8)
roi_ts <- neuroim2::series_roi(run_data$dtseries, dlpfc_roi)
```

### 17.4 Multi-Run Analysis

```r
# Load all WM runs for a subject, concatenated
subj_wm <- subjects(h, subject_id == "100307") |>
  tasks("WM")

wm_data <- load_task(subj_wm, bundle = "tfmri_cifti_min", concat = TRUE)
class(wm_data$dtseries)  # "DenseNeuroVec" or "NeuroVecSeq"
dim(wm_data$dtseries)[4] # 810 timepoints (2 runs × 405)

# Per-run normalization before concatenation
wm_data_norm <- load_task(subj_wm, concat = TRUE) |>
  transform(dtseries = zscore(dtseries, run_boundaries = c(405, 405)))
```

### 17.5 Derived Products

```r
# Create low-rank compression
deriv <- wm_assets |> derive("lowrank_dtseries", params = list(rank = 50))
materialize(deriv)

# Tasks-based derivation (parcel betas)
wm_runs <- subjects(h) |> tasks("WM")
deriv2 <- wm_runs |> derive("parcel_betas", params = list(parcellation = "Schaefer400"))
materialize(deriv2)

# Query and load derived products
derived_assets <- derived(h, recipe == "parcel_betas")
betas <- load_asset(h, derived_assets$derived_id[1])
class(betas)  # "ClusteredNeuroVol"

# Derived products integrate back into workflow
derived(h) |> dplyr::count(recipe)
```

### 17.6 Advanced: Direct neuroim2 Usage

```r
# hcpx returns neuroim2 objects; use neuroim2 directly for analysis
library(neuroim2)

vec <- load_asset(h, asset_id)

# Spatial operations
spacing(vec)          # c(2, 2, 2)
origin(vec)           # World coordinates
trans(vec)            # 4x4 affine matrix

# Coordinate conversion
vox <- coord_to_grid(vec, c(-38, -22, 56))  # MNI → voxel
mni <- grid_to_coord(vec, c(45, 55, 40))    # voxel → MNI

# Direct series extraction
ts_matrix <- series(vec, coords_matrix)  # Multiple coordinates

# Spatial filtering
vec_smooth <- gaussian_blur(vec, sigma = 4)

# Resampling
template_space <- space(load_asset(h, template_id))
vec_resampled <- resample(vec, template_space)
```

---

## Appendix: Skeleton Files Implemented

| File | Purpose |
|------|---------|
| `R/hcpx.R` | Main constructor, print, `hcpx_tbl()` context attachment |
| `R/catalog_connect.R` | DB connection (DuckDB/SQLite) |
| `R/schema.R` | Table creation, `catalog_refresh_task_runs()` |
| `R/catalog_seed.R` | Demo seed data loading |
| `R/subjects.R` | `subjects()` query |
| `R/tasks.R` | `tasks()` query, `task_dictionary()` |
| `R/assets.R` | `assets()` query, `derived()` query |
| `R/bundles.R` | `bundles()`, YAML parsing |
| `R/overview.R` | `overview()` summary printing |
| `R/plan.R` | `plan_download()`, manifest I/O |
| `R/download.R` | `materialize()` dispatch |
| `R/cache.R` | `cache_status()`, `cache_prune()`, pin/unpin |
| `R/derived.R` | `register_recipe()`, `derive()`, `materialize_derivation()` |
| `R/neuroim2_load.R` | `load_asset()`, `load_run()`, `load_task()` |
| `R/neuroim2_parcellate.R` | `parcellate()`, `load_parcellation()` |
| `R/neuroim2_utils.R` | `extract_series()`, `zscore()`, `roi_sphere()` |
