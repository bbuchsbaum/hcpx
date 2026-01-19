# neuroim2_load.R - Data loading layer using neuroim2
#
# This module provides functions to load downloaded HCP assets as neuroim2
# objects (NeuroVol, NeuroVec, ClusteredNeuroVec, etc.) instead of just paths.
#
# See PRD Section 15 for full specification.

#' Load a single asset as a neuroim2 object
#'
#' Loads a downloaded HCP asset and returns the appropriate neuroim2 object
#' based on file type. Requires the asset to be downloaded first (present in
#' the cache ledger).
#'
#' @param h hcpx handle
#' @param asset_id Asset identifier (from assets table)
#' @param mode Loading mode: "normal" (in-memory), "mmap" (memory-mapped),
#'   "filebacked" (disk-backed), or "sparse" (brain-masked only).
#'   Default is "normal", auto-upgraded to "mmap" for files > 2GB.
#'
#' @return neuroim2 object based on file type:
#'   - CIFTI dtseries/dscalar -> DenseNeuroVec or SparseNeuroVec
#'   - CIFTI dlabel -> ClusteredNeuroVol
#'   - NIfTI 3D -> DenseNeuroVol
#'   - NIfTI 4D -> DenseNeuroVec
#'   - Confounds (.tsv) -> data.frame
#'   - EVs (.txt) -> data.frame
#'
#' @seealso [load_run()], [load_task()], [download()]
#' @export
#' @examples
#' \dontrun{
#' h <- hcpx_ya()
#' vec <- load_asset(h, "asset_id_here")
#' }
load_asset <- function(h, asset_id, mode = c("normal", "mmap", "filebacked", "sparse")) {
  h <- get_hcpx(h)
  mode <- match.arg(mode)

  # 1. Look up asset in catalog to get metadata
  con <- get_con(h)
  asset_info <- DBI::dbGetQuery(con,
    "SELECT asset_id, file_type, size_bytes FROM assets WHERE asset_id = ?",
    params = list(asset_id)
  )

  if (nrow(asset_info) == 0) {
    cli::cli_abort(c(
      "Asset not found in catalog",
      "x" = "asset_id: {.val {asset_id}}"
    ))
  }

  # 2. Look up local_path from ledger
  cached <- ledger_lookup(h, asset_id)

  if (nrow(cached) == 0) {
    cli::cli_abort(c(
      "Asset not downloaded",
      "i" = "Use {.code download(plan)} to download assets first",
      "x" = "asset_id: {.val {asset_id}}"
    ))
  }

  local_path <- cached$local_path[1]

  # 3. Check file exists

  if (!file.exists(local_path)) {
    cli::cli_abort(c(
      "Cached file not found on disk",
      "x" = "Expected at: {.path {local_path}}",
      "i" = "The cache may be corrupted. Try re-downloading."
    ))
  }

  # 4. Auto-upgrade mode for large files
  file_size <- file.info(local_path)$size
  if (mode == "normal" && !is.na(file_size) && file_size > .LARGE_FILE_THRESHOLD_BYTES) {
    cli::cli_alert_info("File > 2GB, auto-upgrading to mmap mode")
    mode <- "mmap"
  }

  # 5. Detect file type and dispatch to appropriate loader
  loader_info <- .detect_loader(local_path)

  # Touch ledger to update last_accessed
  ledger_touch(h, asset_id)

  # Dispatch based on loader type
  switch(loader_info$loader,
    "read_vec" = .load_neurovec(local_path, mode),
    "read_vol_clustered" = .load_clustered_vol(local_path),
    "read_auto" = .load_nifti_auto(local_path, mode),
    "read_tsv" = .read_confounds(local_path),
    "read_ev" = .read_ev(local_path),
    cli::cli_abort("Unknown file type: {.val {loader_info$type}}")
  )
}


#' Load all assets for a task run as a named list
#'
#' Loads a complete task run including the time series data, confounds,
#' and event timing files.
#'
#' @param h hcpx handle
#' @param run_id Run identifier (from task_runs table)
#' @param bundle Which bundle to load (determines file set). Default is
#'   "tfmri_cifti_full" which includes dtseries, confounds, and EVs.
#' @param mode Loading mode for neuroimaging files. See [load_asset()].
#'
#' @return Named list with components:
#'   - `$dtseries`: NeuroVec (the CIFTI time series)
#'   - `$confounds`: data.frame (motion parameters, etc.)
#'   - `$evs`: list of data.frames (one per condition)
#'
#' @seealso [load_asset()], [load_task()], [tasks()]
#' @export
#' @examples
#' \dontrun{
#' h <- hcpx_ya()
#' run <- load_run(h, "100307_WM_LR_1")
#' }
load_run <- function(h, run_id, bundle = "tfmri_cifti_full", mode = "normal") {
  h <- get_hcpx(h)
  con <- get_con(h)

  # 1. Parse run_id to get components
  # run_id format: subject_id_task_direction_run (e.g., "100307_WM_LR_1")
  run_parts <- strsplit(run_id, "_")[[1]]
  if (length(run_parts) < 4) {
    cli::cli_abort(c(
      "Invalid run_id format",
      "x" = "Expected: subject_task_direction_run",
      "i" = "Got: {.val {run_id}}"
    ))
  }

  subject_id <- run_parts[1]
  task <- run_parts[2]
  direction <- run_parts[3]
  run_num <- as.integer(run_parts[4])

  # 2. Query assets for this run with the specified bundle
  run_assets <- dplyr::tbl(con, "assets") |>
    dplyr::filter(
      subject_id == !!subject_id,
      task == !!task,
      direction == !!direction,
      run == !!run_num | is.na(run)  # EVs may not have run number
    )

  # Apply bundle filter
  bundle_def <- get_bundle(bundle, h)
  run_assets <- apply_bundle_filter(run_assets, bundle_def)
  run_assets <- dplyr::collect(run_assets)

  if (nrow(run_assets) == 0) {
    cli::cli_abort(c(
      "No assets found for run",
      "x" = "run_id: {.val {run_id}}",
      "x" = "bundle: {.val {bundle}}"
    ))
  }

  # 3. Check all assets are downloaded
  cached <- ledger_lookup(h, run_assets$asset_id)
  missing <- run_assets$asset_id[!run_assets$asset_id %in% cached$asset_id]

  if (length(missing) > 0) {
    cli::cli_abort(c(
      "Some assets not downloaded",
      "i" = "Missing {length(missing)} of {nrow(run_assets)} assets",
      "i" = "Use {.code plan_download() |> download()} first"
    ))
  }

  # 4. Load each asset type
  result <- list()

  # Load dtseries (main time series)
  dtseries_assets <- run_assets[run_assets$file_type == "dtseries", ]
  if (nrow(dtseries_assets) > 0) {
    result$dtseries <- load_asset(h, dtseries_assets$asset_id[1], mode = mode)
  }

  # Load confounds
  confounds_assets <- run_assets[run_assets$file_type == "confounds", ]
  if (nrow(confounds_assets) > 0) {
    result$confounds <- load_asset(h, confounds_assets$asset_id[1])
  }

  # Load EVs (event timing files)
  ev_assets <- run_assets[run_assets$file_type == "ev", ]
  if (nrow(ev_assets) > 0) {
    result$evs <- lapply(ev_assets$asset_id, function(aid) {
      load_asset(h, aid)
    })
    # Name EVs by their condition (extracted from filename)
    ev_names <- gsub(".*_([A-Za-z0-9]+)\\.txt$", "\\1",
                     basename(ev_assets$remote_path))
    names(result$evs) <- ev_names
  }

  result
}


#' Load assets for multiple runs, optionally concatenated
#'
#' Loads data for multiple task runs from a tasks table, with optional
#' concatenation of time series across runs.
#'
#' @param x tasks table (from [tasks()])
#' @param bundle Bundle name determining which files to load
#' @param concat If TRUE, concatenate time series across runs. Returns
#'   a NeuroVecSeq or single concatenated NeuroVec.
#' @param mode Loading mode for neuroimaging files. See [load_asset()].
#'
#' @return If concat=FALSE, list of run data (each element from [load_run()]).
#'   If concat=TRUE, single list with concatenated $dtseries and combined
#'   $confounds and $evs.
#'
#' @seealso [load_run()], [load_asset()], [tasks()], [concat_vecs()]
#' @export
#' @examples
#' \dontrun{
#' h <- hcpx_ya()
#' runs <- tasks(h, task == "WM") |> load_task()
#' }
load_task <- function(x, bundle = "tfmri_cifti_min", concat = FALSE, mode = "normal") {
  # 1. Get hcpx handle
  h <- get_hcpx(x)

  # 2. Extract run_ids from tasks table
  if (!inherits(x, "hcpx_tbl") || attr(x, "kind") != "tasks") {
    cli::cli_abort(c(
      "Expected tasks table",
      "i" = "Use {.code tasks(h, ...)} to create a tasks selection"
    ))
  }

  tasks_df <- dplyr::collect(x)

  if (nrow(tasks_df) == 0) {
    cli::cli_abort("No task runs selected")
  }

  # Build run_ids
  run_ids <- paste(tasks_df$subject_id, tasks_df$task,
                   tasks_df$direction, tasks_df$run, sep = "_")

  # 3. Load each run
  run_data <- lapply(run_ids, function(rid) {
    tryCatch(
      load_run(h, rid, bundle = bundle, mode = mode),
      error = function(e) {
        cli::cli_warn("Failed to load run {.val {rid}}: {e$message}")
        NULL
      }
    )
  })
  names(run_data) <- run_ids

  # Remove failed loads
  run_data <- run_data[!vapply(run_data, is.null, logical(1))]

  if (length(run_data) == 0) {
    cli::cli_abort("No runs could be loaded")
  }

  # 4. Optionally concatenate
  if (concat && length(run_data) > 1) {
    # Extract dtseries from each run
    dtseries_list <- lapply(run_data, `[[`, "dtseries")
    dtseries_list <- dtseries_list[!vapply(dtseries_list, is.null, logical(1))]

    if (length(dtseries_list) > 0) {
      # Record run boundaries for later per-run normalization
      run_boundaries <- vapply(dtseries_list, function(v) {
        if (is.null(v)) 0L else dim(v)[4]
      }, integer(1))

      # Concatenate time series
      combined_dtseries <- do.call(concat_vecs, dtseries_list)

      # Combine confounds
      confounds_list <- lapply(run_data, `[[`, "confounds")
      confounds_list <- confounds_list[!vapply(confounds_list, is.null, logical(1))]
      combined_confounds <- if (length(confounds_list) > 0) {
        do.call(rbind, confounds_list)
      } else {
        NULL
      }

      # Combine EVs (adjust timing for concatenation)
      evs_list <- lapply(run_data, `[[`, "evs")
      combined_evs <- .combine_evs(evs_list, run_boundaries)

      return(list(
        dtseries = combined_dtseries,
        confounds = combined_confounds,
        evs = combined_evs,
        run_boundaries = run_boundaries
      ))
    }
  }

  run_data
}


#' Combine EVs from multiple runs, adjusting onset times
#' @noRd
.combine_evs <- function(evs_list, run_boundaries) {
  if (length(evs_list) == 0) return(NULL)

  # Get all unique condition names
  all_conditions <- unique(unlist(lapply(evs_list, names)))

  # Calculate cumulative time offsets
  time_offsets <- c(0, cumsum(run_boundaries[-length(run_boundaries)]))

  result <- list()
  for (cond in all_conditions) {
    cond_dfs <- list()
    for (i in seq_along(evs_list)) {
      if (!is.null(evs_list[[i]]) && cond %in% names(evs_list[[i]])) {
        df <- evs_list[[i]][[cond]]
        if (nrow(df) > 0 && "onset" %in% names(df)) {
          df$onset <- df$onset + time_offsets[i]
          df$run <- i
        }
        cond_dfs[[i]] <- df
      }
    }
    if (length(cond_dfs) > 0) {
      result[[cond]] <- do.call(rbind, cond_dfs)
    }
  }

  result
}


# Internal helpers ---------------------------------------------------------

#' Load a NeuroVec (4D time series) using neuroim2
#' @noRd
.load_neurovec <- function(path, mode) {
  # Map mode to neuroim2 parameters
  read_args <- list(file_name = path)

  if (mode == "mmap") {
    read_args$mmap <- TRUE
  } else if (mode == "filebacked") {
    read_args$mmap <- TRUE
  } else if (mode == "sparse") {
    # Sparse mode loads only brain voxels
    read_args$mask <- TRUE
  }

  do.call(neuroim2::read_vec, read_args)
}


#' Load a ClusteredNeuroVol (dlabel parcellation)
#' @noRd
.load_clustered_vol <- function(path) {
  # dlabel files are loaded as clustered volumes
  neuroim2::read_vol(path)
}


#' Auto-detect NIfTI dimensionality and load appropriately
#' @noRd
.load_nifti_auto <- function(path, mode) {
  # Peek at header to determine dimensionality
  hdr <- neuroim2::read_header(path)
  ndim <- length(hdr@dims[hdr@dims > 1])

  if (ndim <= 3) {
    # 3D volume
    neuroim2::read_vol(path)
  } else {
    # 4D time series
    .load_neurovec(path, mode)
  }
}


#' Detect file type from extension and determine loader
#' @noRd
.detect_loader <- function(file_path) {
  ext <- tolower(tools::file_ext(file_path))

  # Handle double extensions like .dtseries.nii

  basename_lower <- tolower(basename(file_path))

  if (grepl("\\.dtseries\\.nii(\\.gz)?$", basename_lower)) {
    return(list(type = "cifti_dtseries", loader = "read_vec"))
  }

  if (grepl("\\.dscalar\\.nii(\\.gz)?$", basename_lower)) {
    return(list(type = "cifti_dscalar", loader = "read_vec"))
  }
  if (grepl("\\.dlabel\\.nii(\\.gz)?$", basename_lower)) {
    return(list(type = "cifti_dlabel", loader = "read_vol_clustered"))
  }

  if (ext %in% c("nii", "gz")) {
    # Need to check dimensionality
    return(list(type = "nifti", loader = "read_auto"))
  }

  if (ext == "tsv") {
    return(list(type = "confounds", loader = "read_tsv"))
  }
  if (ext == "txt") {
    return(list(type = "ev", loader = "read_ev"))
  }

  list(type = "unknown", loader = NULL)
}


#' Read confounds TSV file
#' @noRd
.read_confounds <- function(path) {
  utils::read.delim(path, sep = "\t", header = TRUE, stringsAsFactors = FALSE)
}


#' Read EV timing file (FSL 3-column format)
#' @noRd
.read_ev <- function(path) {
  df <- utils::read.table(path, header = FALSE, stringsAsFactors = FALSE)
  if (ncol(df) >= 3) {
    names(df)[1:3] <- c("onset", "duration", "weight")
  }
  df
}
