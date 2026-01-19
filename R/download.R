# Download execution and materialization

#' Download a plan (materialize)
#'
#' Executes a download plan, fetching files that are not already cached.
#' Updates the cache ledger and returns paths to all files.
#'
#' @param plan hcpx_plan object
#' @param parallel Whether to use concurrent downloads (default TRUE)
#' @param workers Number of concurrent transfers (default 8)
#' @param resume Resume partial downloads if possible (default TRUE)
#' @param progress Show progress bar (default TRUE)
#' @return Tibble with columns: asset_id, local_path, status (cached/downloaded/failed)
#' @seealso [plan_download()], [materialize()], [cache_status()], [load_asset()]
#' @export
#' @examples
#' \dontrun{
#' h <- hcpx_ya()
#'
#' # Create and execute plan
#' paths <- subjects(h, gender == "F") |>
#'   tasks("WM") |>
#'   assets(bundle = "tfmri_cifti_min") |>
#'   plan_download() |>
#'   download()
#'
#' # Or use materialize() alias
#' paths <- plan |> materialize()
#' }
download <- function(plan, parallel = TRUE, workers = .DEFAULT_DOWNLOAD_WORKERS, resume = TRUE, progress = TRUE) {
  if (!inherits(plan, "hcpx_plan")) {
    stop("Expected hcpx_plan object. Create one with plan_download().", call. = FALSE)
  }

  h <- attr(plan, "hcpx")
  if (is.null(h)) {
    stop("Plan has no attached hcpx handle. Use read_manifest(path, h) to attach one.", call. = FALSE)
  }

  manifest <- plan$manifest
  n_total <- nrow(manifest)

  if (n_total == 0) {
    cli::cli_alert_info("No files to download")
    return(tibble::tibble(asset_id = character(), local_path = character(), status = character()))
  }

  # Separate cached and to-download
  to_download <- manifest[!manifest$cached, ]
  already_cached <- manifest[manifest$cached, ]

  n_cached <- nrow(already_cached)
  n_to_download <- nrow(to_download)

  cli::cli_alert_info("Plan: {.val {n_total}} files total")
  if (n_cached > 0) {
    cli::cli_bullets(c("v" = "{n_cached} already cached"))
  }
  if (n_to_download > 0) {
    cli::cli_bullets(c("*" = "{n_to_download} to download"))
  }

  # Result tracking
  results <- tibble::tibble(
    asset_id = manifest$asset_id,
    local_path = manifest$local_path,
    status = ifelse(manifest$cached, "cached", "pending")
  )

  if (n_to_download == 0) {
    cli::cli_alert_success("All files already cached!")
    # Touch cached files to update last_accessed
    ledger_touch(h, already_cached$asset_id)
    results$status <- "cached"
    return(results)
  }

  # Download files
  backend <- h$backend

  if (progress) {
    cli::cli_progress_bar("Downloading", total = n_to_download)
  }

  downloaded <- 0
  failed <- 0

  for (i in seq_len(n_to_download)) {
    row <- to_download[i, ]

    # Ensure directory exists
    cache_ensure_dir(row$local_path)

    # Attempt download
    success <- tryCatch({
      download_single(h, row$remote_path, row$local_path, resume = resume)
      TRUE
    }, error = function(e) {
      cli::cli_alert_danger("Failed: {row$remote_path}: {e$message}")
      FALSE
    })

    if (success) {
      # Record in ledger
      ledger_record(h,
        asset_id = row$asset_id,
        local_path = row$local_path,
        size_bytes = row$size_bytes,
        backend = backend$type
      )
      downloaded <- downloaded + 1
      results$status[results$asset_id == row$asset_id] <- "downloaded"
    } else {
      failed <- failed + 1
      results$status[results$asset_id == row$asset_id] <- "failed"
    }

    if (progress) {
      cli::cli_progress_update()
    }
  }

  if (progress) {
    cli::cli_progress_done()
  }

  # Summary
  cli::cli_text("")
  if (downloaded > 0) {
    cli::cli_alert_success("Downloaded {.val {downloaded}} files")
  }
  if (failed > 0) {
    cli::cli_alert_danger("Failed: {.val {failed}} files")
  }

  # Touch cached files
  if (n_cached > 0) {
    ledger_touch(h, already_cached$asset_id)
  }

  results
}

#' Download a single file
#'
#' Internal function to download one file using the appropriate backend.
#'
#' @param h hcpx handle
#' @param remote_path Remote path
#' @param local_path Local destination
#' @param resume Whether to resume partial downloads
#' @return TRUE on success
#' @keywords internal
download_single <- function(h, remote_path, local_path, resume = TRUE) {
  backend <- h$backend

  # For local backend, just copy the file
  if (inherits(backend, "hcpx_backend_local")) {
    src_path <- file.path(backend$root, remote_path)
    if (!file.exists(src_path)) {
      stop("Source file not found: ", src_path)
    }

    # Copy based on link_mode
    if (backend$link_mode == "symlink") {
      file.symlink(src_path, local_path)
    } else if (backend$link_mode == "hardlink") {
      file.link(src_path, local_path)
    } else {
      file.copy(src_path, local_path, overwrite = TRUE)
    }
    return(TRUE)
  }

  # For AWS backend, get presigned URL and download
  if (inherits(backend, "hcpx_backend_aws")) {
    url <- backend_presign(backend, remote_path)
    download_url(url, local_path, resume = resume)
    return(TRUE)
  }

  # For REST backend, download directly with auth
  if (inherits(backend, "hcpx_backend_rest")) {
    backend_download(backend, remote_path, local_path, resume = resume)
    return(TRUE)
  }

  # For BALSA backend, remote_path is expected to be an Aspera source spec
  # (or an https URL) and will be passed through to backend_download().
  if (inherits(backend, "hcpx_backend_balsa")) {
    backend_download(backend, remote_path, local_path, resume = resume)
    return(TRUE)
  }

  stop("Unknown backend type: ", class(backend)[1])
}

#' Download from URL
#'
#' Downloads a file from a URL with optional resume support.
#'
#' @param url URL to download
#' @param dest Destination path
#' @param resume Whether to resume partial downloads
#' @return TRUE on success
#' @keywords internal
download_url <- function(url, dest, resume = TRUE) {
  # Use curl for robust downloads
  curl_handle <- curl::new_handle()

  # Resume support
  if (resume && file.exists(dest)) {
    existing_size <- file.info(dest)$size
    curl::handle_setopt(curl_handle, resume_from = existing_size)
  }

  # Download
  curl::curl_download(url, dest, handle = curl_handle, mode = "wb")

  TRUE
}

#' Materialize a derivation or plan
#'
#' Generic function to execute a plan or derivation.
#'
#' @param x hcpx_plan or hcpx_derivation
#' @param ... Additional arguments passed to methods
#' @return Result of materialization
#' @export
#' @examples
#' \dontrun{
#' h <- hcpx_ya()
#' plan <- assets(h) |> plan_download()
#' materialize(plan)
#' }
materialize <- function(x, ...) {
  UseMethod("materialize")
}

#' Materialize a download plan
#'
#' @param x hcpx_plan object
#' @param ... Additional arguments passed to [download()]
#' @return Result of [download()]
#' @rdname materialize
#' @export
#' @method materialize hcpx_plan
materialize.hcpx_plan <- function(x, ...) {
  download(x, ...)
}

#' @rdname materialize
#' @export
#' @method materialize default
materialize.default <- function(x, ...) {
  cli::cli_abort(c(
    "Cannot materialize object of class {.cls {class(x)[1]}}",
    "i" = "Use {.code materialize()} with an hcpx_plan or hcpx_derivation"
  ))
}

# materialize.hcpx_derivation is implemented in R/derived.R

#' Check download status for a plan
#'
#' Returns summary of which files are downloaded, pending, or failed.
#'
#' @param plan hcpx_plan object
#' @return List with counts
#' @keywords internal
download_status <- function(plan) {
  h <- attr(plan, "hcpx")
  manifest <- plan$manifest

  # Re-check cache status
  cached <- ledger_lookup(h, manifest$asset_id)

  list(
    n_total = nrow(manifest),
    n_cached = sum(manifest$asset_id %in% cached$asset_id),
    n_pending = sum(!manifest$asset_id %in% cached$asset_id)
  )
}
