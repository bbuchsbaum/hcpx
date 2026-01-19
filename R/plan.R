# Download planning and manifest I/O

#' Create a download plan from an assets table
#'
#' Freezes a selection of assets into a portable download plan (manifest).
#' The plan tracks what files are needed, which are already cached, and
#' provides size estimates.
#'
#' @param x assets table, tasks table, or subjects table
#' @param name Optional name for the plan (for labeling)
#' @param dry_run If TRUE (default), skip HEAD requests for unknown sizes
#' @return An object of class `hcpx_plan`
#' @seealso [download()], [materialize()], [assets()], [write_manifest()], [read_manifest()]
#' @export
#' @examples
#' \dontrun{
#' h <- hcpx_ya()
#'
#' # Create plan from assets
#' plan <- subjects(h, gender == "F") |>
#'   tasks("WM") |>
#'   assets(bundle = "tfmri_cifti_min") |>
#'   plan_download(name = "wm_female")
#'
#' # View the plan
#' plan
#'
#' # Download it
#' download(plan)
#' }
plan_download <- function(x, name = NULL, dry_run = TRUE) {
  h <- get_hcpx(x)

  # Convert to assets if needed
  if (inherits(x, "hcpx_tbl")) {
    kind <- attr(x, "kind")
    if (kind == "subjects") {
      x <- assets(x)
    } else if (kind == "tasks") {
      x <- assets(x)
    }
  }

  # Collect the assets
  assets_df <- dplyr::select(x,
    asset_id, release, subject_id, remote_path, size_bytes,
    kind, task, direction, run, file_type
  ) |> dplyr::collect()

  if (nrow(assets_df) == 0) {
    cli::cli_warn("No assets selected for download plan")
  }

  # Compute local paths
  assets_df$local_path <- vapply(seq_len(nrow(assets_df)), function(i) {
    cache_path(h, assets_df$release[i], assets_df$subject_id[i],
               basename(assets_df$remote_path[i]))
  }, character(1))

  # Check which assets are already cached
  cached <- ledger_lookup(h, assets_df$asset_id)
  assets_df$cached <- assets_df$asset_id %in% cached$asset_id

  # Build manifest
  manifest <- tibble::tibble(
    asset_id = assets_df$asset_id,
    release = assets_df$release,
    subject_id = assets_df$subject_id,
    remote_path = assets_df$remote_path,
    local_path = assets_df$local_path,
    size_bytes = assets_df$size_bytes,
    cached = assets_df$cached,
    kind = assets_df$kind,
    task = assets_df$task,
    direction = assets_df$direction,
    run = assets_df$run,
    file_type = assets_df$file_type
  )

  # Compute summary statistics
  n_total <- nrow(manifest)
  n_cached <- sum(manifest$cached)
  n_to_download <- n_total - n_cached

  total_bytes <- sum(manifest$size_bytes, na.rm = TRUE)
  cached_bytes <- sum(manifest$size_bytes[manifest$cached], na.rm = TRUE)
  download_bytes <- total_bytes - cached_bytes

  n_subjects <- length(unique(manifest$subject_id))
  n_tasks <- length(unique(manifest$task[!is.na(manifest$task)]))

  summary <- list(
    n_total = n_total,
    n_cached = n_cached,
    n_to_download = n_to_download,
    cache_hit_rate = if (n_total > 0) n_cached / n_total else 0,
    total_bytes = total_bytes,
    cached_bytes = cached_bytes,
    download_bytes = download_bytes,
    n_subjects = n_subjects,
    n_tasks = n_tasks
  )

  # Create plan object
  plan <- structure(
    list(
      manifest = manifest,
      summary = summary,
      name = name,
      created_at = Sys.time(),
      release = h$release,
      backend_type = h$backend$type,
      dry_run = dry_run
    ),
    class = "hcpx_plan"
  )

  # Attach hcpx context for download()
  attr(plan, "hcpx") <- h

  plan
}

#' Write a plan manifest to disk (JSON)
#'
#' Exports a download plan to a portable JSON manifest that can be shared
#' and re-imported later.
#'
#' @param plan hcpx_plan object
#' @param path File path for output (should end in .json)
#' @return Invisible path
#' @export
#' @examples
#' \dontrun{
#' plan <- assets(h) |> plan_download()
#' write_manifest(plan, "my_plan.json")
#' }
write_manifest <- function(plan, path) {
  if (!inherits(plan, "hcpx_plan")) {
    stop("Expected hcpx_plan object", call. = FALSE)
  }

  # Create export-safe version (no hcpx handle, no local paths)
  export <- list(
    manifest = plan$manifest[, c("asset_id", "release", "subject_id",
                                  "remote_path", "size_bytes", "kind",
                                  "task", "direction", "run", "file_type")],
    summary = plan$summary,
    name = plan$name,
    created_at = format(plan$created_at, "%Y-%m-%dT%H:%M:%S"),
    release = plan$release,
    hcpx_version = as.character(utils::packageVersion("hcpx"))
  )

  # Write as JSON
  jsonlite::write_json(export, path, pretty = TRUE, auto_unbox = TRUE)

  cli::cli_alert_success("Manifest written to {.path {path}}")
  invisible(path)
}

#' Read a plan manifest from disk
#'
#' Imports a manifest file and creates an hcpx_plan object. Note that the
#' plan will need an hcpx handle attached before downloading.
#'
#' @param path File path to manifest JSON
#' @param h Optional hcpx handle to attach
#' @return hcpx_plan object
#' @export
#' @examples
#' \dontrun{
#' # Read manifest
#' plan <- read_manifest("my_plan.json")
#'
#' # Attach handle and download
#' h <- hcpx_ya()
#' attr(plan, "hcpx") <- h
#' download(plan)
#' }
read_manifest <- function(path, h = NULL) {
  if (!file.exists(path)) {
    stop("Manifest file not found: ", path, call. = FALSE)
  }

  # Read JSON
  data <- jsonlite::read_json(path, simplifyVector = TRUE)

  # Convert manifest to tibble
  manifest <- tibble::as_tibble(data$manifest)

  # Add local_path and cached columns (will be populated when handle attached)
  manifest$local_path <- NA_character_
  manifest$cached <- FALSE

  # Rebuild summary
  summary <- data$summary

  # Create plan object
  plan <- structure(
    list(
      manifest = manifest,
      summary = summary,
      name = data$name,
      created_at = as.POSIXct(data$created_at, format = "%Y-%m-%dT%H:%M:%S"),
      release = data$release,
      backend_type = NA_character_,
      dry_run = TRUE,
      from_manifest = TRUE
    ),
    class = "hcpx_plan"
  )

  # Attach handle if provided
  if (!is.null(h)) {
    plan <- plan_attach_handle(plan, h)
  }

  plan
}

#' Attach an hcpx handle to a plan
#'
#' Updates local paths and cache status based on the handle.
#'
#' @param plan hcpx_plan object
#' @param h hcpx handle
#' @return Updated plan
#' @keywords internal
plan_attach_handle <- function(plan, h) {
  # Update local paths
  manifest <- plan$manifest
  manifest$local_path <- vapply(seq_len(nrow(manifest)), function(i) {
    cache_path(h, manifest$release[i], manifest$subject_id[i],
               basename(manifest$remote_path[i]))
  }, character(1))

  # Check cache status
  cached <- ledger_lookup(h, manifest$asset_id)
  manifest$cached <- manifest$asset_id %in% cached$asset_id

  plan$manifest <- manifest
  plan$backend_type <- h$backend$type

  # Update summary
  n_cached <- sum(manifest$cached)
  plan$summary$n_cached <- n_cached
  plan$summary$n_to_download <- plan$summary$n_total - n_cached
  plan$summary$cache_hit_rate <- if (plan$summary$n_total > 0) n_cached / plan$summary$n_total else 0
  plan$summary$cached_bytes <- sum(manifest$size_bytes[manifest$cached], na.rm = TRUE)
  plan$summary$download_bytes <- plan$summary$total_bytes - plan$summary$cached_bytes

  attr(plan, "hcpx") <- h
  plan
}

#' Print a download plan
#'
#' @param x hcpx_plan object
#' @param ... Additional arguments (ignored)
#' @return Invisible x
#' @rdname plan_download
#' @export
#' @method print hcpx_plan
print.hcpx_plan <- function(x, ...) {
  cli::cli_h1("hcpx Download Plan")

  if (!is.null(x$name)) {
    cli::cli_text("Name: {.val {x$name}}")
  }

  cli::cli_text("")

  # Summary stats
  s <- x$summary
  cli::cli_alert_info("Files: {.val {s$n_total}} ({s$n_subjects} subjects)")

  if (s$n_tasks > 0) {
    cli::cli_bullets(c(" " = "Tasks: {s$n_tasks}"))
  }

  cli::cli_text("")

  # Size info
  total_gb <- sprintf("%.2f", s$total_bytes / 1e9)
  download_gb <- sprintf("%.2f", s$download_bytes / 1e9)
  cache_pct <- sprintf("%.0f", s$cache_hit_rate * 100)

  cli::cli_alert_info("Size: {.val {total_gb}} GB total")
  cli::cli_bullets(c(
    " " = "To download: {.val {download_gb}} GB ({s$n_to_download} files)",
    " " = "Cache hits: {.val {cache_pct}}% ({s$n_cached} files)"
  ))

  cli::cli_text("")

  # Backend info
  if (!is.na(x$backend_type)) {
    cli::cli_alert_info("Backend: {.val {x$backend_type}}")
  }

  cli::cli_text("")
  cli::cli_text("Next: {.code download(plan)} to materialize")

  invisible(x)
}

#' Get files that need downloading
#'
#' @param plan hcpx_plan object
#' @return Tibble of files to download
#' @keywords internal
plan_to_download <- function(plan) {
  plan$manifest[!plan$manifest$cached, ]
}

#' Get files that are already cached
#'
#' @param plan hcpx_plan object
#' @return Tibble of cached files
#' @keywords internal
plan_cached <- function(plan) {
  plan$manifest[plan$manifest$cached, ]
}
