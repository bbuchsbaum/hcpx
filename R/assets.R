# Asset queries

#' Query assets
#'
#' Returns a lazy table of assets from the catalog. Can be filtered by
#' bundle (recommended) or individual criteria. Supports chaining from
#' subjects or tasks tables.
#'
#' @param x hcpx handle, subjects table, or tasks table
#' @param bundle Named bundle (recommended). See `bundles()` for available options.
#' @param ... Additional filters (e.g., file_type == "dtseries", kind == "tfmri")
#' @return A lazy table with class `hcpx_tbl` and kind "assets"
#' @seealso [subjects()], [tasks()], [bundles()], [plan_download()], [load_asset()]
#' @export
#' @examples
#' \dontrun{
#' h <- hcpx_ya()
#'
#' # All assets
#' assets(h)
#'
#' # Use a bundle (recommended)
#' assets(h, bundle = "tfmri_cifti_min")
#'
#' # Chain from subjects/tasks
#' subjects(h, gender == "F") |>
#'   tasks("WM") |>
#'   assets(bundle = "tfmri_cifti_min")
#'
#' # Custom filters
#' assets(h, kind == "tfmri", file_type == "ev")
#' }
assets <- function(x, bundle = NULL, ...) {
  h <- get_hcpx(x)
  con <- get_con(h)

  # Get base assets table
  tbl <- dplyr::tbl(con, "assets")

  # Determine input type and apply appropriate filters
  if (inherits(x, "hcpx_tbl")) {
    kind <- attr(x, "kind")

    if (kind == "subjects") {
      # Filter by subject IDs
      subject_ids <- get_subject_ids(x)
      tbl <- dplyr::filter(tbl, subject_id %in% subject_ids)

    } else if (kind == "tasks") {
      # Filter by subject AND task from tasks table
      # Need to join or filter based on task runs
      tasks_data <- dplyr::select(x, subject_id, task, direction) |>
        dplyr::distinct() |>
        dplyr::collect()

      # Filter assets to match task runs
      subject_ids <- unique(tasks_data$subject_id)
      task_names <- unique(tasks_data$task)

      tbl <- dplyr::filter(tbl, subject_id %in% subject_ids)

      # Only filter by task if tasks table had task filters
      if (length(task_names) > 0 && !any(is.na(task_names))) {
        tbl <- dplyr::filter(tbl, task %in% task_names)
      }

    } else if (kind == "assets") {
      # Already an assets table, just continue with it
      tbl <- x
    }
  }

  # Apply bundle filter if specified
  if (!is.null(bundle)) {
    validate_bundle(bundle, h)
    bundle_def <- get_bundle(bundle, h)
    tbl <- apply_bundle_filter(tbl, bundle_def)
  }

  # Apply additional filter expressions
  dots <- rlang::enquos(...)
  if (length(dots) > 0) {
    tbl <- dplyr::filter(tbl, !!!dots)
  }

  hcpx_tbl(tbl, h, "assets")
}

#' Count assets
#'
#' @param x hcpx_tbl assets table
#' @return Integer count
#' @keywords internal
count_assets <- function(x) {
  dplyr::count(x) |>
    dplyr::collect() |>
    dplyr::pull(n)
}

#' Get total size of assets in bytes
#'
#' @param x hcpx_tbl assets table
#' @return Numeric total size in bytes
#' @keywords internal
total_size_bytes <- function(x) {
  dplyr::summarise(x, total = sum(size_bytes, na.rm = TRUE)) |>
    dplyr::collect() |>
    dplyr::pull(total)
}
