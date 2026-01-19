# Subject queries

#' Query subjects
#'
#' Returns a lazy table of subjects from the catalog. Supports dplyr-style
#' filtering using NSE (non-standard evaluation).
#'
#' @param x hcpx handle or hcpx_tbl
#' @param ... dplyr-like filter expressions (e.g., gender == "F", age_range == "26-30")
#' @return A lazy table with class `hcpx_tbl` and kind "subjects"
#' @seealso [tasks()], [assets()], [overview()], [hcpx_ya()]
#' @export
#' @examples
#' \dontrun{
#' h <- hcpx_ya()
#'
#' # All subjects
#' subjects(h)
#'
#' # Filter by gender
#' subjects(h, gender == "F")
#'
#' # Multiple filters
#' subjects(h, gender == "M", age_range == "26-30")
#' }
subjects <- function(x, ...) {
  h <- get_hcpx(x)
  con <- get_con(h)

  # Create lazy table from subjects
  tbl <- dplyr::tbl(con, "subjects")

  # Apply any filter expressions
  dots <- rlang::enquos(...)
  if (length(dots) > 0) {
    tbl <- dplyr::filter(tbl, !!!dots)
  }

  # Wrap with hcpx context
  hcpx_tbl(tbl, h, "subjects")
}

#' Get subject IDs from an hcpx_tbl
#'
#' Extracts subject IDs from a subjects, tasks, or assets table.
#'
#' @param x hcpx_tbl object
#' @return Character vector of subject IDs
#' @keywords internal
get_subject_ids <- function(x) {
  if (!inherits(x, "hcpx_tbl")) {
    stop("Expected hcpx_tbl object", call. = FALSE)
  }

  # Collect subject_id column
  ids <- dplyr::select(x, subject_id) |>
    dplyr::distinct() |>
    dplyr::collect()

  ids$subject_id
}

#' Count subjects
#'
#' @param x hcpx_tbl object
#' @return Integer count
#' @keywords internal
count_subjects <- function(x) {
  dplyr::select(x, subject_id) |>
    dplyr::distinct() |>
    dplyr::count() |>
    dplyr::collect() |>
    dplyr::pull(n)
}
