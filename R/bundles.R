# Bundle registry and validation

#' List available bundles
#'
#' Returns a tibble of all registered bundles with their descriptions
#' and filter definitions.
#'
#' @param h hcpx handle (optional, uses database if provided, otherwise YAML)
#' @return Tibble with columns: bundle, description
#' @export
#' @examples
#' \dontrun{
#' h <- hcpx_ya()
#' bundles(h)
#'
#' # Without handle, reads from YAML
#' bundles()
#' }
bundles <- function(h = NULL) {
  if (!is.null(h)) {
    # Read from database
    con <- get_con(h)
    if (DBI::dbExistsTable(con, "bundles")) {
      result <- DBI::dbGetQuery(con, "SELECT bundle, description FROM bundles ORDER BY bundle")
      return(tibble::as_tibble(result))
    }
  }

  # Fall back to YAML
  bundles_yaml <- load_bundles_yaml()
  tibble::tibble(
    bundle = names(bundles_yaml),
    description = vapply(bundles_yaml, function(x) x$description %||% "", character(1))
  )
}

#' Get bundle definition by name
#'
#' @param bundle_name Name of the bundle
#' @param h hcpx handle (optional)
#' @return List with bundle definition (where clause)
#' @keywords internal
get_bundle <- function(bundle_name, h = NULL) {
  if (!is.null(h)) {
    # Try database first
    con <- get_con(h)
    if (DBI::dbExistsTable(con, "bundles")) {
      result <- DBI::dbGetQuery(con,
        "SELECT definition_json FROM bundles WHERE bundle = ?",
        params = list(bundle_name)
      )
      if (nrow(result) > 0) {
        return(jsonlite::fromJSON(result$definition_json[1], simplifyVector = FALSE))
      }
    }
  }

  # Fall back to YAML
  bundles_yaml <- load_bundles_yaml()
  if (bundle_name %in% names(bundles_yaml)) {
    return(bundles_yaml[[bundle_name]]$where)
  }

  cli::cli_abort("Bundle not found: {.val {bundle_name}}")
}

#' Load bundles from YAML
#'
#' @return List of bundle definitions
#' @keywords internal
load_bundles_yaml <- function() {
  bundles_path <- system.file("extdata", "bundles.yml", package = "hcpx")
  if (!file.exists(bundles_path)) {
    cli::cli_abort("Bundles file not found at {.path {bundles_path}}")
  }
  yaml::read_yaml(bundles_path)
}

#' Apply bundle filter to assets query
#'
#' Applies the bundle's where clause as dplyr filters on the assets table.
#'
#' @param tbl Assets table (dbplyr tbl)
#' @param bundle_def Bundle definition list (the "where" clause)
#' @return Filtered table
#' @keywords internal
apply_bundle_filter <- function(tbl, bundle_def) {
  if (is.null(bundle_def)) return(tbl)

  # Apply each filter condition
  for (col in names(bundle_def)) {
    value <- bundle_def[[col]]

    # Ensure value is a vector, not a list (YAML can return lists)
    if (is.list(value)) {
      value <- unlist(value, use.names = FALSE)
    }

    if (length(value) > 1) {
      # Multiple values -> IN clause
      tbl <- dplyr::filter(tbl, .data[[col]] %in% !!value)
    } else {
      # Single value -> equality
      tbl <- dplyr::filter(tbl, .data[[col]] == !!value)
    }
  }

  tbl
}

#' Validate bundle name
#'
#' @param bundle_name Bundle name to validate
#' @param h hcpx handle (optional)
#' @return TRUE if valid, throws error if not
#' @keywords internal
validate_bundle <- function(bundle_name, h = NULL) {
  available <- bundles(h)$bundle
  if (!bundle_name %in% available) {
    cli::cli_abort(c(
      "Unknown bundle: {.val {bundle_name}}",
      "i" = "Available bundles: {.val {available}}"
    ))
  }
  invisible(TRUE)
}
