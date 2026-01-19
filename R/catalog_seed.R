# Demo seed data loading

#' Load demo seed data into catalog
#'
#' Loads the demo/seed data from inst/extdata CSV files into the catalog
#' database. This provides a minimal working dataset for testing and
#' exploration without needing real HCP credentials.
#'
#' @param h hcpx handle
#' @return Invisible handle
#' @keywords internal
catalog_seed <- function(h) {
  con <- get_con(h)

  # Check if already seeded (has data)
  n_subjects <- DBI::dbGetQuery(con, "SELECT COUNT(*) as n FROM subjects")$n
  if (n_subjects > 0) {
    return(invisible(h))
  }

  # Load demo subjects
  subjects_path <- system.file("extdata", "demo_subjects.csv", package = "hcpx")
  if (file.exists(subjects_path)) {
    subjects <- utils::read.csv(subjects_path, stringsAsFactors = FALSE)
    DBI::dbWriteTable(con, "subjects", subjects, append = TRUE, row.names = FALSE)
  }

  # Load demo assets
  assets_path <- system.file("extdata", "demo_assets.csv", package = "hcpx")
  if (file.exists(assets_path)) {
    assets <- utils::read.csv(assets_path, stringsAsFactors = FALSE)
    # Handle NA task values (resting state fMRI has NA task)
    assets$task[assets$task == "NA" | assets$task == ""] <- NA
    DBI::dbWriteTable(con, "assets", assets, append = TRUE, row.names = FALSE)

    # Refresh task_runs from assets
    catalog_refresh_task_runs(con)
  }

  # Load bundles from YAML
  bundles_path <- system.file("extdata", "bundles.yml", package = "hcpx")
  if (file.exists(bundles_path)) {
    bundles_yaml <- yaml::read_yaml(bundles_path)
    for (bundle_name in names(bundles_yaml)) {
      bundle <- bundles_yaml[[bundle_name]]
      DBI::dbExecute(con,
        "INSERT OR REPLACE INTO bundles (bundle, description, definition_json) VALUES (?, ?, ?)",
        params = list(
          bundle_name,
          bundle$description %||% "",
          jsonlite::toJSON(bundle$where, auto_unbox = TRUE)
        )
      )
    }
  }

  invisible(h)
}

#' Load demo seed data (alias for catalog_seed)
#'
#' @param h hcpx handle
#' @return Invisible handle
#' @keywords internal
catalog_seed_demo <- function(h) {
  catalog_seed(h)
}

#' Check if catalog has seed data
#'
#' @param h hcpx handle
#' @return Logical TRUE if catalog has data
#' @keywords internal
catalog_has_data <- function(h) {
  con <- get_con(h)
  n_subjects <- DBI::dbGetQuery(con, "SELECT COUNT(*) as n FROM subjects")$n
  n_subjects > 0
}

#' Clear all catalog data (for testing)
#'
#' @param h hcpx handle
#' @return Invisible handle
#' @keywords internal
catalog_clear <- function(h) {
  con <- get_con(h)
  DBI::dbExecute(con, "DELETE FROM subjects")
  DBI::dbExecute(con, "DELETE FROM assets")
  DBI::dbExecute(con, "DELETE FROM task_runs")
  DBI::dbExecute(con, "DELETE FROM bundles")
  DBI::dbExecute(con, "DELETE FROM dictionary")
  DBI::dbExecute(con, "DELETE FROM ledger")
  DBI::dbExecute(con, "DELETE FROM derived_ledger")
  DBI::dbExecute(con, "DELETE FROM cohorts")
  invisible(h)
}

# Note: %||% is imported from rlang in hcpx-package.R
