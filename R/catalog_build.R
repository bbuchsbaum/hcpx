# Catalog building from real HCP metadata
#
# Builds the local catalog from HCP metadata files or by scanning the S3 bucket.
# This is for users who want to work with real HCP data, not just the demo set.

#' Build or update the local catalog
#'
#' Builds the catalog from HCP metadata sources. Can use local CSV files or
#' scan the S3 bucket directly. The catalog is stored in the local database
#' and persists across sessions.
#'
#' @param h hcpx handle
#' @param subjects_csv Path to subjects CSV file (optional). Should contain
#'   at minimum: Subject, Gender, Age columns.
#' @param behavioral_csv Path to HCP behavioral CSV (optional). Contains
#'   completion flags and basic subject info.
#' @param scan_s3 Scan S3 bucket to discover assets (default FALSE). Requires
#'   network access and can be slow for full catalog.
#' @param subjects Character vector of subject IDs to include (optional).
#'   If NULL, includes all available subjects.
#' @param restricted_csv Optional restricted measures file. User must have
#'   appropriate data use agreement.
#' @param force Rebuild from scratch (default FALSE). If TRUE, clears existing
#'   catalog data before building.
#' @return Modified hcpx handle (invisibly)
#' @export
#' @examples
#' \dontrun{
#' h <- hcpx_ya()
#'
#' # Build from subjects CSV
#' catalog_build(h, subjects_csv = "HCP_subjects.csv")
#'
#' # Build from behavioral data
#' catalog_build(h, behavioral_csv = "unrestricted_hcp_data.csv")
#'
#' # Scan S3 for specific subjects
#' catalog_build(h, subjects = c("100206", "100307"), scan_s3 = TRUE)
#' }
catalog_build <- function(h,
                          subjects_csv = NULL,
                          behavioral_csv = NULL,
                          scan_s3 = FALSE,
                          subjects = NULL,
                          restricted_csv = NULL,
                          force = FALSE) {
  con <- get_con(h)

  # Clear existing data if force=TRUE
  if (force) {
    cli::cli_alert_info("Clearing existing catalog data...")
    catalog_clear(h)
  }

  # Track what we're building
  n_subjects_added <- 0
  n_assets_added <- 0

  # 1. Ingest subjects from CSV
  if (!is.null(subjects_csv)) {
    cli::cli_alert_info("Ingesting subjects from {.path {subjects_csv}}")
    n_subjects_added <- ingest_subjects_csv(h, subjects_csv, filter_ids = subjects)
    cli::cli_alert_success("Added {.val {n_subjects_added}} subjects")
  }

  # 2. Ingest from behavioral CSV (HCP unrestricted data)
  if (!is.null(behavioral_csv)) {
    cli::cli_alert_info("Ingesting from behavioral CSV...")
    n_subjects_added <- n_subjects_added + ingest_behavioral_csv(h, behavioral_csv, filter_ids = subjects)
    cli::cli_alert_success("Processed behavioral data")
  }

  # 3. Optionally scan S3 for assets
  if (scan_s3) {
    # Get subject IDs to scan
    if (is.null(subjects)) {
      subjects <- DBI::dbGetQuery(con, "SELECT subject_id FROM subjects")$subject_id
    }

    if (length(subjects) == 0) {
      cli::cli_warn("No subjects in catalog. Add subjects first or specify subject IDs.")
    } else {
      cli::cli_alert_info("Scanning S3 for {.val {length(subjects)}} subjects...")
      n_assets_added <- scan_s3_for_subjects(h, subjects)
      cli::cli_alert_success("Added {.val {n_assets_added}} assets")
    }
  }

  # 4. Refresh task_runs from assets
  if (n_assets_added > 0) {
    cli::cli_alert_info("Refreshing task runs...")
    catalog_refresh_task_runs(con)
  }

  # Summary
  cli::cli_text("")
  cli::cli_alert_success("Catalog build complete")
  status <- DBI::dbGetQuery(con, "
    SELECT
      (SELECT COUNT(*) FROM subjects) as n_subjects,
      (SELECT COUNT(*) FROM assets) as n_assets,
      (SELECT COUNT(*) FROM task_runs) as n_runs
  ")
  cli::cli_bullets(c(
    " " = "Subjects: {status$n_subjects}",
    " " = "Assets: {status$n_assets}",
    " " = "Task runs: {status$n_runs}"
  ))

  invisible(h)
}

#' Ingest subjects from a CSV file
#'
#' @param h hcpx handle
#' @param path Path to CSV file
#' @param filter_ids Optional subject IDs to filter to
#' @return Number of subjects added
#' @keywords internal
ingest_subjects_csv <- function(h, path, filter_ids = NULL) {
  con <- get_con(h)

  # Read CSV
  data <- utils::read.csv(path, stringsAsFactors = FALSE)

  # Normalize column names
  names(data) <- tolower(names(data))

  # Map common column names
  col_map <- list(
    subject_id = c("subject", "subject_id", "subjectid", "id"),
    gender = c("gender", "sex"),
    age_range = c("age", "age_range", "agerange")
  )

  # Find and rename columns
  for (target in names(col_map)) {
    for (candidate in col_map[[target]]) {
      if (candidate %in% names(data)) {
        names(data)[names(data) == candidate] <- target
        break
      }
    }
  }

  # Ensure required columns
  if (!"subject_id" %in% names(data)) {
    stop("CSV must have a subject/subject_id column", call. = FALSE)
  }

  # Convert subject_id to character
  data$subject_id <- as.character(data$subject_id)

  # Filter if requested
  if (!is.null(filter_ids)) {
    data <- data[data$subject_id %in% filter_ids, ]
  }

  if (nrow(data) == 0) {
    return(0)
  }

  # Add release column
  data$release <- h$release

  # Select columns that exist in our schema
  schema_cols <- c("subject_id", "release", "gender", "age_range", "release_quarter",
                   "full_mr_compl", "full_task_fmri", "fmri_wm_compl", "fmri_mot_compl",
                   "fmri_gamb_compl", "fmri_lang_compl", "fmri_soc_compl",
                   "fmri_rel_compl", "fmri_emo_compl")

  keep_cols <- intersect(schema_cols, names(data))
  data <- data[, keep_cols, drop = FALSE]

  # Insert into database (upsert behavior)
  for (i in seq_len(nrow(data))) {
    row <- data[i, , drop = FALSE]
    # Check if exists
    exists <- DBI::dbGetQuery(con,
      "SELECT 1 FROM subjects WHERE subject_id = ?",
      params = list(row$subject_id[1])
    )

    if (nrow(exists) == 0) {
      # Insert - use unname() to remove names from params
      cols <- names(row)
      vals <- paste(rep("?", length(cols)), collapse = ", ")
      query <- sprintf("INSERT INTO subjects (%s) VALUES (%s)",
                       paste(cols, collapse = ", "), vals)
      DBI::dbExecute(con, query, params = unname(as.list(row)))
    }
  }

  nrow(data)
}

#' Ingest from HCP behavioral CSV
#'
#' @param h hcpx handle
#' @param path Path to behavioral CSV
#' @param filter_ids Optional subject IDs to filter to
#' @return Number of subjects processed
#' @keywords internal
ingest_behavioral_csv <- function(h, path, filter_ids = NULL) {
  con <- get_con(h)

  data <- utils::read.csv(path, stringsAsFactors = FALSE)
  names(data) <- tolower(names(data))

  # Map HCP behavioral column names
  col_map <- list(
    subject_id = c("subject"),
    gender = c("gender"),
    age_range = c("age"),
    release_quarter = c("release"),
    fmri_wm_compl = c("fmri_wm_compl", "3t_full_mr_compl"),
    fmri_mot_compl = c("fmri_mot_compl"),
    fmri_gamb_compl = c("fmri_gamb_compl"),
    fmri_lang_compl = c("fmri_lang_compl"),
    fmri_soc_compl = c("fmri_soc_compl"),
    fmri_rel_compl = c("fmri_rel_compl"),
    fmri_emo_compl = c("fmri_emo_compl")
  )

  for (target in names(col_map)) {
    for (candidate in col_map[[target]]) {
      if (candidate %in% names(data)) {
        names(data)[names(data) == candidate] <- target
        break
      }
    }
  }

  if (!"subject_id" %in% names(data)) {
    stop("Behavioral CSV must have a Subject column", call. = FALSE)
  }

  data$subject_id <- as.character(data$subject_id)
  data$release <- h$release

  if (!is.null(filter_ids)) {
    data <- data[data$subject_id %in% filter_ids, ]
  }

  # Use same insert logic as ingest_subjects_csv
  schema_cols <- c("subject_id", "release", "gender", "age_range", "release_quarter",
                   "full_mr_compl", "full_task_fmri", "fmri_wm_compl", "fmri_mot_compl",
                   "fmri_gamb_compl", "fmri_lang_compl", "fmri_soc_compl",
                   "fmri_rel_compl", "fmri_emo_compl")

  keep_cols <- intersect(schema_cols, names(data))
  data <- data[, keep_cols, drop = FALSE]

  count <- 0
  for (i in seq_len(nrow(data))) {
    row <- data[i, , drop = FALSE]
    exists <- DBI::dbGetQuery(con,
      "SELECT 1 FROM subjects WHERE subject_id = ?",
      params = list(row$subject_id[1])
    )

    if (nrow(exists) == 0) {
      cols <- names(row)
      vals <- paste(rep("?", length(cols)), collapse = ", ")
      query <- sprintf("INSERT INTO subjects (%s) VALUES (%s)",
                       paste(cols, collapse = ", "), vals)
      DBI::dbExecute(con, query, params = unname(as.list(row)))
      count <- count + 1
    }
  }

  count
}

#' Scan S3 bucket for assets for given subjects
#'
#' @param h hcpx handle
#' @param subjects Character vector of subject IDs
#' @return Number of assets added
#' @keywords internal
scan_s3_for_subjects <- function(h, subjects) {
  backend <- h$backend
  con <- get_con(h)

  if (!inherits(backend, "hcpx_backend_aws")) {
    cli::cli_warn("S3 scanning only available with AWS backend")
    return(0)
  }

  total_added <- 0

  cli::cli_progress_bar("Scanning subjects", total = length(subjects))

  for (subject_id in subjects) {
    # Scan common prefixes for this subject
    prefixes <- c(
      sprintf("HCP_1200/%s/MNINonLinear/Results/", subject_id),
      sprintf("HCP_1200/%s/T1w/", subject_id)
    )

    for (prefix in prefixes) {
      files <- tryCatch({
        backend_list(backend, prefix)
      }, error = function(e) {
        tibble::tibble(remote_path = character(), size_bytes = integer())
      })

      if (nrow(files) > 0) {
        # Parse and insert each file
        for (j in seq_len(nrow(files))) {
          remote_path <- files$remote_path[j]
          size_bytes <- files$size_bytes[j]

          # Parse path to extract metadata
          parsed <- tryCatch({
            parse_hcp_path(remote_path)
          }, error = function(e) NULL)

          if (!is.null(parsed)) {
            # Generate asset_id
            asset_id <- digest::digest(remote_path, algo = "md5", serialize = FALSE)
            asset_id <- substr(asset_id, 1, .ASSET_ID_LENGTH)

            # Check if already exists
            exists <- DBI::dbGetQuery(con,
              "SELECT 1 FROM assets WHERE remote_path = ?",
              params = list(remote_path)
            )

            if (nrow(exists) == 0) {
              DBI::dbExecute(con, "
                INSERT INTO assets (asset_id, release, subject_id, session, kind,
                  task, direction, run, space, derivative_level, file_type,
                  remote_path, size_bytes, access_tier)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
              ", params = list(
                asset_id,
                h$release,
                subject_id,
                parsed$session %||% "3T",
                parsed$kind,
                parsed$task,
                parsed$direction,
                parsed$run,
                parsed$space,
                parsed$derivative_level,
                parsed$file_type,
                remote_path,
                size_bytes,
                "open"
              ))
              total_added <- total_added + 1
            }
          }
        }
      }
    }

    cli::cli_progress_update()
  }

  cli::cli_progress_done()
  total_added
}

#' Add assets from a list of paths
#'
#' Utility function to add assets directly from a list of remote paths.
#'
#' @param h hcpx handle
#' @param paths Character vector of remote paths
#' @param subject_id Subject ID for all paths
#' @param size_bytes Optional size in bytes (single value or vector)
#' @return Number of assets added
#' @keywords internal
add_assets_from_paths <- function(h, paths, subject_id, size_bytes = NA_integer_) {
  con <- get_con(h)

  if (length(size_bytes) == 1) {
    size_bytes <- rep(size_bytes, length(paths))
  }

  count <- 0
  for (i in seq_along(paths)) {
    path <- paths[i]
    parsed <- tryCatch(parse_hcp_path(path), error = function(e) NULL)

    if (!is.null(parsed)) {
      asset_id <- substr(digest::digest(path, algo = "md5", serialize = FALSE), 1, .ASSET_ID_LENGTH)

      exists <- DBI::dbGetQuery(con,
        "SELECT 1 FROM assets WHERE remote_path = ?",
        params = list(path)
      )

      if (nrow(exists) == 0) {
        DBI::dbExecute(con, "
          INSERT INTO assets (asset_id, release, subject_id, session, kind,
            task, direction, run, space, derivative_level, file_type,
            remote_path, size_bytes, access_tier)
          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ", params = list(
          asset_id,
          h$release,
          subject_id,
          parsed$session %||% "3T",
          parsed$kind,
          parsed$task,
          parsed$direction,
          parsed$run,
          parsed$space,
          parsed$derivative_level,
          parsed$file_type,
          path,
          size_bytes[i],
          "open"
        ))
        count <- count + 1
      }
    }
  }

  count
}
