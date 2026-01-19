# Overview and summary functions

#' Show what is available for a subject or cohort
#'
#' Prints a beautiful summary of the data available in the catalog or
#' for a specific selection of subjects/tasks/assets.
#'
#' @param x hcpx handle, subjects table, tasks table, or assets table
#' @return Invisible x, prints summary
#' @export
#' @examples
#' \dontrun{
#' h <- hcpx_ya()
#'
#' # Overview of entire catalog
#' overview(h)
#'
#' # Overview of female subjects
#' subjects(h, gender == "F") |> overview()
#'
#' # Overview of WM task
#' tasks(h, "WM") |> overview()
#' }
overview <- function(x) {
  UseMethod("overview")
}

#' Overview for hcpx handle
#'
#' @param x hcpx handle
#' @return Invisible x
#' @rdname overview
#' @export
#' @method overview hcpx
overview.hcpx <- function(x) {
  h <- x
  con <- get_con(h)

  cli::cli_h1("HCP Catalog Overview")
  cli::cli_text("")

  # Subjects summary
  subjects_count <- DBI::dbGetQuery(con, "SELECT COUNT(*) as n FROM subjects")$n
  cli::cli_alert_info("Subjects: {.val {subjects_count}}")

  # Gender breakdown
  gender_counts <- DBI::dbGetQuery(con,
    "SELECT gender, COUNT(*) as n FROM subjects GROUP BY gender ORDER BY gender")
  if (nrow(gender_counts) > 0) {
    gender_str <- paste(sprintf("%s: %d", gender_counts$gender, gender_counts$n), collapse = ", ")
    cli::cli_bullets(c(" " = "Gender: {gender_str}"))
  }

  cli::cli_text("")

  # Task summary
  task_counts <- DBI::dbGetQuery(con,
    "SELECT task, COUNT(DISTINCT subject_id) as subjects, COUNT(*) as runs
     FROM task_runs WHERE task IS NOT NULL GROUP BY task ORDER BY subjects DESC")
  if (nrow(task_counts) > 0) {
    cli::cli_alert_info("Task Runs:")
    for (i in seq_len(nrow(task_counts))) {
      cli::cli_bullets(c(" " = "{task_counts$task[i]}: {task_counts$subjects[i]} subjects, {task_counts$runs[i]} runs"))
    }
  }

  cli::cli_text("")

  # Assets summary
  assets_count <- DBI::dbGetQuery(con, "SELECT COUNT(*) as n FROM assets")$n
  total_size <- DBI::dbGetQuery(con, "SELECT SUM(size_bytes) as total FROM assets")$total
  total_gb <- if (is.na(total_size)) "unknown" else sprintf("%.2f", total_size / 1e9)

  cli::cli_alert_info("Assets: {.val {assets_count}} files ({total_gb} GB)")

  # Kind breakdown
  kind_counts <- DBI::dbGetQuery(con,
    "SELECT kind, COUNT(*) as n FROM assets WHERE kind IS NOT NULL GROUP BY kind ORDER BY n DESC")
  if (nrow(kind_counts) > 0) {
    kind_str <- paste(sprintf("%s: %d", kind_counts$kind, kind_counts$n), collapse = ", ")
    cli::cli_bullets(c(" " = "By kind: {kind_str}"))
  }

  cli::cli_text("")

  # Bundles available
  bundle_count <- DBI::dbGetQuery(con, "SELECT COUNT(*) as n FROM bundles")$n
  cli::cli_alert_info("Bundles available: {.val {bundle_count}}")

  cli::cli_text("")
  cli::cli_text("Try: {.code subjects(h) |> tasks(\"WM\") |> assets(bundle = \"tfmri_cifti_min\")}")

  invisible(x)
}

#' Overview for hcpx tables
#'
#' @param x hcpx_tbl object
#' @return Invisible x
#' @rdname overview
#' @export
#' @method overview hcpx_tbl
overview.hcpx_tbl <- function(x) {
  h <- get_hcpx(x)
  kind <- attr(x, "kind")

  cli::cli_h1("Selection Overview")
  cli::cli_text("")

  if (kind == "subjects") {
    overview_subjects(x)
  } else if (kind == "tasks") {
    overview_tasks(x)
  } else if (kind == "assets") {
    overview_assets(x)
  }

  invisible(x)
}

#' @keywords internal
overview_subjects <- function(x) {
  data <- dplyr::collect(x)
  n <- nrow(data)

  cli::cli_alert_info("Subjects: {.val {n}}")

  if (n > 0 && "gender" %in% names(data)) {
    gender_counts <- table(data$gender)
    gender_str <- paste(sprintf("%s: %d", names(gender_counts), gender_counts), collapse = ", ")
    cli::cli_bullets(c(" " = "Gender: {gender_str}"))
  }

  if (n > 0 && "age_range" %in% names(data)) {
    age_counts <- table(data$age_range)
    age_str <- paste(sprintf("%s: %d", names(age_counts), age_counts), collapse = ", ")
    cli::cli_bullets(c(" " = "Age: {age_str}"))
  }

  cli::cli_text("")
  cli::cli_text("Next: {.code tasks()} or {.code assets()}")
}

#' @keywords internal
overview_tasks <- function(x) {
  data <- dplyr::collect(x)
  n <- nrow(data)
  n_subjects <- length(unique(data$subject_id))

  cli::cli_alert_info("Task Runs: {.val {n}} ({n_subjects} subjects)")

  if (n > 0 && "task" %in% names(data)) {
    task_counts <- table(data$task)
    for (task in names(task_counts)) {
      cli::cli_bullets(c(" " = "{task}: {task_counts[task]} runs"))
    }
  }

  if (n > 0 && "direction" %in% names(data)) {
    dir_counts <- table(data$direction)
    dir_str <- paste(sprintf("%s: %d", names(dir_counts), dir_counts), collapse = ", ")
    cli::cli_bullets(c(" " = "Directions: {dir_str}"))
  }

  cli::cli_text("")
  cli::cli_text("Next: {.code assets(bundle = \"tfmri_cifti_min\")}")
}

#' @keywords internal
overview_assets <- function(x) {
  # Use SQL aggregation for efficiency
  h <- get_hcpx(x)
  con <- get_con(h)

  # Count and size
  summary <- dplyr::summarise(x,
    n = dplyr::n(),
    total_bytes = sum(size_bytes, na.rm = TRUE),
    n_subjects = dplyr::n_distinct(subject_id)
  ) |> dplyr::collect()

  n <- summary$n
  total_gb <- sprintf("%.2f", summary$total_bytes / 1e9)
  n_subjects <- summary$n_subjects

  cli::cli_alert_info("Assets: {.val {n}} files ({total_gb} GB)")
  cli::cli_bullets(c(" " = "From {n_subjects} subjects"))

  # File type breakdown
  type_counts <- dplyr::count(x, file_type, sort = TRUE) |>
    dplyr::collect() |>
    head(5)
  if (nrow(type_counts) > 0) {
    cli::cli_bullets(c(" " = "File types:"))
    for (i in seq_len(nrow(type_counts))) {
      cli::cli_bullets(c(" " = "  {type_counts$file_type[i]}: {type_counts$n[i]}"))
    }
  }

  cli::cli_text("")
  cli::cli_text("Next: {.code plan_download() |> download()}")
}
