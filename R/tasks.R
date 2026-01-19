# Task queries and task dictionary

#' Query task runs
#'
#' Returns a lazy table of task runs from the catalog. Can be filtered by
#' task name (with synonym resolution) and other criteria.
#'
#' @param x hcpx handle, subjects table, or tasks table
#' @param task Task selector (e.g., "WM", "working memory", "MOTOR"). Resolved to canonical form.
#' @param ... Additional filters (e.g., direction == "LR", complete == TRUE)
#' @return A lazy table with class `hcpx_tbl` and kind "tasks"
#' @seealso [subjects()], [assets()], [load_task()], [overview()]
#' @export
#' @examples
#' \dontrun{
#' h <- hcpx_ya()
#'
#' # All task runs
#' tasks(h)
#'
#' # Filter by task name (synonyms work)
#' tasks(h, "WM")
#' tasks(h, "working memory")  # Same as above
#'
#' # Chain from subjects
#' subjects(h, gender == "F") |> tasks("MOTOR")
#'
#' # Filter by direction
#' tasks(h, "WM", direction == "LR")
#' }
tasks <- function(x, task = NULL, ...) {
  h <- get_hcpx(x)
  con <- get_con(h)

  # Resolve task name if provided
  canonical_task <- NULL
  if (!is.null(task)) {
    canonical_task <- resolve_task_name(task)
    if (is.na(canonical_task)) {
      cli::cli_warn("Unknown task name: {.val {task}}. Showing all tasks.")
      canonical_task <- NULL
    }
  }

  # Get base task_runs table
  tbl <- dplyr::tbl(con, "task_runs")

  # If input is subjects table, filter by subject IDs
  if (inherits(x, "hcpx_tbl") && attr(x, "kind") == "subjects") {
    subject_ids <- get_subject_ids(x)
    tbl <- dplyr::filter(tbl, subject_id %in% subject_ids)
  }

  # Filter by task if specified
  if (!is.null(canonical_task)) {
    tbl <- dplyr::filter(tbl, task == canonical_task)
  }

  # Apply additional filter expressions
  dots <- rlang::enquos(...)
  if (length(dots) > 0) {
    tbl <- dplyr::filter(tbl, !!!dots)
  }

  hcpx_tbl(tbl, h, "tasks")
}

#' Task dictionary (canonical names + synonyms)
#'
#' Returns the task dictionary loaded from tasks.yml. Contains canonical
#' task names, synonyms, completion flags, and expected run counts.
#'
#' @param h hcpx handle (optional, uses package data if NULL)
#' @return Tibble with columns: task, description, synonyms, completion_flag, expected_runs, directions
#' @export
#' @examples
#' \dontrun{
#' # Get task dictionary
#' task_dictionary()
#'
#' # View canonical task names
#' task_dictionary()$task
#' }
task_dictionary <- function(h = NULL) {
  # Load from YAML
  tasks_path <- system.file("extdata", "tasks.yml", package = "hcpx")

  if (!file.exists(tasks_path)) {
    cli::cli_abort("Task dictionary not found at {.path {tasks_path}}")
  }

  tasks_yaml <- yaml::read_yaml(tasks_path)

  # Convert to tibble
  tibble::tibble(
    task = names(tasks_yaml),
    description = vapply(tasks_yaml, function(x) x$description %||% "", character(1)),
    synonyms = lapply(tasks_yaml, function(x) x$synonyms %||% character(0)),
    completion_flag = vapply(tasks_yaml, function(x) x$completion_flag %||% NA_character_, character(1)),
    expected_runs = vapply(tasks_yaml, function(x) x$expected_runs %||% NA_integer_, integer(1)),
    directions = lapply(tasks_yaml, function(x) x$directions %||% character(0))
  )
}

#' Get completion flag name for a task
#'
#' @param task_name Canonical task name
#' @return Completion flag column name or NA
#' @keywords internal
get_completion_flag <- function(task_name) {
  dict <- task_dictionary()
  idx <- match(task_name, dict$task)
  if (is.na(idx)) return(NA_character_)
  dict$completion_flag[idx]
}

#' List all canonical task names
#'
#' @return Character vector of canonical task names
#' @keywords internal
canonical_task_names <- function() {
  task_dictionary()$task
}
