# Database schema definitions and migrations

# Current schema version
.SCHEMA_VERSION <- 1L

#' Initialize database schema
#'
#' Creates all required tables if they don't exist. Idempotent - safe to call multiple times.
#'
#' @param con DBI connection
#' @return Invisible NULL
#' @keywords internal
schema_init <- function(con) {
  engine <- catalog_engine(con)

  # Create tables in dependency order
schema_create_schema_version(con, engine)
  schema_create_subjects(con, engine)
  schema_create_dictionary(con, engine)
  schema_create_assets(con, engine)
  schema_create_task_runs(con, engine)
  schema_create_bundles(con, engine)
  schema_create_ledger(con, engine)
  schema_create_derived_ledger(con, engine)
  schema_create_cohorts(con, engine)

  # Record schema version if not present
  current <- schema_version_get(con)
  if (is.na(current)) {
    DBI::dbExecute(con, sprintf(
      "INSERT INTO schema_version (version, applied_at) VALUES (%d, '%s')",
      .SCHEMA_VERSION,
      format(Sys.time(), "%Y-%m-%d %H:%M:%S")
    ))
  }

  invisible(NULL)
}

#' Get current schema version
#'
#' @param con DBI connection
#' @return Integer schema version, or NA if not initialized
#' @keywords internal
schema_version_get <- function(con) {
  if (!DBI::dbExistsTable(con, "schema_version")) {
    return(NA_integer_)
  }
  result <- DBI::dbGetQuery(con, "SELECT MAX(version) as version FROM schema_version")
  if (nrow(result) == 0 || is.na(result$version[1])) {
    return(NA_integer_)
  }
  as.integer(result$version[1])
}

# Alias for backwards compatibility
schema_version <- schema_version_get

#' Check if schema needs initialization
#'
#' @param con DBI connection
#' @return Logical TRUE if schema needs initialization
#' @keywords internal
schema_needs_init <- function(con) {
  is.na(schema_version_get(con))
}

# --- Table creation functions ---

#' Create schema_version table
#'
#' @param con DBI connection
#' @param engine Database engine identifier (reserved for future use)
#' @return Invisible NULL
#' @keywords internal
schema_create_schema_version <- function(con, engine) {
  if (DBI::dbExistsTable(con, "schema_version")) return(invisible(NULL))

  DBI::dbExecute(con, "
    CREATE TABLE schema_version (
      version INTEGER NOT NULL,
      applied_at TEXT NOT NULL
    )
  ")
  invisible(NULL)
}

#' Create subjects table
#'
#' @param con DBI connection
#' @param engine Database engine identifier (reserved for future use)
#' @return Invisible NULL
#' @keywords internal
schema_create_subjects <- function(con, engine) {
  if (DBI::dbExistsTable(con, "subjects")) return(invisible(NULL))

  DBI::dbExecute(con, "
    CREATE TABLE subjects (
      subject_id TEXT PRIMARY KEY,
      release TEXT NOT NULL,
      gender TEXT,
      age_range TEXT,
      release_quarter TEXT,
      -- Open completion flags
      full_mr_compl INTEGER,
      full_task_fmri INTEGER,
      fmri_wm_compl INTEGER,
      fmri_mot_compl INTEGER,
      fmri_gamb_compl INTEGER,
      fmri_lang_compl INTEGER,
      fmri_soc_compl INTEGER,
      fmri_rel_compl INTEGER,
      fmri_emo_compl INTEGER
    )
  ")
  invisible(NULL)
}

#' Create dictionary table
#'
#' @param con DBI connection
#' @param engine Database engine identifier (reserved for future use)
#' @return Invisible NULL
#' @keywords internal
schema_create_dictionary <- function(con, engine) {
  if (DBI::dbExistsTable(con, "dictionary")) return(invisible(NULL))

  DBI::dbExecute(con, "
    CREATE TABLE dictionary (
      var_name TEXT PRIMARY KEY,
      access_tier TEXT NOT NULL DEFAULT 'open',
      category TEXT,
      instrument TEXT,
      description TEXT,
      values_enum TEXT
    )
  ")
  invisible(NULL)
}

#' Create assets table
#'
#' @param con DBI connection
#' @param engine Database engine identifier (reserved for future use)
#' @return Invisible NULL
#' @keywords internal
schema_create_assets <- function(con, engine) {
  if (DBI::dbExistsTable(con, "assets")) return(invisible(NULL))

  DBI::dbExecute(con, "
    CREATE TABLE assets (
      asset_id TEXT PRIMARY KEY,
      release TEXT NOT NULL,
      subject_id TEXT NOT NULL,
      session TEXT,
      kind TEXT,
      task TEXT,
      direction TEXT,
      run INTEGER,
      space TEXT,
      derivative_level TEXT,
      file_type TEXT,
      remote_path TEXT NOT NULL,
      size_bytes INTEGER,
      access_tier TEXT DEFAULT 'open'
    )
  ")

  # Create indexes for common queries
  DBI::dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_assets_subject ON assets(subject_id)")
  DBI::dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_assets_task ON assets(task)")
  DBI::dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_assets_kind ON assets(kind)")
  DBI::dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_assets_file_type ON assets(file_type)")
  DBI::dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_assets_remote_path ON assets(remote_path)")

  invisible(NULL)
}

#' Create task_runs table
#'
#' @param con DBI connection
#' @param engine Database engine identifier (reserved for future use)
#' @return Invisible NULL
#' @keywords internal
schema_create_task_runs <- function(con, engine) {
  if (DBI::dbExistsTable(con, "task_runs")) return(invisible(NULL))

  DBI::dbExecute(con, "
    CREATE TABLE task_runs (
      run_id TEXT PRIMARY KEY,
      subject_id TEXT NOT NULL,
      task TEXT NOT NULL,
      session TEXT,
      direction TEXT,
      run INTEGER,
      complete INTEGER,
      complete_source TEXT
    )
  ")

  DBI::dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_task_runs_subject ON task_runs(subject_id)")
  DBI::dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_task_runs_task ON task_runs(task)")
  DBI::dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_task_runs_task_complete ON task_runs(task, complete)")

  invisible(NULL)
}

#' Create bundles table
#'
#' @param con DBI connection
#' @param engine Database engine identifier (reserved for future use)
#' @return Invisible NULL
#' @keywords internal
schema_create_bundles <- function(con, engine) {
  if (DBI::dbExistsTable(con, "bundles")) return(invisible(NULL))

  DBI::dbExecute(con, "
    CREATE TABLE bundles (
      bundle TEXT PRIMARY KEY,
      description TEXT,
      definition_json TEXT NOT NULL
    )
  ")
  invisible(NULL)
}

#' Create ledger table
#'
#' @param con DBI connection
#' @param engine Database engine identifier (reserved for future use)
#' @return Invisible NULL
#' @keywords internal
schema_create_ledger <- function(con, engine) {
  if (DBI::dbExistsTable(con, "ledger")) return(invisible(NULL))

  DBI::dbExecute(con, "
    CREATE TABLE ledger (
      asset_id TEXT PRIMARY KEY,
      local_path TEXT NOT NULL,
      downloaded_at TEXT NOT NULL,
      last_accessed TEXT NOT NULL,
      size_bytes INTEGER,
      checksum_md5 TEXT,
      etag TEXT,
      backend TEXT,
      pinned INTEGER DEFAULT 0
    )
  ")

  DBI::dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_ledger_last_accessed ON ledger(last_accessed)")
  DBI::dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_ledger_pinned ON ledger(pinned)")

  invisible(NULL)
}

#' Create derived_ledger table
#'
#' @param con DBI connection
#' @param engine Database engine identifier (reserved for future use)
#' @return Invisible NULL
#' @keywords internal
schema_create_derived_ledger <- function(con, engine) {
  if (DBI::dbExistsTable(con, "derived_ledger")) return(invisible(NULL))

  DBI::dbExecute(con, "
    CREATE TABLE derived_ledger (
      derived_id TEXT PRIMARY KEY,
      recipe TEXT NOT NULL,
      source_id TEXT NOT NULL,
      local_path TEXT NOT NULL,
      created_at TEXT NOT NULL,
      params_json TEXT
    )
  ")

  DBI::dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_derived_recipe ON derived_ledger(recipe)")
  DBI::dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_derived_source ON derived_ledger(source_id)")

  invisible(NULL)
}

#' Create cohorts table
#'
#' @param con DBI connection
#' @param engine Database engine identifier (reserved for future use)
#' @return Invisible NULL
#' @keywords internal
schema_create_cohorts <- function(con, engine) {
  if (DBI::dbExistsTable(con, "cohorts")) return(invisible(NULL))

  DBI::dbExecute(con, "
    CREATE TABLE cohorts (
      cohort_name TEXT PRIMARY KEY,
      created_at TEXT NOT NULL,
      query_sql TEXT NOT NULL,
      note TEXT
    )
  ")
  invisible(NULL)
}

#' Refresh task_runs table from assets
#'
#' Derives task run information from the assets table. Groups assets by
#' subject/task/direction/run and determines completion status.
#'
#' @param con DBI connection
#' @return Invisible NULL
#' @keywords internal
catalog_refresh_task_runs <- function(con) {
  # Clear existing task_runs
  DBI::dbExecute(con, "DELETE FROM task_runs")

  # Derive task runs from assets where task is not null
  # The run_id is a composite key of subject_id + task + direction + run
  DBI::dbExecute(con, "
    INSERT INTO task_runs (run_id, subject_id, task, session, direction, run, complete, complete_source)
    SELECT
      subject_id || '_' || task || '_' || COALESCE(direction, 'NA') || '_' || COALESCE(CAST(run AS TEXT), '1') as run_id,
      subject_id,
      task,
      session,
      direction,
      run,
      NULL as complete,
      'derived' as complete_source
    FROM assets
    WHERE task IS NOT NULL
    GROUP BY subject_id, task, session, direction, run
  ")

  invisible(NULL)
}

#' List all tables in the catalog database
#'
#' @param con DBI connection
#' @return Character vector of table names
#' @keywords internal
schema_tables <- function(con) {
  DBI::dbListTables(con)
}

#' Check if all required tables exist
#'
#' @param con DBI connection
#' @return Logical TRUE if all tables exist
#' @keywords internal
schema_complete <- function(con) {
  required <- c("schema_version", "subjects", "dictionary", "assets",
                "task_runs", "bundles", "ledger", "derived_ledger", "cohorts")
  existing <- schema_tables(con)
  all(required %in% existing)
}

#' Drop all tables (for testing only)
#'
#' @param con DBI connection
#' @return Invisible NULL
#' @keywords internal
schema_drop_all <- function(con) {
  tables <- schema_tables(con)
  for (tbl in tables) {
    DBI::dbExecute(con, sprintf("DROP TABLE IF EXISTS %s", tbl))
  }
  invisible(NULL)
}
