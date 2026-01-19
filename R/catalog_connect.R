# Database connection management

#' Connect to or create catalog database
#'
#' Creates or connects to the hcpx catalog database. Prefers DuckDB for
#' performance, falls back to SQLite if DuckDB is not available.
#'
#' @param cache_root Path to cache directory
#' @param engine One of "duckdb", "sqlite", or "auto" (default)
#' @return DBI connection object
#' @keywords internal
catalog_connect <- function(cache_root, engine = c("auto", "duckdb", "sqlite")) {
  engine <- match.arg(engine)


  # Auto-select engine based on availability

  if (engine == "auto") {
    engine <- if (has_duckdb()) "duckdb" else if (has_sqlite()) "sqlite" else {
      stop("No database backend available. Install 'duckdb' (recommended) or 'RSQLite'.",
           call. = FALSE)
    }
  }

  # Validate selected engine is available

if (engine == "duckdb" && !has_duckdb()) {
    stop("DuckDB requested but 'duckdb' package is not installed.", call. = FALSE)
  }
  if (engine == "sqlite" && !has_sqlite()) {
    stop("SQLite requested but 'RSQLite' package is not installed.", call. = FALSE)
  }

  # Ensure cache directory exists
  if (!dir.exists(cache_root)) {
    dir.create(cache_root, recursive = TRUE)
  }

  # Determine database path
  db_filename <- if (engine == "duckdb") "catalog.duckdb" else "catalog.sqlite"
  db_path <- file.path(cache_root, db_filename)

  # Connect
  con <- if (engine == "duckdb") {
    duckdb::dbConnect(duckdb::duckdb(), dbdir = db_path)
  } else {
    RSQLite::dbConnect(RSQLite::SQLite(), dbname = db_path)
  }

  # Tag connection with metadata for later inspection
  attr(con, "hcpx_engine") <- engine
  attr(con, "hcpx_db_path") <- db_path

  con
}

#' Check if a connection is valid and reconnect if needed
#'
#' @param con DBI connection or NULL
#' @param cache_root Path to cache directory (for reconnection)
#' @param engine Database engine (for reconnection)
#' @return Valid DBI connection
#' @keywords internal
catalog_ensure_connection <- function(con, cache_root, engine = "auto") {
  if (is.null(con) || !catalog_connection_valid(con)) {
    return(catalog_connect(cache_root, engine))
  }
  con
}

#' Check if a database connection is valid
#'
#' @param con DBI connection
#' @return Logical TRUE if connection is valid
#' @keywords internal
catalog_connection_valid <- function(con) {
  if (is.null(con)) return(FALSE)
  tryCatch({
    DBI::dbIsValid(con)
  }, error = function(e) FALSE)
}

#' Disconnect from catalog database
#'
#' @param con DBI connection
#' @return Invisible NULL
#' @keywords internal
catalog_disconnect <- function(con) {
  if (!is.null(con) && catalog_connection_valid(con)) {
    DBI::dbDisconnect(con)
  }
  invisible(NULL)
}

#' Get database engine type from connection
#'
#' @param con DBI connection
#' @return Character: "duckdb" or "sqlite"
#' @keywords internal
catalog_engine <- function(con) {
  engine <- attr(con, "hcpx_engine")
  if (!is.null(engine)) return(engine)

  # Fallback: detect from connection class
  if (inherits(con, "duckdb_connection")) {
    "duckdb"
  } else if (inherits(con, "SQLiteConnection")) {
    "sqlite"
  } else {
    "unknown"
  }
}

#' Get database file path from connection
#'
#' @param con DBI connection
#' @return Character path to database file
#' @keywords internal
catalog_db_path <- function(con) {
  path <- attr(con, "hcpx_db_path")
  if (!is.null(path)) return(path)
  NA_character_
}

#' Check if duckdb is available
#'
#' @return Logical
#' @keywords internal
has_duckdb <- function() {
  requireNamespace("duckdb", quietly = TRUE)
}

#' Check if RSQLite is available
#'
#' @return Logical
#' @keywords internal
has_sqlite <- function() {
  requireNamespace("RSQLite", quietly = TRUE)
}
