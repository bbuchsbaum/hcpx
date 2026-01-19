# Main constructor, print methods, and hcpx_tbl context management

#' Create an HCP-YA catalog handle
#'
#' The main entry point for hcpx. Creates a handle to the HCP catalog with
#' configured backend, cache, and database engine.
#'
#' @param release Character. Default "HCP_1200".
#' @param backend One of "aws", "rest", "balsa", "local".
#' @param cache Path to cache root directory. Defaults to user cache directory.
#' @param engine One of "duckdb", "sqlite", "auto". Auto prefers DuckDB.
#' @param catalog_version One of "seed" (demo data), "latest", or a specific version.
#' @param local_root For local backend: path to HCP data mirror.
#' @param balsa_ascp For BALSA backend: path to `ascp` executable (optional).
#' @param balsa_key For BALSA backend: path to Aspera private key file (optional).
#' @param balsa_host For BALSA backend: Aspera host (optional; required if passing bare remote paths).
#' @param balsa_user For BALSA backend: Aspera username (optional; required if passing bare remote paths).
#' @param balsa_port For BALSA backend: Aspera port (default 33001).
#' @param balsa_rate For BALSA backend: rate limit passed to `ascp -l` (default "300m").
#' @return An object of class `hcpx`.
#' @seealso [subjects()], [tasks()], [assets()], [overview()], [hcpx_cache()], [hcpx_close()]
#' @export
#' @examples
#' \dontrun{
#' # Initialize with defaults (AWS backend, DuckDB, demo data)
#' h <- hcpx_ya()
#'
#' # Use local backend with existing HCP data
#' h <- hcpx_ya(backend = "local", local_root = "/data/HCP")
#'
#' # Force SQLite if DuckDB isn't available
#' h <- hcpx_ya(engine = "sqlite")
#' }
hcpx_ya <- function(release = "HCP_1200",
                    backend = c("aws", "rest", "balsa", "local"),
                    cache = hcpx_cache_default(),
                    engine = c("auto", "duckdb", "sqlite"),
                    catalog_version = "seed",
                    local_root = NULL,
                    balsa_ascp = NULL,
                    balsa_key = NULL,
                    balsa_host = NULL,
                    balsa_user = NULL,
                    balsa_port = 33001L,
                    balsa_rate = "300m") {

  backend <- match.arg(backend)
  engine <- match.arg(engine)

  # Ensure cache directory exists
  if (!dir.exists(cache)) {
    dir.create(cache, recursive = TRUE)
  }

  # Create backend object
  backend_obj <- switch(backend,
    "aws" = hcpx_backend_aws(),
    "rest" = hcpx_backend_rest(),
    "balsa" = hcpx_backend_balsa(
      ascp = balsa_ascp,
      key = balsa_key,
      host = balsa_host,
      user = balsa_user,
      port = balsa_port,
      rate = balsa_rate
    ),
    "local" = {
      if (is.null(local_root)) {
        cli::cli_warn("Local backend requires {.arg local_root}. Using cache directory.")
        local_root <- cache
      }
      hcpx_backend_local(local_root)
    }
  )

  # Connect to database
  con <- catalog_connect(cache, engine)
  actual_engine <- catalog_engine(con)

  # Wrap initialization in tryCatch to ensure connection cleanup on error
  tryCatch({
    # Initialize schema if needed
    if (schema_needs_init(con)) {
      schema_init(con)
    }

    # Create hcpx object
    h <- structure(
      list(
        release = release,
        engine = actual_engine,
        cache = list(
          root = cache,
          max_size_gb = .DEFAULT_CACHE_MAX_GB,
          strategy = "lru"
        ),
        backend = backend_obj,
        con = con,
        policy = list(
          enforce = TRUE,
          allow_export_restricted = FALSE
        ),
        versions = list(
          schema = schema_version_get(con),
          catalog = format(Sys.Date(), "%Y-%m-%d")
        )
      ),
      class = "hcpx"
    )

    # Load seed/demo data if requested
    if (catalog_version == "seed") {
      catalog_seed(h)
    }

    h
  }, error = function(e) {
    # Clean up connection on initialization failure
    catalog_disconnect(con)
    stop(e)
  })
}

#' Get default cache directory
#'
#' Returns the default cache directory for hcpx. Checks HCPX_CACHE environment
#' variable first, then falls back to the standard R user cache directory.
#'
#' @return Path to default cache directory
#' @export
#' @examples
#' hcpx_cache_default()
hcpx_cache_default <- function() {
  rappdirs_path <- Sys.getenv("HCPX_CACHE", unset = "")
  if (nzchar(rappdirs_path)) {
    return(rappdirs_path)
  }
  file.path(tools::R_user_dir("hcpx", "cache"))
}

#' Configure cache for an hcpx handle
#'
#' @param h hcpx handle
#' @param root Cache root directory
#' @param max_size_gb Maximum cache size in GB
#' @param strategy One of "lru", "manual"
#' @return Modified hcpx handle
#' @export
#' @examples
#' \dontrun{
#' h <- hcpx_ya()
#' h <- hcpx_cache(h, root = "/tmp/hcpx-cache", max_size_gb = 50)
#' }
hcpx_cache <- function(h, root = NULL, max_size_gb = .DEFAULT_CACHE_MAX_GB, strategy = c("lru", "manual")) {
  strategy <- match.arg(strategy)

  if (!is.null(root)) {
    h$cache$root <- root
    if (!dir.exists(root)) {
      dir.create(root, recursive = TRUE)
    }
  }

  h$cache$max_size_gb <- max_size_gb
  h$cache$strategy <- strategy

  h
}

# Note: hcpx_auth() is now in R/auth.R with full keyring support

#' Attach hcpx context to a table result
#'
#' Wraps a tibble or dbplyr tbl with hcpx context, enabling downstream
#' functions to access the handle without explicit passing.
#'
#' @param tbl A tibble or dbplyr tbl
#' @param h hcpx handle
#' @param kind One of "subjects", "tasks", "assets"
#' @return Table with hcpx_tbl class and attributes
#' @keywords internal
hcpx_tbl <- function(tbl, h, kind) {
  attr(tbl, "hcpx") <- h
  attr(tbl, "kind") <- kind
  class(tbl) <- c("hcpx_tbl", class(tbl))
  tbl
}

#' Extract hcpx handle from an hcpx_tbl
#'
#' @param x hcpx_tbl object
#' @return hcpx handle
#' @keywords internal
get_hcpx <- function(x) {
  if (inherits(x, "hcpx")) return(x)
  h <- attr(x, "hcpx")
  if (is.null(h)) {
    stop("No hcpx context attached. Did you start with hcpx_ya()?", call. = FALSE)
  }
  h
}

#' Get the database connection from an hcpx handle
#'
#' @param h hcpx handle or hcpx_tbl
#' @return DBI connection
#' @keywords internal
get_con <- function(h) {
  h <- get_hcpx(h)
  # Ensure connection is valid, reconnect if needed
  if (!catalog_connection_valid(h$con)) {
    h$con <- catalog_connect(h$cache$root, h$engine)
  }
  h$con
}

#' Print method for hcpx handle
#'
#' @param x hcpx handle
#' @param ... Additional arguments (ignored)
#' @return Invisible x
#' @rdname hcpx_ya
#' @export
#' @method print hcpx
print.hcpx <- function(x, ...) {
  cli::cli_h1("hcpx: HCP Explorer")
  cli::cli_text("")

  # Release and backend info
  cli::cli_alert_info("Release: {.val {x$release}}")
  cli::cli_alert_info("Backend: {.val {x$backend$type}}")
  cli::cli_alert_info("Engine: {.val {x$engine}}")
  cli::cli_text("")

  # Cache info
  cache_path <- x$cache$root
  cli::cli_alert_info("Cache: {.path {cache_path}}")
  cli::cli_alert_info("Max size: {.val {x$cache$max_size_gb}} GB ({x$cache$strategy})")
  cli::cli_text("")

  # Catalog status
  con <- tryCatch(get_con(x), error = function(e) NULL)
  if (!is.null(con) && schema_complete(con)) {
    # Count records
    n_subjects <- DBI::dbGetQuery(con, "SELECT COUNT(*) as n FROM subjects")$n
    n_assets <- DBI::dbGetQuery(con, "SELECT COUNT(*) as n FROM assets")$n
    cli::cli_alert_success("Catalog: {.val {n_subjects}} subjects, {.val {n_assets}} assets")
  } else {
    cli::cli_alert_warning("Catalog: not initialized")
  }

  cli::cli_text("")
  cli::cli_text("Get started: {.code subjects(h) |> tasks() |> assets()}")

  invisible(x)
}

#' Print method for hcpx_tbl objects
#'
#' @param x hcpx_tbl object
#' @param ... Additional arguments (passed to NextMethod)
#' @return Invisible x
#' @rdname hcpx_ya
#' @export
#' @method print hcpx_tbl
print.hcpx_tbl <- function(x, ...) {
  kind <- attr(x, "kind")
  if (!is.null(kind)) {
    cli::cli_h3("hcpx {kind}")
  }

  # Print the underlying table
  NextMethod()

  # Show helpful hints based on kind
  if (!is.null(kind)) {
    cli::cli_text("")
    hint <- switch(kind,
      "subjects" = "Next: {.code tasks()} or {.code assets()}",
      "tasks" = "Next: {.code assets(bundle = \"tfmri_cifti_min\")} or {.code overview()}",
      "assets" = "Next: {.code plan_download()} |> {.code download()}",
      NULL
    )
    if (!is.null(hint)) {
      cli::cli_alert_info(hint)
    }
  }

  invisible(x)
}

#' Close hcpx connection
#'
#' Closes the database connection associated with an hcpx handle.
#'
#' @param h hcpx handle
#' @return Invisible NULL
#' @export
#' @examples
#' \dontrun{
#' h <- hcpx_ya()
#' hcpx_close(h)
#' }
hcpx_close <- function(h) {
  if (!is.null(h$con)) {
    catalog_disconnect(h$con)
    h$con <- NULL
  }
  invisible(NULL)
}
