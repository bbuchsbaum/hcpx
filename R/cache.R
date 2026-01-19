# Cache management and ledger functions

# --- Ledger Core Functions ---

#' Record an asset in the cache ledger
#'
#' Adds or updates an entry in the ledger after a file is downloaded.
#'
#' @param h hcpx handle
#' @param asset_id Asset identifier
#' @param local_path Local file path where asset is cached
#' @param size_bytes File size in bytes
#' @param checksum_md5 MD5 checksum (optional)
#' @param etag ETag from server (optional)
#' @param backend Backend used for download (optional)
#' @return Invisible h
#' @keywords internal
ledger_record <- function(h, asset_id, local_path, size_bytes = NULL,
                          checksum_md5 = NULL, etag = NULL, backend = NULL) {
  con <- get_con(h)
  now <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")

  # Check if entry exists to preserve pinned status
  existing <- DBI::dbGetQuery(con, "SELECT pinned FROM ledger WHERE asset_id = ?",
                               params = list(asset_id))
  pinned <- if (nrow(existing) > 0) existing$pinned[1] else 0L

  # Delete existing entry if present
  DBI::dbExecute(con, "DELETE FROM ledger WHERE asset_id = ?",
                 params = list(asset_id))

  # Insert new entry
  DBI::dbExecute(con, "
    INSERT INTO ledger
      (asset_id, local_path, downloaded_at, last_accessed, size_bytes,
       checksum_md5, etag, backend, pinned)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
  ", params = list(
    asset_id,
    local_path,
    now,
    now,
    if (is.null(size_bytes)) NA_integer_ else as.integer(size_bytes),
    if (is.null(checksum_md5)) NA_character_ else checksum_md5,
    if (is.null(etag)) NA_character_ else etag,
    if (is.null(backend)) NA_character_ else backend,
    pinned
  ))

  invisible(h)
}

#' Look up cached assets in the ledger
#'
#' Returns local paths for assets that are in the cache.
#'
#' @param h hcpx handle
#' @param asset_ids Character vector of asset IDs to look up
#' @return Tibble with columns: asset_id, local_path, size_bytes, downloaded_at, pinned
#' @keywords internal
ledger_lookup <- function(h, asset_ids) {
  # Handle empty input
  if (length(asset_ids) == 0) {
    return(tibble::tibble(
      asset_id = character(),
      local_path = character(),
      size_bytes = integer(),
      downloaded_at = character(),
      pinned = integer()
    ))
  }

  con <- get_con(h)

  # Query for all requested asset_ids
  placeholders <- paste(rep("?", length(asset_ids)), collapse = ", ")
  query <- sprintf("
    SELECT asset_id, local_path, size_bytes, downloaded_at, pinned
    FROM ledger
    WHERE asset_id IN (%s)
  ", placeholders)

  result <- DBI::dbGetQuery(con, query, params = as.list(asset_ids))
  tibble::as_tibble(result)
}

#' Touch assets in the ledger (update last_accessed)
#'
#' Updates the last_accessed timestamp for LRU tracking.
#'
#' @param h hcpx handle
#' @param asset_ids Character vector of asset IDs to touch
#' @return Invisible h
#' @keywords internal
ledger_touch <- function(h, asset_ids) {
  # Guard against empty input
  if (length(asset_ids) == 0) return(invisible(h))

  con <- get_con(h)
  now <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")

  placeholders <- paste(rep("?", length(asset_ids)), collapse = ", ")
  query <- sprintf("
    UPDATE ledger
    SET last_accessed = ?
    WHERE asset_id IN (%s)
  ", placeholders)

  DBI::dbExecute(con, query, params = c(list(now), as.list(asset_ids)))

  invisible(h)
}

#' Remove assets from ledger
#'
#' Removes entries from the ledger (does not delete files).
#'
#' @param h hcpx handle
#' @param asset_ids Character vector of asset IDs to remove
#' @return Invisible h
#' @keywords internal
ledger_remove <- function(h, asset_ids) {
  # Guard against empty input
  if (length(asset_ids) == 0) return(invisible(h))

  con <- get_con(h)

  placeholders <- paste(rep("?", length(asset_ids)), collapse = ", ")
  query <- sprintf("DELETE FROM ledger WHERE asset_id IN (%s)", placeholders)
  DBI::dbExecute(con, query, params = as.list(asset_ids))

  invisible(h)
}

#' Check which assets are cached
#'
#' @param h hcpx handle
#' @param asset_ids Character vector of asset IDs
#' @return Logical vector (TRUE if cached)
#' @keywords internal
ledger_is_cached <- function(h, asset_ids) {
  cached <- ledger_lookup(h, asset_ids)
  asset_ids %in% cached$asset_id
}

# --- Cache Path Functions ---

#' Get cache file path for an asset
#'
#' Computes the local cache path for an asset based on its metadata.
#' Layout: cache_root/files/<release>/<subject_id>/<...mirrored remote path>
#'
#' @param h hcpx handle
#' @param release Release name (e.g., "HCP_1200")
#' @param subject_id Subject ID
#' @param remote_path Remote path suffix
#' @return Local file path
#' @keywords internal
cache_path <- function(h, release, subject_id, remote_path) {
  cache_root <- h$cache$root

  # Check for path traversal patterns (.. in path components)
  # This catches: "../", "..\\", leading "..", or standalone ".."
  if (grepl("(^|[/\\\\])\\.\\.", remote_path)) {
    cli::cli_abort(c(
      "Invalid path: traversal outside cache directory not allowed",
      "x" = "Remote path: {.path {remote_path}}"
    ))
  }

  full_path <- file.path(cache_root, "files", release, subject_id, remote_path)
  full_path
}

#' Get cache path from asset row
#'
#' @param h hcpx handle
#' @param asset Tibble row with release, subject_id, remote_path columns
#' @return Local file path
#' @keywords internal
cache_path_from_asset <- function(h, asset) {
  cache_path(h, asset$release, asset$subject_id, basename(asset$remote_path))
}

#' Ensure cache directory exists for a path
#'
#' @param path File path
#' @return Invisible path
#' @keywords internal
cache_ensure_dir <- function(path) {
  dir_path <- dirname(path)
  if (!dir.exists(dir_path)) {
    dir.create(dir_path, recursive = TRUE, showWarnings = FALSE)
  }
  invisible(path)
}

# --- Public Cache Functions ---

#' Cache status summary
#'
#' Returns a summary of cache usage including file counts, sizes, and
#' breakdown by release and kind.
#'
#' @param h hcpx handle
#' @return List with cache statistics
#' @seealso [cache_prune()], [cache_list()], [cache_pin()], [cache_unpin()]
#' @export
#' @examples
#' \dontrun{
#' h <- hcpx_ya()
#' cache_status(h)
#' }
cache_status <- function(h) {
  con <- get_con(h)

  # Get overall counts
  stats <- DBI::dbGetQuery(con, "
    SELECT
      COUNT(*) as n_files,
      COALESCE(SUM(size_bytes), 0) as total_bytes,
      COUNT(CASE WHEN pinned = 1 THEN 1 END) as n_pinned
    FROM ledger
  ")

  # Get breakdown by backend
  by_backend <- DBI::dbGetQuery(con, "
    SELECT backend, COUNT(*) as n_files, COALESCE(SUM(size_bytes), 0) as total_bytes
    FROM ledger
    GROUP BY backend
  ")

  list(
    n_files = stats$n_files,
    total_bytes = stats$total_bytes,
    total_gb = stats$total_bytes / 1e9,
    n_pinned = stats$n_pinned,
    max_size_gb = h$cache$max_size_gb,
    root = h$cache$root,
    by_backend = tibble::as_tibble(by_backend)
  )
}

#' Prune cache by LRU
#'
#' Removes least-recently-used files until cache is under the target size.
#' Pinned files are never evicted.
#'
#' @param h hcpx handle
#' @param max_size_gb Target maximum size in GB (defaults to handle setting)
#' @param dry_run If TRUE, show what would be deleted without deleting
#' @return Tibble of pruned/to-be-pruned files
#' @export
#' @examples
#' \dontrun{
#' h <- hcpx_ya()
#' cache_prune(h, max_size_gb = 100)
#' cache_prune(h, dry_run = TRUE)  # Preview
#' }
cache_prune <- function(h, max_size_gb = NULL, dry_run = FALSE) {
  con <- get_con(h)

  if (is.null(max_size_gb)) {
    max_size_gb <- h$cache$max_size_gb
  }
  max_bytes <- max_size_gb * 1e9

  # Get current total size
  current_size <- DBI::dbGetQuery(con, "SELECT COALESCE(SUM(size_bytes), 0) as total FROM ledger")$total

  if (current_size <= max_bytes) {
    cli::cli_alert_info("Cache size ({.val {sprintf('%.2f', current_size/1e9)}} GB) is under limit ({.val {max_size_gb}} GB)")
    return(tibble::tibble(asset_id = character(), local_path = character(), size_bytes = integer()))
  }

  bytes_to_free <- current_size - max_bytes

  # Get files to prune (oldest accessed first, excluding pinned)
  candidates <- DBI::dbGetQuery(con, "
    SELECT asset_id, local_path, size_bytes
    FROM ledger
    WHERE pinned = 0
    ORDER BY last_accessed ASC
  ")

  if (nrow(candidates) == 0) {
    cli::cli_warn("All cached files are pinned. Cannot prune.")
    return(tibble::tibble(asset_id = character(), local_path = character(), size_bytes = integer()))
  }

  # Calculate cumulative size and find cutoff
  candidates$cumsum <- cumsum(candidates$size_bytes)
  to_prune <- candidates[candidates$cumsum <= bytes_to_free | seq_len(nrow(candidates)) == 1, ]

  # If first file alone is bigger than bytes_to_free, still include it
  if (nrow(to_prune) == 0) {
    to_prune <- candidates[1, , drop = FALSE]
  }

  # Extend until we've freed enough
  while (sum(to_prune$size_bytes) < bytes_to_free && nrow(to_prune) < nrow(candidates)) {
    to_prune <- candidates[seq_len(nrow(to_prune) + 1), ]
  }

  to_prune <- to_prune[, c("asset_id", "local_path", "size_bytes")]

  if (dry_run) {
    cli::cli_alert_info("Dry run: would prune {.val {nrow(to_prune)}} files ({.val {sprintf('%.2f', sum(to_prune$size_bytes)/1e9)}} GB)")
    return(tibble::as_tibble(to_prune))
  }

  # Actually delete files and remove from ledger
  for (i in seq_len(nrow(to_prune))) {
    if (file.exists(to_prune$local_path[i])) {
      unlink(to_prune$local_path[i])
    }
  }

  ledger_remove(h, to_prune$asset_id)

  cli::cli_alert_success("Pruned {.val {nrow(to_prune)}} files ({.val {sprintf('%.2f', sum(to_prune$size_bytes)/1e9)}} GB)")
  tibble::as_tibble(to_prune)
}

#' Pin assets so they are not evicted
#'
#' Pinned assets are protected from cache pruning.
#'
#' @param h hcpx handle
#' @param asset_ids Character vector of asset IDs to pin
#' @return Invisible h
#' @export
#' @examples
#' \dontrun{
#' h <- hcpx_ya()
#' cache_pin(h, c("asset_001", "asset_002"))
#' }
cache_pin <- function(h, asset_ids) {
  # Guard against empty input
  if (length(asset_ids) == 0) {
    cli::cli_alert_info("No assets to pin")
    return(invisible(h))
  }

  con <- get_con(h)

  placeholders <- paste(rep("?", length(asset_ids)), collapse = ", ")
  query <- sprintf("UPDATE ledger SET pinned = 1 WHERE asset_id IN (%s)", placeholders)
  DBI::dbExecute(con, query, params = as.list(asset_ids))

  cli::cli_alert_success("Pinned {.val {length(asset_ids)}} assets")
  invisible(h)
}

#' Unpin assets to allow eviction
#'
#' Removes pin protection from assets, allowing them to be pruned.
#'
#' @param h hcpx handle
#' @param asset_ids Character vector of asset IDs to unpin
#' @return Invisible h
#' @export
#' @examples
#' \dontrun{
#' h <- hcpx_ya()
#' cache_unpin(h, c("asset_001", "asset_002"))
#' }
cache_unpin <- function(h, asset_ids) {
  # Guard against empty input
  if (length(asset_ids) == 0) {
    cli::cli_alert_info("No assets to unpin")
    return(invisible(h))
  }

  con <- get_con(h)

  placeholders <- paste(rep("?", length(asset_ids)), collapse = ", ")
  query <- sprintf("UPDATE ledger SET pinned = 0 WHERE asset_id IN (%s)", placeholders)
  DBI::dbExecute(con, query, params = as.list(asset_ids))

  cli::cli_alert_success("Unpinned {.val {length(asset_ids)}} assets")
  invisible(h)
}

#' List cached files
#'
#' @param h hcpx handle
#' @param pinned_only If TRUE, only show pinned files
#' @return Tibble of cached files
#' @export
#' @examples
#' \dontrun{
#' h <- hcpx_ya()
#' cache_list(h)
#' }
cache_list <- function(h, pinned_only = FALSE) {
  con <- get_con(h)

  query <- if (pinned_only) {
    "SELECT * FROM ledger WHERE pinned = 1 ORDER BY last_accessed DESC"
  } else {
    "SELECT * FROM ledger ORDER BY last_accessed DESC"
  }

  result <- DBI::dbGetQuery(con, query)
  tibble::as_tibble(result)
}
