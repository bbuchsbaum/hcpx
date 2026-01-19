# Local filesystem backend implementation
#
# For users who have HCP data mirrored locally or on a network drive.
# Supports copy, symlink, or hardlink modes for "downloading" to cache.

#' Create local backend
#'
#' Creates a backend for accessing HCP data from a local filesystem mirror.
#' This is useful for users who have downloaded HCP data to a local drive
#' or have access to a shared network mount.
#'
#' @param root Root directory of local HCP mirror
#' @param link_mode How to handle files: "copy" (default), "symlink", or "hardlink"
#' @return hcpx_backend_local object
#' @keywords internal
hcpx_backend_local <- function(root, link_mode = c("copy", "symlink", "hardlink")) {
  link_mode <- match.arg(link_mode)

  if (!dir.exists(root)) {
    cli::cli_warn("Local backend root does not exist: {.path {root}}")
  }

  structure(
    list(
      type = "local",
      root = normalizePath(root, mustWork = FALSE),
      link_mode = link_mode
    ),
    class = c("hcpx_backend_local", "hcpx_backend")
  )
}

#' Build local path for a remote path
#'
#' @param backend Local backend object
#' @param remote_path Remote path within the mirror
#' @return Character path
#' @keywords internal
local_path <- function(backend, remote_path) {
  # Remove leading slash if present
  remote_path <- sub("^/", "", remote_path)
  full_path <- file.path(backend$root, remote_path)

  # Validate path stays within root (prevent path traversal)
  normalized_path <- normalizePath(full_path, mustWork = FALSE)
  normalized_root <- normalizePath(backend$root, mustWork = FALSE)

  if (!startsWith(normalized_path, normalized_root)) {
    cli::cli_abort(c(
      "Invalid path: traversal outside root directory not allowed",
      "x" = "Path: {.path {remote_path}}"
    ))
  }

  full_path
}

#' List files via local backend
#'
#' @rdname backend_list
#' @export
#' @method backend_list hcpx_backend_local
backend_list.hcpx_backend_local <- function(backend, prefix, ...) {
  prefix <- sub("^/", "", prefix)
  search_dir <- file.path(backend$root, prefix)

  if (!dir.exists(search_dir)) {
    # Maybe prefix is a file pattern, try parent directory
    search_dir <- dirname(search_dir)
    if (!dir.exists(search_dir)) {
      return(tibble::tibble(
        remote_path = character(),
        size_bytes = integer(),
        last_modified = character()
      ))
    }
  }

  # List files recursively
  files <- list.files(search_dir, recursive = TRUE, full.names = TRUE)

  if (length(files) == 0) {
    return(tibble::tibble(
      remote_path = character(),
      size_bytes = integer(),
      last_modified = character()
    ))
  }

  # Get file info
  info <- file.info(files)

  # Convert to relative paths
  rel_paths <- sub(paste0("^", backend$root, "/?"), "", files)

  tibble::tibble(
    remote_path = rel_paths,
    size_bytes = as.integer(info$size),
    last_modified = as.character(info$mtime)
  )
}

#' Get file metadata via local backend
#'
#' @rdname backend_head
#' @export
#' @method backend_head hcpx_backend_local
backend_head.hcpx_backend_local <- function(backend, remote_path, ...) {
  path <- local_path(backend, remote_path)

  if (!file.exists(path)) {
    cli::cli_abort("File not found: {.path {path}}")
  }

  info <- file.info(path)

  tibble::tibble(
    size_bytes = as.integer(info$size),
    etag = NA_character_,  # Local files don't have etags
    last_modified = as.character(info$mtime)
  )
}

#' Generate presigned URL via local backend
#'
#' @rdname backend_presign
#' @export
#' @method backend_presign hcpx_backend_local
backend_presign.hcpx_backend_local <- function(backend, remote_path, expires_sec = .DEFAULT_PRESIGN_EXPIRY_SEC) {
  # Local backend returns the actual file path (no signing needed)
  local_path(backend, remote_path)
}

#' Download file via local backend
#'
#' @rdname backend_download
#' @export
#' @method backend_download hcpx_backend_local
backend_download.hcpx_backend_local <- function(backend, url_or_path, dest, resume = TRUE, ...) {
  # Determine source path
  if (grepl("^file://", url_or_path)) {
    src <- sub("^file://", "", url_or_path)
  } else if (file.exists(url_or_path)) {
    src <- url_or_path
  } else {
    src <- local_path(backend, url_or_path)
  }

  if (!file.exists(src)) {
    cli::cli_abort("Source file not found: {.path {src}}")
  }

  # Ensure destination directory exists
  dest_dir <- dirname(dest)
  if (!dir.exists(dest_dir)) {
    dir.create(dest_dir, recursive = TRUE)
  }

  # Handle based on link mode
  success <- switch(backend$link_mode,
    "copy" = {
      file.copy(src, dest, overwrite = TRUE)
    },
    "symlink" = {
      if (file.exists(dest)) unlink(dest)
      file.symlink(src, dest)
    },
    "hardlink" = {
      if (file.exists(dest)) unlink(dest)
      file.link(src, dest)
    }
  )

  if (!success) {
    cli::cli_abort("Failed to {backend$link_mode} file to cache")
  }

  # Return result
  final_size <- file.info(dest)$size

  tibble::tibble(
    status = "success",
    bytes = as.integer(final_size)
  )
}
