# Checksum and hashing utilities

#' Compute MD5 hash of a file
#'
#' @param path File path
#' @return Character MD5 hash
#' @keywords internal
hash_md5 <- function(path) {
  # Input validation
  if (is.null(path) || length(path) == 0 || is.na(path) || !nzchar(path)) {
    cli::cli_abort("path must be a non-empty string")
  }
  if (!file.exists(path)) {
    cli::cli_abort("File not found: {.path {path}}")
  }
  digest::digest(file = path, algo = "md5")
}

#' Generate deterministic asset ID from release and path
#'
#' @param release Release name (e.g., "HCP_1200")
#' @param remote_path Remote path
#' @return Character asset ID
#' @keywords internal
asset_id_from_path <- function(release, remote_path) {
  # Input validation
  if (is.null(release) || length(release) == 0 || is.na(release) || !nzchar(release)) {
    cli::cli_abort("release must be a non-empty string")
  }
  if (is.null(remote_path) || length(remote_path) == 0 || is.na(remote_path) || !nzchar(remote_path)) {
    cli::cli_abort("remote_path must be a non-empty string")
  }
  digest::digest(paste0(release, ":", remote_path), algo = "xxhash32")
}

#' Verify file checksum
#'
#' @param path File path
#' @param expected Expected checksum
#' @param algo Hash algorithm
#' @return Logical TRUE if match
#' @keywords internal
verify_checksum <- function(path, expected, algo = "md5") {
  # Input validation
  if (is.null(path) || length(path) == 0 || is.na(path) || !nzchar(path)) {
    cli::cli_abort("path must be a non-empty string")
  }
  if (!file.exists(path)) {
    cli::cli_abort("File not found: {.path {path}}")
  }
  if (is.null(expected) || length(expected) == 0 || is.na(expected)) {
    cli::cli_abort("expected checksum must be provided")
  }
  actual <- digest::digest(file = path, algo = algo)
  identical(actual, expected)
}
