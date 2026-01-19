# Backend interface definitions

#' List files at a path prefix
#'
#' @param backend Backend object
#' @param prefix Path prefix to list
#' @param ... Additional arguments
#' @return Tibble with remote_path, size_bytes, last_modified
#' @keywords internal
backend_list <- function(backend, prefix, ...) {
 UseMethod("backend_list")
}

#' Get file metadata via HEAD request
#'
#' @param backend Backend object
#' @param remote_path Remote file path
#' @param ... Additional arguments
#' @return Tibble with size_bytes, etag, last_modified
#' @keywords internal
backend_head <- function(backend, remote_path, ...) {
 UseMethod("backend_head")
}
#' Generate presigned URL for download
#'
#' @param backend Backend object
#' @param remote_path Remote file path
#' @param expires_sec Seconds until URL expires
#' @return Character URL
#' @keywords internal
backend_presign <- function(backend, remote_path, expires_sec = .DEFAULT_PRESIGN_EXPIRY_SEC) {
 UseMethod("backend_presign")
}

#' Download a file
#'
#' @param backend Backend object
#' @param url_or_path URL or path to download
#' @param dest Local destination path
#' @param resume Whether to resume partial downloads
#' @param ... Additional arguments
#' @return Tibble with status, bytes
#' @keywords internal
backend_download <- function(backend, url_or_path, dest, resume = TRUE, ...) {
 UseMethod("backend_download")
}
