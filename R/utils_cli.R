# CLI printing utilities for premium UX

#' Format bytes as human-readable size
#'
#' @param bytes Numeric bytes
#' @return Character formatted size
#' @keywords internal
format_bytes <- function(bytes) {
 if (is.na(bytes)) return("unknown")
 units <- c("B", "KB", "MB", "GB", "TB")
 idx <- 1
 while (bytes >= .BYTES_PER_KB && idx < length(units)) {
   bytes <- bytes / .BYTES_PER_KB
   idx <- idx + 1
 }
 sprintf("%.1f %s", bytes, units[idx])
}

#' Print a progress bar
#'
#' @param current Current progress
#' @param total Total items
#' @param width Bar width in characters
#' @return Invisible NULL
#' @keywords internal
cli_progress <- function(current, total, width = 40) {
 # Uses cli package progress bars
 # TODO: Implement in hcpx-zks
 invisible(NULL)
}
