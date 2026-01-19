# Structured error conditions

#' Create an hcpx error condition
#'
#' @param message Error message
#' @param class Subclass name
#' @param ... Additional condition data
#' @return Condition object
#' @keywords internal
hcpx_error <- function(message, class = NULL, ...) {
 # TODO: Implement in hcpx-4gm
 classes <- c(class, "hcpx_error", "error", "condition")
 structure(
   list(message = message, ...),
   class = classes
 )
}

#' Signal that a resource was not found
#'
#' @param what Description of missing resource
#' @param path Path or identifier
#' @keywords internal
hcpx_not_found <- function(what, path = NULL) {
 msg <- if (!is.null(path)) {
   sprintf("%s not found: %s", what, path)
 } else {
   sprintf("%s not found", what)
 }
 stop(hcpx_error(msg, "hcpx_not_found", what = what, path = path))
}

#' Signal an authentication error
#'
#' @param message Error message
#' @param backend Backend type
#' @keywords internal
hcpx_auth_error <- function(message, backend = NULL) {
 stop(hcpx_error(message, "hcpx_auth_error", backend = backend))
}

#' Signal a download error
#'
#' @param message Error message
#' @param path Remote path
#' @param status HTTP status code (if applicable)
#' @keywords internal
hcpx_download_error <- function(message, path = NULL, status = NULL) {
 stop(hcpx_error(message, "hcpx_download_error", path = path, status = status))
}
