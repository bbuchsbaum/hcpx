# Package startup and shutdown hooks

# Declare global variables used in NSE (dplyr) contexts
# This prevents R CMD check NOTEs about "no visible binding"
utils::globalVariables(c(
  # Column names used in dplyr expressions
  "asset_id", "subject_id", "task", "direction", "run",
  "release", "remote_path", "local_path", "size_bytes",
  "file_type", "kind", "space", "cached", "n", "total",
  # Additional variables
  ".data"
))

#' @importFrom utils head
NULL

.onLoad <- function(libname, pkgname) {
  # Initialize package-level state if needed
}

.onAttach <- function(libname, pkgname) {
  packageStartupMessage("hcpx: HCP eXplorer - A Dataset Interface for Human Connectome Project Data")
}
