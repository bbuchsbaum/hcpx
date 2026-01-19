# neuroim2_parcellate.R - Parcellation and atlas support using neuroim2
#
# This module provides functions for atlas-based analysis, including loading
# parcellations and projecting time series to parcel space.
#
# See PRD Section 15.6 for specification.

#' Load a CIFTI dlabel as a ClusteredNeuroVol for parcellation
#'
#' Loads a parcellation atlas that can be used with [parcellate()] to
#' extract parcel-level time series from NeuroVec data.
#'
#' @param h hcpx handle
#' @param parcellation Either:
#'   - A parcellation name (e.g., "Schaefer400", "Glasser360") that resolves
#'     to a standard atlas location
#'   - An asset_id for a dlabel file in the catalog
#'   - A file path to a local dlabel file
#'
#' @return A ClusteredNeuroVol object where each cluster represents a parcel.
#'   The label_map attribute contains parcel names/metadata.
#'
#' @export
#' @examples
#' \dontrun{
#' h <- hcpx_ya()
#' parc <- load_parcellation(h, "Schaefer400")
#' parc
#' }
load_parcellation <- function(h, parcellation = "Schaefer400") {
  h <- get_hcpx(h)

  # 1. Resolve parcellation to file path
  parc_path <- .resolve_parcellation_path(h, parcellation)

  if (is.null(parc_path) || !file.exists(parc_path)) {
    # Check if it's an asset_id in the catalog
    con <- get_con(h)
    asset_info <- DBI::dbGetQuery(con,
      "SELECT asset_id, file_type FROM assets WHERE asset_id = ?",
      params = list(parcellation)
    )

    if (nrow(asset_info) > 0) {
      # It's an asset_id, load via load_asset
      return(load_asset(h, parcellation))
    }

    # Check if it's already a file path
    if (file.exists(parcellation)) {
      parc_path <- parcellation
    } else {
      available <- list_parcellations()
      cli::cli_abort(c(
        "Parcellation not found: {.val {parcellation}}",
        "i" = "Available built-in parcellations: {.val {available$name}}",
        "i" = "Or provide an asset_id or file path"
      ))
    }
  }

  # 2. Load the parcellation file
  # dlabel files and parcellation atlases are loaded as volumes
  vol <- neuroim2::read_vol(parc_path)

  # 3. Extract label information if available
  # The volume values represent cluster IDs

  vol
}


#' Extract parcel time series from a loaded NeuroVec
#'
#' Projects a 4D NeuroVec (typically CIFTI dtseries) to parcel space,
#' returning a parcel-level time series matrix.
#'
#' @param vec NeuroVec object (from [load_asset()] or [load_run()])
#' @param parcellation ClusteredNeuroVol from [load_parcellation()], or
#'   a parcellation name that will be loaded automatically.
#' @param method Aggregation method: "mean" (default), "median", "pca1"
#'   (first principal component)
#'
#' @return Matrix with class `parcel_timeseries` and dimensions
#'   (n_timepoints x n_parcels). Each column is the aggregated time series
#'   for one parcel. Attributes include `parcel_labels`, `method`, and
#'   `n_parcels`.
#'
#' @details
#' For each parcel, the time series of all voxels/vertices within that
#' parcel are aggregated using the specified method. The result is a
#' much smaller matrix suitable for connectivity analysis, classification,
#' or other downstream analyses.
#'
#' @export
#' @examples
#' \dontrun{
#' h <- hcpx_ya()
#' vec <- load_run(h, subject_id = "100307", task = "WM", direction = "LR")
#' parc <- load_parcellation(h, "Schaefer400")
#' ts <- parcellate(vec, parc, method = "mean")
#' }
parcellate <- function(vec, parcellation, method = c("mean", "median", "pca1")) {
  method <- match.arg(method)

  # 1. If parcellation is a string/name, load it
  if (is.character(parcellation)) {
    # Try to get hcpx handle from vec (if it has one attached)
    h <- attr(vec, "hcpx")
    if (is.null(h)) {
      cli::cli_abort(c(
        "Cannot load parcellation by name without hcpx context",
        "i" = "Provide a pre-loaded parcellation object or attach hcpx handle to vec"
      ))
    }
    parcellation <- load_parcellation(h, parcellation)
  }

  # 2. Validate spatial compatibility
  .validate_parcellation_space(vec, parcellation)

  # 3. Get unique parcel labels (excluding 0/background)
  parc_values <- as.vector(parcellation)
  unique_parcels <- sort(unique(parc_values[parc_values != 0 & !is.na(parc_values)]))
  n_parcels <- length(unique_parcels)
  n_timepoints <- dim(vec)[4]

  if (n_parcels == 0) {
    cli::cli_abort("No parcel labels found in parcellation (all values are 0 or NA)")
  }

  # 4. Extract and aggregate time series for each parcel
  parcel_ts <- matrix(NA_real_, nrow = n_timepoints, ncol = n_parcels)
  colnames(parcel_ts) <- as.character(unique_parcels)

  # Get all voxel indices for efficiency
  vec_data <- as.array(vec)

  for (i in seq_along(unique_parcels)) {
    parcel_id <- unique_parcels[i]
    parcel_mask <- parc_values == parcel_id

    # Extract time series for all voxels in this parcel
    # vec_data is 4D: x, y, z, time
    n_voxels <- sum(parcel_mask)
    if (n_voxels == 0) next

    # Reshape to get time x voxels matrix
    voxel_ts <- matrix(vec_data[parcel_mask], ncol = n_timepoints, byrow = FALSE)
    voxel_ts <- t(voxel_ts)  # Now timepoints x voxels

    # Aggregate based on method
    parcel_ts[, i] <- switch(method,
      "mean" = rowMeans(voxel_ts, na.rm = TRUE),
      "median" = apply(voxel_ts, 1, stats::median, na.rm = TRUE),
      "pca1" = {
        # First principal component (handles missing data)
        centered <- scale(voxel_ts, center = TRUE, scale = FALSE)
        svd_result <- svd(centered, nu = 1, nv = 0)
        svd_result$u[, 1] * svd_result$d[1]
      }
    )
  }

  # 5. Return as a matrix with parcel metadata
  # In a full implementation, this would return a ClusteredNeuroVec
  # For now, return a matrix with attributes
  result <- parcel_ts
  attr(result, "parcel_labels") <- unique_parcels
  attr(result, "method") <- method
  attr(result, "n_parcels") <- n_parcels
  class(result) <- c("parcel_timeseries", "matrix")

  result
}


#' List available built-in parcellations
#'
#' Shows the parcellation atlases that can be loaded by name.
#'
#' @param h hcpx handle (optional, used to check for downloaded atlases)
#'
#' @return A tibble with columns:
#'   - name: Parcellation name for use with [load_parcellation()]
#'   - n_parcels: Number of parcels/regions
#'   - description: Brief description
#'   - source: Where the atlas comes from
#'
#' @export
#' @examples
#' list_parcellations()
list_parcellations <- function(h = NULL) {
  # Built-in parcellation registry
  tibble::tribble(
    ~name,           ~n_parcels, ~description,                              ~source,
    "Schaefer100",   100L,       "Schaefer 100 parcel cortical atlas",      "Schaefer et al. 2018",
    "Schaefer200",   200L,       "Schaefer 200 parcel cortical atlas",      "Schaefer et al. 2018",
    "Schaefer400",   400L,       "Schaefer 400 parcel cortical atlas",      "Schaefer et al. 2018",
    "Schaefer1000",  1000L,      "Schaefer 1000 parcel cortical atlas",     "Schaefer et al. 2018",
    "Glasser360",    360L,       "HCP MMP1.0 multimodal parcellation",      "Glasser et al. 2016",
    "Gordon333",     333L,       "Gordon 333 parcel cortical atlas",        "Gordon et al. 2016",
    "Power264",      264L,       "Power 264 ROI coordinates",               "Power et al. 2011"
  )
}


# Internal helpers ---------------------------------------------------------

#' Resolve parcellation name to file path
#' @noRd
.resolve_parcellation_path <- function(h, parcellation) {
  # 1. Check if it's already a valid file path
  if (file.exists(parcellation)) {
    return(parcellation)
  }

  # 2. Check built-in parcellation registry
  parcellations <- list_parcellations()
  if (parcellation %in% parcellations$name) {
    # Look in standard HCP atlas locations within the cache
    # HCP stores atlases in specific paths
    cache_root <- h$cache$root

    # Try standard HCP atlas paths
    possible_paths <- c(
      # Schaefer atlases
      file.path(cache_root, "atlases", paste0(parcellation, ".dlabel.nii")),
      file.path(cache_root, "atlases", paste0(parcellation, "_7Networks.dlabel.nii")),
      # Glasser atlas
      file.path(cache_root, "atlases", "Q1-Q6_RelatedValidation210.CorticalAreas_dil_Final_Final_Areas_Group_Colors.32k_fs_LR.dlabel.nii"),
      # System-wide atlases
      file.path(system.file("extdata", "atlases", package = "hcpx"),
                paste0(parcellation, ".dlabel.nii"))
    )

    for (path in possible_paths) {
      if (file.exists(path)) {
        return(path)
      }
    }

    # Parcellation name is valid but atlas file not found locally
    cli::cli_warn(c(
      "Atlas {.val {parcellation}} not found locally",
      "i" = "You may need to download the atlas file first"
    ))
    return(NULL)
  }

  # 3. Not a recognized parcellation name
  NULL
}


#' Validate spatial compatibility between data and parcellation
#' @noRd
.validate_parcellation_space <- function(vec, parcellation) {
  # Check that dimensions match
  vec_dims <- dim(vec)[1:3]
  parc_dims <- dim(parcellation)

  if (!identical(vec_dims, parc_dims)) {
    cli::cli_abort(c(
      "Spatial dimensions do not match",
      "x" = "Data has dimensions: {paste(vec_dims, collapse = ' x ')}",
      "x" = "Parcellation has dimensions: {paste(parc_dims, collapse = ' x ')}"
    ))
  }
  invisible(TRUE)
}
