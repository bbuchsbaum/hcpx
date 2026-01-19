# neuroim2_utils.R - Convenience wrappers for neuroim2 operations
#
# These functions wrap neuroim2 operations with hcpx context, making
# common operations more ergonomic within the hcpx workflow.
#
# See PRD Section 15.8 for specification.

#' Extract time series at MNI coordinates
#'
#' Convenience function to extract time series from a NeuroVec at specified
#' MNI coordinates. Handles coordinate conversion automatically.
#'
#' @param vec NeuroVec from [load_asset()] or [load_run()]
#' @param coords Either:
#'   - A numeric vector of length 3 (single MNI coordinate)
#'   - A matrix with 3 columns (multiple MNI coordinates, one per row)
#'
#' @return If single coordinate: numeric vector (time series).
#'   If multiple coordinates: matrix with dimensions (n_timepoints x n_coords).
#'
#' @examples
#' \dontrun{
#' # Extract time series from left motor cortex
#' ts <- extract_series(run_data$dtseries, c(-38, -22, 56))
#'
#' # Extract from multiple coordinates
#' coords <- rbind(
#'   c(-38, -22, 56),  # Left motor
#'   c(38, -22, 56)    # Right motor
#' )
#' ts_matrix <- extract_series(run_data$dtseries, coords)
#' }
#'
#' @export
extract_series <- function(vec, coords) {
  single_coord <- is.numeric(coords) && length(coords) == 3

  # Ensure coords is a matrix
  if (single_coord) {
    coords <- matrix(coords, nrow = 1)
  }

  if (!is.matrix(coords) || ncol(coords) != 3) {
    cli::cli_abort("coords must be a 3-element vector or Nx3 matrix of MNI coordinates")
  }

  # Convert MNI (world) coordinates to voxel grid coordinates
  vec_space <- neuroim2::space(vec)
  vox_coords <- neuroim2::coord_to_grid(vec_space, coords)

  # Extract time series using neuroim2::series()
  if (is.numeric(vox_coords) && length(vox_coords) == 3) {
    vox_coords <- matrix(vox_coords, nrow = 1)
  }

  ts <- neuroim2::series(vec, vox_coords)
  if (single_coord) {
    return(drop(ts))
  }
  ts
}


#' Z-score normalize time series (per-run aware)
#'
#' Normalizes a NeuroVec time series to zero mean and unit variance.
#' Supports per-run normalization to handle multi-run concatenated data.
#'
#' @param vec NeuroVec to normalize
#' @param run_boundaries Optional integer vector of run lengths for split
#'   normalization. If provided, each run is normalized independently before
#'   being recombined. This prevents run-to-run baseline differences from
#'   affecting the normalization.
#'
#' @return Normalized NeuroVec with same dimensions as input.
#'
#' @examples
#' \dontrun{
#' # Simple normalization
#' vec_z <- zscore(run_data$dtseries)
#'
#' # Per-run normalization for concatenated data (2 runs of 405 TRs each)
#' vec_z <- zscore(combined_data$dtseries, run_boundaries = c(405, 405))
#' }
#'
#' @export
zscore <- function(vec, run_boundaries = NULL) {
  if (is.null(run_boundaries)) {
    # Simple whole-scan normalization
    neuroim2::scale_series(vec, center = TRUE, scale = TRUE)
  } else {
    # Per-run normalization

    n_timepoints <- dim(vec)[4]
    if (sum(run_boundaries) != n_timepoints) {
      cli::cli_abort(c(
        "run_boundaries do not match data length",
        "x" = "Sum of run_boundaries: {sum(run_boundaries)}",
        "x" = "Number of timepoints: {n_timepoints}"
      ))
    }

    run_factor <- rep(seq_along(run_boundaries), run_boundaries)
    neuroim2::split_scale(vec, factor(run_factor), center = TRUE, scale = TRUE)
  }
}


#' Create spherical ROI from MNI coordinates
#'
#' Creates a spherical region of interest centered at specified MNI coordinates.
#' Wraps neuroim2::spherical_roi() with hcpx context.
#'
#' @param h hcpx handle (used to get default space parameters)
#' @param center MNI coordinates (x, y, z) for the sphere center
#' @param radius_mm Radius in millimeters. Default is 6mm.
#' @param space Space name ("MNINonLinear", "T1w", etc.) to determine the
#'   coordinate system. Default is "MNINonLinear".
#'
#' @return An ROIVol object that can be used with neuroim2::series_roi()
#'   or passed to other ROI-based functions.
#'
#' @examples
#' \dontrun{
#' # Create 8mm sphere at DLPFC
#' dlpfc_roi <- roi_sphere(h, center = c(-46, 10, 30), radius_mm = 8)
#'
#' # Extract time series from ROI
#' roi_ts <- neuroim2::series_roi(run_data$dtseries, dlpfc_roi)
#' }
#'
#' @export
roi_sphere <- function(h, center, radius_mm = 6, space = "MNINonLinear") {
  h <- get_hcpx(h)

  # Validate center
  if (!is.numeric(center) || length(center) != 3) {
    cli::cli_abort("center must be a numeric vector of length 3 (MNI x, y, z)")
  }

  # 1. Get template NeuroSpace for the specified space
  template_space <- get_template_space(h, space)

  # 2. Convert MNI (world) coordinates to voxel coordinates
  # Transform from world to voxel space using the template's affine
  vox_center <- neuroim2::coord_to_grid(template_space, matrix(center, nrow = 1))

  # 3. Calculate radius in voxels (assumes isotropic spacing)
  voxel_spacing <- neuroim2::spacing(template_space)
  radius_vox <- radius_mm / mean(voxel_spacing)

  # 4. Create spherical ROI
  # neuroim2::spherical_roi creates a logical mask of voxels within the sphere
  roi <- neuroim2::spherical_roi(template_space, vox_center, radius_vox)

  # Add metadata
  attr(roi, "center_mni") <- center
  attr(roi, "radius_mm") <- radius_mm
  attr(roi, "space_name") <- space

  roi
}


#' Get template NeuroSpace for a given space name
#'
#' Returns a NeuroSpace object representing the coordinate system for
#' standard HCP spaces like MNINonLinear or T1w.
#'
#' @param h hcpx handle
#' @param space Space name: "MNINonLinear" (default), "T1w", etc.
#'
#' @return NeuroSpace object with appropriate dimensions, spacing, and transforms.
#'
#' @keywords internal
get_template_space <- function(h, space = "MNINonLinear") {
  # Standard HCP MNINonLinear space parameters
  if (space == "MNINonLinear") {
    return(neuroim2::NeuroSpace(
      dim = c(91L, 109L, 91L),
      spacing = c(2, 2, 2),
      origin = c(-90, -126, -72)  # Approximate MNI origin
    ))
  }

  cli::cli_abort("Unknown space: {space}")
}


#' Concatenate multiple NeuroVec objects
#'
#' Combines multiple NeuroVec objects along the time dimension.
#' Wrapper around neuroim2::concat() with validation.
#'
#' @param ... NeuroVec objects to concatenate
#' @param along Dimension to concatenate along (default 4 = time)
#'
#' @return Combined NeuroVec with all time points.
#'
#' @export
#' @examples
#' \dontrun{
#' h <- hcpx_ya()
#' run1 <- load_run(h, "100307_WM_LR_1")$dtseries
#' run2 <- load_run(h, "100307_WM_LR_2")$dtseries
#' combined <- concat_vecs(run1, run2)
#' }
concat_vecs <- function(..., along = 4L) {
  vecs <- unname(list(...))

  if (length(vecs) == 0) {
    cli::cli_abort("No vectors provided to concatenate")
  }

  if (!identical(along, 4L)) {
    cli::cli_abort("Only concatenation along time (along = 4L) is supported")
  }

  if (length(vecs) == 1) {
    return(vecs[[1]])
  }

  # Validate spatial dimensions match
  ref_dims <- dim(vecs[[1]])[1:3]
  for (i in 2:length(vecs)) {
    if (!identical(dim(vecs[[i]])[1:3], ref_dims)) {
      cli::cli_abort(c(
        "Spatial dimensions do not match",
        "x" = "Vector 1 has dimensions: {paste(ref_dims, collapse = ' x ')}",
        "x" = "Vector {i} has dimensions: {paste(dim(vecs[[i]])[1:3], collapse = ' x ')}"
      ))
    }
  }

  # Use neuroim2 concat
  do.call(neuroim2::concat, vecs)
}
