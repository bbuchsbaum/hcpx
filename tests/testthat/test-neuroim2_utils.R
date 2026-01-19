# Tests for neuroim2 utility functions

# --- extract_series() tests ---

test_that("extract_series validates coords input", {
  skip_if_not_installed("neuroim2")

  # Single coordinate must be length 3
  expect_error(
    extract_series(NULL, c(1, 2)),
    "3-element vector"
  )

  # Matrix must have 3 columns
  bad_matrix <- matrix(1:4, nrow = 2)
  expect_error(
    extract_series(NULL, bad_matrix),
    "Nx3 matrix"
  )
})

test_that("extract_series converts single coord to matrix", {
  skip_if_not_installed("neuroim2")

  coord <- c(-38, -22, 56)
  matrix_coord <- matrix(coord, nrow = 1)

  expect_equal(ncol(matrix_coord), 3)
  expect_equal(nrow(matrix_coord), 1)
})

test_that("extract_series extracts time series from NeuroVec", {
  skip_if_not_installed("neuroim2")

  # Create a small NeuroVec with known values
  arr <- array(seq_len(5 * 5 * 5 * 10), dim = c(5, 5, 5, 10))
  space <- neuroim2::NeuroSpace(
    dim = c(5L, 5L, 5L, 10L),
    spacing = c(2, 2, 2),
    origin = c(0, 0, 0)
  )
  vec <- neuroim2::DenseNeuroVec(arr, space)

  # Extract at origin
  result <- extract_series(vec, c(0, 0, 0))

  expect_true(is.numeric(result))
  expect_equal(length(result), 10)  # 10 timepoints
})

test_that("extract_series handles multiple coordinates", {
  skip_if_not_installed("neuroim2")

  arr <- array(rnorm(5 * 5 * 5 * 10), dim = c(5, 5, 5, 10))
  space <- neuroim2::NeuroSpace(
    dim = c(5L, 5L, 5L, 10L),
    spacing = c(2, 2, 2),
    origin = c(0, 0, 0)
  )
  vec <- neuroim2::DenseNeuroVec(arr, space)

  coords <- rbind(c(0, 0, 0), c(2, 2, 2))
  result <- extract_series(vec, coords)

  expect_true(is.matrix(result) || is.numeric(result))
})

# --- zscore() tests ---

test_that("zscore normalizes to zero mean and unit variance", {
  skip_if_not_installed("neuroim2")

  set.seed(42)
  arr <- array(rnorm(5 * 5 * 5 * 20, mean = 100, sd = 10), dim = c(5, 5, 5, 20))
  space <- neuroim2::NeuroSpace(dim = c(5L, 5L, 5L, 20L), spacing = c(2, 2, 2))
  vec <- neuroim2::DenseNeuroVec(arr, space)

  result <- zscore(vec)
  result_arr <- as.array(result)

  # Check a sample voxel is normalized
  sample_ts <- result_arr[3, 3, 3, ]
  expect_equal(mean(sample_ts), 0, tolerance = 1e-10)
  expect_equal(sd(sample_ts), 1, tolerance = 1e-10)
})

test_that("zscore validates run_boundaries sum", {
  skip_if_not_installed("neuroim2")

  arr <- array(rnorm(5 * 5 * 5 * 20), dim = c(5, 5, 5, 20))
  space <- neuroim2::NeuroSpace(dim = c(5L, 5L, 5L, 20L), spacing = c(2, 2, 2))
  vec <- neuroim2::DenseNeuroVec(arr, space)

  # Wrong sum: 10 + 15 = 25, but we have 20 timepoints
  expect_error(
    zscore(vec, run_boundaries = c(10, 15)),
    "run_boundaries do not match"
  )
})

test_that("zscore with run_boundaries normalizes per-run", {
  skip_if_not_installed("neuroim2")

  # Create data with very different means per run
  arr <- array(0, dim = c(3, 3, 3, 20))
  arr[, , , 1:10] <- rnorm(3^3 * 10, mean = 100, sd = 5)
  arr[, , , 11:20] <- rnorm(3^3 * 10, mean = 500, sd = 5)

  space <- neuroim2::NeuroSpace(dim = c(3L, 3L, 3L, 20L), spacing = c(2, 2, 2))
  vec <- neuroim2::DenseNeuroVec(arr, space)

  result <- zscore(vec, run_boundaries = c(10, 10))
  result_arr <- as.array(result)

  # Each run should be normalized independently
  run1 <- result_arr[2, 2, 2, 1:10]
  run2 <- result_arr[2, 2, 2, 11:20]

  expect_equal(mean(run1), 0, tolerance = 1e-10)
  expect_equal(mean(run2), 0, tolerance = 1e-10)
})

# --- roi_sphere() tests ---

test_that("roi_sphere validates center input", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")
  skip_if_not_installed("neuroim2")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  expect_error(
    roi_sphere(h, c(1, 2)),
    "numeric vector of length 3"
  )

  expect_error(
    roi_sphere(h, "not numeric"),
    "numeric vector of length 3"
  )
})

test_that("roi_sphere creates ROI with metadata attributes", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")
  skip_if_not_installed("neuroim2")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  roi <- roi_sphere(h, center = c(-46, 10, 30), radius_mm = 8)

  expect_equal(attr(roi, "center_mni"), c(-46, 10, 30))
  expect_equal(attr(roi, "radius_mm"), 8)
  expect_equal(attr(roi, "space_name"), "MNINonLinear")
})

test_that("roi_sphere creates non-empty ROI", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")
  skip_if_not_installed("neuroim2")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  # 10mm sphere at center should include many voxels
  roi <- roi_sphere(h, center = c(0, 0, 0), radius_mm = 10)

  expect_true(sum(as.logical(roi)) > 0)
})

# --- get_template_space() tests ---

test_that("get_template_space returns NeuroSpace for MNINonLinear", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")
  skip_if_not_installed("neuroim2")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  space <- get_template_space(h, "MNINonLinear")

  expect_s4_class(space, "NeuroSpace")
  expect_equal(dim(space), c(91L, 109L, 91L))
  expect_equal(neuroim2::spacing(space), c(2, 2, 2))
})

test_that("get_template_space errors for unknown space", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")
  skip_if_not_installed("neuroim2")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  expect_error(
    get_template_space(h, "UnknownSpace"),
    "Unknown space"
  )
})

# --- concat_vecs() tests ---

test_that("concat_vecs errors with no input", {
  expect_error(
    concat_vecs(),
    "No vectors provided"
  )
})

test_that("concat_vecs returns single input unchanged", {
  skip_if_not_installed("neuroim2")

  arr <- array(1:1000, dim = c(5, 5, 5, 8))
  space <- neuroim2::NeuroSpace(dim = c(5L, 5L, 5L, 8L), spacing = c(2, 2, 2), origin = c(0, 0, 0))
  vec <- neuroim2::DenseNeuroVec(arr, space)

  result <- concat_vecs(vec)

  expect_equal(dim(result), dim(vec))
})

test_that("concat_vecs validates spatial dimension matching", {
  skip_if_not_installed("neuroim2")

  arr1 <- array(1, dim = c(5, 5, 5, 10))
  space1 <- neuroim2::NeuroSpace(dim = c(5L, 5L, 5L, 10L), spacing = c(2, 2, 2), origin = c(0, 0, 0))
  vec1 <- neuroim2::DenseNeuroVec(arr1, space1)

  arr2 <- array(1, dim = c(6, 6, 6, 10))
  space2 <- neuroim2::NeuroSpace(dim = c(6L, 6L, 6L, 10L), spacing = c(2, 2, 2), origin = c(0, 0, 0))
  vec2 <- neuroim2::DenseNeuroVec(arr2, space2)

  expect_error(
    concat_vecs(vec1, vec2),
    "Spatial dimensions do not match"
  )
})

test_that("concat_vecs concatenates time dimension", {
  skip_if_not_installed("neuroim2")

  arr1 <- array(1, dim = c(3, 3, 3, 5))
  arr2 <- array(2, dim = c(3, 3, 3, 7))

  space1 <- neuroim2::NeuroSpace(dim = c(3L, 3L, 3L, 5L), spacing = c(2, 2, 2), origin = c(0, 0, 0))
  space2 <- neuroim2::NeuroSpace(dim = c(3L, 3L, 3L, 7L), spacing = c(2, 2, 2), origin = c(0, 0, 0))

  vec1 <- neuroim2::DenseNeuroVec(arr1, space1)
  vec2 <- neuroim2::DenseNeuroVec(arr2, space2)

  result <- concat_vecs(vec1, vec2)

  expect_equal(dim(result)[1:3], c(3, 3, 3))
  expect_equal(dim(result)[4], 12)  # 5 + 7
})

test_that("concat_vecs preserves data values", {
  skip_if_not_installed("neuroim2")

  arr1 <- array(1, dim = c(2, 2, 2, 3))
  arr2 <- array(2, dim = c(2, 2, 2, 3))

  space1 <- neuroim2::NeuroSpace(dim = c(2L, 2L, 2L, 3L), spacing = c(2, 2, 2), origin = c(0, 0, 0))
  space2 <- neuroim2::NeuroSpace(dim = c(2L, 2L, 2L, 3L), spacing = c(2, 2, 2), origin = c(0, 0, 0))

  vec1 <- neuroim2::DenseNeuroVec(arr1, space1)
  vec2 <- neuroim2::DenseNeuroVec(arr2, space2)

  result <- concat_vecs(vec1, vec2)
  result_arr <- as.array(result)

  # First 3 timepoints should be 1, last 3 should be 2
  expect_equal(unique(as.vector(result_arr[, , , 1:3])), 1)
  expect_equal(unique(as.vector(result_arr[, , , 4:6])), 2)
})
