# Tests for neuroim2 parcellation functions

# --- list_parcellations() tests ---

test_that("list_parcellations returns a tibble", {
  result <- list_parcellations()

  expect_s3_class(result, "tbl_df")
})

test_that("list_parcellations has required columns", {
  result <- list_parcellations()

  expect_true("name" %in% names(result))
  expect_true("n_parcels" %in% names(result))
  expect_true("description" %in% names(result))
  expect_true("source" %in% names(result))
})

test_that("list_parcellations includes Schaefer atlases", {
  result <- list_parcellations()

  expect_true("Schaefer100" %in% result$name)
  expect_true("Schaefer200" %in% result$name)
  expect_true("Schaefer400" %in% result$name)
  expect_true("Schaefer1000" %in% result$name)
})

test_that("list_parcellations includes Glasser atlas", {
  result <- list_parcellations()

  expect_true("Glasser360" %in% result$name)
})

test_that("list_parcellations includes Gordon atlas", {
  result <- list_parcellations()

  expect_true("Gordon333" %in% result$name)
})

test_that("list_parcellations includes Power atlas", {
  result <- list_parcellations()

  expect_true("Power264" %in% result$name)
})

test_that("list_parcellations has correct parcel counts", {
  result <- list_parcellations()

  # Check specific counts
  expect_equal(result$n_parcels[result$name == "Schaefer100"], 100L)
  expect_equal(result$n_parcels[result$name == "Schaefer200"], 200L)
  expect_equal(result$n_parcels[result$name == "Schaefer400"], 400L)
  expect_equal(result$n_parcels[result$name == "Glasser360"], 360L)
  expect_equal(result$n_parcels[result$name == "Gordon333"], 333L)
  expect_equal(result$n_parcels[result$name == "Power264"], 264L)
})

test_that("list_parcellations has descriptions for all entries", {
  result <- list_parcellations()

  expect_true(all(nzchar(result$description)))
  expect_false(any(is.na(result$description)))
})

test_that("list_parcellations has sources for all entries", {
  result <- list_parcellations()

  expect_true(all(nzchar(result$source)))
  expect_false(any(is.na(result$source)))
})

test_that("list_parcellations accepts optional h parameter", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  # Should work with or without h
  result1 <- list_parcellations()

  result2 <- list_parcellations(h)

  expect_equal(result1, result2)
})

# --- .resolve_parcellation_path() tests ---

test_that(".resolve_parcellation_path returns NULL for unrecognized name", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  # Suppress cli warnings about atlas not found locally
  suppressWarnings({
    result <- hcpx:::.resolve_parcellation_path(h, "NonexistentAtlas12345")
  })

  expect_null(result)
})

test_that(".resolve_parcellation_path returns file path if file exists", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  # Create a test file
  test_file <- file.path(tmp_dir, "test_atlas.dlabel.nii")
  writeLines("test", test_file)

  result <- hcpx:::.resolve_parcellation_path(h, test_file)

  expect_equal(result, test_file)
})

test_that(".resolve_parcellation_path warns for recognized but not found atlas", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  # Schaefer400 is recognized but likely not downloaded
  expect_warning(
    hcpx:::.resolve_parcellation_path(h, "Schaefer400"),
    "not found locally"
  )
})

# --- .validate_parcellation_space() tests ---

test_that(".validate_parcellation_space passes for matching dimensions", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  # Create mock objects with matching dimensions
  mock_vec <- array(1:24, dim = c(2, 3, 4, 5))
  class(mock_vec) <- "NeuroVec"
  dim(mock_vec) <- c(2, 3, 4, 5)

  mock_parc <- array(1:24, dim = c(2, 3, 4))
  class(mock_parc) <- "ClusteredNeuroVol"
  dim(mock_parc) <- c(2, 3, 4)

  expect_true(hcpx:::.validate_parcellation_space(mock_vec, mock_parc))
})

test_that(".validate_parcellation_space errors for mismatched dimensions", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  # Create mock objects with mismatched dimensions
  mock_vec <- array(1:120, dim = c(3, 4, 5, 2))
  class(mock_vec) <- "NeuroVec"
  dim(mock_vec) <- c(3, 4, 5, 2)

  mock_parc <- array(1:24, dim = c(2, 3, 4))
  class(mock_parc) <- "ClusteredNeuroVol"
  dim(mock_parc) <- c(2, 3, 4)

  expect_error(
    hcpx:::.validate_parcellation_space(mock_vec, mock_parc),
    "Spatial dimensions do not match"
  )
})

# --- parcellate() tests (basic validation) ---

test_that("parcellate validates method argument", {
  # Create minimal mock data
  mock_vec <- array(rnorm(24), dim = c(2, 3, 4, 1))
  mock_parc <- array(c(1, 1, 2, 2, 0, 0), dim = c(2, 3, 4))

  # Invalid method should error
  expect_error(
    parcellate(mock_vec, mock_parc, method = "invalid_method"),
    "'arg' should be one of"
  )
})

test_that("parcellate errors when vec has no hcpx context for character parcellation", {
  # Create minimal mock data without hcpx context
  mock_vec <- array(rnorm(24), dim = c(2, 3, 4, 1))
  class(mock_vec) <- "NeuroVec"

  expect_error(
    parcellate(mock_vec, "Schaefer400"),
    "Cannot load parcellation by name without hcpx context"
  )
})

# --- load_parcellation() tests (basic validation) ---

test_that("load_parcellation errors for invalid parcellation name", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  expect_error(
    load_parcellation(h, "CompletelyFakeAtlasName12345"),
    "Parcellation not found"
  )
})

test_that("load_parcellation lists available parcellations in error", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  expect_error(
    load_parcellation(h, "CompletelyFakeAtlasName12345"),
    "Available built-in parcellations"
  )
})

test_that("load_parcellation loads a local file path via neuroim2::read_vol", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")
  skip_if_not_installed("neuroim2")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  calls <- new.env(parent = emptyenv())
  calls$path <- NULL

  testthat::local_mocked_bindings(
    read_vol = function(file_name, ...) {
      calls$path <- file_name
      structure(list(path = file_name), class = "mock_vol")
    },
    .package = "neuroim2"
  )

  test_file <- file.path(tmp_dir, "atlas.dlabel.nii")
  writeLines("test", test_file)

  result <- load_parcellation(h, test_file)

  expect_s3_class(result, "mock_vol")
  expect_equal(result$path, test_file)
  expect_equal(calls$path, test_file)
})

test_that("load_parcellation dispatches to load_asset for catalog asset_id", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  con <- get_con(h)
  parc_asset_id <- "parc_asset_1"

  DBI::dbExecute(con, "
    INSERT INTO assets
      (asset_id, release, subject_id, session, kind, task, direction, run,
       space, derivative_level, file_type, remote_path, size_bytes, access_tier)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  ", params = list(
    parc_asset_id,
    "HCP_1200",
    "100206",
    "3T",
    "atlas",
    NA_character_,
    NA_character_,
    NA_integer_,
    "MNINonLinear",
    "preprocessed",
    "dlabel",
    "HCP_1200/100206/MNINonLinear/atlas/parc_asset_1.dlabel.nii",
    1024L,
    "open"
  ))

  testthat::local_mocked_bindings(
    load_asset = function(h, asset_id, ...) {
      structure(list(asset_id = asset_id), class = "mock_asset")
    },
    .env = asNamespace("hcpx")
  )

  result <- load_parcellation(h, parc_asset_id)

  expect_s3_class(result, "mock_asset")
  expect_equal(result$asset_id, parc_asset_id)
})

# --- parcellate() tests (aggregation) ---

test_that("parcellate computes parcel mean time series", {
  vec <- array(1:12, dim = c(2, 2, 1, 3))
  parc <- array(c(1, 1, 2, 0), dim = c(2, 2, 1))

  result <- parcellate(vec, parc, method = "mean")

  expect_s3_class(result, "parcel_timeseries")
  expect_equal(dim(result), c(3, 2))
  expect_equal(colnames(result), c("1", "2"))
  expect_equal(result[, "1"], c(1.5, 5.5, 9.5))
  expect_equal(result[, "2"], c(3, 7, 11))
})

test_that("parcellate supports median aggregation", {
  vec <- array(c(1, 100, 101, 2, 200, 201), dim = c(3, 1, 1, 2))
  parc <- array(c(1, 1, 1), dim = c(3, 1, 1))

  result <- parcellate(vec, parc, method = "median")

  expect_equal(dim(result), c(2, 1))
  expect_equal(result[, "1"], c(100, 200))
})

test_that("parcellate supports pca1 aggregation", {
  vec <- array(c(1, 1, 2, 2, 3, 3, 4, 4), dim = c(2, 1, 1, 4))
  parc <- array(c(1, 1), dim = c(2, 1, 1))

  result <- parcellate(vec, parc, method = "pca1")

  expected <- as.numeric(scale(c(1, 2, 3, 4), center = TRUE, scale = FALSE))
  pc <- as.numeric(result[, "1"])

  expect_equal(abs(stats::cor(pc, expected)), 1, tolerance = 1e-12)
})

test_that("parcellate errors when parcellation has no labels", {
  vec <- array(rnorm(12), dim = c(2, 2, 1, 3))
  parc <- array(0, dim = c(2, 2, 1))

  expect_error(
    parcellate(vec, parc),
    "No parcel labels found"
  )
})
