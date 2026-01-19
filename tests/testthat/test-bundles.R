# Tests for bundle functions

# --- bundles() tests ---

test_that("bundles returns a tibble with expected columns", {
  result <- bundles()

  expect_s3_class(result, "tbl_df")
  expect_named(result, c("bundle", "description"))
})

test_that("bundles contains expected default bundles", {
  result <- bundles()

  # Default bundles from PRD.md
  expected_bundles <- c(
    "tfmri_cifti_min",
    "tfmri_cifti_full",
    "rfmri_cifti_min",
    "evs_only",
    "qc_min"
  )

  for (bundle_name in expected_bundles) {
    expect_true(bundle_name %in% result$bundle,
                info = paste("Bundle", bundle_name, "should exist"))
  }
})

test_that("bundles has valid descriptions", {
  result <- bundles()

  expect_type(result$description, "character")
})

# --- load_bundles_yaml() tests ---

test_that("load_bundles_yaml returns a list", {
  result <- load_bundles_yaml()

  expect_type(result, "list")
  expect_true(length(result) > 0)
})

test_that("load_bundles_yaml bundles have where clauses", {
  result <- load_bundles_yaml()

  for (bundle_name in names(result)) {
    bundle <- result[[bundle_name]]
    # Each bundle should have either 'where' or 'description' or both
    expect_true("description" %in% names(bundle) || "where" %in% names(bundle),
                info = paste("Bundle", bundle_name, "needs description or where"))
  }
})

# --- get_bundle() tests ---

test_that("get_bundle returns where clause for valid bundle", {
  result <- get_bundle("tfmri_cifti_min")

  expect_type(result, "list")
  # Should have filter conditions
  expect_true(length(result) > 0)
})

test_that("get_bundle errors for unknown bundle", {
  expect_error(get_bundle("nonexistent_bundle"), "Bundle not found")
})

test_that("get_bundle returns correct filters for tfmri bundles", {
  tfmri_min <- get_bundle("tfmri_cifti_min")

  # Check expected filter conditions
  expect_true("kind" %in% names(tfmri_min) || "file_type" %in% names(tfmri_min))
})

test_that("get_bundle returns correct filters for evs_only", {
  evs <- get_bundle("evs_only")

  # EV bundle should filter for ev file type
  if ("file_type" %in% names(evs)) {
    expect_equal(evs$file_type, "ev")
  }
})

# --- validate_bundle() tests ---

test_that("validate_bundle returns TRUE for valid bundles", {
  expect_true(validate_bundle("tfmri_cifti_min"))
  expect_true(validate_bundle("evs_only"))
})

test_that("validate_bundle errors for invalid bundles", {
  expect_error(validate_bundle("not_a_bundle"), "Unknown bundle")
})

# --- apply_bundle_filter() tests ---

test_that("apply_bundle_filter returns input unchanged when bundle_def is NULL", {
  # Create a simple mock table (data.frame works for testing)
  mock_tbl <- data.frame(
    kind = c("tfmri", "rfmri", "struct"),
    file_type = c("dtseries", "dtseries", "nii.gz"),
    stringsAsFactors = FALSE
  )

  result <- apply_bundle_filter(mock_tbl, NULL)
  expect_equal(result, mock_tbl)
})

test_that("apply_bundle_filter filters by single value", {
  mock_tbl <- data.frame(
    kind = c("tfmri", "rfmri", "struct", "tfmri"),
    file_type = c("dtseries", "dtseries", "nii.gz", "ev"),
    stringsAsFactors = FALSE
  )

  bundle_def <- list(kind = "tfmri")
  result <- apply_bundle_filter(mock_tbl, bundle_def)

  expect_equal(nrow(result), 2)
  expect_true(all(result$kind == "tfmri"))
})

test_that("apply_bundle_filter filters by multiple values", {
  mock_tbl <- data.frame(
    kind = c("tfmri", "rfmri", "struct", "diff"),
    file_type = c("dtseries", "dtseries", "nii.gz", "bval"),
    stringsAsFactors = FALSE
  )

  bundle_def <- list(kind = c("tfmri", "rfmri"))
  result <- apply_bundle_filter(mock_tbl, bundle_def)

  expect_equal(nrow(result), 2)
  expect_true(all(result$kind %in% c("tfmri", "rfmri")))
})

test_that("apply_bundle_filter combines multiple filter conditions", {
  mock_tbl <- data.frame(
    kind = c("tfmri", "tfmri", "tfmri", "rfmri"),
    file_type = c("dtseries", "ev", "dtseries", "dtseries"),
    direction = c("LR", "LR", "RL", "LR"),
    stringsAsFactors = FALSE
  )

  bundle_def <- list(kind = "tfmri", file_type = "dtseries")
  result <- apply_bundle_filter(mock_tbl, bundle_def)

  expect_equal(nrow(result), 2)
  expect_true(all(result$kind == "tfmri"))
  expect_true(all(result$file_type == "dtseries"))
})
