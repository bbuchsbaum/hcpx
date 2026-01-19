# Tests for CLI utility functions

# --- format_bytes() tests ---

test_that("format_bytes handles bytes", {
  expect_equal(format_bytes(0), "0.0 B")
  expect_equal(format_bytes(100), "100.0 B")
  expect_equal(format_bytes(1023), "1023.0 B")
})

test_that("format_bytes handles kilobytes", {
  expect_equal(format_bytes(1024), "1.0 KB")
  expect_equal(format_bytes(1536), "1.5 KB")
  expect_equal(format_bytes(1024 * 100), "100.0 KB")
})

test_that("format_bytes handles megabytes", {
  expect_equal(format_bytes(1024^2), "1.0 MB")
  expect_equal(format_bytes(1024^2 * 500), "500.0 MB")
})

test_that("format_bytes handles gigabytes", {
  expect_equal(format_bytes(1024^3), "1.0 GB")
  expect_equal(format_bytes(1024^3 * 10), "10.0 GB")
})

test_that("format_bytes handles terabytes", {
  expect_equal(format_bytes(1024^4), "1.0 TB")
  expect_equal(format_bytes(1024^4 * 2.5), "2.5 TB")
})

test_that("format_bytes handles NA", {
  expect_equal(format_bytes(NA), "unknown")
})

test_that("format_bytes handles typical HCP file sizes", {
  # Common HCP file sizes
  expect_match(format_bytes(524288000), "500\\.0 MB")  # ~500MB dtseries
  expect_match(format_bytes(1073741824), "1\\.0 GB")   # ~1GB rest fMRI
  expect_match(format_bytes(1024), "1\\.0 KB")          # Small text files
})

# --- format_bytes edge cases ---

test_that("format_bytes handles negative values", {
  # While unusual, function should handle gracefully
  result <- format_bytes(-100)
  expect_type(result, "character")
})

test_that("format_bytes handles very large numbers", {
  # Petabyte range
  pb <- 1024^5
  result <- format_bytes(pb)
  expect_true(grepl("TB", result))  # Will be shown as TB
})

test_that("format_bytes handles decimal inputs", {
  result <- format_bytes(1024.5)
  expect_type(result, "character")
})

test_that("format_bytes returns string type", {
  expect_type(format_bytes(0), "character")
  expect_type(format_bytes(1000), "character")
  expect_type(format_bytes(NA), "character")
})

test_that("format_bytes uses consistent precision", {
  # All outputs should have one decimal place
  expect_match(format_bytes(100), "\\d+\\.\\d")
  expect_match(format_bytes(1024), "\\d+\\.\\d")
  expect_match(format_bytes(1024^2), "\\d+\\.\\d")
})

test_that("format_bytes transitions at correct boundaries", {
  # Just under 1 KB should be in bytes
  expect_match(format_bytes(1023), "B$")
  # At 1 KB should be in KB
  expect_match(format_bytes(1024), "KB$")

  # Just under 1 MB should be in KB
  expect_match(format_bytes(1024^2 - 1), "KB$")
  # At 1 MB should be in MB
  expect_match(format_bytes(1024^2), "MB$")

  # Just under 1 GB should be in MB
  expect_match(format_bytes(1024^3 - 1), "MB$")
  # At 1 GB should be in GB
  expect_match(format_bytes(1024^3), "GB$")
})

# --- cli_progress tests ---

test_that("cli_progress returns invisible NULL", {
  result <- cli_progress(1, 10)

  expect_null(result)
})

test_that("cli_progress handles edge cases", {
  # Zero progress
  expect_null(cli_progress(0, 10))

  # Complete
  expect_null(cli_progress(10, 10))

  # Over complete
  expect_null(cli_progress(15, 10))
})

test_that("cli_progress accepts width parameter", {
  # Should not error with custom width
  expect_null(cli_progress(5, 10, width = 20))
  expect_null(cli_progress(5, 10, width = 80))
})
