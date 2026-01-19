# Tests for structured error conditions

# --- hcpx_error() tests ---

test_that("hcpx_error creates a condition with correct classes", {
  err <- hcpx_error("Test error message")

  expect_s3_class(err, "hcpx_error")
  expect_s3_class(err, "error")
  expect_s3_class(err, "condition")
  expect_equal(err$message, "Test error message")
})

test_that("hcpx_error accepts custom subclass", {
  err <- hcpx_error("Custom error", class = "custom_type")

  expect_s3_class(err, "custom_type")
  expect_s3_class(err, "hcpx_error")
  expect_s3_class(err, "error")
})

test_that("hcpx_error stores additional data", {
  err <- hcpx_error("Error with data", foo = "bar", count = 42)

  expect_equal(err$message, "Error with data")
  expect_equal(err$foo, "bar")
  expect_equal(err$count, 42)
})

# --- hcpx_not_found() tests ---

test_that("hcpx_not_found throws error with correct class", {
  expect_error(
    hcpx_not_found("Subject"),
    class = "hcpx_not_found"
  )
})
test_that("hcpx_not_found includes path in message", {
  expect_error(
    hcpx_not_found("Asset", path = "/path/to/asset.nii"),
    regexp = "Asset not found.*asset\\.nii"
  )
})

test_that("hcpx_not_found works without path", {
  expect_error(
    hcpx_not_found("Resource"),
    regexp = "Resource not found"
  )
})

test_that("hcpx_not_found stores what and path in condition", {
  tryCatch(
    hcpx_not_found("Bundle", path = "custom_bundle"),
    hcpx_not_found = function(e) {
      expect_equal(e$what, "Bundle")
      expect_equal(e$path, "custom_bundle")
    }
  )
})

# --- hcpx_auth_error() tests ---

test_that("hcpx_auth_error throws error with correct class", {
  expect_error(
    hcpx_auth_error("Authentication failed"),
    class = "hcpx_auth_error"
  )
})

test_that("hcpx_auth_error includes backend info", {
  tryCatch(
    hcpx_auth_error("Invalid credentials", backend = "aws"),
    hcpx_auth_error = function(e) {
      expect_equal(e$backend, "aws")
      expect_match(e$message, "Invalid credentials")
    }
  )
})

test_that("hcpx_auth_error works without backend", {
  expect_error(
    hcpx_auth_error("Auth required"),
    regexp = "Auth required"
  )
})

# --- hcpx_download_error() tests ---

test_that("hcpx_download_error throws error with correct class", {
  expect_error(
    hcpx_download_error("Download failed"),
    class = "hcpx_download_error"
  )
})

test_that("hcpx_download_error stores path and status", {
  tryCatch(
    hcpx_download_error("HTTP error", path = "/data/file.nii", status = 404),
    hcpx_download_error = function(e) {
      expect_equal(e$path, "/data/file.nii")
      expect_equal(e$status, 404)
    }
  )
})

test_that("hcpx_download_error works with only message", {
  expect_error(
    hcpx_download_error("Network timeout"),
    regexp = "Network timeout"
  )
})

# --- Error hierarchy tests ---

test_that("all hcpx errors inherit from hcpx_error", {
  # Test hcpx_not_found
 tryCatch(
    hcpx_not_found("Test"),
    error = function(e) {
      expect_true(inherits(e, "hcpx_error"))
    }
  )

  # Test hcpx_auth_error
  tryCatch(
    hcpx_auth_error("Test"),
    error = function(e) {
      expect_true(inherits(e, "hcpx_error"))
    }
  )

  # Test hcpx_download_error
  tryCatch(
    hcpx_download_error("Test"),
    error = function(e) {
      expect_true(inherits(e, "hcpx_error"))
    }
  )
})

# --- Catch specific error types ---

test_that("specific error types can be caught selectively", {
  # This should catch only hcpx_not_found
  result <- tryCatch(
    {
      hcpx_not_found("Missing item")
      "not caught"
    },
    hcpx_not_found = function(e) "caught not_found",
    hcpx_auth_error = function(e) "caught auth",
    error = function(e) "caught generic"
  )

  expect_equal(result, "caught not_found")
})

test_that("auth errors are caught separately from not_found", {
  result <- tryCatch(
    {
      hcpx_auth_error("Bad token")
      "not caught"
    },
    hcpx_not_found = function(e) "caught not_found",
    hcpx_auth_error = function(e) "caught auth",
    error = function(e) "caught generic"
  )

  expect_equal(result, "caught auth")
})
