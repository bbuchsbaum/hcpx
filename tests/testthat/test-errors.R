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

test_that("download errors are caught separately", {
  result <- tryCatch(
    {
      hcpx_download_error("Connection refused")
      "not caught"
    },
    hcpx_not_found = function(e) "caught not_found",
    hcpx_auth_error = function(e) "caught auth",
    hcpx_download_error = function(e) "caught download",
    error = function(e) "caught generic"
  )

  expect_equal(result, "caught download")
})

# --- hcpx_error edge cases ---

test_that("hcpx_error with empty message works", {
  err <- hcpx_error("")

  expect_s3_class(err, "hcpx_error")
  expect_equal(err$message, "")
})

test_that("hcpx_error with NULL class works", {
  err <- hcpx_error("Test", class = NULL)

  expect_s3_class(err, "hcpx_error")
  expect_false("NULL" %in% class(err))
})

test_that("hcpx_error preserves message with special characters", {
  err <- hcpx_error("Error with 'quotes' and \"double quotes\" and\nnewlines")

  expect_match(err$message, "quotes")
  expect_match(err$message, "newlines")
})

test_that("hcpx_error with multiple custom classes", {
  err <- hcpx_error("Test", class = c("class1", "class2"))

  expect_s3_class(err, "class1")
  expect_s3_class(err, "class2")
  expect_s3_class(err, "hcpx_error")
})

# --- hcpx_not_found edge cases ---

test_that("hcpx_not_found with empty what", {
  expect_error(
    hcpx_not_found(""),
    class = "hcpx_not_found"
  )
})

test_that("hcpx_not_found with long path", {
  long_path <- paste(rep("a", 200), collapse = "/")
  expect_error(
    hcpx_not_found("File", path = long_path),
    class = "hcpx_not_found"
  )
})

test_that("hcpx_not_found message includes what", {
  tryCatch(
    hcpx_not_found("Subject", path = "12345"),
    hcpx_not_found = function(e) {
      expect_match(e$message, "Subject")
    }
  )
})

# --- hcpx_auth_error edge cases ---

test_that("hcpx_auth_error with NULL backend", {
  tryCatch(
    hcpx_auth_error("Test", backend = NULL),
    hcpx_auth_error = function(e) {
      expect_null(e$backend)
    }
  )
})

test_that("hcpx_auth_error stores backend correctly", {
  backends <- c("aws", "rest", "local")
  for (b in backends) {
    tryCatch(
      hcpx_auth_error("Test", backend = b),
      hcpx_auth_error = function(e) {
        expect_equal(e$backend, b)
      }
    )
  }
})

# --- hcpx_download_error edge cases ---

test_that("hcpx_download_error with various status codes", {
  status_codes <- c(400, 401, 403, 404, 500, 502, 503)
  for (code in status_codes) {
    tryCatch(
      hcpx_download_error("HTTP error", status = code),
      hcpx_download_error = function(e) {
        expect_equal(e$status, code)
      }
    )
  }
})

test_that("hcpx_download_error with NULL path and status", {
  tryCatch(
    hcpx_download_error("Generic download error"),
    hcpx_download_error = function(e) {
      expect_null(e$path)
      expect_null(e$status)
    }
  )
})

test_that("hcpx_download_error stores path correctly", {
  test_path <- "/remote/hcp/data/file.nii.gz"
  tryCatch(
    hcpx_download_error("Failed", path = test_path),
    hcpx_download_error = function(e) {
      expect_equal(e$path, test_path)
    }
  )
})

# --- Error condition inspection ---

test_that("error conditions can be inspected with conditionMessage", {
  err <- hcpx_error("Inspection test")

  expect_equal(conditionMessage(err), "Inspection test")
})

test_that("error conditions have correct class hierarchy length", {
  err <- hcpx_error("Test")

  # Should have: hcpx_error, error, condition
  expect_gte(length(class(err)), 3)
})

test_that("custom subclass error has correct class order", {
  err <- hcpx_error("Test", class = "custom_subclass")
  classes <- class(err)

  # Custom class should come before hcpx_error
  custom_pos <- which(classes == "custom_subclass")
  hcpx_pos <- which(classes == "hcpx_error")

  expect_lt(custom_pos, hcpx_pos)
})

# --- Error recovery patterns ---

test_that("errors can be caught and re-thrown with additional context", {
  result <- tryCatch(
    {
      tryCatch(
        hcpx_not_found("Asset"),
        hcpx_not_found = function(e) {
          stop(hcpx_error(
            paste("Wrapped:", e$message),
            class = "wrapped_error",
            original = e
          ))
        }
      )
    },
    wrapped_error = function(e) {
      expect_match(e$message, "Wrapped:")
      expect_s3_class(e$original, "hcpx_not_found")
      "handled"
    }
  )

  expect_equal(result, "handled")
})

test_that("generic error handler catches all hcpx errors", {
  errors_to_test <- list(
    function() hcpx_not_found("Test"),
    function() hcpx_auth_error("Test"),
    function() hcpx_download_error("Test")
  )

  for (err_fn in errors_to_test) {
    result <- tryCatch(
      err_fn(),
      hcpx_error = function(e) "caught hcpx_error"
    )
    expect_equal(result, "caught hcpx_error")
  }
})

# --- Condition data access ---

test_that("error data is accessible via $ operator", {
  tryCatch(
    hcpx_download_error("Test", path = "/test/path", status = 404),
    hcpx_download_error = function(e) {
      expect_equal(e$message, "Test")
      expect_equal(e$path, "/test/path")
      expect_equal(e$status, 404)
    }
  )
})

test_that("error data is accessible via [[ operator", {
  tryCatch(
    hcpx_not_found("Asset", path = "/asset/path"),
    hcpx_not_found = function(e) {
      expect_equal(e[["what"]], "Asset")
      expect_equal(e[["path"]], "/asset/path")
    }
  )
})
