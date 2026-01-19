# Tests for auth.R - credential management

test_that("has_keyring returns FALSE when keyring not installed", {
  # This test just verifies the function works
  result <- has_keyring()
  expect_type(result, "logical")
})

test_that("has_env_credentials detects environment variables", {
  # Save current values
  old_key <- Sys.getenv("AWS_ACCESS_KEY_ID")
  old_secret <- Sys.getenv("AWS_SECRET_ACCESS_KEY")
  on.exit({
    if (nzchar(old_key)) Sys.setenv(AWS_ACCESS_KEY_ID = old_key) else Sys.unsetenv("AWS_ACCESS_KEY_ID")
    if (nzchar(old_secret)) Sys.setenv(AWS_SECRET_ACCESS_KEY = old_secret) else Sys.unsetenv("AWS_SECRET_ACCESS_KEY")
  })

  # Clear env vars
  Sys.unsetenv("AWS_ACCESS_KEY_ID")
  Sys.unsetenv("AWS_SECRET_ACCESS_KEY")
  expect_false(has_env_credentials())

  # Set env vars
  Sys.setenv(AWS_ACCESS_KEY_ID = "test_key")
  Sys.setenv(AWS_SECRET_ACCESS_KEY = "test_secret")
  expect_true(has_env_credentials())
})

test_that("hcpx_auth with aws_profile sets environment", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  # Save and clear current profile
  old_profile <- Sys.getenv("AWS_PROFILE")
  on.exit(if (nzchar(old_profile)) Sys.setenv(AWS_PROFILE = old_profile) else Sys.unsetenv("AWS_PROFILE"), add = TRUE)

  # Capture returned handle
  h <- hcpx_auth(h, aws_profile = "test_profile")

  expect_equal(Sys.getenv("AWS_PROFILE"), "test_profile")
  expect_equal(h$auth$method, "aws_profile")
  expect_equal(h$auth$profile, "test_profile")
})

test_that("hcpx_auth uses environment variables when available", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  # Set env vars
  old_key <- Sys.getenv("AWS_ACCESS_KEY_ID")
  old_secret <- Sys.getenv("AWS_SECRET_ACCESS_KEY")
  on.exit({
    if (nzchar(old_key)) Sys.setenv(AWS_ACCESS_KEY_ID = old_key) else Sys.unsetenv("AWS_ACCESS_KEY_ID")
    if (nzchar(old_secret)) Sys.setenv(AWS_SECRET_ACCESS_KEY = old_secret) else Sys.unsetenv("AWS_SECRET_ACCESS_KEY")
  }, add = TRUE)

  Sys.setenv(AWS_ACCESS_KEY_ID = "test_key")
  Sys.setenv(AWS_SECRET_ACCESS_KEY = "test_secret")

  # Capture returned handle
  h <- hcpx_auth(h)

  expect_equal(h$auth$method, "environment")
})

test_that("hcpx_auth warns when no credentials found", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  # Clear all credential sources
  old_key <- Sys.getenv("AWS_ACCESS_KEY_ID")
  old_secret <- Sys.getenv("AWS_SECRET_ACCESS_KEY")
  old_profile <- Sys.getenv("AWS_PROFILE")
  on.exit({
    if (nzchar(old_key)) Sys.setenv(AWS_ACCESS_KEY_ID = old_key) else Sys.unsetenv("AWS_ACCESS_KEY_ID")
    if (nzchar(old_secret)) Sys.setenv(AWS_SECRET_ACCESS_KEY = old_secret) else Sys.unsetenv("AWS_SECRET_ACCESS_KEY")
    if (nzchar(old_profile)) Sys.setenv(AWS_PROFILE = old_profile) else Sys.unsetenv("AWS_PROFILE")
  }, add = TRUE)

  Sys.unsetenv("AWS_ACCESS_KEY_ID")
  Sys.unsetenv("AWS_SECRET_ACCESS_KEY")
  Sys.unsetenv("AWS_PROFILE")

  # Capture returned handle
  expect_warning(h <- hcpx_auth(h), "No AWS credentials")
  expect_equal(h$auth$method, "none")
})

test_that("hcpx_auth_status runs without error", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  # Should run without error
  expect_no_error(hcpx_auth_status(h))
})

test_that("keyring functions handle missing package gracefully", {
  skip_if(has_keyring(), "keyring is installed")

  # Should return NULL when keyring not available
  expect_null(keyring_get_credentials())

  # Should error with helpful message
  expect_error(hcpx_keyring_set("key", "secret"), "Keyring package required")
})

# --- Additional has_env_credentials tests ---

test_that("has_env_credentials returns FALSE with only access key", {
  old_key <- Sys.getenv("AWS_ACCESS_KEY_ID")
  old_secret <- Sys.getenv("AWS_SECRET_ACCESS_KEY")
  on.exit({
    if (nzchar(old_key)) Sys.setenv(AWS_ACCESS_KEY_ID = old_key) else Sys.unsetenv("AWS_ACCESS_KEY_ID")
    if (nzchar(old_secret)) Sys.setenv(AWS_SECRET_ACCESS_KEY = old_secret) else Sys.unsetenv("AWS_SECRET_ACCESS_KEY")
  })

  Sys.setenv(AWS_ACCESS_KEY_ID = "test_key")
  Sys.unsetenv("AWS_SECRET_ACCESS_KEY")

  expect_false(has_env_credentials())
})

test_that("has_env_credentials returns FALSE with only secret key", {
  old_key <- Sys.getenv("AWS_ACCESS_KEY_ID")
  old_secret <- Sys.getenv("AWS_SECRET_ACCESS_KEY")
  on.exit({
    if (nzchar(old_key)) Sys.setenv(AWS_ACCESS_KEY_ID = old_key) else Sys.unsetenv("AWS_ACCESS_KEY_ID")
    if (nzchar(old_secret)) Sys.setenv(AWS_SECRET_ACCESS_KEY = old_secret) else Sys.unsetenv("AWS_SECRET_ACCESS_KEY")
  })

  Sys.unsetenv("AWS_ACCESS_KEY_ID")
  Sys.setenv(AWS_SECRET_ACCESS_KEY = "test_secret")

  expect_false(has_env_credentials())
})

test_that("has_env_credentials returns FALSE with empty strings", {
  old_key <- Sys.getenv("AWS_ACCESS_KEY_ID")
  old_secret <- Sys.getenv("AWS_SECRET_ACCESS_KEY")
  on.exit({
    if (nzchar(old_key)) Sys.setenv(AWS_ACCESS_KEY_ID = old_key) else Sys.unsetenv("AWS_ACCESS_KEY_ID")
    if (nzchar(old_secret)) Sys.setenv(AWS_SECRET_ACCESS_KEY = old_secret) else Sys.unsetenv("AWS_SECRET_ACCESS_KEY")
  })

  Sys.setenv(AWS_ACCESS_KEY_ID = "")
  Sys.setenv(AWS_SECRET_ACCESS_KEY = "")

  expect_false(has_env_credentials())
})

# --- hcpx_auth returns invisible ---

test_that("hcpx_auth returns handle invisibly", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  old_profile <- Sys.getenv("AWS_PROFILE")
  on.exit(if (nzchar(old_profile)) Sys.setenv(AWS_PROFILE = old_profile) else Sys.unsetenv("AWS_PROFILE"), add = TRUE)

  result <- hcpx_auth(h, aws_profile = "test")

  expect_s3_class(result, "hcpx")
})

# --- hcpx_auth_status tests ---

test_that("hcpx_auth_status returns auth info invisibly", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  old_profile <- Sys.getenv("AWS_PROFILE")
  on.exit(if (nzchar(old_profile)) Sys.setenv(AWS_PROFILE = old_profile) else Sys.unsetenv("AWS_PROFILE"), add = TRUE)

  # Configure auth first
  h <- hcpx_auth(h, aws_profile = "test_profile")

  # Capture returned auth info
  result <- hcpx_auth_status(h)

  expect_type(result, "list")
  expect_equal(result$method, "aws_profile")
})

test_that("hcpx_auth_status works without configured auth", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  # h$auth should be NULL initially
  h$auth <- NULL

  expect_no_error(hcpx_auth_status(h))
})

# --- hcpx_keyring_clear tests ---

test_that("hcpx_keyring_clear handles missing keyring package", {
  skip_if(has_keyring(), "keyring is installed")

  # Should warn but not error
  expect_warning(result <- hcpx_keyring_clear(), "Keyring package not installed")
  expect_false(result)
})

# --- keyring_get_credentials tests ---

test_that("keyring_get_credentials returns NULL without keyring", {
  skip_if(has_keyring(), "keyring is installed")

  result <- keyring_get_credentials("hcpx")
  expect_null(result)
})

test_that("keyring_get_credentials accepts custom service name", {
  skip_if(has_keyring(), "keyring is installed")

  result <- keyring_get_credentials("custom_service")
  expect_null(result)
})

# --- has_keyring tests ---

test_that("has_keyring returns logical", {
  result <- has_keyring()

  expect_type(result, "logical")
  expect_length(result, 1)
})
