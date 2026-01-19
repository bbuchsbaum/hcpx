# Tests for REST/ConnectomeDB backend

test_that("hcpx_backend_rest creates backend object", {
  backend <- hcpx_backend_rest()

  expect_s3_class(backend, "hcpx_backend_rest")
  expect_s3_class(backend, "hcpx_backend")
  expect_equal(backend$type, "rest")
  expect_equal(backend$base_url, "https://db.humanconnectome.org")
  expect_equal(backend$project, "HCP_1200")
  expect_null(backend$session_cookie)
})

test_that("hcpx_backend_rest accepts custom parameters", {
  backend <- hcpx_backend_rest(
    base_url = "https://custom.xnat.org",
    project = "MyProject"
  )

  expect_equal(backend$base_url, "https://custom.xnat.org")
  expect_equal(backend$project, "MyProject")
})

test_that("get_session_cookie returns NULL when no session", {
  backend <- hcpx_backend_rest()
  rest_session_clear()

  expect_null(get_session_cookie(backend))
})

test_that("get_session_cookie returns backend cookie if set", {
  backend <- hcpx_backend_rest()
  backend$session_cookie <- "test_session_123"

  expect_equal(get_session_cookie(backend), "test_session_123")
})

test_that("rest_session_clear clears global session", {
  # Set a session
  assign("session", "test_session", envir = hcpx:::.rest_session)
  assign("expires", Sys.time() + 3600, envir = hcpx:::.rest_session)

  # Clear it
  rest_session_clear()

  # Should be empty
  expect_false(exists("session", envir = hcpx:::.rest_session))
})

test_that("backend_presign.hcpx_backend_rest errors appropriately", {
  backend <- hcpx_backend_rest()

  expect_error(
    backend_presign(backend, "/some/path"),
    "does not support presigned URLs"
  )
})

test_that("rest_authenticate errors without credentials", {
  backend <- hcpx_backend_rest()
  rest_session_clear()

  # Without keyring or credentials, should error
  expect_error(
    rest_authenticate(backend),
    "ConnectomeDB credentials required"
  )
})

test_that("hcpx_connectome_auth requires keyring package", {
  skip_if(has_keyring(), "keyring is installed")

  expect_error(
    hcpx_connectome_auth("user", "pass"),
    "Keyring package required"
  )
})

test_that("hcpx_connectome_clear handles missing keyring", {
  skip_if(has_keyring(), "keyring is installed")

  # Should warn, not error
  expect_warning(
    result <- hcpx_connectome_clear(),
    "Keyring package not installed"
  )
  expect_false(result)
})

# Integration tests that require actual ConnectomeDB credentials
# These are skipped by default

test_that("rest_authenticate works with valid credentials", {
  skip("Requires ConnectomeDB credentials")

  backend <- hcpx_backend_rest()
  # Would need actual credentials here
  # backend <- rest_authenticate(backend, "username", "password")
  # expect_true(!is.null(backend$session_cookie))
})

test_that("backend_list.hcpx_backend_rest returns resources", {
  skip("Requires ConnectomeDB credentials")

  backend <- hcpx_backend_rest()
  # backend <- rest_authenticate(backend, "username", "password")
  # result <- backend_list(backend, "HCP_1200/100307")
  # expect_s3_class(result, "tbl_df")
})

test_that("backend_download.hcpx_backend_rest downloads files", {
  skip("Requires ConnectomeDB credentials")

  backend <- hcpx_backend_rest()
  # Would test actual download here
})
