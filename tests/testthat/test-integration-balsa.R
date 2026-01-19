# Integration tests for BALSA/Aspera downloads
#
# These are skipped by default. To enable:
#   Sys.setenv(HCPX_INTEGRATION = "true")
#   Sys.setenv(HCPX_BALSA_TEST_SOURCE = "user@host:/path/to/small/file")
#   Sys.setenv(HCPX_ASPERA_KEY = "/path/to/key")
#   (optional) Sys.setenv(HCPX_ASCP = "/path/to/ascp")

test_that("balsa backend can download via ascp (opt-in)", {
  testthat::skip_if(Sys.getenv("HCPX_INTEGRATION") != "true", "Set HCPX_INTEGRATION=true to run integration tests")

  source <- Sys.getenv("HCPX_BALSA_TEST_SOURCE", unset = "")
  testthat::skip_if(!nzchar(source), "Set HCPX_BALSA_TEST_SOURCE to an Aspera source spec")

  key <- Sys.getenv("HCPX_ASPERA_KEY", unset = "")
  testthat::skip_if(!nzchar(key), "Set HCPX_ASPERA_KEY to Aspera private key path")

  ascp <- Sys.getenv("HCPX_ASCP", unset = "")
  if (!nzchar(ascp)) ascp <- Sys.which("ascp")
  testthat::skip_if(!nzchar(ascp), "ascp not found; set HCPX_ASCP or install Aspera CLI")

  backend <- hcpx_backend_balsa(ascp = ascp, key = key)
  dest <- file.path(withr::local_tempdir(), "balsa-integration.bin")

  res <- backend_download(backend, source, dest, resume = TRUE)
  expect_equal(res$status, "success")
  expect_true(file.exists(dest))
  expect_true(file.info(dest)$size > 0)
})

