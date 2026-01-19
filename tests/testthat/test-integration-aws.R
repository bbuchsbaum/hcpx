# Integration tests for AWS S3 downloads
#
# These are skipped by default. To enable:
#   Sys.setenv(HCPX_INTEGRATION = "true")
#   Sys.setenv(HCPX_AWS_TEST_BUCKET = "your-bucket")
#   Sys.setenv(HCPX_AWS_TEST_KEY = "path/to/a/small/object")
#   (optional) Sys.setenv(HCPX_AWS_REGION = "us-east-1")
#   (optional) Sys.setenv(HCPX_AWS_PROFILE = "your-profile")
#   (optional) Sys.setenv(HCPX_AWS_REQUEST_PAYER = "requester")

test_that("aws backend can head+download an object (opt-in)", {
  testthat::skip_if(Sys.getenv("HCPX_INTEGRATION") != "true", "Set HCPX_INTEGRATION=true to run integration tests")

  aws <- Sys.which("aws")
  testthat::skip_if(!nzchar(aws), "aws CLI not found; install AWS CLI or set PATH")

  bucket <- Sys.getenv("HCPX_AWS_TEST_BUCKET", unset = "")
  key <- Sys.getenv("HCPX_AWS_TEST_KEY", unset = "")
  testthat::skip_if(!nzchar(bucket), "Set HCPX_AWS_TEST_BUCKET")
  testthat::skip_if(!nzchar(key), "Set HCPX_AWS_TEST_KEY")

  region <- Sys.getenv("HCPX_AWS_REGION", unset = "us-east-1")
  profile <- Sys.getenv("HCPX_AWS_PROFILE", unset = "")

  request_payer <- Sys.getenv("HCPX_AWS_REQUEST_PAYER", unset = "")
  if (!nzchar(request_payer)) request_payer <- NULL

  backend <- hcpx_backend_aws(
    bucket = bucket,
    region = region,
    profile = if (nzchar(profile)) profile else NULL,
    request_payer = request_payer
  )

  meta <- backend_head(backend, key)
  expect_s3_class(meta, "tbl_df")
  expect_true(is.numeric(meta$size_bytes[[1]]) || is.integer(meta$size_bytes[[1]]))
  expect_true(meta$size_bytes[[1]] > 0)

  dest <- file.path(withr::local_tempdir(), "aws-integration.bin")
  res <- backend_download(backend, key, dest, resume = TRUE)

  expect_equal(res$status, "success")
  expect_true(file.exists(dest))
  expect_true(file.info(dest)$size > 0)
  expect_equal(file.info(dest)$size, meta$size_bytes[[1]])
})

