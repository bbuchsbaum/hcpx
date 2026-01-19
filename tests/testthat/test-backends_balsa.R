# Tests for BALSA/Aspera backend

test_that("hcpx_backend_balsa creates backend object", {
  backend <- hcpx_backend_balsa()

  expect_s3_class(backend, "hcpx_backend_balsa")
  expect_s3_class(backend, "hcpx_backend")
  expect_equal(backend$type, "balsa")
  expect_true(is.integer(backend$port))
})

test_that("backend_list/head/presign are not supported for balsa backend", {
  backend <- hcpx_backend_balsa()

  expect_error(backend_list(backend, ""), "not supported")
  expect_error(backend_head(backend, "x"), "not supported")
  expect_error(backend_presign(backend, "x"), "not supported")
})

test_that("backend_download.hcpx_backend_balsa uses ascp for non-http sources", {
  backend <- hcpx_backend_balsa(
    ascp = "ascp",
    key = "/tmp/fake_key",
    host = "example.org",
    user = "user",
    port = 33001L,
    rate = "10m"
  )

  tmp_dir <- withr::local_tempdir()
  dest <- file.path(tmp_dir, "out.bin")

  seen <- new.env(parent = emptyenv())
  seen$args <- NULL

  testthat::local_mocked_bindings(
    .hcpx_run_system2 = function(command, args, stdout, stderr) {
      seen$command <- command
      seen$args <- args
      # Create an output file in the destination directory as if ascp succeeded
      out_dir <- args[length(args)]
      dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
      writeBin(as.raw(c(1, 2, 3)), file.path(out_dir, "out.bin"))
      structure(character(), status = 0L)
    },
    .package = "hcpx"
  )

  res <- backend_download(backend, "/path/to/out.bin", dest, resume = TRUE)

  expect_s3_class(res, "tbl_df")
  expect_equal(res$status, "success")
  expect_true(file.exists(dest))
  expect_equal(seen$command, "ascp")
  expect_true(any(seen$args == "-k"))
  expect_true("2" %in% seen$args) # resume enabled
})

test_that("backend_download.hcpx_backend_balsa uses HTTP path for https URLs", {
  backend <- hcpx_backend_balsa(ascp = "ascp", key = "/tmp/fake_key")

  tmp_dir <- withr::local_tempdir()
  dest <- file.path(tmp_dir, "http.bin")

  testthat::local_mocked_bindings(
    download_url = function(url, dest, resume = TRUE) {
      writeBin(as.raw(1:5), dest)
      TRUE
    },
    .package = "hcpx"
  )

  res <- backend_download(backend, "https://example.org/file.bin", dest, resume = FALSE)
  expect_equal(res$status, "success")
  expect_true(file.exists(dest))
})
