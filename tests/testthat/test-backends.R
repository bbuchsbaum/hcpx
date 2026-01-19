# Tests for backend implementations

# --- AWS Backend Tests ---

test_that("hcpx_backend_aws creates valid object", {
  backend <- hcpx_backend_aws()

  expect_s3_class(backend, "hcpx_backend_aws")
  expect_s3_class(backend, "hcpx_backend")
  expect_equal(backend$type, "aws")
  expect_equal(backend$bucket, "hcp-openaccess")
  expect_equal(backend$region, "us-east-1")
  expect_equal(backend$request_payer, "requester")
})

test_that("hcpx_backend_aws accepts custom bucket and region", {
  backend <- hcpx_backend_aws(bucket = "my-bucket", region = "eu-west-1")

  expect_equal(backend$bucket, "my-bucket")
  expect_equal(backend$region, "eu-west-1")
  expect_null(backend$request_payer)
})

test_that("backend_presign.hcpx_backend_aws returns HTTPS URL", {
  backend <- hcpx_backend_aws()

  url <- backend_presign(backend, "HCP_1200/100206/file.nii")

  expect_true(grepl("^https://", url))
  expect_true(grepl("hcp-openaccess", url))
  expect_true(grepl("100206", url))
})

test_that("aws backend errors clearly when aws CLI is missing", {
  withr::local_envvar(PATH = "")
  backend <- hcpx_backend_aws()

  dest <- file.path(withr::local_tempdir(), "aws-missing.bin")
  expect_error(
    backend_download(backend, "HCP_1200/100206/file.nii", dest),
    "AWS CLI required for requester-pays buckets"
  )
})

# --- Local Backend Tests ---

test_that("hcpx_backend_local creates valid object", {
  tmp_dir <- withr::local_tempdir()

  backend <- hcpx_backend_local(tmp_dir)

  expect_s3_class(backend, "hcpx_backend_local")
  expect_s3_class(backend, "hcpx_backend")
  expect_equal(backend$type, "local")
  expect_equal(backend$link_mode, "copy")
})

test_that("hcpx_backend_local accepts link_mode parameter", {
  tmp_dir <- withr::local_tempdir()

  backend_copy <- hcpx_backend_local(tmp_dir, link_mode = "copy")
  backend_sym <- hcpx_backend_local(tmp_dir, link_mode = "symlink")
  backend_hard <- hcpx_backend_local(tmp_dir, link_mode = "hardlink")

  expect_equal(backend_copy$link_mode, "copy")
  expect_equal(backend_sym$link_mode, "symlink")
  expect_equal(backend_hard$link_mode, "hardlink")
})

test_that("backend_list.hcpx_backend_local lists files", {
  tmp_dir <- withr::local_tempdir()

  # Create some test files
  dir.create(file.path(tmp_dir, "subdir"), recursive = TRUE)
  writeLines("test1", file.path(tmp_dir, "file1.txt"))
  writeLines("test2", file.path(tmp_dir, "subdir", "file2.txt"))

  backend <- hcpx_backend_local(tmp_dir)
  result <- backend_list(backend, "")

  expect_s3_class(result, "tbl_df")
  expect_true("remote_path" %in% names(result))
  expect_true("size_bytes" %in% names(result))
  expect_equal(nrow(result), 2)
})

test_that("backend_head.hcpx_backend_local returns file metadata", {
  tmp_dir <- withr::local_tempdir()
  test_file <- file.path(tmp_dir, "test.txt")
  writeLines("hello world", test_file)

  backend <- hcpx_backend_local(tmp_dir)
  result <- backend_head(backend, "test.txt")

  expect_s3_class(result, "tbl_df")
  expect_true(result$size_bytes > 0)
})

test_that("backend_head.hcpx_backend_local errors on missing file", {
  tmp_dir <- withr::local_tempdir()
  backend <- hcpx_backend_local(tmp_dir)

  expect_error(backend_head(backend, "nonexistent.txt"), "not found")
})

test_that("backend_presign.hcpx_backend_local returns local path", {
  tmp_dir <- withr::local_tempdir()
  backend <- hcpx_backend_local(tmp_dir)

  path <- backend_presign(backend, "subdir/file.txt")

  # Path should contain the root and the relative path
  expect_true(grepl("subdir", path))
  expect_true(grepl("file.txt", path))
})

test_that("backend_download.hcpx_backend_local copies files", {
  tmp_dir <- withr::local_tempdir()
  src_dir <- file.path(tmp_dir, "src")
  dest_dir <- file.path(tmp_dir, "dest")
  dir.create(src_dir)
  dir.create(dest_dir)

  # Create source file
  src_file <- file.path(src_dir, "test.txt")
  writeLines("test content", src_file)

  backend <- hcpx_backend_local(src_dir, link_mode = "copy")
  dest_file <- file.path(dest_dir, "copied.txt")

  result <- backend_download(backend, "test.txt", dest_file)

  expect_true(file.exists(dest_file))
  expect_equal(readLines(dest_file), "test content")
  expect_equal(result$status, "success")
})

test_that("backend_download.hcpx_backend_local creates symlinks", {
  skip_on_os("windows")  # Symlinks work differently on Windows

  tmp_dir <- withr::local_tempdir()
  src_dir <- file.path(tmp_dir, "src")
  dest_dir <- file.path(tmp_dir, "dest")
  dir.create(src_dir)
  dir.create(dest_dir)

  src_file <- file.path(src_dir, "test.txt")
  writeLines("test content", src_file)

  backend <- hcpx_backend_local(src_dir, link_mode = "symlink")
  dest_file <- file.path(dest_dir, "linked.txt")

  result <- backend_download(backend, "test.txt", dest_file)

  expect_true(file.exists(dest_file))
  expect_true(Sys.readlink(dest_file) != "")  # Is a symlink
  expect_equal(readLines(dest_file), "test content")
})

test_that("backend_download.hcpx_backend_local errors on missing source", {
  tmp_dir <- withr::local_tempdir()
  backend <- hcpx_backend_local(tmp_dir)

  expect_error(
    backend_download(backend, "nonexistent.txt", file.path(tmp_dir, "dest.txt")),
    "not found"
  )
})
