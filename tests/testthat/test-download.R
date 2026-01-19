# Tests for download.R - download execution

test_that("download requires hcpx_plan object", {
  expect_error(download("not a plan"), "Expected hcpx_plan")
})

test_that("download requires attached handle", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  plan <- assets(h) |> plan_download()

  # Remove the handle
  attr(plan, "hcpx") <- NULL

  expect_error(download(plan), "no attached hcpx handle")
})

test_that("download returns early when all cached", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  # Get assets and cache them all
  assets_df <- dplyr::collect(assets(h))
  for (i in seq_len(nrow(assets_df))) {
    ledger_record(h, assets_df$asset_id[i], sprintf("/fake/path/%d.nii", i), size_bytes = 1000)
  }

  plan <- assets(h) |> plan_download()

  # All should be cached
  expect_equal(plan$summary$n_cached, nrow(assets_df))
  expect_equal(plan$summary$n_to_download, 0)

  # Download should return immediately
  result <- download(plan)

  expect_equal(nrow(result), nrow(assets_df))
  expect_true(all(result$status == "cached"))
})

test_that("plan_download warns for empty selection", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  # Create a plan with no matching assets (filter to non-existent asset_id)
  filtered <- assets(h) |>
    dplyr::filter(asset_id == "nonexistent_id")

  expect_warning(
    plan <- plan_download(filtered),
    "No assets selected"
  )

  expect_equal(nrow(plan$manifest), 0)
})

test_that("download_status reports correct counts", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  # Cache some but not all
  assets_df <- dplyr::collect(assets(h))
  ledger_record(h, assets_df$asset_id[1], "/fake/path/1.nii", size_bytes = 1000)
  ledger_record(h, assets_df$asset_id[2], "/fake/path/2.nii", size_bytes = 1000)

  plan <- assets(h) |> plan_download()
  status <- download_status(plan)

  expect_equal(status$n_total, nrow(assets_df))
  expect_equal(status$n_cached, 2)
  expect_equal(status$n_pending, nrow(assets_df) - 2)
})

test_that("materialize.hcpx_plan calls download", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  # Cache all assets
  assets_df <- dplyr::collect(assets(h))
  for (i in seq_len(nrow(assets_df))) {
    ledger_record(h, assets_df$asset_id[i], sprintf("/fake/path/%d.nii", i), size_bytes = 1000)
  }

  plan <- assets(h) |> plan_download()

  # materialize should work as alias for download
  result <- materialize(plan)

  expect_s3_class(result, "tbl_df")
  expect_true("status" %in% names(result))
})

# --- download_single tests ---

test_that("download_single works with local backend", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  local_root <- file.path(tmp_dir, "hcp_data")
  dir.create(local_root, recursive = TRUE)

  # Create a test file in the "remote" location
  test_file_dir <- file.path(local_root, "HCP_1200", "100206")
  dir.create(test_file_dir, recursive = TRUE)
  test_file <- file.path(test_file_dir, "test.txt")
  writeLines("test content", test_file)

  # Create handle with local backend
  h <- hcpx_ya(cache = tmp_dir, backend = "local", local_root = local_root)
  on.exit(hcpx_close(h), add = TRUE)

  # Download (copy) the file
  dest <- file.path(tmp_dir, "downloaded.txt")
  result <- hcpx:::download_single(h, "HCP_1200/100206/test.txt", dest)

  expect_true(result)
  expect_true(file.exists(dest))
  expect_equal(readLines(dest), "test content")
})

test_that("download_single errors for missing source file", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  local_root <- file.path(tmp_dir, "hcp_data")
  dir.create(local_root, recursive = TRUE)

  h <- hcpx_ya(cache = tmp_dir, backend = "local", local_root = local_root)
  on.exit(hcpx_close(h), add = TRUE)

  dest <- file.path(tmp_dir, "downloaded.txt")

  expect_error(
    hcpx:::download_single(h, "nonexistent/file.txt", dest),
    "Source file not found"
  )
})

# --- download_url tests ---

test_that("download_url creates destination file", {
  skip_if_offline()

  tmp_file <- tempfile(fileext = ".txt")
  on.exit(unlink(tmp_file), add = TRUE)

  # Use a small, reliable test URL
  result <- hcpx:::download_url(
    "https://httpbin.org/robots.txt",
    tmp_file,
    resume = FALSE
  )

  expect_true(result)
  expect_true(file.exists(tmp_file))
})

# --- materialize generic tests ---

test_that("materialize is an S3 generic", {
  expect_true(is.function(materialize))
})

test_that("materialize dispatches to hcpx_plan method", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  # Cache all assets to avoid actual downloads
  assets_df <- dplyr::collect(assets(h))
  for (i in seq_len(nrow(assets_df))) {
    ledger_record(h, assets_df$asset_id[i], sprintf("/fake/path/%d.nii", i), size_bytes = 1000)
  }

  plan <- assets(h) |> plan_download()

  # Should work via materialize()
  result <- materialize(plan)
  expect_s3_class(result, "tbl_df")
})

# --- download result structure tests ---

test_that("download returns expected columns", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  # Cache all assets
  assets_df <- dplyr::collect(assets(h))
  for (i in seq_len(nrow(assets_df))) {
    ledger_record(h, assets_df$asset_id[i], sprintf("/fake/path/%d.nii", i), size_bytes = 1000)
  }

  plan <- assets(h) |> plan_download()
  result <- download(plan)

  expect_true("asset_id" %in% names(result))
  expect_true("local_path" %in% names(result))
  expect_true("status" %in% names(result))
})

test_that("download handles empty plan gracefully", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  # Create empty plan
  filtered <- assets(h) |>
    dplyr::filter(asset_id == "nonexistent")

  suppressWarnings({
    plan <- plan_download(filtered)
  })

  result <- download(plan)

  expect_equal(nrow(result), 0)
  expect_true("asset_id" %in% names(result))
  expect_true("status" %in% names(result))
})

# --- download_status tests ---

test_that("download_status returns expected structure", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  plan <- assets(h) |> plan_download()
  status <- hcpx:::download_status(plan)

  expect_type(status, "list")
  expect_true("n_total" %in% names(status))
  expect_true("n_cached" %in% names(status))
  expect_true("n_pending" %in% names(status))
})

test_that("download_status counts match plan", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  plan <- assets(h) |> plan_download()
  status <- hcpx:::download_status(plan)

  expect_equal(status$n_total, status$n_cached + status$n_pending)
})
