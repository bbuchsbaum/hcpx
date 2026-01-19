# Tests for plan.R - download planning and manifest I/O

test_that("plan_download creates plan from assets", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  plan <- assets(h) |> plan_download()

  expect_s3_class(plan, "hcpx_plan")
  expect_true("manifest" %in% names(plan))
  expect_true("summary" %in% names(plan))
  expect_gt(nrow(plan$manifest), 0)
})

test_that("plan_download works from subjects", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  plan <- subjects(h) |> plan_download()

  expect_s3_class(plan, "hcpx_plan")
  expect_gt(nrow(plan$manifest), 0)
})

test_that("plan_download works from tasks", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  plan <- tasks(h, "WM") |> plan_download()

  expect_s3_class(plan, "hcpx_plan")
  expect_true(all(plan$manifest$task == "WM"))
})

test_that("plan_download accepts name parameter", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  plan <- assets(h) |> plan_download(name = "test_plan")

  expect_equal(plan$name, "test_plan")
})

test_that("plan_download summary has correct fields", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  plan <- assets(h) |> plan_download()

  expect_true("n_total" %in% names(plan$summary))
  expect_true("n_cached" %in% names(plan$summary))
  expect_true("n_to_download" %in% names(plan$summary))
  expect_true("total_bytes" %in% names(plan$summary))
  expect_true("download_bytes" %in% names(plan$summary))
  expect_true("n_subjects" %in% names(plan$summary))
})

test_that("plan_download detects cached files", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  # Get an asset ID from the catalog
  first_asset <- dplyr::collect(assets(h))[1, ]

  # Simulate it being cached
  ledger_record(h, first_asset$asset_id, "/fake/path.nii", size_bytes = 1000)

  # Create plan - should detect cache hit
  plan <- assets(h) |> plan_download()

  expect_true(any(plan$manifest$cached))
  expect_gt(plan$summary$n_cached, 0)
})

test_that("plan_to_download returns uncached files", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  plan <- assets(h) |> plan_download()

  to_dl <- plan_to_download(plan)

  expect_true(all(!to_dl$cached))
  expect_equal(nrow(to_dl), plan$summary$n_to_download)
})

test_that("plan_cached returns cached files", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  # Cache one file
  first_asset <- dplyr::collect(assets(h))[1, ]
  ledger_record(h, first_asset$asset_id, "/fake/path.nii", size_bytes = 1000)

  plan <- assets(h) |> plan_download()
  cached <- plan_cached(plan)

  expect_true(all(cached$cached))
  expect_equal(nrow(cached), plan$summary$n_cached)
})

test_that("write_manifest exports to JSON", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  plan <- assets(h) |> plan_download(name = "export_test")

  manifest_path <- file.path(tmp_dir, "test_manifest.json")
  write_manifest(plan, manifest_path)

  expect_true(file.exists(manifest_path))

  # Check it's valid JSON
  data <- jsonlite::read_json(manifest_path)
  expect_true("manifest" %in% names(data))
  expect_true("summary" %in% names(data))
})

test_that("write_manifest excludes local paths", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  plan <- assets(h) |> plan_download()

  manifest_path <- file.path(tmp_dir, "test_manifest.json")
  write_manifest(plan, manifest_path)

  data <- jsonlite::read_json(manifest_path, simplifyVector = TRUE)

  # Should not have local_path or cached columns (privacy/portability)
  expect_false("local_path" %in% names(data$manifest))
  expect_false("cached" %in% names(data$manifest))
})

test_that("read_manifest imports JSON", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  plan <- assets(h) |> plan_download(name = "roundtrip_test")

  manifest_path <- file.path(tmp_dir, "test_manifest.json")
  write_manifest(plan, manifest_path)

  # Read it back
  plan2 <- read_manifest(manifest_path)

  expect_s3_class(plan2, "hcpx_plan")
  expect_equal(plan2$name, "roundtrip_test")
  expect_equal(nrow(plan2$manifest), nrow(plan$manifest))
})

test_that("read_manifest with handle updates cache status", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  plan <- assets(h) |> plan_download()

  manifest_path <- file.path(tmp_dir, "test_manifest.json")
  write_manifest(plan, manifest_path)

  # Read with handle attached
  plan2 <- read_manifest(manifest_path, h = h)

  expect_true("local_path" %in% names(plan2$manifest))
  expect_true("cached" %in% names(plan2$manifest))
  expect_true(all(!is.na(plan2$manifest$local_path)))
})

test_that("print.hcpx_plan returns invisible plan", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  plan <- assets(h) |> plan_download(name = "print_test")

  # print should return invisible plan
  result <- print(plan)
  expect_identical(result, plan)
})

# --- plan_download manifest structure tests ---

test_that("plan manifest has expected columns", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  plan <- assets(h) |> plan_download()

  expected_cols <- c("asset_id", "release", "subject_id", "remote_path",
                     "local_path", "size_bytes", "cached", "kind",
                     "task", "direction", "run", "file_type")

  for (col in expected_cols) {
    expect_true(col %in% names(plan$manifest),
                info = paste("Missing column:", col))
  }
})

test_that("plan_download attaches hcpx context", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  plan <- assets(h) |> plan_download()

  expect_s3_class(attr(plan, "hcpx"), "hcpx")
})

test_that("plan_download records created_at timestamp", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  before <- Sys.time()
  plan <- assets(h) |> plan_download()
  after <- Sys.time()

  expect_true(plan$created_at >= before)
  expect_true(plan$created_at <= after)
})

test_that("plan_download records backend type", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  plan <- assets(h) |> plan_download()

  expect_equal(plan$backend_type, h$backend$type)
})

test_that("plan_download records release", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  plan <- assets(h) |> plan_download()

  expect_equal(plan$release, h$release)
})

# --- write_manifest error handling ---

test_that("write_manifest errors for non-plan object", {
  expect_error(
    write_manifest("not a plan", "test.json"),
    "Expected hcpx_plan"
  )
})

test_that("write_manifest includes hcpx version", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  plan <- assets(h) |> plan_download()

  manifest_path <- file.path(tmp_dir, "version_test.json")
  write_manifest(plan, manifest_path)

  data <- jsonlite::read_json(manifest_path)
  expect_true("hcpx_version" %in% names(data))
})

# --- read_manifest error handling ---

test_that("read_manifest errors for missing file", {
  expect_error(
    read_manifest("/nonexistent/path/manifest.json"),
    "not found"
  )
})

test_that("read_manifest sets from_manifest flag", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  plan <- assets(h) |> plan_download()

  manifest_path <- file.path(tmp_dir, "test.json")
  write_manifest(plan, manifest_path)

  plan2 <- read_manifest(manifest_path)

  expect_true(plan2$from_manifest)
})

# --- plan_attach_handle tests ---

test_that("plan_attach_handle updates local paths", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  plan <- assets(h) |> plan_download()

  manifest_path <- file.path(tmp_dir, "attach_test.json")
  write_manifest(plan, manifest_path)

  # Read without handle
  plan2 <- read_manifest(manifest_path)
  expect_true(all(is.na(plan2$manifest$local_path)))

  # Attach handle
  plan3 <- hcpx:::plan_attach_handle(plan2, h)
  expect_true(all(!is.na(plan3$manifest$local_path)))
})

test_that("plan_attach_handle updates cache status", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  # Cache one file
  first_asset <- dplyr::collect(assets(h))[1, ]
  ledger_record(h, first_asset$asset_id, "/fake/path.nii", size_bytes = 1000)

  plan <- assets(h) |> plan_download()

  manifest_path <- file.path(tmp_dir, "cache_test.json")
  write_manifest(plan, manifest_path)

  # Read and attach
  plan2 <- read_manifest(manifest_path)
  plan3 <- hcpx:::plan_attach_handle(plan2, h)

  expect_true(any(plan3$manifest$cached))
  expect_gt(plan3$summary$n_cached, 0)
})

# --- print.hcpx_plan output tests ---

test_that("print.hcpx_plan shows plan name when set", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  plan <- assets(h) |> plan_download(name = "named_plan")

  # Capture output
  output <- capture.output(print(plan), type = "message")
  output_str <- paste(output, collapse = "\n")

  expect_true(grepl("named_plan", output_str))
})

test_that("plan_download correctly counts subjects", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  plan <- assets(h) |> plan_download()

  n_unique_subjects <- length(unique(plan$manifest$subject_id))
  expect_equal(plan$summary$n_subjects, n_unique_subjects)
})

test_that("plan_download correctly counts tasks", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  plan <- assets(h) |> plan_download()

  n_unique_tasks <- length(unique(plan$manifest$task[!is.na(plan$manifest$task)]))
  expect_equal(plan$summary$n_tasks, n_unique_tasks)
})
