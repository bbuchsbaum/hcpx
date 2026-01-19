# Tests for catalog_build.R

test_that("ingest_subjects_csv adds subjects from CSV", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()

  # Create test CSV
  csv_path <- file.path(tmp_dir, "subjects.csv")
  write.csv(data.frame(
    Subject = c("111111", "222222", "333333"),
    Gender = c("M", "F", "M"),
    Age = c("26-30", "31-35", "22-25")
  ), csv_path, row.names = FALSE)

  h <- hcpx_ya(cache = tmp_dir, catalog_version = "none")
  on.exit(hcpx_close(h), add = TRUE)

  # Force initialize schema without seed data
  schema_init(get_con(h))

  n_added <- ingest_subjects_csv(h, csv_path)

  expect_equal(n_added, 3)

  # Verify subjects in database
  con <- get_con(h)
  result <- DBI::dbGetQuery(con, "SELECT * FROM subjects ORDER BY subject_id")
  expect_equal(nrow(result), 3)
  expect_true("111111" %in% result$subject_id)
})

test_that("ingest_subjects_csv filters by subject IDs", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()

  csv_path <- file.path(tmp_dir, "subjects.csv")
  write.csv(data.frame(
    Subject = c("111111", "222222", "333333"),
    Gender = c("M", "F", "M"),
    Age = c("26-30", "31-35", "22-25")
  ), csv_path, row.names = FALSE)

  h <- hcpx_ya(cache = tmp_dir, catalog_version = "none")
  on.exit(hcpx_close(h), add = TRUE)
  schema_init(get_con(h))

  n_added <- ingest_subjects_csv(h, csv_path, filter_ids = c("111111", "333333"))

  expect_equal(n_added, 2)
})

test_that("catalog_build with force clears existing data", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()

  # Create test CSV
  csv_path <- file.path(tmp_dir, "subjects.csv")
  write.csv(data.frame(
    Subject = c("111111"),
    Gender = c("M"),
    Age = c("26-30")
  ), csv_path, row.names = FALSE)

  h <- hcpx_ya(cache = tmp_dir)  # Starts with seed data
  on.exit(hcpx_close(h), add = TRUE)

  # Should have seed data
  con <- get_con(h)
  initial_count <- DBI::dbGetQuery(con, "SELECT COUNT(*) as n FROM subjects")$n
  expect_gt(initial_count, 0)

  # Build with force - should replace
  catalog_build(h, subjects_csv = csv_path, force = TRUE)

  final_count <- DBI::dbGetQuery(con, "SELECT COUNT(*) as n FROM subjects")$n
  expect_equal(final_count, 1)
})

test_that("add_assets_from_paths parses and adds assets", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  paths <- c(
    "HCP_1200/999999/MNINonLinear/Results/tfMRI_WM_LR/tfMRI_WM_LR_Atlas_MSMAll.dtseries.nii",
    "HCP_1200/999999/MNINonLinear/Results/tfMRI_WM_RL/tfMRI_WM_RL_Atlas_MSMAll.dtseries.nii"
  )

  n_added <- add_assets_from_paths(h, paths, "999999", size_bytes = 1000)

  expect_equal(n_added, 2)

  # Verify assets
  con <- get_con(h)
  result <- DBI::dbGetQuery(con, "SELECT * FROM assets WHERE subject_id = '999999'")
  expect_equal(nrow(result), 2)
  expect_true(all(result$task == "WM"))
})

test_that("catalog_build handles empty CSV gracefully", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()

  # Create empty CSV with headers
  csv_path <- file.path(tmp_dir, "empty.csv")
  write.csv(data.frame(Subject = character(), Gender = character()), csv_path, row.names = FALSE)

  h <- hcpx_ya(cache = tmp_dir, catalog_version = "none")
  on.exit(hcpx_close(h), add = TRUE)
  schema_init(get_con(h))

  n_added <- ingest_subjects_csv(h, csv_path)
  expect_equal(n_added, 0)
})

test_that("ingest_subjects_csv handles different column name formats", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()

  # Test with different column names
  csv_path <- file.path(tmp_dir, "subjects.csv")
  write.csv(data.frame(
    SubjectID = c("111111"),  # Different name
    Sex = c("M"),  # Different name
    AgeRange = c("26-30")  # Different name
  ), csv_path, row.names = FALSE)

  h <- hcpx_ya(cache = tmp_dir, catalog_version = "none")
  on.exit(hcpx_close(h), add = TRUE)
  schema_init(get_con(h))

  n_added <- ingest_subjects_csv(h, csv_path)
  expect_equal(n_added, 1)

  # Verify gender was mapped
  con <- get_con(h)
  result <- DBI::dbGetQuery(con, "SELECT gender FROM subjects WHERE subject_id = '111111'")
  expect_equal(result$gender[1], "M")
})

# --- ingest_subjects_csv error handling ---

test_that("ingest_subjects_csv errors without subject column", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()

  csv_path <- file.path(tmp_dir, "no_subjects.csv")
  write.csv(data.frame(
    Name = c("John", "Jane"),
    Gender = c("M", "F")
  ), csv_path, row.names = FALSE)

  h <- hcpx_ya(cache = tmp_dir, catalog_version = "none")
  on.exit(hcpx_close(h), add = TRUE)
  schema_init(get_con(h))

  expect_error(
    ingest_subjects_csv(h, csv_path),
    "subject/subject_id column"
  )
})

# --- ingest_behavioral_csv tests ---

test_that("ingest_behavioral_csv adds subjects from HCP-style CSV", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()

  csv_path <- file.path(tmp_dir, "behavioral.csv")
  write.csv(data.frame(
    Subject = c("100206", "100307"),
    Gender = c("M", "F"),
    Age = c("26-30", "22-25"),
    Release = c("Q1", "Q2")
  ), csv_path, row.names = FALSE)

  h <- hcpx_ya(cache = tmp_dir, catalog_version = "none")
  on.exit(hcpx_close(h), add = TRUE)
  schema_init(get_con(h))

  n_added <- hcpx:::ingest_behavioral_csv(h, csv_path)

  expect_equal(n_added, 2)
})

test_that("ingest_behavioral_csv filters by subject IDs", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()

  csv_path <- file.path(tmp_dir, "behavioral.csv")
  write.csv(data.frame(
    Subject = c("100206", "100307", "100408"),
    Gender = c("M", "F", "M"),
    Age = c("26-30", "22-25", "31-35")
  ), csv_path, row.names = FALSE)

  h <- hcpx_ya(cache = tmp_dir, catalog_version = "none")
  on.exit(hcpx_close(h), add = TRUE)
  schema_init(get_con(h))

  n_added <- hcpx:::ingest_behavioral_csv(h, csv_path, filter_ids = "100206")

  expect_equal(n_added, 1)
})

test_that("ingest_behavioral_csv errors without Subject column", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()

  csv_path <- file.path(tmp_dir, "bad_behavioral.csv")
  write.csv(data.frame(
    ID = c("100206"),
    Gender = c("M")
  ), csv_path, row.names = FALSE)

  h <- hcpx_ya(cache = tmp_dir, catalog_version = "none")
  on.exit(hcpx_close(h), add = TRUE)
  schema_init(get_con(h))

  expect_error(
    hcpx:::ingest_behavioral_csv(h, csv_path),
    "Subject column"
  )
})

# --- scan_s3_for_subjects tests ---

test_that("scan_s3_for_subjects warns for non-AWS backend", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir, backend = "local", local_root = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  expect_warning(
    result <- hcpx:::scan_s3_for_subjects(h, "100206"),
    "S3 scanning only available with AWS backend"
  )

  expect_equal(result, 0)
})

# --- add_assets_from_paths tests ---

test_that("add_assets_from_paths handles single size_bytes value", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  paths <- c(
    "HCP_1200/888888/MNINonLinear/Results/tfMRI_MOTOR_LR/tfMRI_MOTOR_LR_Atlas_MSMAll.dtseries.nii",
    "HCP_1200/888888/MNINonLinear/Results/tfMRI_MOTOR_RL/tfMRI_MOTOR_RL_Atlas_MSMAll.dtseries.nii"
  )

  # Single size_bytes should be applied to all
  n_added <- hcpx:::add_assets_from_paths(h, paths, "888888", size_bytes = 5000)

  expect_equal(n_added, 2)

  con <- get_con(h)
  result <- DBI::dbGetQuery(con, "SELECT size_bytes FROM assets WHERE subject_id = '888888'")
  expect_true(all(result$size_bytes == 5000))
})

test_that("add_assets_from_paths handles vector of size_bytes", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  paths <- c(
    "HCP_1200/777777/MNINonLinear/Results/tfMRI_GAMBLING_LR/tfMRI_GAMBLING_LR_Atlas_MSMAll.dtseries.nii",
    "HCP_1200/777777/MNINonLinear/Results/tfMRI_GAMBLING_RL/tfMRI_GAMBLING_RL_Atlas_MSMAll.dtseries.nii"
  )

  n_added <- hcpx:::add_assets_from_paths(h, paths, "777777", size_bytes = c(1000, 2000))

  expect_equal(n_added, 2)

  con <- get_con(h)
  result <- DBI::dbGetQuery(con, "SELECT size_bytes FROM assets WHERE subject_id = '777777' ORDER BY size_bytes")
  expect_equal(result$size_bytes, c(1000, 2000))
})

test_that("add_assets_from_paths handles paths that parser may accept", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  paths <- c(
    "HCP_1200/666666/MNINonLinear/Results/tfMRI_WM_LR/tfMRI_WM_LR_Atlas.dtseries.nii"
  )

  n_added <- hcpx:::add_assets_from_paths(h, paths, "666666", size_bytes = 1000)

  # Path should be added
  expect_equal(n_added, 1)

  con <- get_con(h)
  result <- DBI::dbGetQuery(con, "SELECT * FROM assets WHERE subject_id = '666666'")
  expect_equal(nrow(result), 1)
})

test_that("add_assets_from_paths doesn't duplicate existing assets", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  path <- "HCP_1200/555555/MNINonLinear/Results/tfMRI_LANGUAGE_LR/tfMRI_LANGUAGE_LR_Atlas.dtseries.nii"

  # Add once
  n_added1 <- hcpx:::add_assets_from_paths(h, path, "555555", size_bytes = 1000)
  expect_equal(n_added1, 1)

  # Try to add again
  n_added2 <- hcpx:::add_assets_from_paths(h, path, "555555", size_bytes = 1000)
  expect_equal(n_added2, 0)  # Should not duplicate
})

# --- catalog_build integration tests ---

test_that("catalog_build returns handle invisibly", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()

  csv_path <- file.path(tmp_dir, "subjects.csv")
  write.csv(data.frame(Subject = "123456", Gender = "M"), csv_path, row.names = FALSE)

  h <- hcpx_ya(cache = tmp_dir, catalog_version = "none")
  on.exit(hcpx_close(h), add = TRUE)
  schema_init(get_con(h))

  result <- catalog_build(h, subjects_csv = csv_path)

  expect_s3_class(result, "hcpx")
})

test_that("catalog_build warns when scanning with no subjects", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir, catalog_version = "none")
  on.exit(hcpx_close(h), add = TRUE)
  schema_init(get_con(h))

  expect_warning(
    catalog_build(h, scan_s3 = TRUE),
    "No subjects in catalog"
  )
})
