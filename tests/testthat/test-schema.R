# Tests for schema.R

test_that("schema_init creates all tables", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  con <- catalog_connect(tmp_dir, engine = "auto")
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  # Initialize schema
  schema_init(con)

  # Check all 9 tables exist
  tables <- schema_tables(con)
  expect_true("schema_version" %in% tables)
  expect_true("subjects" %in% tables)
  expect_true("dictionary" %in% tables)
  expect_true("assets" %in% tables)
  expect_true("task_runs" %in% tables)
  expect_true("bundles" %in% tables)
  expect_true("ledger" %in% tables)
  expect_true("derived_ledger" %in% tables)
  expect_true("cohorts" %in% tables)
})

test_that("schema_init is idempotent", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  con <- catalog_connect(tmp_dir, engine = "auto")
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  # Initialize twice - should not error
  schema_init(con)
  expect_silent(schema_init(con))

  # Still have all tables
  expect_true(schema_complete(con))
})

test_that("schema_version_get returns correct version", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  con <- catalog_connect(tmp_dir, engine = "auto")
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  # Before init, should be NA
  expect_true(is.na(schema_version_get(con)))

  # After init, should be 1
  schema_init(con)
  expect_equal(schema_version_get(con), 1L)
})

test_that("schema_needs_init detects uninitialized database", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  con <- catalog_connect(tmp_dir, engine = "auto")
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  expect_true(schema_needs_init(con))

  schema_init(con)

  expect_false(schema_needs_init(con))
})

test_that("schema_complete checks for all required tables", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  con <- catalog_connect(tmp_dir, engine = "auto")
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  expect_false(schema_complete(con))

  schema_init(con)

  expect_true(schema_complete(con))
})

test_that("schema_drop_all removes all tables", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  con <- catalog_connect(tmp_dir, engine = "auto")
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  schema_init(con)
  expect_true(length(schema_tables(con)) >= 9)

  schema_drop_all(con)
  expect_equal(length(schema_tables(con)), 0)
})

test_that("assets table has correct structure", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  con <- catalog_connect(tmp_dir, engine = "auto")
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  schema_init(con)

  # Insert a test row
  DBI::dbExecute(con, "
    INSERT INTO assets (asset_id, release, subject_id, session, kind, task,
                        direction, run, space, derivative_level, file_type,
                        remote_path, size_bytes, access_tier)
    VALUES ('test1', 'HCP_1200', '100206', '3T', 'tfmri', 'WM',
            'LR', 1, 'MNINonLinear', 'preprocessed', 'dtseries',
            'path/to/file.dtseries.nii', 1000000, 'open')
  ")

  result <- DBI::dbGetQuery(con, "SELECT * FROM assets WHERE asset_id = 'test1'")
  expect_equal(nrow(result), 1)
  expect_equal(result$task[1], "WM")
  expect_equal(result$direction[1], "LR")
})

test_that("catalog_refresh_task_runs derives runs from assets", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  con <- catalog_connect(tmp_dir, engine = "auto")
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  schema_init(con)

  # Insert some assets
  DBI::dbExecute(con, "
    INSERT INTO assets (asset_id, release, subject_id, session, kind, task,
                        direction, run, space, remote_path)
    VALUES
      ('a1', 'HCP_1200', '100206', '3T', 'tfmri', 'WM', 'LR', 1, 'MNINonLinear', 'path1'),
      ('a2', 'HCP_1200', '100206', '3T', 'tfmri', 'WM', 'RL', 1, 'MNINonLinear', 'path2'),
      ('a3', 'HCP_1200', '100307', '3T', 'tfmri', 'MOTOR', 'LR', 1, 'MNINonLinear', 'path3')
  ")

  # Refresh task_runs
  catalog_refresh_task_runs(con)

  # Should have 3 distinct task runs
  result <- DBI::dbGetQuery(con, "SELECT * FROM task_runs ORDER BY run_id")
  expect_equal(nrow(result), 3)

  # Check the run_ids are correct
  expect_true("100206_WM_LR_1" %in% result$run_id)
  expect_true("100206_WM_RL_1" %in% result$run_id)
  expect_true("100307_MOTOR_LR_1" %in% result$run_id)
})

test_that("subjects table can store completion flags", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  con <- catalog_connect(tmp_dir, engine = "auto")
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  schema_init(con)

  DBI::dbExecute(con, "
    INSERT INTO subjects (subject_id, release, gender, age_range, fmri_wm_compl)
    VALUES ('100206', 'HCP_1200', 'M', '26-30', 1)
  ")

  result <- DBI::dbGetQuery(con, "SELECT * FROM subjects WHERE subject_id = '100206'")
  expect_equal(nrow(result), 1)
  expect_equal(result$fmri_wm_compl[1], 1)
})

test_that("ledger table supports pinning", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  con <- catalog_connect(tmp_dir, engine = "auto")
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  schema_init(con)

  # Insert a ledger entry
  DBI::dbExecute(con, "
    INSERT INTO ledger (asset_id, local_path, downloaded_at, last_accessed, pinned)
    VALUES ('a1', '/path/to/file', '2026-01-18 12:00:00', '2026-01-18 12:00:00', 1)
  ")

  result <- DBI::dbGetQuery(con, "SELECT * FROM ledger WHERE asset_id = 'a1'")
  expect_equal(result$pinned[1], 1)
})
