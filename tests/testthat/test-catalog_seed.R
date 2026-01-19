# Tests for catalog seed functions

# --- catalog_seed() tests ---

test_that("catalog_seed loads demo data into empty catalog", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  # Should have data after initialization (seed is called by default)
  expect_true(catalog_has_data(h))

  # Verify subjects loaded
  n_subjects <- DBI::dbGetQuery(get_con(h), "SELECT COUNT(*) as n FROM subjects")$n
  expect_gt(n_subjects, 0)

  # Verify assets loaded
  n_assets <- DBI::dbGetQuery(get_con(h), "SELECT COUNT(*) as n FROM assets")$n
  expect_gt(n_assets, 0)
})

test_that("catalog_seed is idempotent", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  # Get counts after first seed (from hcpx_ya)
  n_subjects_before <- DBI::dbGetQuery(get_con(h), "SELECT COUNT(*) as n FROM subjects")$n
  n_assets_before <- DBI::dbGetQuery(get_con(h), "SELECT COUNT(*) as n FROM assets")$n

  # Call seed again
  catalog_seed(h)

  # Counts should be the same (no duplicates)
  n_subjects_after <- DBI::dbGetQuery(get_con(h), "SELECT COUNT(*) as n FROM subjects")$n
  n_assets_after <- DBI::dbGetQuery(get_con(h), "SELECT COUNT(*) as n FROM assets")$n

  expect_equal(n_subjects_before, n_subjects_after)
  expect_equal(n_assets_before, n_assets_after)
})

test_that("catalog_seed loads bundles from YAML", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  # Check bundles were loaded
  n_bundles <- DBI::dbGetQuery(get_con(h), "SELECT COUNT(*) as n FROM bundles")$n
  expect_gt(n_bundles, 0)

  # Verify bundle structure
  bundle_row <- DBI::dbGetQuery(get_con(h), "SELECT * FROM bundles LIMIT 1")
  expect_true("bundle" %in% names(bundle_row))
  expect_true("description" %in% names(bundle_row))
  expect_true("definition_json" %in% names(bundle_row))
})

test_that("catalog_seed populates task_runs from assets", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  # Check task_runs were populated
  n_task_runs <- DBI::dbGetQuery(get_con(h), "SELECT COUNT(*) as n FROM task_runs")$n
  expect_gt(n_task_runs, 0)
})

# --- catalog_has_data() tests ---

test_that("catalog_has_data returns TRUE when data exists", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  expect_true(catalog_has_data(h))
})

test_that("catalog_has_data returns FALSE for empty catalog", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  # Clear the catalog
  catalog_clear(h)

  expect_false(catalog_has_data(h))
})

# --- catalog_clear() tests ---

test_that("catalog_clear removes all data", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  # Verify data exists
  expect_true(catalog_has_data(h))

  # Clear
  catalog_clear(h)

  # Verify all tables are empty
  expect_equal(DBI::dbGetQuery(get_con(h), "SELECT COUNT(*) as n FROM subjects")$n, 0)
  expect_equal(DBI::dbGetQuery(get_con(h), "SELECT COUNT(*) as n FROM assets")$n, 0)
  expect_equal(DBI::dbGetQuery(get_con(h), "SELECT COUNT(*) as n FROM task_runs")$n, 0)
  expect_equal(DBI::dbGetQuery(get_con(h), "SELECT COUNT(*) as n FROM bundles")$n, 0)
})

test_that("catalog_clear allows re-seeding", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  # Clear and re-seed
  catalog_clear(h)
  expect_false(catalog_has_data(h))

  catalog_seed(h)
  expect_true(catalog_has_data(h))
})

# --- catalog_seed_demo() tests ---

test_that("catalog_seed_demo is alias for catalog_seed", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  # Clear and re-seed with demo alias
  catalog_clear(h)
  catalog_seed_demo(h)

  expect_true(catalog_has_data(h))
})
