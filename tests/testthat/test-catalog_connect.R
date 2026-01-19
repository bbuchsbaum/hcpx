# Tests for catalog_connect.R

test_that("has_duckdb and has_sqlite work", {
  # These just check if packages are available - should not error
expect_type(has_duckdb(), "logical")
  expect_type(has_sqlite(), "logical")
})

test_that("catalog_connect creates database with SQLite", {
  skip_if_not(has_sqlite(), "RSQLite not available")

  tmp_dir <- withr::local_tempdir()
  con <- catalog_connect(tmp_dir, engine = "sqlite")

  expect_true(DBI::dbIsValid(con))
  expect_equal(catalog_engine(con), "sqlite")
  expect_true(file.exists(file.path(tmp_dir, "catalog.sqlite")))

  DBI::dbDisconnect(con)
})

test_that("catalog_connect creates database with DuckDB", {
  skip_if_not(has_duckdb(), "duckdb not available")

  tmp_dir <- withr::local_tempdir()
  con <- catalog_connect(tmp_dir, engine = "duckdb")

  expect_true(DBI::dbIsValid(con))
  expect_equal(catalog_engine(con), "duckdb")
  expect_true(file.exists(file.path(tmp_dir, "catalog.duckdb")))

  DBI::dbDisconnect(con)
})

test_that("catalog_connect auto-selects available engine", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()

  # Should pick whichever is available
  con <- catalog_connect(tmp_dir, engine = "auto")

  expect_true(DBI::dbIsValid(con))
  expect_true(catalog_engine(con) %in% c("duckdb", "sqlite"))

  DBI::dbDisconnect(con)
})

test_that("catalog_connect creates cache directory if missing", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_base <- withr::local_tempdir()
  nested_dir <- file.path(tmp_base, "deep", "nested", "cache")

  expect_false(dir.exists(nested_dir))

  con <- catalog_connect(nested_dir, engine = "auto")

  expect_true(dir.exists(nested_dir))
  expect_true(DBI::dbIsValid(con))

  DBI::dbDisconnect(con)
})

test_that("catalog_connection_valid detects invalid connections", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  expect_false(catalog_connection_valid(NULL))

  tmp_dir <- withr::local_tempdir()
  con <- catalog_connect(tmp_dir, engine = "auto")

  expect_true(catalog_connection_valid(con))

  DBI::dbDisconnect(con)

  expect_false(catalog_connection_valid(con))
})

test_that("catalog_ensure_connection reconnects when needed", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()

  # Start with NULL - should create new connection
  con <- catalog_ensure_connection(NULL, tmp_dir, engine = "auto")
  expect_true(catalog_connection_valid(con))
  DBI::dbDisconnect(con)

  # Start with invalid connection - should reconnect
  con2 <- catalog_ensure_connection(con, tmp_dir, engine = "auto")
  expect_true(catalog_connection_valid(con2))
  DBI::dbDisconnect(con2)
})

test_that("catalog_db_path returns correct path", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  con <- catalog_connect(tmp_dir, engine = "auto")

  db_path <- catalog_db_path(con)
  expect_true(file.exists(db_path))
  expect_true(grepl("catalog\\.(duckdb|sqlite)$", db_path))

  DBI::dbDisconnect(con)
})

test_that("catalog_disconnect handles various inputs gracefully", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  # Should not error on NULL
  expect_silent(catalog_disconnect(NULL))

  # Should work on valid connection
  tmp_dir <- withr::local_tempdir()
  con <- catalog_connect(tmp_dir, engine = "auto")
  expect_silent(catalog_disconnect(con))

  # Should not error on already-disconnected connection
  expect_silent(catalog_disconnect(con))
})
