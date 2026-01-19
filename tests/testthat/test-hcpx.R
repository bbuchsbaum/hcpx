# Tests for hcpx core handle functions

# --- hcpx_ya() tests ---

test_that("hcpx_ya creates an hcpx object", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  expect_s3_class(h, "hcpx")
})

test_that("hcpx_ya has required components", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  expect_true("release" %in% names(h))
  expect_true("engine" %in% names(h))
  expect_true("cache" %in% names(h))
  expect_true("backend" %in% names(h))
  expect_true("con" %in% names(h))
  expect_true("policy" %in% names(h))
  expect_true("versions" %in% names(h))
})

test_that("hcpx_ya sets default release", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  expect_equal(h$release, "HCP_1200")
})

test_that("hcpx_ya accepts custom release", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(release = "HCP_S1200", cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  expect_equal(h$release, "HCP_S1200")
})

test_that("hcpx_ya creates cache directory", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  new_cache <- file.path(tmp_dir, "new_cache_dir")
  expect_false(dir.exists(new_cache))

  h <- hcpx_ya(cache = new_cache)
  on.exit(hcpx_close(h), add = TRUE)

  expect_true(dir.exists(new_cache))
})

test_that("hcpx_ya selects backend correctly", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()

  # AWS backend (default)
  h_aws <- hcpx_ya(cache = tmp_dir, backend = "aws")
  on.exit(hcpx_close(h_aws), add = TRUE)
  expect_equal(h_aws$backend$type, "aws")

  # BALSA backend
  tmp_dir_balsa <- withr::local_tempdir()
  h_balsa <- hcpx_ya(cache = tmp_dir_balsa, backend = "balsa")
  on.exit(hcpx_close(h_balsa), add = TRUE)
  expect_equal(h_balsa$backend$type, "balsa")

  # Local backend
  tmp_dir2 <- withr::local_tempdir()
  h_local <- hcpx_ya(cache = tmp_dir2, backend = "local", local_root = tmp_dir2)
  on.exit(hcpx_close(h_local), add = TRUE)
  expect_equal(h_local$backend$type, "local")
})

test_that("hcpx_ya initializes policy with defaults", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  expect_true(h$policy$enforce)
  expect_false(h$policy$allow_export_restricted)
})

test_that("hcpx_ya seeds demo data by default", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir, catalog_version = "seed")
  on.exit(hcpx_close(h), add = TRUE)

  # Should have subjects
  n_subjects <- DBI::dbGetQuery(h$con, "SELECT COUNT(*) as n FROM subjects")$n
  expect_gt(n_subjects, 0)
})

# --- hcpx_cache_default() tests ---

test_that("hcpx_cache_default returns a path", {
  result <- hcpx_cache_default()

  expect_type(result, "character")
  expect_length(result, 1)
  expect_true(nzchar(result))
})

test_that("hcpx_cache_default respects HCPX_CACHE env var", {
  # Save original value
  original <- Sys.getenv("HCPX_CACHE", unset = NA)

  # Set custom value
  Sys.setenv(HCPX_CACHE = "/custom/cache/path")
  on.exit({
    if (is.na(original)) {
      Sys.unsetenv("HCPX_CACHE")
    } else {
      Sys.setenv(HCPX_CACHE = original)
    }
  })

  result <- hcpx_cache_default()
  expect_equal(result, "/custom/cache/path")
})

# --- hcpx_cache() tests ---

test_that("hcpx_cache modifies cache settings", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  # Modify settings
  h2 <- hcpx_cache(h, max_size_gb = 500, strategy = "manual")

  expect_equal(h2$cache$max_size_gb, 500)
  expect_equal(h2$cache$strategy, "manual")
})

test_that("hcpx_cache creates new cache directory", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  new_root <- file.path(tmp_dir, "new_cache_root")
  expect_false(dir.exists(new_root))

  h2 <- hcpx_cache(h, root = new_root)

  expect_true(dir.exists(new_root))
  expect_equal(h2$cache$root, new_root)
})

# --- hcpx_tbl() tests ---

test_that("hcpx_tbl attaches hcpx context", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  tbl <- tibble::tibble(x = 1:3)
  result <- hcpx:::hcpx_tbl(tbl, h, "test")

  expect_s3_class(result, "hcpx_tbl")
  expect_equal(attr(result, "kind"), "test")
  expect_identical(attr(result, "hcpx"), h)
})

test_that("hcpx_tbl preserves original class", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  tbl <- tibble::tibble(x = 1:3)
  result <- hcpx:::hcpx_tbl(tbl, h, "test")

  # Should have hcpx_tbl as first class
  expect_equal(class(result)[1], "hcpx_tbl")
  # Should also retain tibble classes
  expect_true("tbl_df" %in% class(result))
})

# --- get_hcpx() tests ---

test_that("get_hcpx extracts handle from hcpx_tbl", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  subj <- subjects(h)
  extracted <- hcpx:::get_hcpx(subj)

  expect_s3_class(extracted, "hcpx")
  expect_equal(extracted$release, h$release)
})

test_that("get_hcpx returns handle unchanged", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  extracted <- hcpx:::get_hcpx(h)

  expect_identical(extracted, h)
})

test_that("get_hcpx errors for object without hcpx context", {
  tbl <- tibble::tibble(x = 1:3)

  expect_error(
    hcpx:::get_hcpx(tbl),
    "No hcpx context"
  )
})

# --- get_con() tests ---

test_that("get_con returns DBI connection", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  con <- hcpx:::get_con(h)

  expect_true(inherits(con, "DBIConnection"))
})

test_that("get_con works with hcpx_tbl", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  subj <- subjects(h)
  con <- hcpx:::get_con(subj)

  expect_true(inherits(con, "DBIConnection"))
})

# --- print.hcpx tests ---

test_that("print.hcpx produces output", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  # Helper to capture cli output
  capture_cli <- function(expr) {
    msgs <- character()
    withCallingHandlers(
      stdout <- capture.output(expr),
      message = function(m) {
        msgs <<- c(msgs, conditionMessage(m))
        invokeRestart("muffleMessage")
      }
    )
    paste(c(stdout, msgs), collapse = "\n")
  }

  output <- capture_cli(print(h))

  expect_true(grepl("hcpx", output, ignore.case = TRUE))
  expect_true(grepl("HCP_1200", output))
})

test_that("print.hcpx returns invisibly", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  ret <- NULL
  capture.output(ret <- print(h), type = "message")

  expect_identical(ret, h)
})

# --- print.hcpx_tbl tests ---

test_that("print.hcpx_tbl includes kind header", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  # Helper to capture cli output
  capture_cli <- function(expr) {
    msgs <- character()
    withCallingHandlers(
      stdout <- capture.output(expr),
      message = function(m) {
        msgs <<- c(msgs, conditionMessage(m))
        invokeRestart("muffleMessage")
      }
    )
    paste(c(stdout, msgs), collapse = "\n")
  }

  subj <- subjects(h)
  output <- capture_cli(print(subj))

  expect_true(grepl("subjects", output, ignore.case = TRUE))
})

test_that("print.hcpx_tbl returns invisibly", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  subj <- subjects(h)
  ret <- NULL
  capture.output(ret <- print(subj), type = "message")

  expect_s3_class(ret, "hcpx_tbl")
})

# --- hcpx_close() tests ---

test_that("hcpx_close disconnects database", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)

  # Verify connection is valid before
  expect_true(DBI::dbIsValid(h$con))

  # Close
  hcpx_close(h)

  # Connection should be invalid now
  expect_false(DBI::dbIsValid(h$con))
})

test_that("hcpx_close returns invisibly", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)

  result <- hcpx_close(h)

  expect_null(result)
})

test_that("hcpx_close handles NULL connection gracefully", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)

  # Close twice - should not error
  hcpx_close(h)
  expect_no_error(hcpx_close(h))
})
