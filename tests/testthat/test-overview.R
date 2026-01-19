# Tests for overview functions

# Helper to capture cli output (uses messages, not stdout)
capture_cli <- function(expr) {
  msgs <- character()
  withCallingHandlers(
    {
      # Also capture stdout for cli_h1 etc that might use cat
      stdout <- capture.output(expr)
    },
    message = function(m) {
      msgs <<- c(msgs, conditionMessage(m))
      invokeRestart("muffleMessage")
    }
  )
  paste(c(stdout, msgs), collapse = "\n")
}

# --- overview.hcpx tests ---

test_that("overview.hcpx runs without error on initialized catalog", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  # Should run without error
  expect_no_error(capture_cli(overview(h)))
})

test_that("overview.hcpx shows subject count", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  output_str <- capture_cli(overview(h))

  # Should mention subjects
  expect_true(grepl("[Ss]ubject", output_str))
})

test_that("overview.hcpx shows asset count", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  output_str <- capture_cli(overview(h))

  # Should mention assets
  expect_true(grepl("[Aa]sset", output_str))
})

# --- overview.hcpx_tbl for subjects ---

test_that("overview works on subjects selection", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  subj <- subjects(h)
  output_str <- capture_cli(overview(subj))

  expect_true(grepl("[Ss]ubject", output_str))
})

# --- overview.hcpx_tbl for tasks ---

test_that("overview works on tasks selection", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  tsk <- tasks(h)
  output_str <- capture_cli(overview(tsk))

  expect_true(grepl("[Tt]ask|[Rr]un", output_str))
})

# --- overview.hcpx_tbl for assets ---

test_that("overview works on assets selection", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  ast <- assets(h)
  output_str <- capture_cli(overview(ast))

  expect_true(grepl("[Aa]sset|file", output_str))
})

# --- overview returns invisibly ---

test_that("overview returns input invisibly", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  ret <- NULL
  capture_cli(ret <- overview(h))

  expect_identical(ret, h)
})

# --- overview.hcpx additional tests ---

test_that("overview.hcpx shows gender breakdown", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  output_str <- capture_cli(overview(h))

  # Should show gender info (M and F)
  expect_true(grepl("[Gg]ender", output_str))
})

test_that("overview.hcpx shows task info", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  output_str <- capture_cli(overview(h))

  # Should show task runs
  expect_true(grepl("[Tt]ask", output_str))
})

test_that("overview.hcpx shows bundle count", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  output_str <- capture_cli(overview(h))

  # Should mention bundles
  expect_true(grepl("[Bb]undle", output_str))
})

test_that("overview.hcpx shows usage hint", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  output_str <- capture_cli(overview(h))

  # Should show usage hint with subjects() or tasks()
  expect_true(grepl("subjects|tasks|assets", output_str))
})

# --- overview.hcpx_tbl returns invisibly ---

test_that("overview.hcpx_tbl returns subjects invisibly", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  subj <- subjects(h)
  ret <- NULL
  capture_cli(ret <- overview(subj))

  expect_identical(ret, subj)
})

test_that("overview.hcpx_tbl returns tasks invisibly", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  tsk <- tasks(h)
  ret <- NULL
  capture_cli(ret <- overview(tsk))

  expect_identical(ret, tsk)
})

test_that("overview.hcpx_tbl returns assets invisibly", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  ast <- assets(h)
  ret <- NULL
  capture_cli(ret <- overview(ast))

  expect_identical(ret, ast)
})

# --- overview_subjects tests ---

test_that("overview on subjects shows age breakdown", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  subj <- subjects(h)
  output_str <- capture_cli(overview(subj))

  # Should show age info
  expect_true(grepl("[Aa]ge", output_str))
})

test_that("overview on subjects shows next step hint", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  subj <- subjects(h)
  output_str <- capture_cli(overview(subj))

  # Should suggest tasks() or assets() as next step
  expect_true(grepl("tasks|assets", output_str))
})

test_that("overview works on filtered subjects", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  female <- subjects(h, gender == "F")
  output_str <- capture_cli(overview(female))

  # Should show filtered count
  expect_true(grepl("[Ss]ubject", output_str))
})

# --- overview_tasks tests ---

test_that("overview on tasks shows run counts", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  tsk <- tasks(h)
  output_str <- capture_cli(overview(tsk))

  # Should mention runs
  expect_true(grepl("[Rr]un", output_str))
})

test_that("overview on tasks shows subject count", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  tsk <- tasks(h)
  output_str <- capture_cli(overview(tsk))

  # Should mention subjects
  expect_true(grepl("[Ss]ubject", output_str))
})

test_that("overview on tasks shows directions", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  tsk <- tasks(h)
  output_str <- capture_cli(overview(tsk))

  # Should show direction info (LR, RL)
  expect_true(grepl("[Dd]irection|LR|RL", output_str))
})

test_that("overview on tasks shows next step hint", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  tsk <- tasks(h)
  output_str <- capture_cli(overview(tsk))

  # Should suggest assets() as next step
  expect_true(grepl("assets", output_str))
})

test_that("overview works on filtered tasks", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  wm <- tasks(h, "WM")
  output_str <- capture_cli(overview(wm))

  # Should work without error
  expect_true(grepl("[Tt]ask|WM|[Rr]un", output_str))
})

# --- overview_assets tests ---

test_that("overview on assets shows file count", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  ast <- assets(h)
  output_str <- capture_cli(overview(ast))

  # Should show file count
  expect_true(grepl("files|[Aa]sset", output_str))
})

test_that("overview on assets shows size", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  ast <- assets(h)
  output_str <- capture_cli(overview(ast))

  # Should show size in GB
  expect_true(grepl("GB", output_str))
})

test_that("overview on assets shows file types", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  ast <- assets(h)
  output_str <- capture_cli(overview(ast))

  # Should show file type info
  expect_true(grepl("[Ff]ile type|dtseries|nifti|txt", output_str))
})

test_that("overview on assets shows next step hint", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  ast <- assets(h)
  output_str <- capture_cli(overview(ast))

  # Should suggest download as next step
  expect_true(grepl("download|plan", output_str))
})

test_that("overview works on filtered assets", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  ast <- assets(h, bundle = "tfmri_cifti_min")
  output_str <- capture_cli(overview(ast))

  # Should work without error
  expect_true(grepl("files|[Aa]sset", output_str))
})

test_that("overview on assets shows subject count", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  ast <- assets(h)
  output_str <- capture_cli(overview(ast))

  # Should show subject count
  expect_true(grepl("[Ss]ubject", output_str))
})
