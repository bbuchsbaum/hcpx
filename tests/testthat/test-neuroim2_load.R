# Tests for neuroim2 data loading functions

# --- .detect_loader() tests ---

test_that(".detect_loader identifies CIFTI dtseries files", {
  result <- hcpx:::.detect_loader("subject/Results/tfMRI_WM_LR_Atlas.dtseries.nii")
  expect_equal(result$type, "cifti_dtseries")
  expect_equal(result$loader, "read_vec")

  # Also handles .gz
  result2 <- hcpx:::.detect_loader("file.dtseries.nii.gz")
  expect_equal(result2$type, "cifti_dtseries")
})

test_that(".detect_loader identifies CIFTI dscalar files", {
  result <- hcpx:::.detect_loader("subject/atlas.dscalar.nii")
  expect_equal(result$type, "cifti_dscalar")
  expect_equal(result$loader, "read_vec")
})

test_that(".detect_loader identifies CIFTI dlabel files", {
  result <- hcpx:::.detect_loader("subject/parcellation.dlabel.nii")
  expect_equal(result$type, "cifti_dlabel")
  expect_equal(result$loader, "read_vol_clustered")
})

test_that(".detect_loader identifies NIfTI files", {
  result <- hcpx:::.detect_loader("subject/T1w_acpc_dc_restore.nii.gz")
  expect_equal(result$type, "nifti")
  expect_equal(result$loader, "read_auto")

  result2 <- hcpx:::.detect_loader("file.nii")
  expect_equal(result2$type, "nifti")
})

test_that(".detect_loader identifies confounds TSV files", {
  result <- hcpx:::.detect_loader("subject/confounds.tsv")
  expect_equal(result$type, "confounds")
  expect_equal(result$loader, "read_tsv")
})

test_that(".detect_loader identifies EV text files", {
  result <- hcpx:::.detect_loader("subject/EVs/2BK_body.txt")
  expect_equal(result$type, "ev")
  expect_equal(result$loader, "read_ev")
})

test_that(".detect_loader handles unknown files", {
  result <- hcpx:::.detect_loader("file.unknown")
  expect_equal(result$type, "unknown")
  expect_null(result$loader)
})

# --- .read_confounds() tests ---

test_that(".read_confounds reads TSV files", {
  tmp <- tempfile(fileext = ".tsv")
  on.exit(unlink(tmp), add = TRUE)

  writeLines("col1\tcol2\tcol3\n1\t2\t3\n4\t5\t6", tmp)

  result <- hcpx:::.read_confounds(tmp)

  expect_s3_class(result, "data.frame")
  expect_equal(ncol(result), 3)
  expect_equal(nrow(result), 2)
  expect_equal(names(result), c("col1", "col2", "col3"))
})

# --- .read_ev() tests ---

test_that(".read_ev reads FSL 3-column format", {
  tmp <- tempfile(fileext = ".txt")
  on.exit(unlink(tmp), add = TRUE)

  writeLines("0.0 2.5 1.0\n10.0 2.5 1.0\n20.0 2.5 1.0", tmp)

  result <- hcpx:::.read_ev(tmp)

  expect_s3_class(result, "data.frame")
  expect_equal(ncol(result), 3)
  expect_equal(nrow(result), 3)
  expect_equal(names(result)[1:3], c("onset", "duration", "weight"))
  expect_equal(result$onset, c(0, 10, 20))
  expect_equal(result$duration, c(2.5, 2.5, 2.5))
})

# --- .combine_evs() tests ---

test_that(".combine_evs handles empty input", {
  result <- hcpx:::.combine_evs(list(), c())
  expect_null(result)
})

test_that(".combine_evs combines EVs from multiple runs", {
  evs1 <- list(
    cond_A = data.frame(onset = c(0, 10), duration = c(2, 2), weight = c(1, 1)),
    cond_B = data.frame(onset = c(5, 15), duration = c(2, 2), weight = c(1, 1))
  )
  evs2 <- list(
    cond_A = data.frame(onset = c(0, 10), duration = c(2, 2), weight = c(1, 1))
  )

  run_boundaries <- c(100, 100)  # 100 time points each

  result <- hcpx:::.combine_evs(list(evs1, evs2), run_boundaries)

  expect_type(result, "list")
  expect_true("cond_A" %in% names(result))
  expect_true("cond_B" %in% names(result))

  # cond_A should have 4 rows (2 from each run)
  expect_equal(nrow(result$cond_A), 4)

  # Second run onsets should be offset by 100
  expect_equal(result$cond_A$onset, c(0, 10, 100, 110))
})

# --- load_asset() tests (structure only, requires DB) ---

test_that("load_asset errors for non-existent asset", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  expect_error(
    load_asset(h, "nonexistent_asset_id"),
    "Asset not found"
  )
})

test_that("load_asset errors for uncached asset", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  # Get a real asset_id from the demo data
  con <- get_con(h)
  asset <- DBI::dbGetQuery(con, "SELECT asset_id FROM assets LIMIT 1")

  if (nrow(asset) > 0) {
    # Should error because it's not downloaded
    expect_error(
      load_asset(h, asset$asset_id[1]),
      "not downloaded"
    )
  }
})

test_that("load_asset dispatches to the detected loader", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  con <- get_con(h)
  asset <- DBI::dbGetQuery(con, "SELECT asset_id FROM assets WHERE file_type = 'dtseries' LIMIT 1")

  if (nrow(asset) == 0) {
    skip("No dtseries asset available in demo catalog")
  }

  local_path <- file.path(tmp_dir, "dummy.dtseries.nii")
  writeBin(raw(1), local_path)

  ledger_record(h, asset$asset_id[1], local_path, size_bytes = 1)

  called <- new.env(parent = emptyenv())
  called$path <- NULL
  called$mode <- NULL

  testthat::local_mocked_bindings(
    .load_neurovec = function(path, mode) {
      called$path <- path
      called$mode <- mode
      "mock_vec"
    },
    .env = asNamespace("hcpx")
  )

  result <- load_asset(h, asset$asset_id[1], mode = "normal")

  expect_equal(result, "mock_vec")
  expect_equal(called$path, local_path)
  expect_equal(called$mode, "normal")
})

# --- load_run() tests (structure only) ---

test_that("load_run errors for invalid run_id format", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  expect_error(
    load_run(h, "invalid_format"),
    "Invalid run_id format"
  )
})

test_that("load_run errors when no assets exist for the run", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  expect_error(
    load_run(h, "100206_WM_LR_99"),
    "No assets found for run"
  )
})

test_that("load_run returns dtseries, confounds, and named EVs", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  con <- get_con(h)

  # Ensure there's a confounds asset for this run (demo data ships dtseries + EVs)
  conf_asset_id <- "conf_test_1"
  DBI::dbExecute(con, "
    INSERT INTO assets
      (asset_id, release, subject_id, session, kind, task, direction, run,
       space, derivative_level, file_type, remote_path, size_bytes, access_tier)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  ", params = list(
    conf_asset_id,
    "HCP_1200",
    "100206",
    "3T",
    "tfmri",
    "WM",
    "LR",
    1L,
    "MNINonLinear",
    "preprocessed",
    "confounds",
    "HCP_1200/100206/MNINonLinear/Results/tfMRI_WM_LR/confounds.tsv",
    1024L,
    "open"
  ))

  # Mark required assets as cached so load_run passes the "downloaded" check
  for (aid in c("a001", "a009", "a010", conf_asset_id)) {
    ledger_record(h, aid, file.path(tmp_dir, paste0(aid, ".dummy")), size_bytes = 1)
  }

  testthat::local_mocked_bindings(
    load_asset = function(h, asset_id, ...) {
      if (identical(asset_id, "a001")) return("DT")
      if (identical(asset_id, conf_asset_id)) return(data.frame(c1 = 1))
      if (identical(asset_id, "a009")) return("EV_BODY")
      if (identical(asset_id, "a010")) return("EV_FACES")
      stop("Unexpected asset_id: ", asset_id)
    },
    .env = asNamespace("hcpx")
  )

  result <- load_run(h, "100206_WM_LR_1", bundle = "tfmri_cifti_full")

  expect_equal(result$dtseries, "DT")
  expect_s3_class(result$confounds, "data.frame")

  expect_true(all(c("body", "faces") %in% names(result$evs)))
  expect_equal(result$evs$body, "EV_BODY")
  expect_equal(result$evs$faces, "EV_FACES")
})

# --- load_task() tests ---

test_that("load_task requires tasks table input", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  # Passing subjects table instead of tasks should error
  subj <- subjects(h)

  expect_error(
    load_task(subj),
    "Expected tasks table"
  )
})

test_that("load_task concat=TRUE concatenates dtseries and adjusts EVs", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")
  skip_if_not_installed("neuroim2")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  task_tbl <- tasks(h) |> dplyr::slice_min(order_by = run_id, n = 2, with_ties = FALSE)

  if (nrow(dplyr::collect(task_tbl)) < 2) {
    skip("Need at least 2 task runs in demo catalog to test concatenation")
  }

  calls <- new.env(parent = emptyenv())
  calls$i <- 0L

  testthat::local_mocked_bindings(
    load_run = function(h, run_id, bundle, mode) {
      calls$i <- calls$i + 1L
      n_tp <- if (calls$i == 1L) 3L else 2L

      dt_space <- neuroim2::NeuroSpace(
        dim = c(2L, 2L, 1L, n_tp),
        spacing = c(2, 2, 2),
        origin = c(0, 0, 0)
      )
      dt <- neuroim2::DenseNeuroVec(array(calls$i, dim = c(2, 2, 1, n_tp)), dt_space)
      conf <- data.frame(run = calls$i)
      evs <- list(cond = data.frame(onset = 0, duration = 1, weight = 1))

      list(dtseries = dt, confounds = conf, evs = evs)
    },
    .env = asNamespace("hcpx")
  )

  result <- load_task(task_tbl, concat = TRUE)

  expect_s4_class(result$dtseries, "DenseNeuroVec")
  expect_equal(dim(result$dtseries)[4], 5)
  expect_equal(unname(result$run_boundaries), c(3L, 2L))
  expect_equal(nrow(result$confounds), 2)
  expect_equal(result$confounds$run, c(1L, 2L))

  expect_true("cond" %in% names(result$evs))
  expect_equal(result$evs$cond$onset, c(0, 3))
  expect_equal(result$evs$cond$run, c(1, 2))
})
