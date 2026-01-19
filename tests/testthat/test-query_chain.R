# Tests for the core query chain: subjects() |> tasks() |> assets()

test_that("hcpx_ya creates valid handle with seed data", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  expect_s3_class(h, "hcpx")
  expect_equal(h$release, "HCP_1200")
  expect_true(h$engine %in% c("duckdb", "sqlite"))
})

test_that("subjects() returns all subjects", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  s <- subjects(h)

  expect_s3_class(s, "hcpx_tbl")
  expect_equal(attr(s, "kind"), "subjects")

  count <- dplyr::count(s) |> dplyr::collect()
  expect_equal(count$n, 10)  # Demo data has 10 subjects
})

test_that("subjects() supports filtering", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  females <- subjects(h, gender == "F") |> dplyr::collect()
  males <- subjects(h, gender == "M") |> dplyr::collect()

  expect_true(all(females$gender == "F"))
  expect_true(all(males$gender == "M"))
  expect_equal(nrow(females) + nrow(males), 10)
})

test_that("tasks() returns all task runs", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  t <- tasks(h)

  expect_s3_class(t, "hcpx_tbl")
  expect_equal(attr(t, "kind"), "tasks")

  count <- dplyr::count(t) |> dplyr::collect()
  expect_gt(count$n, 0)
})

test_that("tasks() filters by canonical task name", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  wm_tasks <- tasks(h, "WM") |> dplyr::collect()

  expect_true(all(wm_tasks$task == "WM"))
})

test_that("tasks() chains from subjects", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  # Get female subjects and their tasks
  result <- subjects(h, gender == "F") |> tasks() |> dplyr::collect()

  # Get female subject IDs directly
  female_ids <- subjects(h, gender == "F") |>
    dplyr::select(subject_id) |>
    dplyr::collect() |>
    dplyr::pull(subject_id)

  # All task runs should be from female subjects
  expect_true(all(result$subject_id %in% female_ids))
})

test_that("assets() returns all assets", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  a <- assets(h)

  expect_s3_class(a, "hcpx_tbl")
  expect_equal(attr(a, "kind"), "assets")

  count <- dplyr::count(a) |> dplyr::collect()
  expect_equal(count$n, 16)  # Demo data has 16 assets
})

test_that("assets() filters by bundle", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  # This bundle filters for kind=tfmri and file_type=dtseries
  result <- assets(h, bundle = "tfmri_cifti_min") |> dplyr::collect()

  expect_true(all(result$kind == "tfmri"))
  expect_true(all(result$file_type == "dtseries"))
})

test_that("full chain subjects |> tasks |> assets works", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  # Full chain: female subjects, WM task, all assets
  result <- subjects(h, gender == "F") |>
    tasks("WM") |>
    assets() |>
    dplyr::collect()

  # Should get assets for female subjects doing WM task
  female_ids <- subjects(h, gender == "F") |>
    dplyr::select(subject_id) |>
    dplyr::collect() |>
    dplyr::pull(subject_id)

  expect_true(all(result$subject_id %in% female_ids))
  expect_true(all(result$task == "WM"))
})

test_that("bundles() lists available bundles", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  b <- bundles(h)

  expect_s3_class(b, "tbl_df")
  expect_true("bundle" %in% names(b))
  expect_true("description" %in% names(b))
  expect_true("tfmri_cifti_min" %in% b$bundle)
})

test_that("task_dictionary() returns task info", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  dict <- task_dictionary()

  expect_s3_class(dict, "tbl_df")
  expect_true("task" %in% names(dict))
  expect_true("description" %in% names(dict))
  expect_true("synonyms" %in% names(dict))
  expect_true("WM" %in% dict$task)
})

test_that("hcpx_tbl carries context through chain", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  s <- subjects(h)
  t <- tasks(s)
  a <- assets(t)

  # All should have the same hcpx handle attached
  h_from_s <- get_hcpx(s)
  h_from_t <- get_hcpx(t)
  h_from_a <- get_hcpx(a)

  expect_identical(h_from_s$release, h$release)
  expect_identical(h_from_t$release, h$release)
  expect_identical(h_from_a$release, h$release)
})

# --- dplyr verb compatibility tests ---

test_that("subjects() supports dplyr select", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  result <- subjects(h) |>
    dplyr::select(subject_id, gender) |>
    dplyr::collect()

  expect_equal(ncol(result), 2)
  expect_true("subject_id" %in% names(result))
  expect_true("gender" %in% names(result))
})

test_that("tasks() supports dplyr distinct", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  result <- tasks(h) |>
    dplyr::select(task) |>
    dplyr::distinct() |>
    dplyr::collect()

  # Should have unique tasks
  expect_equal(nrow(result), length(unique(result$task)))
})

test_that("assets() supports dplyr arrange", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  result <- assets(h) |>
    dplyr::arrange(subject_id, task) |>
    dplyr::collect()

  # Should be sorted
  expect_equal(result$subject_id, sort(result$subject_id))
})

test_that("assets() supports dplyr group_by and summarise", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  result <- assets(h) |>
    dplyr::group_by(task) |>
    dplyr::summarise(n = dplyr::n(), .groups = "drop") |>
    dplyr::collect()

  expect_true("task" %in% names(result))
  expect_true("n" %in% names(result))
})

# --- multiple filter expressions tests ---

test_that("subjects() supports multiple filter expressions", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  result <- subjects(h, gender == "M", age_range == "26-30") |>
    dplyr::collect()

  expect_true(all(result$gender == "M"))
  expect_true(all(result$age_range == "26-30"))
})

test_that("tasks() accepts synonyms", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  # "WORKING_MEMORY" should resolve to "WM"
  result <- tasks(h, "WORKING_MEMORY") |> dplyr::collect()

  expect_true(all(result$task == "WM"))
})

# --- edge cases ---

test_that("assets() handles empty subject filter gracefully", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  # Filter to non-existent subject
  result <- subjects(h, subject_id == "nonexistent") |>
    assets() |>
    dplyr::collect()

  expect_equal(nrow(result), 0)
})

test_that("assets() returns correct kind attribute", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  a <- assets(h)

  expect_equal(attr(a, "kind"), "assets")
})

test_that("subjects() returns correct kind attribute", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  s <- subjects(h)

  expect_equal(attr(s, "kind"), "subjects")
})

test_that("tasks() returns correct kind attribute", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  t <- tasks(h)

  expect_equal(attr(t, "kind"), "tasks")
})

test_that("chain preserves lazy evaluation", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  # Chain should not materialize until collect()
  result <- subjects(h) |>
    tasks() |>
    assets()

  # Should still be a lazy table
  expect_true(inherits(result, "tbl_lazy") || inherits(result, "tbl_sql"))
})
