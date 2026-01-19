# Tests for subject queries

# --- subjects() tests ---

test_that("subjects returns an hcpx_tbl with kind 'subjects'", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  result <- subjects(h)

  expect_s3_class(result, "hcpx_tbl")
  expect_equal(attr(result, "kind"), "subjects")
})

test_that("subjects returns data from demo_subjects", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  result <- subjects(h) |> dplyr::collect()

  expect_equal(nrow(result), 10)  # Demo has 10 subjects
  expect_true("subject_id" %in% names(result))
  expect_true("gender" %in% names(result))
})

test_that("subjects filters by gender", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  female <- subjects(h, gender == "F") |> dplyr::collect()
  male <- subjects(h, gender == "M") |> dplyr::collect()

  expect_true(all(female$gender == "F"))
  expect_true(all(male$gender == "M"))
  expect_equal(nrow(female) + nrow(male), 10)
})

test_that("subjects supports multiple filters", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  result <- subjects(h, gender == "M", age_range == "26-30") |> dplyr::collect()

  expect_true(all(result$gender == "M"))
  expect_true(all(result$age_range == "26-30"))
})

test_that("subjects preserves hcpx context", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  result <- subjects(h)

  attached_h <- attr(result, "hcpx")
  expect_s3_class(attached_h, "hcpx")
  expect_equal(attached_h$release, h$release)
})

# --- get_subject_ids() tests ---

test_that("get_subject_ids extracts subject IDs from subjects table", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  subj <- subjects(h)
  ids <- get_subject_ids(subj)

  expect_type(ids, "character")
  expect_length(ids, 10)
  expect_true(all(nchar(ids) == 6))  # HCP IDs are 6 digits
})

test_that("get_subject_ids works with filtered subjects", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  female <- subjects(h, gender == "F")
  ids <- get_subject_ids(female)

  # Should have fewer than 10 IDs
  expect_lt(length(ids), 10)
  expect_true(all(nchar(ids) == 6))
})

test_that("get_subject_ids errors for non-hcpx_tbl input", {
  expect_error(
    get_subject_ids(data.frame(subject_id = "100206")),
    "Expected hcpx_tbl"
  )
})

# --- count_subjects() tests ---

test_that("count_subjects returns correct count", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  subj <- subjects(h)
  count <- count_subjects(subj)

  expect_equal(count, 10)
})

test_that("count_subjects works with filtered subjects", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  female <- subjects(h, gender == "F")
  count <- count_subjects(female)

  expect_type(count, "integer")
  expect_lt(count, 10)
})

# --- subjects() column tests ---

test_that("subjects returns expected columns", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  result <- subjects(h) |> dplyr::collect()

  expected_cols <- c("subject_id", "release", "gender", "age_range")
  for (col in expected_cols) {
    expect_true(col %in% names(result), info = paste("Missing column:", col))
  }
})

test_that("subjects is lazy until collect", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  result <- subjects(h)

  # Should be a lazy table, not a data frame
  expect_true(inherits(result, "tbl_lazy") || inherits(result, "tbl_sql"))
})

test_that("subjects chains to tasks correctly", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  # Filter subjects, then get their tasks
  result <- subjects(h, gender == "F") |> tasks() |> dplyr::collect()

  # All task runs should be from female subjects
  female_ids <- subjects(h, gender == "F") |>
    dplyr::select(subject_id) |>
    dplyr::collect() |>
    dplyr::pull(subject_id)

  expect_true(all(result$subject_id %in% female_ids))
})

test_that("subjects chains to assets correctly", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  result <- subjects(h, gender == "M") |> assets() |> dplyr::collect()

  male_ids <- subjects(h, gender == "M") |>
    dplyr::select(subject_id) |>
    dplyr::collect() |>
    dplyr::pull(subject_id)

  expect_true(all(result$subject_id %in% male_ids))
})

test_that("subjects filter with OR condition", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  # Use dplyr filter for OR condition
  result <- subjects(h) |>
    dplyr::filter(age_range == "22-25" | age_range == "26-30") |>
    dplyr::collect()

  expect_true(all(result$age_range %in% c("22-25", "26-30")))
})

test_that("subjects filter with IN condition", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  target_ages <- c("22-25", "26-30")
  result <- subjects(h) |>
    dplyr::filter(age_range %in% target_ages) |>
    dplyr::collect()

  expect_true(all(result$age_range %in% target_ages))
})

test_that("subjects filter returns empty when no match", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  result <- subjects(h, subject_id == "nonexistent") |> dplyr::collect()

  expect_equal(nrow(result), 0)
  expect_s3_class(result, "tbl_df")
})

test_that("subjects works with specific subject_id filter", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  # Get first subject ID
  all_ids <- get_subject_ids(subjects(h))
  first_id <- all_ids[1]

  result <- subjects(h, subject_id == first_id) |> dplyr::collect()

  expect_equal(nrow(result), 1)
  expect_equal(result$subject_id[1], first_id)
})

# --- get_subject_ids additional tests ---

test_that("get_subject_ids works from tasks table", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  task_tbl <- tasks(h, "WM")
  ids <- get_subject_ids(task_tbl)

  expect_type(ids, "character")
  expect_true(length(ids) > 0)
})

test_that("get_subject_ids works from assets table", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  asset_tbl <- assets(h)
  ids <- get_subject_ids(asset_tbl)

  expect_type(ids, "character")
  expect_true(length(ids) > 0)
})

test_that("get_subject_ids returns unique IDs", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  # Assets table has multiple rows per subject
  asset_tbl <- assets(h)
  ids <- get_subject_ids(asset_tbl)

  # Should return unique IDs only
  expect_equal(ids, unique(ids))
})

test_that("get_subject_ids returns empty for empty table", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  # Filter to get empty result
  empty_tbl <- subjects(h, subject_id == "nonexistent")
  ids <- get_subject_ids(empty_tbl)

  expect_type(ids, "character")
  expect_equal(length(ids), 0)
})

# --- count_subjects additional tests ---

test_that("count_subjects works from tasks table", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  count <- count_subjects(tasks(h))

  expect_type(count, "integer")
  expect_gt(count, 0)
})

test_that("count_subjects works from assets table", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  count <- count_subjects(assets(h))

  expect_type(count, "integer")
  expect_gt(count, 0)
})

test_that("count_subjects returns 0 for empty table", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  empty_tbl <- subjects(h, subject_id == "nonexistent")
  count <- count_subjects(empty_tbl)

  expect_equal(count, 0L)
})

test_that("count_subjects is consistent with get_subject_ids", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  tbl <- assets(h)
  count <- count_subjects(tbl)
  ids <- get_subject_ids(tbl)

  expect_equal(count, length(ids))
})

# --- subjects() dplyr verb tests ---

test_that("subjects supports dplyr::select", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  result <- subjects(h) |>
    dplyr::select(subject_id, gender) |>
    dplyr::collect()

  expect_equal(ncol(result), 2)
  expect_equal(names(result), c("subject_id", "gender"))
})

test_that("subjects supports dplyr::arrange", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  result <- subjects(h) |>
    dplyr::arrange(subject_id) |>
    dplyr::collect()

  expect_equal(result$subject_id, sort(result$subject_id))
})

test_that("subjects supports dplyr::mutate", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  result <- subjects(h) |>
    dplyr::mutate(is_female = gender == "F") |>
    dplyr::collect()

  expect_true("is_female" %in% names(result))
  expect_true(all(result$is_female == (result$gender == "F")))
})

test_that("subjects supports dplyr::group_by and summarise", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  result <- subjects(h) |>
    dplyr::group_by(gender) |>
    dplyr::summarise(n = dplyr::n(), .groups = "drop") |>
    dplyr::collect()

  expect_true("gender" %in% names(result))
  expect_true("n" %in% names(result))
  expect_equal(sum(result$n), 10)
})
