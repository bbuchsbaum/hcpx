# Tests for asset queries

# --- assets() tests ---

test_that("assets returns an hcpx_tbl with kind 'assets'", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  result <- assets(h)

  expect_s3_class(result, "hcpx_tbl")
  expect_equal(attr(result, "kind"), "assets")
})

test_that("assets returns data from demo_assets", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  result <- assets(h) |> dplyr::collect()

  expect_gt(nrow(result), 0)
  expect_true("asset_id" %in% names(result))
  expect_true("subject_id" %in% names(result))
  expect_true("remote_path" %in% names(result))
})

test_that("assets preserves hcpx context", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  result <- assets(h)

  attached_h <- attr(result, "hcpx")
  expect_s3_class(attached_h, "hcpx")
  expect_equal(attached_h$release, h$release)
})

test_that("assets filters by bundle", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  # Get all assets
  all_assets <- assets(h) |> dplyr::collect()

  # Filter by a bundle if bundles exist
  available_bundles <- bundles(h)
  if (nrow(available_bundles) > 0) {
    bundle_name <- available_bundles$bundle[1]
    filtered <- assets(h, bundle = bundle_name) |> dplyr::collect()

    # Should have fewer or equal assets
    expect_lte(nrow(filtered), nrow(all_assets))
  }
})

test_that("assets chains from subjects", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  # Get female subjects' assets
  female_assets <- subjects(h, gender == "F") |>
    assets() |>
    dplyr::collect()

  # Get all female subject IDs
  female_ids <- subjects(h, gender == "F") |>
    dplyr::collect() |>
    dplyr::pull(subject_id)

  # All assets should belong to female subjects
  if (nrow(female_assets) > 0) {
    expect_true(all(female_assets$subject_id %in% female_ids))
  }
})

test_that("assets chains from tasks", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  # Get WM task assets
  wm_assets <- tasks(h, "WM") |>
    assets() |>
    dplyr::collect()

  # All assets should be for WM task (or NA for task-agnostic files)
  if (nrow(wm_assets) > 0) {
    expect_true(all(is.na(wm_assets$task) | wm_assets$task == "WM"))
  }
})

test_that("assets accepts additional filter expressions", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  all_assets <- assets(h) |> dplyr::collect()

  # Get unique file types
  if ("file_type" %in% names(all_assets) && nrow(all_assets) > 0) {
    file_types <- unique(all_assets$file_type)
    if (length(file_types) > 0 && !all(is.na(file_types))) {
      first_type <- na.omit(file_types)[1]
      # Filter using dplyr on the lazy table
      filtered <- assets(h) |>
        dplyr::filter(file_type == !!first_type) |>
        dplyr::collect()

      expect_true(all(filtered$file_type == first_type))
      expect_lte(nrow(filtered), nrow(all_assets))
    }
  }
})

# --- count_assets() tests ---

test_that("count_assets returns correct count", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  ast <- assets(h)
  count <- count_assets(ast)

  # Verify by collecting
  collected <- dplyr::collect(ast)

  expect_equal(count, nrow(collected))
})

test_that("count_assets works with filtered assets", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  all_count <- count_assets(assets(h))

  # Filter to specific subject
  first_subject <- subjects(h) |> dplyr::collect() |> dplyr::pull(subject_id) |> head(1)
  if (length(first_subject) > 0) {
    filtered <- subjects(h, subject_id == first_subject) |> assets()
    filtered_count <- count_assets(filtered)

    expect_lte(filtered_count, all_count)
  }
})

# --- total_size_bytes() tests ---

test_that("total_size_bytes returns numeric value", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  ast <- assets(h)
  size <- total_size_bytes(ast)

  expect_type(size, "double")
  expect_gte(size, 0)
})

test_that("total_size_bytes returns smaller value for filtered assets", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  all_size <- total_size_bytes(assets(h))

  # Filter to one subject
  first_subject <- subjects(h) |> dplyr::collect() |> dplyr::pull(subject_id) |> head(1)
  if (length(first_subject) > 0) {
    filtered <- subjects(h, subject_id == first_subject) |> assets()
    filtered_size <- total_size_bytes(filtered)

    expect_lte(filtered_size, all_size)
  }
})

# --- derived() from assets.R tests ---

test_that("derived returns an hcpx_tbl with kind 'derived'", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  result <- derived(h)

  expect_s3_class(result, "hcpx_tbl")
  expect_equal(attr(result, "kind"), "derived")
})

test_that("derived preserves hcpx context", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  result <- derived(h)

  attached_h <- attr(result, "hcpx")
  expect_s3_class(attached_h, "hcpx")
})

test_that("derived can filter by recipe name", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  # Filter by a recipe name (even if empty)
  result <- derived(h, recipe = "test_recipe")

  expect_s3_class(result, "hcpx_tbl")
  # Empty result is fine if no derived products exist
  collected <- dplyr::collect(result)
  expect_s3_class(collected, "tbl_df")
})
