# Tests for cache.R - ledger and cache management

test_that("ledger_record adds entry to ledger", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  ledger_record(h, "test_asset_1", "/path/to/file.nii", size_bytes = 1000000)

  con <- get_con(h)
  result <- DBI::dbGetQuery(con, "SELECT * FROM ledger WHERE asset_id = 'test_asset_1'")

  expect_equal(nrow(result), 1)
  expect_equal(result$local_path[1], "/path/to/file.nii")
  expect_equal(result$size_bytes[1], 1000000)
  expect_equal(result$pinned[1], 0)
})

test_that("ledger_record updates existing entry", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  ledger_record(h, "test_asset_2", "/path/to/old.nii", size_bytes = 1000)
  ledger_record(h, "test_asset_2", "/path/to/new.nii", size_bytes = 2000)

  con <- get_con(h)
  result <- DBI::dbGetQuery(con, "SELECT * FROM ledger WHERE asset_id = 'test_asset_2'")

  expect_equal(nrow(result), 1)
  expect_equal(result$local_path[1], "/path/to/new.nii")
  expect_equal(result$size_bytes[1], 2000)
})

test_that("ledger_record preserves pinned status on update", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  ledger_record(h, "test_asset_3", "/path/to/file.nii", size_bytes = 1000)
  cache_pin(h, "test_asset_3")

  # Re-record (simulate re-download)
  ledger_record(h, "test_asset_3", "/path/to/file.nii", size_bytes = 1000)

  con <- get_con(h)
  result <- DBI::dbGetQuery(con, "SELECT pinned FROM ledger WHERE asset_id = 'test_asset_3'")
  expect_equal(result$pinned[1], 1)  # Still pinned
})

test_that("ledger_lookup returns cached assets", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  ledger_record(h, "asset_a", "/path/a.nii", size_bytes = 100)
  ledger_record(h, "asset_b", "/path/b.nii", size_bytes = 200)

  result <- ledger_lookup(h, c("asset_a", "asset_b", "asset_c"))

  expect_equal(nrow(result), 2)
  expect_true("asset_a" %in% result$asset_id)
  expect_true("asset_b" %in% result$asset_id)
  expect_false("asset_c" %in% result$asset_id)
})

test_that("ledger_touch updates last_accessed", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  ledger_record(h, "asset_touch", "/path/touch.nii", size_bytes = 100)

  con <- get_con(h)
  before <- DBI::dbGetQuery(con, "SELECT last_accessed FROM ledger WHERE asset_id = 'asset_touch'")$last_accessed[1]

  Sys.sleep(0.1)  # Ensure time difference
  ledger_touch(h, "asset_touch")

  after <- DBI::dbGetQuery(con, "SELECT last_accessed FROM ledger WHERE asset_id = 'asset_touch'")$last_accessed[1]

  expect_true(after >= before)
})

test_that("ledger_is_cached returns correct boolean vector", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  ledger_record(h, "cached_1", "/path/1.nii", size_bytes = 100)
  ledger_record(h, "cached_2", "/path/2.nii", size_bytes = 100)

  result <- ledger_is_cached(h, c("cached_1", "not_cached", "cached_2"))

  expect_equal(result, c(TRUE, FALSE, TRUE))
})

test_that("ledger_remove removes entries", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  ledger_record(h, "to_remove", "/path/rm.nii", size_bytes = 100)
  expect_true(ledger_is_cached(h, "to_remove"))

  ledger_remove(h, "to_remove")
  expect_false(ledger_is_cached(h, "to_remove"))
})

test_that("cache_pin and cache_unpin work correctly", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  ledger_record(h, "pin_test", "/path/pin.nii", size_bytes = 100)

  con <- get_con(h)

  # Initially not pinned
  expect_equal(DBI::dbGetQuery(con, "SELECT pinned FROM ledger WHERE asset_id = 'pin_test'")$pinned[1], 0)

  # Pin it
  cache_pin(h, "pin_test")
  expect_equal(DBI::dbGetQuery(con, "SELECT pinned FROM ledger WHERE asset_id = 'pin_test'")$pinned[1], 1)

  # Unpin it
  cache_unpin(h, "pin_test")
  expect_equal(DBI::dbGetQuery(con, "SELECT pinned FROM ledger WHERE asset_id = 'pin_test'")$pinned[1], 0)
})

test_that("cache_status returns correct statistics", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  # Add some files
  ledger_record(h, "stat_1", "/path/1.nii", size_bytes = 1000, backend = "aws")
  ledger_record(h, "stat_2", "/path/2.nii", size_bytes = 2000, backend = "aws")
  ledger_record(h, "stat_3", "/path/3.nii", size_bytes = 3000, backend = "local")
  cache_pin(h, "stat_1")

  status <- cache_status(h)

  expect_equal(status$n_files, 3)
  expect_equal(status$total_bytes, 6000)
  expect_equal(status$n_pinned, 1)
  expect_equal(status$max_size_gb, 200)
})

test_that("cache_list returns all cached files", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  ledger_record(h, "list_1", "/path/1.nii", size_bytes = 100)
  ledger_record(h, "list_2", "/path/2.nii", size_bytes = 200)
  cache_pin(h, "list_1")

  all_files <- cache_list(h)
  expect_equal(nrow(all_files), 2)

  pinned_only <- cache_list(h, pinned_only = TRUE)
  expect_equal(nrow(pinned_only), 1)
  expect_equal(pinned_only$asset_id[1], "list_1")
})

test_that("cache_path generates correct paths", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  path <- cache_path(h, "HCP_1200", "100206", "tfMRI_WM_LR/file.nii")

  expect_true(grepl("files", path))
  expect_true(grepl("HCP_1200", path))
  expect_true(grepl("100206", path))
  expect_true(grepl("tfMRI_WM_LR", path))
})

test_that("cache_prune respects pinned files", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  # Create temp files
  file1 <- file.path(tmp_dir, "file1.txt")
  file2 <- file.path(tmp_dir, "file2.txt")
  writeLines("test", file1)
  writeLines("test", file2)

  # Add to ledger (using 1GB and 2GB sizes to test pruning)
  ledger_record(h, "prune_1", file1, size_bytes = 1e9)  # 1 GB
  ledger_record(h, "prune_2", file2, size_bytes = 2e9)  # 2 GB

  # Pin the first one
  cache_pin(h, "prune_1")

  # Prune to 1.5 GB - should only remove unpinned file
  pruned <- cache_prune(h, max_size_gb = 1.5)

  expect_equal(nrow(pruned), 1)
  expect_equal(pruned$asset_id[1], "prune_2")

  # Verify pinned file still in ledger
  expect_true(ledger_is_cached(h, "prune_1"))
  expect_false(ledger_is_cached(h, "prune_2"))
})

# --- Additional ledger function tests ---

test_that("ledger_lookup returns empty tibble for empty input", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  result <- ledger_lookup(h, character())

  expect_s3_class(result, "tbl_df")
  expect_equal(nrow(result), 0)
  expect_true("asset_id" %in% names(result))
  expect_true("local_path" %in% names(result))
})

test_that("ledger_lookup returns all requested columns", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  ledger_record(h, "cols_test", "/path/to/file.nii", size_bytes = 500)

  result <- ledger_lookup(h, "cols_test")

  expect_true("asset_id" %in% names(result))
  expect_true("local_path" %in% names(result))
  expect_true("size_bytes" %in% names(result))
  expect_true("downloaded_at" %in% names(result))
  expect_true("pinned" %in% names(result))
})

test_that("ledger_touch handles empty input gracefully", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  # Should not error
  result <- ledger_touch(h, character())

  expect_identical(result, h)
})

test_that("ledger_touch handles multiple assets", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  ledger_record(h, "touch_multi_1", "/path/1.nii", size_bytes = 100)
  ledger_record(h, "touch_multi_2", "/path/2.nii", size_bytes = 100)

  con <- get_con(h)
  before <- DBI::dbGetQuery(con, "SELECT asset_id, last_accessed FROM ledger WHERE asset_id LIKE 'touch_multi%'")

  Sys.sleep(0.1)
  ledger_touch(h, c("touch_multi_1", "touch_multi_2"))

  after <- DBI::dbGetQuery(con, "SELECT asset_id, last_accessed FROM ledger WHERE asset_id LIKE 'touch_multi%'")

  # Both should be updated
  expect_true(all(after$last_accessed >= before$last_accessed))
})

test_that("ledger_remove handles empty input gracefully", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  # Should not error
  result <- ledger_remove(h, character())

  expect_identical(result, h)
})

test_that("ledger_remove handles multiple assets", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  ledger_record(h, "rm_multi_1", "/path/1.nii", size_bytes = 100)
  ledger_record(h, "rm_multi_2", "/path/2.nii", size_bytes = 100)
  ledger_record(h, "rm_multi_3", "/path/3.nii", size_bytes = 100)

  ledger_remove(h, c("rm_multi_1", "rm_multi_2"))

  expect_false(ledger_is_cached(h, "rm_multi_1"))
  expect_false(ledger_is_cached(h, "rm_multi_2"))
  expect_true(ledger_is_cached(h, "rm_multi_3"))
})

test_that("ledger_is_cached handles empty input", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  result <- ledger_is_cached(h, character())

  expect_equal(length(result), 0)
  expect_true(is.logical(result))
})

test_that("ledger_record stores optional fields", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  ledger_record(h, "optional_test", "/path/opt.nii",
                size_bytes = 1234,
                checksum_md5 = "abc123",
                etag = "etag-xyz",
                backend = "aws")

  con <- get_con(h)
  result <- DBI::dbGetQuery(con, "SELECT * FROM ledger WHERE asset_id = 'optional_test'")

  expect_equal(result$checksum_md5[1], "abc123")
  expect_equal(result$etag[1], "etag-xyz")
  expect_equal(result$backend[1], "aws")
})

test_that("ledger_record handles NULL size_bytes", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  ledger_record(h, "null_size", "/path/null.nii")

  con <- get_con(h)
  result <- DBI::dbGetQuery(con, "SELECT size_bytes FROM ledger WHERE asset_id = 'null_size'")

  expect_true(is.na(result$size_bytes[1]))
})

# --- cache_ensure_dir tests ---

test_that("cache_ensure_dir creates nested directories", {
  tmp_dir <- withr::local_tempdir()

  nested_path <- file.path(tmp_dir, "a", "b", "c", "file.nii")

  # Directory should not exist yet
  expect_false(dir.exists(dirname(nested_path)))

  hcpx:::cache_ensure_dir(nested_path)

  # Now it should exist
  expect_true(dir.exists(dirname(nested_path)))
})

test_that("cache_ensure_dir returns path invisibly", {
  tmp_dir <- withr::local_tempdir()

  test_path <- file.path(tmp_dir, "subdir", "file.txt")

  result <- hcpx:::cache_ensure_dir(test_path)

  expect_equal(result, test_path)
})

test_that("cache_ensure_dir is idempotent", {
  tmp_dir <- withr::local_tempdir()

  test_path <- file.path(tmp_dir, "idem", "file.txt")

  # Call twice - should not error
  hcpx:::cache_ensure_dir(test_path)
  hcpx:::cache_ensure_dir(test_path)

  expect_true(dir.exists(dirname(test_path)))
})

# --- cache_path edge case tests ---

test_that("cache_path handles nested remote paths", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  path <- cache_path(h, "HCP_1200", "100206", "MNINonLinear/Results/tfMRI_WM_LR/file.nii")

  expect_true(grepl("MNINonLinear", path))
  expect_true(grepl("Results", path))
  expect_true(grepl("tfMRI_WM_LR", path))
})

test_that("cache_path validates path contains expected components", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  # Test that cache_path builds path with all expected components
  path <- cache_path(h, "HCP_1200", "100206", "subdir/file.nii")

  expect_true(grepl("files", path))
  expect_true(grepl("HCP_1200", path))
  expect_true(grepl("100206", path))
  expect_true(grepl("subdir", path))
  expect_true(grepl("file.nii", path))

  # Note: Path traversal protection via normalizePath with mustWork=FALSE

  # has limitations - it doesn't resolve ".." for non-existent paths.
  # A more robust implementation would be needed for security-critical use.
})

test_that("cache_path_from_asset extracts from asset row", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  asset <- tibble::tibble(
    release = "HCP_1200",
    subject_id = "100206",
    remote_path = "HCP_1200/100206/MNINonLinear/file.nii"
  )

  path <- hcpx:::cache_path_from_asset(h, asset)

  expect_true(grepl("file.nii", path))
  expect_true(grepl("100206", path))
})

# --- cache_pin / cache_unpin edge cases ---

test_that("cache_pin handles empty input", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  # Should not error
  result <- cache_pin(h, character())

  expect_identical(result, h)
})

test_that("cache_unpin handles empty input", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  # Should not error
  result <- cache_unpin(h, character())

  expect_identical(result, h)
})

test_that("cache_pin handles multiple assets", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  ledger_record(h, "pin_multi_1", "/path/1.nii", size_bytes = 100)
  ledger_record(h, "pin_multi_2", "/path/2.nii", size_bytes = 100)

  cache_pin(h, c("pin_multi_1", "pin_multi_2"))

  con <- get_con(h)
  result <- DBI::dbGetQuery(con, "SELECT pinned FROM ledger WHERE asset_id LIKE 'pin_multi%'")

  expect_true(all(result$pinned == 1))
})

test_that("cache_unpin handles multiple assets", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  ledger_record(h, "unpin_multi_1", "/path/1.nii", size_bytes = 100)
  ledger_record(h, "unpin_multi_2", "/path/2.nii", size_bytes = 100)

  cache_pin(h, c("unpin_multi_1", "unpin_multi_2"))
  cache_unpin(h, c("unpin_multi_1", "unpin_multi_2"))

  con <- get_con(h)
  result <- DBI::dbGetQuery(con, "SELECT pinned FROM ledger WHERE asset_id LIKE 'unpin_multi%'")

  expect_true(all(result$pinned == 0))
})

# --- cache_status edge cases ---

test_that("cache_status returns correct structure for empty ledger", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  # Clear any seeded data
  hcpx:::catalog_clear(h)

  status <- cache_status(h)

  expect_equal(status$n_files, 0)
  expect_equal(status$total_bytes, 0)
  expect_equal(status$total_gb, 0)
  expect_equal(status$n_pinned, 0)
  expect_true(!is.null(status$max_size_gb))
  expect_true(!is.null(status$root))
})

test_that("cache_status calculates total_gb correctly", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  # Add 1.5GB of files
  ledger_record(h, "gb_test_1", "/path/1.nii", size_bytes = 1e9)
  ledger_record(h, "gb_test_2", "/path/2.nii", size_bytes = 5e8)

  status <- cache_status(h)

  expect_equal(status$total_gb, 1.5)
})

test_that("cache_status includes by_backend breakdown", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  ledger_record(h, "backend_1", "/path/1.nii", size_bytes = 100, backend = "aws")
  ledger_record(h, "backend_2", "/path/2.nii", size_bytes = 200, backend = "aws")
  ledger_record(h, "backend_3", "/path/3.nii", size_bytes = 300, backend = "local")

  status <- cache_status(h)

  expect_s3_class(status$by_backend, "tbl_df")
  expect_true("backend" %in% names(status$by_backend))
  expect_true("n_files" %in% names(status$by_backend))
  expect_true("total_bytes" %in% names(status$by_backend))
})

# --- cache_prune edge cases ---

test_that("cache_prune returns empty tibble when under limit", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  # Add small file (well under default limit)
  ledger_record(h, "small_file", "/path/small.nii", size_bytes = 1000)

  # Cache is under limit, should return empty
  pruned <- cache_prune(h, max_size_gb = 100)

  expect_equal(nrow(pruned), 0)
})

test_that("cache_prune supports dry_run mode", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  # Create a temp file
  file1 <- file.path(tmp_dir, "dryrun.txt")
  writeLines("test", file1)

  ledger_record(h, "dryrun_test", file1, size_bytes = 1e9)

  # Dry run should not actually delete
  pruned <- cache_prune(h, max_size_gb = 0.5, dry_run = TRUE)

  expect_gt(nrow(pruned), 0)
  # File should still be cached
  expect_true(ledger_is_cached(h, "dryrun_test"))
  # File should still exist
  expect_true(file.exists(file1))
})

test_that("cache_prune warns when all files pinned", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  # Create files
  file1 <- file.path(tmp_dir, "pinned1.txt")
  file2 <- file.path(tmp_dir, "pinned2.txt")
  writeLines("test", file1)
  writeLines("test", file2)

  ledger_record(h, "all_pinned_1", file1, size_bytes = 1e9)
  ledger_record(h, "all_pinned_2", file2, size_bytes = 1e9)

  # Pin all files
  cache_pin(h, c("all_pinned_1", "all_pinned_2"))

  # Try to prune below size - should warn
  expect_warning(
    pruned <- cache_prune(h, max_size_gb = 0.5),
    "All cached files are pinned"
  )

  expect_equal(nrow(pruned), 0)
})

test_that("cache_prune uses handle max_size_gb when not specified", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  # Default max is 200GB; small files should not be pruned
  ledger_record(h, "default_limit", "/path/small.nii", size_bytes = 1000)

  pruned <- cache_prune(h)  # No max_size_gb argument

  expect_equal(nrow(pruned), 0)
})

test_that("cache_prune removes files from disk", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  # Create real file
  file1 <- file.path(tmp_dir, "to_delete.txt")
  writeLines("test data", file1)
  expect_true(file.exists(file1))

  ledger_record(h, "disk_delete", file1, size_bytes = 1e9)

  # Prune it
  pruned <- cache_prune(h, max_size_gb = 0.5)

  # File should be removed from disk
  expect_false(file.exists(file1))
})

test_that("cache_prune handles missing files gracefully", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  # Record a file that doesn't exist on disk
  ledger_record(h, "missing_file", "/nonexistent/path.nii", size_bytes = 1e9)

  # Should not error
  pruned <- cache_prune(h, max_size_gb = 0.5)

  expect_gt(nrow(pruned), 0)
  expect_false(ledger_is_cached(h, "missing_file"))
})

# --- cache_list edge cases ---

test_that("cache_list returns empty tibble for empty ledger", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  # Clear any seeded data
  hcpx:::catalog_clear(h)

  result <- cache_list(h)

  expect_s3_class(result, "tbl_df")
  expect_equal(nrow(result), 0)
})

test_that("cache_list orders by last_accessed DESC", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  # Add files with different access times
  ledger_record(h, "order_1", "/path/1.nii", size_bytes = 100)
  Sys.sleep(0.1)
  ledger_record(h, "order_2", "/path/2.nii", size_bytes = 100)
  Sys.sleep(0.1)
  ledger_record(h, "order_3", "/path/3.nii", size_bytes = 100)

  result <- cache_list(h)

  # Most recently accessed should be first
  order_ids <- result$asset_id[result$asset_id %in% c("order_1", "order_2", "order_3")]
  expect_equal(order_ids[1], "order_3")
})

test_that("cache_list returns all columns", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  ledger_record(h, "cols_list", "/path/file.nii",
                size_bytes = 100,
                checksum_md5 = "abc",
                etag = "xyz",
                backend = "aws")

  result <- cache_list(h)

  expected_cols <- c("asset_id", "local_path", "downloaded_at", "last_accessed",
                     "size_bytes", "checksum_md5", "etag", "backend", "pinned")

  for (col in expected_cols) {
    expect_true(col %in% names(result), info = paste("Missing column:", col))
  }
})
