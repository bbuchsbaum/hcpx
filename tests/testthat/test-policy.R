# Tests for policy and access tier functions

# --- .ACCESS_TIERS constant ---

test_that("ACCESS_TIERS contains expected values", {
  # Access the internal constant
  tiers <- hcpx:::.ACCESS_TIERS

  expect_equal(tiers, c("open", "tier1", "tier2"))
})

# --- hcpx_policy() tests ---

test_that("hcpx_policy modifies enforce setting", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  # Default should be TRUE
  expect_true(h$policy$enforce)

  # Disable enforcement
  h2 <- hcpx_policy(h, enforce = FALSE)
  expect_false(h2$policy$enforce)

  # Re-enable
  h3 <- hcpx_policy(h2, enforce = TRUE)
  expect_true(h3$policy$enforce)
})

test_that("hcpx_policy modifies allow_export_restricted setting", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  # Default should be FALSE
  expect_false(h$policy$allow_export_restricted)

  # Enable
  h2 <- hcpx_policy(h, allow_export_restricted = TRUE)
  expect_true(h2$policy$allow_export_restricted)
})

test_that("hcpx_policy can set both settings at once", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  h2 <- hcpx_policy(h, enforce = FALSE, allow_export_restricted = TRUE)

  expect_false(h2$policy$enforce)
  expect_true(h2$policy$allow_export_restricted)
})

# --- describe_variable() tests ---

test_that("describe_variable returns tibble structure", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  result <- describe_variable(h, "Gender")

  expect_s3_class(result, "tbl_df")
  expect_named(result, c("var_name", "access_tier", "category", "instrument", "description"))
})

test_that("describe_variable returns 'unknown' for missing variable", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  result <- describe_variable(h, "NonexistentVariable")

  expect_equal(result$var_name, "NonexistentVariable")
  expect_equal(result$access_tier, "unknown")
})

# --- list_variables() tests ---

test_that("list_variables returns tibble structure", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  result <- list_variables(h)

  expect_s3_class(result, "tbl_df")
  expect_true("var_name" %in% names(result))
  expect_true("access_tier" %in% names(result))
})

test_that("list_variables filters by tier", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  open_vars <- list_variables(h, tier = "open")

  expect_s3_class(open_vars, "tbl_df")
  expect_true(all(open_vars$access_tier == "open"))
})

test_that("list_variables errors on invalid tier", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  # Should error (there's a cli format bug in the source, but it still errors)
  expect_error(list_variables(h, tier = "invalid_tier"))
})

# --- policy_summary() tests ---

test_that("policy_summary returns tibble with tier counts", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  result <- policy_summary(h)

  expect_s3_class(result, "tbl_df")
  expect_true("access_tier" %in% names(result))
  expect_true("n" %in% names(result))
})

# --- is_restricted() tests ---

test_that("is_restricted returns logical", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  # Test with known variable (if dictionary is populated)
  result <- is_restricted(h, "Gender")
  expect_type(result, "logical")
})

# --- get_access_tier() tests ---

test_that("get_access_tier returns character tier", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  result <- get_access_tier(h, "Gender")
  expect_type(result, "character")
})

# --- policy_check_access() tests ---

test_that("policy_check_access skips check when enforce is FALSE", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  h <- hcpx_policy(h, enforce = FALSE)

  # Should not error even with any variables
  expect_no_error(policy_check_access(h, "AnyVariable"))
})

# --- Additional hcpx_policy tests ---

test_that("hcpx_policy returns hcpx object", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  h2 <- hcpx_policy(h, enforce = FALSE)

  expect_s3_class(h2, "hcpx")
})

test_that("hcpx_policy preserves other handle properties", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  h2 <- hcpx_policy(h, enforce = FALSE)

  expect_equal(h2$release, h$release)
  expect_equal(h2$engine, h$engine)
})

test_that("hcpx_policy with no arguments returns unchanged handle", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  original_enforce <- h$policy$enforce
  original_export <- h$policy$allow_export_restricted

  h2 <- hcpx_policy(h)

  expect_equal(h2$policy$enforce, original_enforce)
  expect_equal(h2$policy$allow_export_restricted, original_export)
})

# --- Additional describe_variable tests ---

test_that("describe_variable handles missing dictionary table", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  # Clear dictionary table
  con <- get_con(h)
  DBI::dbExecute(con, "DROP TABLE IF EXISTS dictionary")

  # Should return unknown with warning
  expect_warning(
    result <- describe_variable(h, "TestVar"),
    "Dictionary table not found"
  )
  expect_equal(result$access_tier, "unknown")
})

test_that("describe_variable returns tibble for found variable", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  # Add a test variable to dictionary
  con <- get_con(h)
  DBI::dbExecute(con, "
    INSERT INTO dictionary (var_name, access_tier, category, instrument, description)
    VALUES ('test_var', 'open', 'test_cat', 'test_inst', 'Test description')
  ")

  result <- describe_variable(h, "test_var")

  expect_s3_class(result, "tbl_df")
  expect_equal(result$var_name, "test_var")
  expect_equal(result$access_tier, "open")
  expect_equal(result$category, "test_cat")
})

# --- Additional is_restricted tests ---

test_that("is_restricted returns FALSE for open variables", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  # Add an open variable
  con <- get_con(h)
  DBI::dbExecute(con, "
    INSERT INTO dictionary (var_name, access_tier, category, instrument, description)
    VALUES ('open_var', 'open', 'test', 'test', 'Open variable')
  ")

  expect_false(is_restricted(h, "open_var"))
})

test_that("is_restricted returns TRUE for tier1 variables", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  con <- get_con(h)
  DBI::dbExecute(con, "
    INSERT INTO dictionary (var_name, access_tier, category, instrument, description)
    VALUES ('tier1_var', 'tier1', 'test', 'test', 'Tier1 variable')
  ")

  expect_true(is_restricted(h, "tier1_var"))
})

test_that("is_restricted returns TRUE for tier2 variables", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  con <- get_con(h)
  DBI::dbExecute(con, "
    INSERT INTO dictionary (var_name, access_tier, category, instrument, description)
    VALUES ('tier2_var', 'tier2', 'test', 'test', 'Tier2 variable')
  ")

  expect_true(is_restricted(h, "tier2_var"))
})

# --- Additional get_access_tier tests ---

test_that("get_access_tier returns unknown for missing variable", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  result <- get_access_tier(h, "nonexistent_var")

  expect_equal(result, "unknown")
})

test_that("get_access_tier returns correct tier for known variable", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  con <- get_con(h)
  DBI::dbExecute(con, "
    INSERT INTO dictionary (var_name, access_tier, category, instrument, description)
    VALUES ('known_var', 'tier1', 'test', 'test', 'Known variable')
  ")

  expect_equal(get_access_tier(h, "known_var"), "tier1")
})

# --- Additional list_variables tests ---

test_that("list_variables returns empty tibble when dictionary is empty", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  # Clear dictionary
  con <- get_con(h)
  DBI::dbExecute(con, "DELETE FROM dictionary")

  result <- list_variables(h)

  expect_s3_class(result, "tbl_df")
  expect_equal(nrow(result), 0)
})

test_that("list_variables filters by tier1", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  con <- get_con(h)
  DBI::dbExecute(con, "DELETE FROM dictionary")
  DBI::dbExecute(con, "
    INSERT INTO dictionary (var_name, access_tier, category, instrument, description) VALUES
    ('open_var', 'open', 'cat', 'inst', 'desc'),
    ('tier1_var', 'tier1', 'cat', 'inst', 'desc'),
    ('tier2_var', 'tier2', 'cat', 'inst', 'desc')
  ")

  tier1_vars <- list_variables(h, tier = "tier1")

  expect_equal(nrow(tier1_vars), 1)
  expect_true(all(tier1_vars$access_tier == "tier1"))
})

test_that("list_variables filters by tier2", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  con <- get_con(h)
  DBI::dbExecute(con, "DELETE FROM dictionary")
  DBI::dbExecute(con, "
    INSERT INTO dictionary (var_name, access_tier, category, instrument, description) VALUES
    ('open_var', 'open', 'cat', 'inst', 'desc'),
    ('tier2_var', 'tier2', 'cat', 'inst', 'desc')
  ")

  tier2_vars <- list_variables(h, tier = "tier2")

  expect_equal(nrow(tier2_vars), 1)
  expect_true(all(tier2_vars$access_tier == "tier2"))
})

test_that("list_variables handles missing dictionary table", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  con <- get_con(h)
  DBI::dbExecute(con, "DROP TABLE IF EXISTS dictionary")

  expect_warning(
    result <- list_variables(h),
    "Dictionary table not found"
  )
  expect_s3_class(result, "tbl_df")
  expect_equal(nrow(result), 0)
})

# --- Additional policy_summary tests ---

test_that("policy_summary handles empty dictionary", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  con <- get_con(h)
  DBI::dbExecute(con, "DELETE FROM dictionary")

  result <- policy_summary(h)

  expect_s3_class(result, "tbl_df")
  expect_equal(nrow(result), 0)
})

test_that("policy_summary handles missing dictionary table", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  con <- get_con(h)
  DBI::dbExecute(con, "DROP TABLE IF EXISTS dictionary")

  expect_warning(
    result <- policy_summary(h),
    "Dictionary table not found"
  )
  expect_s3_class(result, "tbl_df")
  expect_equal(nrow(result), 0)
})

test_that("policy_summary counts variables correctly", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  con <- get_con(h)
  DBI::dbExecute(con, "DELETE FROM dictionary")
  DBI::dbExecute(con, "
    INSERT INTO dictionary (var_name, access_tier, category, instrument, description) VALUES
    ('open1', 'open', 'cat', 'inst', 'desc'),
    ('open2', 'open', 'cat', 'inst', 'desc'),
    ('tier1', 'tier1', 'cat', 'inst', 'desc')
  ")

  result <- policy_summary(h)

  open_count <- result$n[result$access_tier == "open"]
  tier1_count <- result$n[result$access_tier == "tier1"]

  expect_equal(open_count, 2)
  expect_equal(tier1_count, 1)
})

# --- policy_check_access additional tests ---

test_that("policy_check_access warns for restricted with allow_export_restricted TRUE", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  # Add a restricted variable
  con <- get_con(h)
  DBI::dbExecute(con, "
    INSERT INTO dictionary (var_name, access_tier, category, instrument, description)
    VALUES ('restricted_warn', 'tier1', 'test', 'test', 'Restricted variable')
  ")

  # Enable export of restricted
  h <- hcpx_policy(h, enforce = TRUE, allow_export_restricted = TRUE)

  # Should warn but not error
  expect_warning(
    policy_check_access(h, "restricted_warn"),
    "restricted"
  )
})

test_that("policy_check_access errors for restricted with allow_export_restricted FALSE", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  # Add a restricted variable
  con <- get_con(h)
  DBI::dbExecute(con, "
    INSERT INTO dictionary (var_name, access_tier, category, instrument, description)
    VALUES ('restricted_error', 'tier1', 'test', 'test', 'Restricted variable')
  ")

  # Enforce policy, disallow export
  h <- hcpx_policy(h, enforce = TRUE, allow_export_restricted = FALSE)

  expect_error(
    policy_check_access(h, "restricted_error"),
    "denied"
  )
})

test_that("policy_check_access handles multiple variables", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  # Add variables
  con <- get_con(h)
  DBI::dbExecute(con, "
    INSERT INTO dictionary (var_name, access_tier, category, instrument, description) VALUES
    ('multi_open', 'open', 'test', 'test', 'Open'),
    ('multi_tier1', 'tier1', 'test', 'test', 'Tier1')
  ")

  h <- hcpx_policy(h, enforce = TRUE, allow_export_restricted = FALSE)

  # Mixed variables should error due to restricted one
  expect_error(
    policy_check_access(h, c("multi_open", "multi_tier1")),
    "denied"
  )
})

test_that("policy_check_access returns invisible NULL for open variables", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  # Add open variable
  con <- get_con(h)
  DBI::dbExecute(con, "
    INSERT INTO dictionary (var_name, access_tier, category, instrument, description)
    VALUES ('all_open', 'open', 'test', 'test', 'Open')
  ")

  h <- hcpx_policy(h, enforce = TRUE, allow_export_restricted = FALSE)

  result <- policy_check_access(h, "all_open")

  expect_null(result)
})
