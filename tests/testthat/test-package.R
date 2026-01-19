# Basic package tests

test_that("package loads", {
 expect_true(requireNamespace("hcpx", quietly = TRUE))
})

test_that("hcpx_cache_default returns a path", {
 path <- hcpx_cache_default()
 expect_type(path, "character")
 expect_true(nzchar(path))
})

test_that("YAML configs exist", {
 tasks_path <- system.file("extdata", "tasks.yml", package = "hcpx")
 bundles_path <- system.file("extdata", "bundles.yml", package = "hcpx")

 expect_true(nzchar(tasks_path))
 expect_true(nzchar(bundles_path))
 expect_true(file.exists(tasks_path))
 expect_true(file.exists(bundles_path))
})

test_that("demo data files exist", {
 subjects_path <- system.file("extdata", "demo_subjects.csv", package = "hcpx")
 assets_path <- system.file("extdata", "demo_assets.csv", package = "hcpx")

 expect_true(nzchar(subjects_path))
 expect_true(nzchar(assets_path))
 expect_true(file.exists(subjects_path))
 expect_true(file.exists(assets_path))
})

# --- Package exports tests ---

test_that("main entry points are exported", {
  expect_true(exists("hcpx_ya", envir = asNamespace("hcpx")))
  expect_true(exists("subjects", envir = asNamespace("hcpx")))
  expect_true(exists("tasks", envir = asNamespace("hcpx")))
  expect_true(exists("assets", envir = asNamespace("hcpx")))
})

test_that("query functions are exported", {
  expect_true(exists("bundles", envir = asNamespace("hcpx")))
  expect_true(exists("task_dictionary", envir = asNamespace("hcpx")))
  expect_true(exists("overview", envir = asNamespace("hcpx")))
})

test_that("download functions are exported", {
  expect_true(exists("plan_download", envir = asNamespace("hcpx")))
  expect_true(exists("download", envir = asNamespace("hcpx")))
  expect_true(exists("write_manifest", envir = asNamespace("hcpx")))
  expect_true(exists("read_manifest", envir = asNamespace("hcpx")))
})

test_that("cache functions are exported", {
  expect_true(exists("cache_status", envir = asNamespace("hcpx")))
  expect_true(exists("cache_prune", envir = asNamespace("hcpx")))
  expect_true(exists("cache_pin", envir = asNamespace("hcpx")))
  expect_true(exists("cache_unpin", envir = asNamespace("hcpx")))
  expect_true(exists("cache_list", envir = asNamespace("hcpx")))
})

test_that("policy functions are exported", {
  expect_true(exists("hcpx_policy", envir = asNamespace("hcpx")))
  expect_true(exists("describe_variable", envir = asNamespace("hcpx")))
  expect_true(exists("list_variables", envir = asNamespace("hcpx")))
  expect_true(exists("policy_summary", envir = asNamespace("hcpx")))
})

test_that("auth functions are exported", {
  expect_true(exists("hcpx_auth", envir = asNamespace("hcpx")))
  expect_true(exists("hcpx_auth_status", envir = asNamespace("hcpx")))
})

# --- YAML config content tests ---

test_that("tasks.yml has valid content", {
  tasks_path <- system.file("extdata", "tasks.yml", package = "hcpx")
  tasks_data <- yaml::read_yaml(tasks_path)

  expect_type(tasks_data, "list")
  expect_true(length(tasks_data) > 0)
})

test_that("bundles.yml has valid content", {
  bundles_path <- system.file("extdata", "bundles.yml", package = "hcpx")
  bundles_data <- yaml::read_yaml(bundles_path)

  expect_type(bundles_data, "list")
  expect_true(length(bundles_data) > 0)
})

# --- Demo data content tests ---

test_that("demo_subjects.csv has required columns", {
  subjects_path <- system.file("extdata", "demo_subjects.csv", package = "hcpx")
  subjects <- utils::read.csv(subjects_path)

  expect_true("subject_id" %in% names(subjects))
  expect_true("gender" %in% names(subjects))
})

test_that("demo_assets.csv has required columns", {
  assets_path <- system.file("extdata", "demo_assets.csv", package = "hcpx")
  assets <- utils::read.csv(assets_path)

  expect_true("asset_id" %in% names(assets))
  expect_true("remote_path" %in% names(assets))
})

test_that("demo_subjects.csv has expected row count", {
  subjects_path <- system.file("extdata", "demo_subjects.csv", package = "hcpx")
  subjects <- utils::read.csv(subjects_path)

  expect_equal(nrow(subjects), 10)  # Demo has 10 subjects
})

# --- Package constants tests ---

test_that("ACCESS_TIERS constant exists and has correct values", {
  tiers <- hcpx:::.ACCESS_TIERS

  expect_type(tiers, "character")
  expect_equal(tiers, c("open", "tier1", "tier2"))
})

test_that("BYTES_PER_KB constant exists", {
  kb <- hcpx:::.BYTES_PER_KB

  expect_true(is.numeric(kb))
  expect_equal(kb, 1024)
})

# --- Database backend detection tests ---

test_that("has_duckdb returns logical", {
  result <- hcpx:::has_duckdb()

  expect_type(result, "logical")
})

test_that("has_sqlite returns logical", {
  result <- hcpx:::has_sqlite()

  expect_type(result, "logical")
})

test_that("at least one database backend is available", {
  # Package should work with at least one backend
  expect_true(hcpx:::has_duckdb() || hcpx:::has_sqlite())
})
