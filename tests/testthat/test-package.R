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
