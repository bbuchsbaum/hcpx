# Tests for derived products and recipe system

# --- register_recipe() tests ---

test_that("register_recipe creates a recipe in the registry", {
  # Clear first
  clear_recipes()

  register_recipe(
    name = "test_recipe_1",
    description = "A test recipe",
    input = "assets",
    compute = function(paths, ...) { paths }
  )

  result <- recipes()
  expect_true("test_recipe_1" %in% result$name)
  expect_equal(result$description[result$name == "test_recipe_1"], "A test recipe")

  clear_recipes()
})

test_that("register_recipe validates name is non-empty string", {
  expect_error(
    register_recipe(name = "", description = "test", input = "assets", compute = identity),
    "non-empty string"
  )

  expect_error(
    register_recipe(name = NULL, description = "test", input = "assets", compute = identity),
    "non-empty string"
  )
})

test_that("register_recipe validates description is string", {
  expect_error(
    register_recipe(name = "test", description = NULL, input = "assets", compute = identity),
    "must be a string"
  )
})

test_that("register_recipe validates compute is a function", {
  expect_error(
    register_recipe(name = "test", description = "test", input = "assets", compute = "not_a_function"),
    "must be a function"
  )
})

test_that("register_recipe validates input argument", {
  expect_error(
    register_recipe(name = "test", description = "test", input = "invalid", compute = identity),
    "'arg' should be one of"
  )
})

test_that("register_recipe warns when overwriting existing recipe", {
  clear_recipes()

  register_recipe(name = "overwrite_test", description = "v1", input = "assets", compute = identity)

  # cli_warn produces warnings, not messages
  expect_warning(
    register_recipe(name = "overwrite_test", description = "v2", input = "assets", compute = identity),
    "Overwriting"
  )

  clear_recipes()
})

test_that("register_recipe stores requires_bundle correctly", {
  clear_recipes()

  register_recipe(
    name = "bundle_recipe",
    description = "Needs bundle",
    input = "assets",
    compute = identity,
    requires_bundle = "tfmri_cifti_min"
  )

  result <- recipes()
  expect_equal(result$requires_bundle[result$name == "bundle_recipe"], "tfmri_cifti_min")

  clear_recipes()
})

test_that("register_recipe stores output_ext correctly", {
  clear_recipes()

  register_recipe(
    name = "csv_recipe",
    description = "CSV output",
    input = "assets",
    compute = identity,
    output_ext = "csv"
  )

  result <- recipes()
  expect_equal(result$output_ext[result$name == "csv_recipe"], "csv")

  clear_recipes()
})

# --- unregister_recipe() tests ---

test_that("unregister_recipe removes a recipe", {
  clear_recipes()

  register_recipe(name = "to_unregister", description = "temp", input = "assets", compute = identity)
  expect_true("to_unregister" %in% recipes()$name)

  unregister_recipe("to_unregister")
  expect_false("to_unregister" %in% recipes()$name)

  clear_recipes()
})

test_that("unregister_recipe warns for non-existent recipe", {
  clear_recipes()

  # cli_warn produces warnings, not messages
  expect_warning(
    unregister_recipe("does_not_exist"),
    "Recipe not found"
  )
})

# --- get_recipe() tests ---

test_that("get_recipe returns recipe definition", {
  clear_recipes()

  register_recipe(
    name = "get_test",
    description = "Test get",
    input = "tasks",
    compute = function(x) x * 2
  )

  recipe <- get_recipe("get_test")

  expect_type(recipe, "list")
  expect_equal(recipe$name, "get_test")
  expect_equal(recipe$description, "Test get")
  expect_equal(recipe$input, "tasks")
  expect_true(is.function(recipe$compute))

  clear_recipes()
})

test_that("get_recipe errors for non-existent recipe", {
  clear_recipes()

  expect_error(
    get_recipe("nonexistent"),
    "Recipe not found"
  )
})

# --- recipes() tests ---

test_that("recipes returns empty tibble when no recipes registered", {
  clear_recipes()

  result <- recipes()

  expect_s3_class(result, "tbl_df")
  expect_equal(nrow(result), 0)
  expect_named(result, c("name", "description", "input", "requires_bundle", "output_ext"))
})

test_that("recipes returns all registered recipes", {
  clear_recipes()

  register_recipe(name = "r1", description = "Recipe 1", input = "assets", compute = identity)
  register_recipe(name = "r2", description = "Recipe 2", input = "tasks", compute = identity)
  register_recipe(name = "r3", description = "Recipe 3", input = "assets", compute = identity)

  result <- recipes()

  # Check that our recipes are present (other tests may have added recipes too)
  expect_true(all(c("r1", "r2", "r3") %in% result$name))
  expect_true(nrow(result) >= 3)

  clear_recipes()
})

test_that("recipes has correct column types", {
  clear_recipes()

  register_recipe(
    name = "type_test",
    description = "Check types",
    input = "assets",
    compute = identity,
    requires_bundle = "bundle_name",
    output_ext = "rds"
  )

  result <- recipes()

  expect_type(result$name, "character")
  expect_type(result$description, "character")
  expect_type(result$input, "character")
  expect_type(result$requires_bundle, "character")
  expect_type(result$output_ext, "character")

  clear_recipes()
})

# --- clear_recipes() tests ---

test_that("clear_recipes removes all recipes", {
  clear_recipes()

  # Record baseline count (may have recipes from parallel tests)
  baseline <- nrow(recipes())

  register_recipe(name = "c1", description = "test", input = "assets", compute = identity)
  register_recipe(name = "c2", description = "test", input = "assets", compute = identity)

  # Should have at least 2 more than baseline
  expect_true(nrow(recipes()) >= baseline + 2)
  expect_true(all(c("c1", "c2") %in% recipes()$name))

  clear_recipes()

  # After clear, should be empty
  expect_equal(nrow(recipes()), 0)
})

# --- derive() tests (requires database) ---

test_that("derive creates an hcpx_derivation object", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  register_recipe(
    name = "derive_test",
    description = "Test derive",
    input = "assets",
    compute = function(paths, ...) paths
  )

  ast <- assets(h)
  deriv <- derive(ast, "derive_test")

  expect_s3_class(deriv, "hcpx_derivation")
  expect_equal(deriv$recipe$name, "derive_test")
  expect_true(deriv$n_inputs > 0)
})

test_that("derive stores additional parameters", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  register_recipe(
    name = "param_test",
    description = "Test params",
    input = "assets",
    compute = function(paths, param1, param2) { list(p1 = param1, p2 = param2) }
  )

  ast <- assets(h)
  deriv <- derive(ast, "param_test", param1 = "value1", param2 = 42)

  expect_equal(deriv$params$param1, "value1")
  expect_equal(deriv$params$param2, 42)
})

test_that("derive errors for non-existent recipe", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  ast <- assets(h)

  expect_error(
    derive(ast, "nonexistent_recipe"),
    "Recipe not found"
  )
})

# --- print.hcpx_derivation tests ---

test_that("print.hcpx_derivation produces output", {
  skip_if_not(has_duckdb() || has_sqlite(), "No database backend available")

  tmp_dir <- withr::local_tempdir()
  h <- hcpx_ya(cache = tmp_dir)
  on.exit(hcpx_close(h), add = TRUE)

  register_recipe(
    name = "print_test",
    description = "Test printing",
    input = "assets",
    compute = identity
  )

  ast <- assets(h)
  deriv <- derive(ast, "print_test")

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

  output <- capture_cli(print(deriv))

  expect_true(grepl("print_test", output))
  expect_true(grepl("materialize", output))
})
