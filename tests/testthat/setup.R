# Setup file for testthat
# This file runs before any tests

# Clear recipe registry to ensure test isolation
# The clear_recipes function may not be available until the package is loaded
withr::defer({
  if (exists("clear_recipes", mode = "function", where = "package:hcpx")) {
    clear_recipes()
  }
}, envir = testthat::teardown_env())

