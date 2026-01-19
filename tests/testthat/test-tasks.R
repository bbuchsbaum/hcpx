# Tests for task dictionary and task query functions

# --- task_dictionary() tests ---

test_that("task_dictionary returns a tibble with expected columns", {
  dict <- task_dictionary()

  expect_s3_class(dict, "tbl_df")
  expect_named(dict, c("task", "description", "synonyms", "completion_flag", "expected_runs", "directions"))
})

test_that("task_dictionary contains all 7 canonical tasks", {
  dict <- task_dictionary()
  expected_tasks <- c("WM", "MOTOR", "GAMBLING", "LANGUAGE", "SOCIAL", "RELATIONAL", "EMOTION")

  expect_setequal(dict$task, expected_tasks)
  expect_equal(nrow(dict), 7)
})

test_that("task_dictionary has valid descriptions", {
  dict <- task_dictionary()

  expect_type(dict$description, "character")
  expect_true(all(nzchar(dict$description)))
})

test_that("task_dictionary has synonyms as lists", {
  dict <- task_dictionary()

  expect_type(dict$synonyms, "list")
  expect_true(all(vapply(dict$synonyms, is.character, logical(1))))
})

test_that("task_dictionary has valid completion flags", {
  dict <- task_dictionary()

  expect_type(dict$completion_flag, "character")
  expect_true(all(nzchar(dict$completion_flag)))
})

test_that("task_dictionary has expected_runs = 2 for all tasks", {
  dict <- task_dictionary()

  expect_type(dict$expected_runs, "integer")
  expect_true(all(dict$expected_runs == 2L))
})

test_that("task_dictionary has LR/RL directions for all tasks", {
  dict <- task_dictionary()

  expect_type(dict$directions, "list")
  for (dirs in dict$directions) {
    expect_setequal(dirs, c("LR", "RL"))
  }
})

# --- get_completion_flag() tests ---

test_that("get_completion_flag returns correct flags", {
  # unname() because the function returns named values
  expect_equal(unname(get_completion_flag("WM")), "fMRI_WM_Compl")
  expect_equal(unname(get_completion_flag("MOTOR")), "fMRI_Mot_Compl")
  expect_equal(unname(get_completion_flag("GAMBLING")), "fMRI_Gamb_Compl")
  expect_equal(unname(get_completion_flag("LANGUAGE")), "fMRI_Lang_Compl")
  expect_equal(unname(get_completion_flag("SOCIAL")), "fMRI_Soc_Compl")
  expect_equal(unname(get_completion_flag("RELATIONAL")), "fMRI_Rel_Compl")
  expect_equal(unname(get_completion_flag("EMOTION")), "fMRI_Emo_Compl")
})

test_that("get_completion_flag returns NA for unknown tasks", {
  expect_true(is.na(get_completion_flag("UNKNOWN")))
  expect_true(is.na(get_completion_flag("REST")))
})

# --- canonical_task_names() tests ---

test_that("canonical_task_names returns all 7 tasks", {
  names <- canonical_task_names()

  expect_type(names, "character")
  expect_length(names, 7)
  expect_setequal(names, c("WM", "MOTOR", "GAMBLING", "LANGUAGE", "SOCIAL", "RELATIONAL", "EMOTION"))
})

# --- resolve_task_name() integration with dictionary ---

test_that("resolve_task_name works for all dictionary synonyms", {
  dict <- task_dictionary()

  for (i in seq_len(nrow(dict))) {
    task <- dict$task[i]
    synonyms <- dict$synonyms[[i]]

    for (syn in synonyms) {
      # Skip if synonym contains spaces (not in the lookup table)
      if (!grepl("\\s", syn) && tolower(syn) %in% names(.hcp_task_synonyms)) {
        result <- resolve_task_name(syn)
        expect_equal(result, task,
                     info = paste("Synonym", syn, "should resolve to", task))
      }
    }
  }
})
