# Tests for path parsing utilities
# These tests are CRITICAL - the path parser is foundational for all catalog operations

# -- Test paths from demo_assets.csv and common HCP patterns --

# Task fMRI dtseries paths
tfmri_wm_lr <- "HCP_1200/100206/MNINonLinear/Results/tfMRI_WM_LR/tfMRI_WM_LR_Atlas.dtseries.nii"
tfmri_wm_rl <- "HCP_1200/100206/MNINonLinear/Results/tfMRI_WM_RL/tfMRI_WM_RL_Atlas.dtseries.nii"
tfmri_motor_lr <- "HCP_1200/100206/MNINonLinear/Results/tfMRI_MOTOR_LR/tfMRI_MOTOR_LR_Atlas.dtseries.nii"
tfmri_gambling_lr <- "HCP_1200/100307/MNINonLinear/Results/tfMRI_GAMBLING_LR/tfMRI_GAMBLING_LR_Atlas.dtseries.nii"
tfmri_language_lr <- "HCP_1200/100610/MNINonLinear/Results/tfMRI_LANGUAGE_LR/tfMRI_LANGUAGE_LR_Atlas.dtseries.nii"

# Event timing files
ev_wm_body <- "HCP_1200/100206/MNINonLinear/Results/tfMRI_WM_LR/EVs/0bk_body.txt"
ev_wm_faces <- "HCP_1200/100206/MNINonLinear/Results/tfMRI_WM_LR/EVs/0bk_faces.txt"

# Resting-state fMRI
rfmri_rest1_lr <- "HCP_1200/100206/MNINonLinear/Results/rfMRI_REST1_LR/rfMRI_REST1_LR_Atlas.dtseries.nii"
rfmri_rest1_rl <- "HCP_1200/100206/MNINonLinear/Results/rfMRI_REST1_RL/rfMRI_REST1_RL_Atlas.dtseries.nii"
rfmri_rest2_lr <- "HCP_1200/100206/MNINonLinear/Results/rfMRI_REST2_LR/rfMRI_REST2_LR_Atlas.dtseries.nii"

# ============================================================================
# extract_kind() tests
# ============================================================================

test_that("extract_kind identifies task fMRI", {
  expect_equal(extract_kind(tfmri_wm_lr), "tfmri")
  expect_equal(extract_kind(tfmri_motor_lr), "tfmri")
  expect_equal(extract_kind(tfmri_gambling_lr), "tfmri")
  expect_equal(extract_kind(ev_wm_body), "tfmri")
})

test_that("extract_kind identifies resting-state fMRI", {
  expect_equal(extract_kind(rfmri_rest1_lr), "rfmri")
  expect_equal(extract_kind(rfmri_rest1_rl), "rfmri")
  expect_equal(extract_kind(rfmri_rest2_lr), "rfmri")
})

test_that("extract_kind identifies structural", {
  expect_equal(extract_kind("HCP_1200/100206/T1w/T1w_acpc_dc.nii.gz"), "struct")
  expect_equal(extract_kind("HCP_1200/100206/T2w/T2w_acpc_dc.nii.gz"), "struct")
  expect_equal(extract_kind("HCP_1200/100206/MNINonLinear/T1w.nii.gz"), "struct")
})

test_that("extract_kind identifies diffusion", {
  expect_equal(extract_kind("HCP_1200/100206/Diffusion/data.nii.gz"), "diff")
  expect_equal(extract_kind("HCP_1200/100206/T1w/Diffusion/bvals"), "diff")
  expect_equal(extract_kind("HCP_1200/100206/data.bval"), "diff")
  expect_equal(extract_kind("HCP_1200/100206/data.bvec"), "diff")
})

test_that("extract_kind identifies MEG", {
  expect_equal(extract_kind("HCP_1200/100206/MEG/Restin/rmeg.mat"), "meg")
})

test_that("extract_kind handles edge cases", {
  expect_true(is.na(extract_kind(NA)))
  expect_true(is.na(extract_kind("")))
  expect_true(is.na(extract_kind("some/random/path.txt")))
})

# ============================================================================
# extract_task() tests
# ============================================================================

test_that("extract_task extracts canonical task names", {
  expect_equal(extract_task(tfmri_wm_lr), "WM")
  expect_equal(extract_task(tfmri_wm_rl), "WM")
  expect_equal(extract_task(tfmri_motor_lr), "MOTOR")
  expect_equal(extract_task(tfmri_gambling_lr), "GAMBLING")
  expect_equal(extract_task(tfmri_language_lr), "LANGUAGE")
})
test_that("extract_task works for EV files", {
  expect_equal(extract_task(ev_wm_body), "WM")
  expect_equal(extract_task(ev_wm_faces), "WM")
})

test_that("extract_task returns NA for non-task data", {
  expect_true(is.na(extract_task(rfmri_rest1_lr)))
  expect_true(is.na(extract_task("HCP_1200/100206/T1w/T1w.nii.gz")))
  expect_true(is.na(extract_task(NA)))
  expect_true(is.na(extract_task("")))
})

test_that("extract_task handles all 7 canonical tasks", {
  tasks <- c("WM", "MOTOR", "GAMBLING", "LANGUAGE", "SOCIAL", "RELATIONAL", "EMOTION")
  for (task in tasks) {
    path <- sprintf("HCP_1200/100206/MNINonLinear/Results/tfMRI_%s_LR/tfMRI_%s_LR_Atlas.dtseries.nii", task, task)
    expect_equal(extract_task(path), task, info = paste("Task:", task))
  }
})

# ============================================================================
# extract_direction() tests
# ============================================================================

test_that("extract_direction extracts LR correctly", {
  expect_equal(extract_direction(tfmri_wm_lr), "LR")
  expect_equal(extract_direction(rfmri_rest1_lr), "LR")
  expect_equal(extract_direction(ev_wm_body), "LR")
})

test_that("extract_direction extracts RL correctly", {
  expect_equal(extract_direction(tfmri_wm_rl), "RL")
  expect_equal(extract_direction(rfmri_rest1_rl), "RL")
})

test_that("extract_direction returns NA when no direction present", {
  expect_true(is.na(extract_direction("HCP_1200/100206/T1w/T1w.nii.gz")))
  expect_true(is.na(extract_direction(NA)))
  expect_true(is.na(extract_direction("")))
})

# ============================================================================
# extract_run() tests
# ============================================================================

test_that("extract_run extracts resting-state run numbers", {
  expect_equal(extract_run(rfmri_rest1_lr), 1L)
  expect_equal(extract_run(rfmri_rest1_rl), 1L)
  expect_equal(extract_run(rfmri_rest2_lr), 2L)
})

test_that("extract_run defaults to 1 for task fMRI", {
  expect_equal(extract_run(tfmri_wm_lr), 1L)
  expect_equal(extract_run(tfmri_motor_lr), 1L)
  expect_equal(extract_run(ev_wm_body), 1L)
})

test_that("extract_run returns NA for non-functional data", {
  expect_true(is.na(extract_run("HCP_1200/100206/T1w/T1w.nii.gz")))
  expect_true(is.na(extract_run(NA)))
})

# ============================================================================
# extract_file_type() tests
# ============================================================================

test_that("extract_file_type identifies CIFTI dtseries", {
  expect_equal(extract_file_type(tfmri_wm_lr), "dtseries")
  expect_equal(extract_file_type(rfmri_rest1_lr), "dtseries")
})

test_that("extract_file_type identifies event files", {
  expect_equal(extract_file_type(ev_wm_body), "ev")
  expect_equal(extract_file_type(ev_wm_faces), "ev")
})

test_that("extract_file_type identifies NIfTI files", {
  expect_equal(extract_file_type("HCP_1200/100206/T1w/T1w_acpc_dc.nii.gz"), "nii.gz")
  expect_equal(extract_file_type("HCP_1200/100206/data.nii"), "nii.gz")
})

test_that("extract_file_type identifies CIFTI variants", {
  expect_equal(extract_file_type("file.dscalar.nii"), "dscalar")
  expect_equal(extract_file_type("file.dlabel.nii"), "dlabel")
  expect_equal(extract_file_type("file.ptseries.nii"), "ptseries")
  expect_equal(extract_file_type("file.pscalar.nii"), "pscalar")
})

test_that("extract_file_type identifies diffusion files", {
  expect_equal(extract_file_type("data.bval"), "bval")
  expect_equal(extract_file_type("data.bvec"), "bvec")
})

test_that("extract_file_type identifies motion/confounds", {
  expect_equal(extract_file_type("Movement_Regressors.txt"), "confounds")
  expect_equal(extract_file_type("Confounds_tsv.txt"), "confounds")
})

test_that("extract_file_type identifies QC files", {
  expect_equal(extract_file_type("scan_QC_metrics.csv"), "qc")
})

test_that("extract_file_type identifies SBRef", {
  expect_equal(extract_file_type("tfMRI_WM_LR_SBRef.nii.gz"), "sbref")
})

test_that("extract_file_type handles edge cases", {
  expect_true(is.na(extract_file_type(NA)))
  expect_true(is.na(extract_file_type("")))
})

# ============================================================================
# extract_space() tests
# ============================================================================

test_that("extract_space identifies MNINonLinear", {
  expect_equal(extract_space(tfmri_wm_lr), "MNINonLinear")
  expect_equal(extract_space(rfmri_rest1_lr), "MNINonLinear")
})

test_that("extract_space identifies T1w", {
  expect_equal(extract_space("HCP_1200/100206/T1w/T1w_acpc_dc.nii.gz"), "T1w")
})

test_that("extract_space identifies T2w", {
  expect_equal(extract_space("HCP_1200/100206/T2w/T2w_acpc_dc.nii.gz"), "T2w")
})

test_that("extract_space identifies unprocessed", {
  expect_equal(extract_space("HCP_1200/100206/unprocessed/3T/tfMRI_WM_LR.nii.gz"), "unprocessed")
})

test_that("extract_space handles edge cases", {
  expect_true(is.na(extract_space(NA)))
  expect_true(is.na(extract_space("")))
})

# ============================================================================
# extract_derivative_level() tests
# ============================================================================

test_that("extract_derivative_level identifies processed data", {
  expect_equal(extract_derivative_level(tfmri_wm_lr), "MNINonLinear")
  expect_equal(extract_derivative_level("HCP_1200/100206/T1w/T1w.nii.gz"), "preprocessed")
})

test_that("extract_derivative_level identifies unprocessed data", {
  expect_equal(extract_derivative_level("HCP_1200/100206/unprocessed/data.nii.gz"), "unprocessed")
})

# ============================================================================
# extract_subject_id() tests
# ============================================================================

test_that("extract_subject_id extracts 6-digit IDs", {
  expect_equal(extract_subject_id(tfmri_wm_lr), "100206")
  expect_equal(extract_subject_id(tfmri_gambling_lr), "100307")
  expect_equal(extract_subject_id(tfmri_language_lr), "100610")
})

test_that("extract_subject_id handles edge cases", {
  expect_true(is.na(extract_subject_id(NA)))
  expect_true(is.na(extract_subject_id("")))
  expect_true(is.na(extract_subject_id("no/subject/here")))
})

# ============================================================================
# extract_release() tests
# ============================================================================

test_that("extract_release extracts HCP releases", {
  expect_equal(extract_release(tfmri_wm_lr), "HCP_1200")
  expect_equal(extract_release("HCP_900/100206/T1w/T1w.nii.gz"), "HCP_900")
  expect_equal(extract_release("HCP_S1200/100206/T1w/T1w.nii.gz"), "HCP_S1200")
})

test_that("extract_release handles edge cases", {
  expect_true(is.na(extract_release(NA)))
  expect_true(is.na(extract_release("")))
  expect_true(is.na(extract_release("no/release/here")))
})

# ============================================================================
# extract_session() tests
# ============================================================================

test_that("extract_session identifies 3T data", {
  expect_equal(extract_session(tfmri_wm_lr), "3T")
  expect_equal(extract_session(rfmri_rest1_lr), "3T")
})

test_that("extract_session identifies 7T data", {
  expect_equal(extract_session("HCP_1200/100206/7T/tfMRI_WM_LR.nii.gz"), "7T")
})

test_that("extract_session identifies MEG data", {
  expect_equal(extract_session("HCP_1200/100206/MEG/Restin/data.mat"), "MEG")
})

# ============================================================================
# resolve_task_name() tests
# ============================================================================

test_that("resolve_task_name handles canonical names", {
  expect_equal(resolve_task_name("WM"), "WM")
  expect_equal(resolve_task_name("MOTOR"), "MOTOR")
  expect_equal(resolve_task_name("GAMBLING"), "GAMBLING")
})

test_that("resolve_task_name handles synonyms", {
  expect_equal(resolve_task_name("wm"), "WM")
  expect_equal(resolve_task_name("working_memory"), "WM")
  expect_equal(resolve_task_name("tfMRI_WM"), "WM")
  expect_equal(resolve_task_name("gamb"), "GAMBLING")
  expect_equal(resolve_task_name("lang"), "LANGUAGE")
})

test_that("resolve_task_name handles case insensitivity", {
  expect_equal(resolve_task_name("wm"), "WM")
  expect_equal(resolve_task_name("Wm"), "WM")
  expect_equal(resolve_task_name("WM"), "WM")
})

test_that("resolve_task_name returns NA for unknown tasks", {
  expect_true(is.na(resolve_task_name("UNKNOWN")))
  expect_true(is.na(resolve_task_name("")))
  expect_true(is.na(resolve_task_name(NA)))
})

# ============================================================================
# parse_hcp_path() integration tests
# ============================================================================

test_that("parse_hcp_path returns complete metadata for task fMRI", {
  result <- parse_hcp_path(tfmri_wm_lr)

  expect_equal(result$kind, "tfmri")
  expect_equal(result$task, "WM")
  expect_equal(result$direction, "LR")
  expect_equal(result$run, 1L)
  expect_equal(result$space, "MNINonLinear")
  expect_equal(result$derivative_level, "MNINonLinear")
  expect_equal(result$file_type, "dtseries")
})

test_that("parse_hcp_path returns complete metadata for resting fMRI", {
  result <- parse_hcp_path(rfmri_rest1_lr)

  expect_equal(result$kind, "rfmri")
  expect_true(is.na(result$task))
  expect_equal(result$direction, "LR")
  expect_equal(result$run, 1L)
  expect_equal(result$space, "MNINonLinear")
  expect_equal(result$file_type, "dtseries")
})

test_that("parse_hcp_path returns complete metadata for EV files", {
  result <- parse_hcp_path(ev_wm_body)

  expect_equal(result$kind, "tfmri")
  expect_equal(result$task, "WM")
  expect_equal(result$direction, "LR")
  expect_equal(result$file_type, "ev")
})

# ============================================================================
# parse_hcp_paths() vectorized tests
# ============================================================================

test_that("parse_hcp_paths handles multiple paths", {
  paths <- c(tfmri_wm_lr, rfmri_rest1_lr, ev_wm_body)
  result <- parse_hcp_paths(paths)

  expect_s3_class(result, "data.frame")
  expect_equal(nrow(result), 3)
  expect_equal(result$kind, c("tfmri", "rfmri", "tfmri"))
  expect_equal(result$task, c("WM", NA_character_, "WM"))
  expect_equal(result$file_type, c("dtseries", "dtseries", "ev"))
})

# ============================================================================
# Test with demo_assets.csv paths
# ============================================================================

test_that("parser handles all demo_assets.csv paths correctly", {
  # These are the actual paths from demo_assets.csv
  demo_paths <- c(
    "HCP_1200/100206/MNINonLinear/Results/tfMRI_WM_LR/tfMRI_WM_LR_Atlas.dtseries.nii",
    "HCP_1200/100206/MNINonLinear/Results/tfMRI_WM_RL/tfMRI_WM_RL_Atlas.dtseries.nii",
    "HCP_1200/100206/MNINonLinear/Results/tfMRI_MOTOR_LR/tfMRI_MOTOR_LR_Atlas.dtseries.nii",
    "HCP_1200/100307/MNINonLinear/Results/tfMRI_GAMBLING_LR/tfMRI_GAMBLING_LR_Atlas.dtseries.nii",
    "HCP_1200/100206/MNINonLinear/Results/tfMRI_WM_LR/EVs/0bk_body.txt",
    "HCP_1200/100206/MNINonLinear/Results/rfMRI_REST1_LR/rfMRI_REST1_LR_Atlas.dtseries.nii",
    "HCP_1200/100610/MNINonLinear/Results/tfMRI_LANGUAGE_LR/tfMRI_LANGUAGE_LR_Atlas.dtseries.nii"
  )

  result <- parse_hcp_paths(demo_paths)

  # All should parse without NA kinds
  expect_true(all(!is.na(result$kind)))

  # Check expected kinds
  expect_equal(result$kind[1:4], rep("tfmri", 4))
  expect_equal(result$kind[5], "tfmri")  # EV file is still tfmri kind
  expect_equal(result$kind[6], "rfmri")
  expect_equal(result$kind[7], "tfmri")

  # Check tasks
  expect_equal(result$task[1:2], c("WM", "WM"))
  expect_equal(result$task[3], "MOTOR")
  expect_equal(result$task[4], "GAMBLING")
  expect_true(is.na(result$task[6]))  # rfMRI has no task
  expect_equal(result$task[7], "LANGUAGE")

  # Check file types
  expect_equal(result$file_type[1:4], rep("dtseries", 4))
  expect_equal(result$file_type[5], "ev")
  expect_equal(result$file_type[6], "dtseries")
})
