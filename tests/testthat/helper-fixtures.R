# Test fixtures and helpers for hcpx
# Automatically sourced before tests run (testthat 3 convention)

# Load demo assets as a data frame
load_demo_assets <- function() {
  path <- system.file("extdata", "demo_assets.csv", package = "hcpx")
  if (!nzchar(path) || !file.exists(path)) {
    stop("Demo assets file not found. Is the package installed?")
  }
  utils::read.csv(path, stringsAsFactors = FALSE)
}

# Load demo subjects as a data frame
load_demo_subjects <- function() {
  path <- system.file("extdata", "demo_subjects.csv", package = "hcpx")
  if (!nzchar(path) || !file.exists(path)) {
    stop("Demo subjects file not found. Is the package installed?")
  }
  utils::read.csv(path, stringsAsFactors = FALSE)
}

# Load tasks YAML as a list
load_tasks_yaml <- function() {
  path <- system.file("extdata", "tasks.yml", package = "hcpx")
  if (!nzchar(path) || !file.exists(path)) {
    stop("Tasks YAML file not found. Is the package installed?")
  }
  yaml::read_yaml(path)
}

# Load bundles YAML as a list
load_bundles_yaml <- function() {
  path <- system.file("extdata", "bundles.yml", package = "hcpx")
  if (!nzchar(path) || !file.exists(path)) {
    stop("Bundles YAML file not found. Is the package installed?")
  }
  yaml::read_yaml(path)
}

# Get sample HCP paths for different kinds
sample_hcp_paths <- function() {
  list(
    # Task fMRI paths
    tfmri_dtseries_lr = "HCP_1200/100206/MNINonLinear/Results/tfMRI_WM_LR/tfMRI_WM_LR_Atlas.dtseries.nii",
    tfmri_dtseries_rl = "HCP_1200/100206/MNINonLinear/Results/tfMRI_WM_RL/tfMRI_WM_RL_Atlas.dtseries.nii",
    tfmri_motor = "HCP_1200/100206/MNINonLinear/Results/tfMRI_MOTOR_LR/tfMRI_MOTOR_LR_Atlas.dtseries.nii",
    tfmri_gambling = "HCP_1200/100307/MNINonLinear/Results/tfMRI_GAMBLING_LR/tfMRI_GAMBLING_LR_Atlas.dtseries.nii",
    tfmri_language = "HCP_1200/100610/MNINonLinear/Results/tfMRI_LANGUAGE_RL/tfMRI_LANGUAGE_RL_Atlas.dtseries.nii",
    tfmri_social = "HCP_1200/100206/MNINonLinear/Results/tfMRI_SOCIAL_LR/tfMRI_SOCIAL_LR_Atlas.dtseries.nii",
    tfmri_relational = "HCP_1200/100206/MNINonLinear/Results/tfMRI_RELATIONAL_RL/tfMRI_RELATIONAL_RL_Atlas.dtseries.nii",
    tfmri_emotion = "HCP_1200/100206/MNINonLinear/Results/tfMRI_EMOTION_LR/tfMRI_EMOTION_LR_Atlas.dtseries.nii",

    # Event files (EVs)
    ev_file = "HCP_1200/100206/MNINonLinear/Results/tfMRI_WM_LR/EVs/0bk_body.txt",
    ev_file2 = "HCP_1200/100206/MNINonLinear/Results/tfMRI_WM_LR/EVs/2bk_faces.txt",

    # Resting-state fMRI paths
    rfmri_rest1_lr = "HCP_1200/100206/MNINonLinear/Results/rfMRI_REST1_LR/rfMRI_REST1_LR_Atlas.dtseries.nii",
    rfmri_rest1_rl = "HCP_1200/100206/MNINonLinear/Results/rfMRI_REST1_RL/rfMRI_REST1_RL_Atlas.dtseries.nii",
    rfmri_rest2_lr = "HCP_1200/100206/MNINonLinear/Results/rfMRI_REST2_LR/rfMRI_REST2_LR_Atlas.dtseries.nii",
    rfmri_rest2_rl = "HCP_1200/100206/MNINonLinear/Results/rfMRI_REST2_RL/rfMRI_REST2_RL_Atlas.dtseries.nii",

    # Structural paths
    struct_t1w = "HCP_1200/100206/T1w/T1w_acpc_dc.nii.gz",
    struct_t2w = "HCP_1200/100206/T2w/T2w_acpc_dc.nii.gz",
    struct_mni = "HCP_1200/100206/MNINonLinear/T1w.nii.gz",
    struct_anat = "HCP_1200/100206/MNINonLinear/aparc+aseg.nii.gz",

    # Diffusion paths
    diff_bval = "HCP_1200/100206/Diffusion/data.bval",
    diff_bvec = "HCP_1200/100206/Diffusion/data.bvec",
    diff_data = "HCP_1200/100206/Diffusion/data.nii.gz",

    # MEG paths
    meg_data = "HCP_1200/100206/MEG/Restin/rmegpreproc/100206_MEG_3-Restin_rmegpreproc.mat",

    # Other file types
    confounds = "HCP_1200/100206/MNINonLinear/Results/tfMRI_WM_LR/Movement_Regressors.txt",
    sbref = "HCP_1200/100206/MNINonLinear/Results/tfMRI_WM_LR/tfMRI_WM_LR_SBRef.nii.gz",
    physio = "HCP_1200/100206/MNINonLinear/Results/tfMRI_WM_LR/tfMRI_WM_LR_Physio.txt",
    json = "HCP_1200/100206/MNINonLinear/Results/tfMRI_WM_LR/tfMRI_WM_LR.json",

    # CIFTI variants
    dscalar = "HCP_1200/100206/MNINonLinear/100206.sulc.32k_fs_LR.dscalar.nii",
    dlabel = "HCP_1200/100206/MNINonLinear/100206.aparc.32k_fs_LR.dlabel.nii",
    ptseries = "HCP_1200/100206/MNINonLinear/Results/tfMRI_WM_LR/tfMRI_WM_LR_Atlas_hp200_s4.ptseries.nii",

    # Surface files
    gii_surface = "HCP_1200/100206/MNINonLinear/100206.L.midthickness.32k_fs_LR.surf.gii",

    # Unprocessed data
    unprocessed = "HCP_1200/100206/unprocessed/3T/tfMRI_WM_LR/100206_3T_tfMRI_WM_LR.nii.gz",

    # 7T data
    data_7t = "HCP_1200/100206/7T/tfMRI_MOVIE1_7T_AP/tfMRI_MOVIE1_7T_AP_Atlas.dtseries.nii",

    # Edge cases
    empty = "",
    na_path = NA_character_,

    # Different releases
    hcp_900 = "HCP_900/100206/MNINonLinear/Results/tfMRI_WM_LR/tfMRI_WM_LR_Atlas.dtseries.nii",
    hcp_500 = "HCP_500/100206/MNINonLinear/Results/tfMRI_WM_LR/tfMRI_WM_LR_Atlas.dtseries.nii"
  )
}

# Canonical task names for testing
canonical_tasks <- function() {
  c("WM", "MOTOR", "GAMBLING", "LANGUAGE", "SOCIAL", "RELATIONAL", "EMOTION")
}

# Task synonyms for testing
task_synonyms <- function() {
  list(
    WM = c("wm", "working_memory", "tfmri_wm", "WORKING_MEMORY"),
    MOTOR = c("motor", "tfmri_motor"),
    GAMBLING = c("gambling", "gamb", "tfmri_gambling", "GAMB"),
    LANGUAGE = c("language", "lang", "tfmri_language", "LANG"),
    SOCIAL = c("social", "soc", "tfmri_social", "SOC"),
    RELATIONAL = c("relational", "rel", "tfmri_relational", "REL"),
    EMOTION = c("emotion", "emot", "tfmri_emotion", "EMOT")
  )
}

# Create a temporary test database
create_test_db <- function(engine = c("duckdb", "sqlite")) {

  engine <- match.arg(engine)
  tmp_file <- tempfile(fileext = if (engine == "duckdb") ".duckdb" else ".sqlite")
  # Return path; caller is responsible for cleanup
  tmp_file
}

# Skip test if required package is not available
skip_if_no_pkg <- function(pkg) {
  if (!requireNamespace(pkg, quietly = TRUE)) {
    testthat::skip(paste0("Package '", pkg, "' not available"))
  }
}
