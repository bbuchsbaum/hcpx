# Path parsing utilities - CRITICAL: Must be heavily unit-tested
# Derives task, direction, run, file_type, space, kind from HCP paths

# Canonical task names (uppercase)
.hcp_canonical_tasks <- c("WM", "MOTOR", "GAMBLING", "LANGUAGE", "SOCIAL", "RELATIONAL", "EMOTION")

# Task synonym mapping (lowercase keys -> canonical)
.hcp_task_synonyms <- c(

"wm" = "WM", "working_memory" = "WM", "tfmri_wm" = "WM",
  "motor" = "MOTOR", "tfmri_motor" = "MOTOR",
  "gambling" = "GAMBLING", "gamb" = "GAMBLING", "tfmri_gambling" = "GAMBLING",
  "language" = "LANGUAGE", "lang" = "LANGUAGE", "tfmri_language" = "LANGUAGE",
  "social" = "SOCIAL", "soc" = "SOCIAL", "tfmri_social" = "SOCIAL",
  "relational" = "RELATIONAL", "rel" = "RELATIONAL", "tfmri_relational" = "RELATIONAL",
"emotion" = "EMOTION", "emot" = "EMOTION", "tfmri_emotion" = "EMOTION"
)

#' Resolve a task name to its canonical form
#'
#' @param task Task name (any synonym)
#' @return Canonical task name or NA if not recognized
#' @keywords internal
resolve_task_name <- function(task) {
  if (is.na(task) || is.null(task) || !nzchar(task)) {
    return(NA_character_)
  }
  task_lower <- tolower(task)
  # Check if already canonical
  if (toupper(task) %in% .hcp_canonical_tasks) {
    return(toupper(task))
  }
  # Check synonyms
  if (task_lower %in% names(.hcp_task_synonyms)) {
    return(.hcp_task_synonyms[[task_lower]])
  }
  NA_character_
}

#' Parse HCP path to extract metadata
#'
#' @param path HCP remote path
#' @return Named list with kind, task, direction, run, space, derivative_level, file_type
#' @keywords internal
parse_hcp_path <- function(path) {
  list(
    kind = extract_kind(path),
    task = extract_task(path),
    direction = extract_direction(path),
    run = extract_run(path),
    space = extract_space(path),
    derivative_level = extract_derivative_level(path),
    file_type = extract_file_type(path)
  )
}

#' Vectorized path parsing
#'
#' @param paths Character vector of HCP remote paths
#' @return Data frame with columns: kind, task, direction, run, space, derivative_level, file_type
#' @keywords internal
parse_hcp_paths <- function(paths) {
  data.frame(
    kind = vapply(paths, extract_kind, character(1), USE.NAMES = FALSE),
    task = vapply(paths, extract_task, character(1), USE.NAMES = FALSE),
    direction = vapply(paths, extract_direction, character(1), USE.NAMES = FALSE),
    run = vapply(paths, extract_run, integer(1), USE.NAMES = FALSE),
    space = vapply(paths, extract_space, character(1), USE.NAMES = FALSE),
    derivative_level = vapply(paths, extract_derivative_level, character(1), USE.NAMES = FALSE),
    file_type = vapply(paths, extract_file_type, character(1), USE.NAMES = FALSE),
    stringsAsFactors = FALSE
  )
}

#' Extract kind (struct, diff, rfmri, tfmri, meg) from path
#'
#' Determines the data modality based on path patterns.
#'
#' @param path HCP remote path
#' @return Character kind
#' @keywords internal
extract_kind <- function(path) {
  if (is.na(path) || !nzchar(path)) return(NA_character_)

  # Task fMRI: contains tfMRI_ in folder name
  if (grepl("tfMRI_", path, ignore.case = FALSE)) {
    return("tfmri")
  }

  # Resting-state fMRI: contains rfMRI_ in folder name
  if (grepl("rfMRI_", path, ignore.case = FALSE)) {
    return("rfmri")
  }

  # Diffusion: contains /Diffusion/ or /diff/ or diffusion-related files
  if (grepl("/Diffusion/|/diff/|\\.bval$|\\.bvec$", path, ignore.case = TRUE)) {
    return("diff")
  }

  # MEG: contains /MEG/ in path
  if (grepl("/MEG/", path, ignore.case = FALSE)) {
    return("meg")
  }

  # Structural: T1w, T2w, or anatomical markers
  if (grepl("/T1w/|/T2w/|/anat/|_T1w|_T2w", path, ignore.case = FALSE)) {
    return("struct")
  }

  # Check for structural in MNINonLinear without functional markers
  if (grepl("/MNINonLinear/", path) && !grepl("/Results/", path)) {
    return("struct")
  }

  # Default to unknown
  NA_character_
}

#' Extract task name from path
#'
#' Parses the task name from tfMRI paths and resolves to canonical form.
#'
#' @param path HCP remote path
#' @return Character task name (canonical) or NA
#' @keywords internal
extract_task <- function(path) {
  if (is.na(path) || !nzchar(path)) return(NA_character_)

  # Match tfMRI_{TASKNAME}_{DIRECTION} pattern
  # Task name is uppercase letters/numbers between tfMRI_ and _{LR|RL}
  match <- regmatches(path, regexpr("tfMRI_([A-Z]+)_(LR|RL)", path))

  if (length(match) == 0 || !nzchar(match)) {
    return(NA_character_)
  }

  # Extract just the task name portion
  task_match <- regmatches(match, regexpr("(?<=tfMRI_)[A-Z]+(?=_)", match, perl = TRUE))

  if (length(task_match) == 0 || !nzchar(task_match)) {
    return(NA_character_)
  }

  resolve_task_name(task_match)
}

#' Extract direction (LR/RL) from path
#'
#' @param path HCP remote path
#' @return Character direction or NA
#' @keywords internal
extract_direction <- function(path) {
  if (is.na(path) || !nzchar(path)) return(NA_character_)

  # Match _LR or _RL in the path (typically in folder or file names)
  # Common patterns:
  #   tfMRI_WM_LR, rfMRI_REST1_LR, etc.
  if (grepl("_(LR)(/|_|\\.|$)", path)) {
    return("LR")
  }
  if (grepl("_(RL)(/|_|\\.|$)", path)) {
    return("RL")
  }

  NA_character_
}

#' Extract run number from path
#'
#' For resting-state fMRI, extracts the REST number (REST1, REST2, etc.)
#' For task fMRI, the run is typically implicit (1 per direction).
#'
#' @param path HCP remote path
#' @return Integer run number or NA
#' @keywords internal
extract_run <- function(path) {
  if (is.na(path) || !nzchar(path)) return(NA_integer_)

  # Resting-state: rfMRI_REST{N}_{DIR}
  rest_match <- regmatches(path, regexpr("rfMRI_REST([0-9]+)", path))
  if (length(rest_match) > 0 && nzchar(rest_match)) {
    run_num <- regmatches(rest_match, regexpr("[0-9]+", rest_match))
    if (length(run_num) > 0) {
      return(as.integer(run_num))
    }
  }

  # Task fMRI: typically run 1 (implicit), but check for explicit _run{N} pattern
  run_match <- regmatches(path, regexpr("_run([0-9]+)", path, ignore.case = TRUE))
  if (length(run_match) > 0 && nzchar(run_match)) {
    run_num <- regmatches(run_match, regexpr("[0-9]+", run_match))
    if (length(run_num) > 0) {
      return(as.integer(run_num))
    }
  }

  # Default run for task fMRI is 1
  if (grepl("tfMRI_", path)) {
    return(1L)
  }

  NA_integer_
}

#' Extract file type from path
#'
#' Determines the file type based on extension and path context.
#'
#' @param path HCP remote path
#' @return Character file type
#' @keywords internal
extract_file_type <- function(path) {
  if (is.na(path) || !nzchar(path)) return(NA_character_)

  # Get the filename
  filename <- basename(path)

  # CIFTI formats (check first as they have compound extensions)
  if (grepl("\\.dtseries\\.nii$", filename, ignore.case = TRUE)) {
    return("dtseries")
  }
  if (grepl("\\.dscalar\\.nii$", filename, ignore.case = TRUE)) {
    return("dscalar")
  }
  if (grepl("\\.dlabel\\.nii$", filename, ignore.case = TRUE)) {
    return("dlabel")
  }
  if (grepl("\\.ptseries\\.nii$", filename, ignore.case = TRUE)) {
    return("ptseries")
  }
  if (grepl("\\.pscalar\\.nii$", filename, ignore.case = TRUE)) {
    return("pscalar")
  }

  # Event timing files (in EVs folder)
  if (grepl("/EVs/", path) && grepl("\\.txt$", filename, ignore.case = TRUE)) {
    return("ev")
  }

  # Confounds/motion parameters
  if (grepl("Movement_Regressors|Confounds|confounds|motion", filename, ignore.case = TRUE)) {
    return("confounds")
  }

  # QC files
  if (grepl("_QC|qc_|quality", filename, ignore.case = TRUE)) {
    return("qc")
  }

  # Physiological data
  if (grepl("Physio|physio|\\.puls$|\\.resp$", filename, ignore.case = TRUE)) {
    return("physio")
  }

  # SBRef (single-band reference)
  if (grepl("_SBRef", filename)) {
    return("sbref")
  }

  # Diffusion-specific
  if (grepl("\\.bval$", filename, ignore.case = TRUE)) {
    return("bval")
  }
  if (grepl("\\.bvec$", filename, ignore.case = TRUE)) {
    return("bvec")
  }

  # Generic NIfTI
  if (grepl("\\.nii\\.gz$|\\.nii$", filename, ignore.case = TRUE)) {
    return("nii.gz")
  }

  # GIFTI surfaces
  if (grepl("\\.gii$", filename, ignore.case = TRUE)) {
    return("gii")
  }

  # Text files (generic, after EV check)
  if (grepl("\\.txt$", filename, ignore.case = TRUE)) {
    return("txt")
  }

  # JSON sidecars
  if (grepl("\\.json$", filename, ignore.case = TRUE)) {
    return("json")
  }

  NA_character_
}

#' Extract space/coordinate system from path
#'
#' @param path HCP remote path
#' @return Character space name
#' @keywords internal
extract_space <- function(path) {
  if (is.na(path) || !nzchar(path)) return(NA_character_)

  # MNI space (most common for processed data)
  if (grepl("/MNINonLinear/", path)) {
    return("MNINonLinear")
  }

  # T1w native space
  if (grepl("/T1w/", path)) {
    return("T1w")
  }

  # T2w native space
  if (grepl("/T2w/", path)) {
    return("T2w")
  }

  # Unprocessed data
  if (grepl("/unprocessed/", path, ignore.case = TRUE)) {
    return("unprocessed")
  }

  # Native space indicator
  if (grepl("_native|/native/", path, ignore.case = TRUE)) {
    return("native")
  }

  NA_character_
}

#' Extract derivative level from path
#'
#' Determines the processing stage based on path structure.
#'
#' @param path HCP remote path
#' @return Character derivative level
#' @keywords internal
extract_derivative_level <- function(path) {
  if (is.na(path) || !nzchar(path)) return(NA_character_)

  # Unprocessed data
  if (grepl("/unprocessed/", path, ignore.case = TRUE)) {
    return("unprocessed")
  }

  # MNINonLinear processed data (fully processed)
  if (grepl("/MNINonLinear/", path)) {
    return("MNINonLinear")
  }

  # T1w space (minimally processed)
  if (grepl("/T1w/", path) && !grepl("/unprocessed/", path, ignore.case = TRUE)) {
    return("preprocessed")
  }

  NA_character_
}

#' Extract subject ID from path
#'
#' @param path HCP remote path
#' @return Character subject ID or NA
#' @keywords internal
extract_subject_id <- function(path) {
  if (is.na(path) || !nzchar(path)) return(NA_character_)

  # HCP subject IDs are 6-digit numbers
  # Pattern: after release name, before next slash
  # e.g., HCP_1200/100206/... -> 100206
  match <- regmatches(path, regexpr("/([0-9]{6})/", path))
  if (length(match) > 0 && nzchar(match)) {
    return(gsub("/", "", match))
  }

  NA_character_
}

#' Extract release from path
#'
#' @param path HCP remote path
#' @return Character release name or NA
#' @keywords internal
extract_release <- function(path) {
  if (is.na(path) || !nzchar(path)) return(NA_character_)

  # Common HCP releases
  if (grepl("^HCP_1200/|/HCP_1200/", path)) {
    return("HCP_1200")
  }
  if (grepl("^HCP_S1200/|/HCP_S1200/", path)) {
    return("HCP_S1200")
  }
  if (grepl("^HCP_900/|/HCP_900/", path)) {
    return("HCP_900")
  }
  if (grepl("^HCP_500/|/HCP_500/", path)) {
    return("HCP_500")
  }

  # Try to extract any HCP_XXX pattern at the start
  match <- regmatches(path, regexpr("^HCP_[A-Za-z0-9]+", path))
  if (length(match) > 0 && nzchar(match)) {
    return(match)
  }

  NA_character_
}

#' Extract session type from path
#'
#' @param path HCP remote path
#' @return Character session type (3T, 7T, MEG) or NA
#' @keywords internal
extract_session <- function(path) {
  if (is.na(path) || !nzchar(path)) return(NA_character_)

  # MEG data
  if (grepl("/MEG/", path)) {
    return("MEG")
  }

  # 7T data (explicit marker)
  if (grepl("/7T/|_7T_|7T", path)) {
    return("7T")
  }

  # Default to 3T for standard paths
  if (grepl("/MNINonLinear/|/T1w/|/T2w/|/Diffusion/", path)) {
    return("3T")
  }

  NA_character_
}
