# Access tier tagging and enforcement
#
# HCP data has three access tiers:
# - open: Publicly available (e.g., gender, age_range, completion flags)
# - tier1: Restricted, requires Data Use Agreement (e.g., some behavioral measures)
# - tier2: Highly restricted (e.g., genetic data, psychiatric diagnoses)
#
# The dictionary table tracks access tiers for each variable.

# Valid access tiers
.ACCESS_TIERS <- c("open", "tier1", "tier2")

#' Describe a behavioral variable and its access tier
#'
#' Returns metadata about a variable including its access tier, category,
#' and description from the dictionary table.
#'
#' @param h hcpx handle
#' @param var_name Variable name to describe
#' @return Tibble with variable metadata: var_name, access_tier, category, instrument, description
#' @export
#' @examples
#' \dontrun{
#' h <- hcpx_ya()
#' describe_variable(h, "Age_in_Yrs")
#' describe_variable(h, "Gender")
#' }
describe_variable <- function(h, var_name) {
  h <- get_hcpx(h)
  con <- get_con(h)

  # Query dictionary table
  if (!DBI::dbExistsTable(con, "dictionary")) {
    cli::cli_warn("Dictionary table not found. Variable metadata unavailable.")
    return(tibble::tibble(
      var_name = var_name,
      access_tier = "unknown",
      category = NA_character_,
      instrument = NA_character_,
      description = NA_character_
    ))
  }

  result <- DBI::dbGetQuery(con,
    "SELECT var_name, access_tier, category, instrument, description
     FROM dictionary
     WHERE var_name = ?",
    params = list(var_name)
  )

  if (nrow(result) == 0) {
    cli::cli_alert_info("Variable {.val {var_name}} not found in dictionary")
    return(tibble::tibble(
      var_name = var_name,
      access_tier = "unknown",
      category = NA_character_,
      instrument = NA_character_,
      description = NA_character_
    ))
  }

  tibble::as_tibble(result)
}

#' Check if a variable is restricted
#'
#' Returns TRUE if the variable requires tier1 or tier2 access.
#'
#' @param h hcpx handle
#' @param var_name Variable name
#' @return Logical TRUE if restricted
#' @keywords internal
is_restricted <- function(h, var_name) {
  info <- describe_variable(h, var_name)
  info$access_tier %in% c("tier1", "tier2")
}

#' Check access tier of a variable
#'
#' @param h hcpx handle
#' @param var_name Variable name
#' @return Character access tier ("open", "tier1", "tier2", or "unknown")
#' @keywords internal
get_access_tier <- function(h, var_name) {
  info <- describe_variable(h, var_name)
  info$access_tier
}

#' Enforce policy on restricted data access
#'
#' Checks if access to restricted variables is allowed based on handle policy.
#' Issues a warning or error depending on policy settings.
#'
#' @param h hcpx handle
#' @param var_names Character vector of variable names being accessed
#' @return Invisible NULL (or throws error if policy violated)
#' @keywords internal
policy_check_access <- function(h, var_names) {
  h <- get_hcpx(h)

  # Skip if policy enforcement is disabled
  if (!isTRUE(h$policy$enforce)) {
    return(invisible(NULL))
  }

  # Check each variable
  restricted <- vapply(var_names, function(v) is_restricted(h, v), logical(1))

  if (any(restricted)) {
    restricted_vars <- var_names[restricted]

    if (isTRUE(h$policy$allow_export_restricted)) {
      cli::cli_warn(c(
        "Accessing restricted variables: {.val {restricted_vars}}",
        "i" = "These require appropriate data use agreements"
      ))
    } else {
      cli::cli_abort(c(
        "Access to restricted variables denied: {.val {restricted_vars}}",
        "i" = "Set {.code h$policy$allow_export_restricted <- TRUE} to allow",
        "i" = "Or use {.code hcpx_policy(h, allow_export_restricted = TRUE)}"
      ))
    }
  }

  invisible(NULL)
}

#' Configure policy settings for an hcpx handle
#'
#' Updates the policy settings that control access to restricted data.
#'
#' @param h hcpx handle
#' @param enforce Whether to enforce access tier policies (default TRUE)
#' @param allow_export_restricted Whether to allow export of restricted data (default FALSE)
#' @return Modified hcpx handle
#' @export
#' @examples
#' \dontrun{
#' h <- hcpx_ya()
#'
#' # Disable policy enforcement (not recommended)
#' h <- hcpx_policy(h, enforce = FALSE)
#'
#' # Allow restricted data access (requires DUA)
#' h <- hcpx_policy(h, allow_export_restricted = TRUE)
#' }
hcpx_policy <- function(h, enforce = NULL, allow_export_restricted = NULL) {
  h <- get_hcpx(h)

  if (!is.null(enforce)) {
    h$policy$enforce <- enforce
  }

  if (!is.null(allow_export_restricted)) {
    h$policy$allow_export_restricted <- allow_export_restricted
  }

  h
}

#' List all variables by access tier
#'
#' Returns a summary of variables grouped by their access tier.
#'
#' @param h hcpx handle
#' @param tier Optional. Filter to specific tier ("open", "tier1", "tier2")
#' @return Tibble with var_name, access_tier, category, description
#' @export
#' @examples
#' \dontrun{
#' h <- hcpx_ya()
#'
#' # List all variables
#' list_variables(h)
#'
#' # List only restricted tier1 variables
#' list_variables(h, tier = "tier1")
#' }
list_variables <- function(h, tier = NULL) {
  h <- get_hcpx(h)
  con <- get_con(h)

  if (!DBI::dbExistsTable(con, "dictionary")) {
    cli::cli_warn("Dictionary table not found")
    return(tibble::tibble(
      var_name = character(),
      access_tier = character(),
      category = character(),
      description = character()
    ))
  }

  if (!is.null(tier)) {
    valid_tiers <- .ACCESS_TIERS
    if (!tier %in% valid_tiers) {
      cli::cli_abort("Invalid tier: {.val {tier}}. Must be one of: {.val {valid_tiers}}")
    }
    query <- "SELECT var_name, access_tier, category, description FROM dictionary WHERE access_tier = ? ORDER BY access_tier, category, var_name"
    result <- DBI::dbGetQuery(con, query, params = list(tier))
  } else {
    query <- "SELECT var_name, access_tier, category, description FROM dictionary ORDER BY access_tier, category, var_name"
    result <- DBI::dbGetQuery(con, query)
  }

  tibble::as_tibble(result)
}

#' Get summary of access tiers
#'
#' Returns count of variables in each access tier.
#'
#' @param h hcpx handle
#' @return Tibble with access_tier and count
#' @export
#' @examples
#' \dontrun{
#' h <- hcpx_ya()
#' policy_summary(h)
#' }
policy_summary <- function(h) {
  h <- get_hcpx(h)
  con <- get_con(h)

  if (!DBI::dbExistsTable(con, "dictionary")) {
    cli::cli_warn("Dictionary table not found")
    return(tibble::tibble(
      access_tier = character(),
      n = integer()
    ))
  }

  result <- DBI::dbGetQuery(con, "
    SELECT access_tier, COUNT(*) as n
    FROM dictionary
    GROUP BY access_tier
    ORDER BY access_tier
  ")

  tibble::as_tibble(result)
}
