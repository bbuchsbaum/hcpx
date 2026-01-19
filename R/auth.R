# Authentication and credential management
#
# Provides secure credential storage using the keyring package.
# NEVER prints or logs secrets.

#' Authenticate for a backend using secure storage
#'
#' Configures authentication for the hcpx handle. Supports AWS profiles,
#' keyring-based credential storage, and interactive prompts.
#'
#' @param h hcpx handle
#' @param aws_profile AWS profile name (optional). If provided, uses this
#'   profile from ~/.aws/credentials
#' @param keyring_service Keyring service name for storing credentials
#'   (default: "hcpx")
#' @param interactive If TRUE, prompt for missing credentials
#' @return Modified hcpx handle (invisibly)
#' @seealso [hcpx_auth_status()], [hcpx_keyring_set()], [hcpx_keyring_clear()]
#' @export
#' @examples
#' \dontrun{
#' h <- hcpx_ya()
#'
#' # Use AWS profile
#' hcpx_auth(h, aws_profile = "hcp")
#'
#' # Use keyring (will prompt if credentials not stored)
#' hcpx_auth(h, interactive = TRUE)
#'
#' # Store credentials in keyring first
#' hcpx_keyring_set("my_access_key", "my_secret_key")
#' hcpx_auth(h)
#' }
hcpx_auth <- function(h, aws_profile = NULL, keyring_service = "hcpx",
                      interactive = FALSE) {

  # Priority order:
  # 1. AWS profile if specified
  # 2. Keyring credentials
 # 3. Environment variables
  # 4. Interactive prompt (if enabled)

  if (!is.null(aws_profile)) {
    # Use AWS profile
    Sys.setenv(AWS_PROFILE = aws_profile)
    cli::cli_alert_success("Using AWS profile: {.val {aws_profile}}")
    h$auth <- list(method = "aws_profile", profile = aws_profile)
    if (inherits(h$backend, "hcpx_backend_aws")) {
      h$backend$profile <- aws_profile
    }
    return(invisible(h))
  }

  # Try keyring
  if (has_keyring()) {
    creds <- keyring_get_credentials(keyring_service)
    if (!is.null(creds)) {
      Sys.setenv(
        AWS_ACCESS_KEY_ID = creds$access_key,
        AWS_SECRET_ACCESS_KEY = creds$secret_key
      )
      cli::cli_alert_success("Using credentials from keyring")
      h$auth <- list(method = "keyring", service = keyring_service)
      return(invisible(h))
    }
  }

  # Check environment variables
  if (has_env_credentials()) {
    cli::cli_alert_success("Using credentials from environment")
    h$auth <- list(method = "environment")
    return(invisible(h))
  }

  # Interactive prompt
if (interactive) {
    if (!has_keyring()) {
      cli::cli_abort(c(
        "Keyring package required for interactive authentication",
        "i" = "Install with: {.code install.packages('keyring')}"
      ))
    }

    cli::cli_alert_info("No credentials found. Please enter AWS credentials.")
    cli::cli_alert_warning("Credentials will be stored securely in your system keyring.")

    access_key <- readline("AWS Access Key ID: ")

    # Use secure input for secret key if available
    if (requireNamespace("askpass", quietly = TRUE)) {
      secret_key <- askpass::askpass("AWS Secret Access Key: ")
    } else {
      cli::cli_alert_warning("Install {.pkg askpass} for secure password entry")
      secret_key <- readline("AWS Secret Access Key: ")
    }

    if (nzchar(access_key) && nzchar(secret_key)) {
      keyring_set_credentials(keyring_service, access_key, secret_key)
      Sys.setenv(
        AWS_ACCESS_KEY_ID = access_key,
        AWS_SECRET_ACCESS_KEY = secret_key
      )
      cli::cli_alert_success("Credentials stored in keyring")
      h$auth <- list(method = "keyring", service = keyring_service)
      return(invisible(h))
    }
  }

  # No credentials found
  cli::cli_warn(c(
    "No AWS credentials configured",
    "i" = "The HCP open-access S3 bucket is requester-pays and typically requires AWS credentials",
    "i" = "For metadata-only exploration, use {.code catalog_version = 'seed'} or a local mirror",
    "i" = "For private buckets, use {.code hcpx_auth(h, aws_profile = 'myprofile')}"
  ))

  h$auth <- list(method = "none")
  invisible(h)
}

#' Check if keyring package is available
#'
#' @return TRUE if keyring is available
#' @keywords internal
has_keyring <- function() {
  requireNamespace("keyring", quietly = TRUE)
}

#' Check if AWS credentials are in environment
#'
#' @return TRUE if credentials found
#' @keywords internal
has_env_credentials <- function() {
  nzchar(Sys.getenv("AWS_ACCESS_KEY_ID")) &&
    nzchar(Sys.getenv("AWS_SECRET_ACCESS_KEY"))
}

#' Get credentials from keyring
#'
#' @param service Keyring service name
#' @return List with access_key and secret_key, or NULL
#' @keywords internal
keyring_get_credentials <- function(service = "hcpx") {
  if (!has_keyring()) return(NULL)

  tryCatch({
    access_key <- keyring::key_get(service, username = "aws_access_key_id")
    secret_key <- keyring::key_get(service, username = "aws_secret_access_key")
    list(access_key = access_key, secret_key = secret_key)
  }, error = function(e) {
    NULL
  })
}

#' Store credentials in keyring
#'
#' @param service Keyring service name
#' @param access_key AWS access key ID
#' @param secret_key AWS secret access key
#' @return Invisible TRUE
#' @keywords internal
keyring_set_credentials <- function(service, access_key, secret_key) {
  if (!has_keyring()) {
    stop("keyring package required", call. = FALSE)
  }

  keyring::key_set_with_value(service, username = "aws_access_key_id", password = access_key)
  keyring::key_set_with_value(service, username = "aws_secret_access_key", password = secret_key)
  invisible(TRUE)
}

#' Set AWS credentials in keyring
#'
#' Stores AWS credentials securely in the system keyring for later use
#' with hcpx_auth().
#'
#' @param access_key AWS access key ID
#' @param secret_key AWS secret access key
#' @param service Keyring service name (default: "hcpx")
#' @return Invisible TRUE
#' @export
#' @examples
#' \dontrun{
#' # Store credentials (only need to do this once)
#' hcpx_keyring_set("AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY")
#'
#' # Now auth will use these automatically
#' h <- hcpx_ya()
#' hcpx_auth(h)
#' }
hcpx_keyring_set <- function(access_key, secret_key, service = "hcpx") {
  if (!has_keyring()) {
    cli::cli_abort(c(
      "Keyring package required",
      "i" = "Install with: {.code install.packages('keyring')}"
    ))
  }

  keyring_set_credentials(service, access_key, secret_key)
  cli::cli_alert_success("Credentials stored in keyring service: {.val {service}}")
  invisible(TRUE)
}

#' Clear AWS credentials from keyring
#'
#' Removes stored credentials from the system keyring.
#'
#' @param service Keyring service name (default: "hcpx")
#' @return Invisible TRUE
#' @export
#' @examples
#' \dontrun{
#' hcpx_keyring_clear()
#' }
hcpx_keyring_clear <- function(service = "hcpx") {
  if (!has_keyring()) {
    cli::cli_warn("Keyring package not installed")
    return(invisible(FALSE))
  }

  tryCatch({
    keyring::key_delete(service, username = "aws_access_key_id")
    keyring::key_delete(service, username = "aws_secret_access_key")
    cli::cli_alert_success("Credentials cleared from keyring")
    invisible(TRUE)
  }, error = function(e) {
    cli::cli_alert_info("No credentials found in keyring")
    invisible(FALSE)
  })
}

#' Check authentication status
#'
#' Reports current authentication method and status.
#'
#' @param h hcpx handle
#' @return List with auth info (invisibly)
#' @export
#' @examples
#' \dontrun{
#' h <- hcpx_ya()
#' hcpx_auth_status(h)
#' }
hcpx_auth_status <- function(h) {
  cli::cli_h3("Authentication Status")

  # Check handle auth
  if (!is.null(h$auth)) {
    method <- h$auth$method
    cli::cli_alert_info("Current method: {.val {method}}")

    if (method == "aws_profile") {
      cli::cli_bullets(c(" " = "Profile: {h$auth$profile}"))
    } else if (method == "keyring") {
      cli::cli_bullets(c(" " = "Service: {h$auth$service}"))
    }
  } else {
    cli::cli_alert_info("No authentication configured")
  }

  # Check what's available
  cli::cli_text("")
  cli::cli_alert_info("Available methods:")

  if (has_env_credentials()) {
    cli::cli_bullets(c("v" = "Environment variables (AWS_ACCESS_KEY_ID)"))
  } else {
    cli::cli_bullets(c("x" = "Environment variables"))
  }

  if (has_keyring() && !is.null(keyring_get_credentials())) {
    cli::cli_bullets(c("v" = "Keyring credentials"))
  } else if (has_keyring()) {
    cli::cli_bullets(c("x" = "Keyring (no credentials stored)"))
  } else {
    cli::cli_bullets(c("x" = "Keyring (package not installed)"))
  }

  # Check AWS config file
  aws_config <- path.expand("~/.aws/credentials")
  if (file.exists(aws_config)) {
    cli::cli_bullets(c("v" = "AWS config file"))
  } else {
    cli::cli_bullets(c("x" = "AWS config file"))
  }

  invisible(h$auth)
}
