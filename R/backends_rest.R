# REST/ConnectomeDB backend implementation
#
# ConnectomeDB uses XNAT platform with session-based authentication.
# Users need an account at db.humanconnectome.org and appropriate data access.
# This backend handles cookie/session management and authenticated downloads.

# Package-level session storage for REST backend
.rest_session <- new.env(parent = emptyenv())

#' Create REST backend
#'
#' @param base_url Base URL for ConnectomeDB/XNAT
#' @param project XNAT project name (default: "HCP_1200")
#' @return hcpx_backend_rest object
#' @keywords internal
hcpx_backend_rest <- function(base_url = "https://db.humanconnectome.org",
                               project = "HCP_1200") {
  structure(
    list(
      type = "rest",
      base_url = base_url,
      project = project,
      session_cookie = NULL
    ),
    class = c("hcpx_backend_rest", "hcpx_backend")
  )
}

#' Authenticate with ConnectomeDB
#'
#' Creates a session with ConnectomeDB using stored credentials.
#'
#' @param backend REST backend object
#' @param username ConnectomeDB username (optional, uses keyring if not provided)
#' @param password ConnectomeDB password (optional, uses keyring if not provided)
#' @return Updated backend with session cookie
#' @keywords internal
rest_authenticate <- function(backend, username = NULL, password = NULL) {
  # Get credentials from keyring if not provided
  if (is.null(username) || is.null(password)) {
    if (has_keyring()) {
      creds <- tryCatch({
        list(
          username = keyring::key_get("hcpx_connectome", username = "username"),
          password = keyring::key_get("hcpx_connectome", username = "password")
        )
      }, error = function(e) NULL)

      if (!is.null(creds)) {
        username <- creds$username
        password <- creds$password
      }
    }
  }

  if (is.null(username) || is.null(password)) {
    cli::cli_abort(c(
      "ConnectomeDB credentials required",
      "i" = "Store credentials with: {.code hcpx_connectome_auth('username', 'password')}"
    ))
  }

  # XNAT session endpoint
  auth_url <- paste0(backend$base_url, "/data/JSESSION")

  # Create session
  curl_h <- curl::new_handle()
  curl::handle_setopt(curl_h,
    userpwd = paste0(username, ":", password),
    post = TRUE,
    postfields = ""
  )

  response <- tryCatch({
    curl::curl_fetch_memory(auth_url, handle = curl_h)
  }, error = function(e) {
    cli::cli_abort("Failed to authenticate: {e$message}")
  })

  if (response$status_code != 200) {
    cli::cli_abort("Authentication failed with status {response$status_code}")
  }

  # Extract session cookie
  session_id <- rawToChar(response$content)

  # Store in backend
  backend$session_cookie <- session_id

  # Also store globally for reuse
  assign("session", session_id, envir = .rest_session)
  assign("expires", Sys.time() + .DEFAULT_PRESIGN_EXPIRY_SEC, envir = .rest_session)  # 1 hour

  cli::cli_alert_success("Authenticated with ConnectomeDB")
  backend
}

#' Get current session cookie
#'
#' @param backend REST backend object
#' @return Session cookie string or NULL
#' @keywords internal
get_session_cookie <- function(backend) {
  # Check backend first

  if (!is.null(backend$session_cookie)) {
    return(backend$session_cookie)
  }

  # Check global session
  if (exists("session", envir = .rest_session)) {
    expires <- get("expires", envir = .rest_session)
    if (Sys.time() < expires) {
      return(get("session", envir = .rest_session))
    }
  }

  NULL
}

#' Ensure authenticated session exists
#'
#' @param backend REST backend object
#' @return Backend with valid session
#' @keywords internal
ensure_session <- function(backend) {
  cookie <- get_session_cookie(backend)
  if (is.null(cookie)) {
    backend <- rest_authenticate(backend)
  }
  backend
}

#' Create curl handle with session cookie
#'
#' @param backend REST backend object
#' @return curl handle
#' @keywords internal
rest_handle <- function(backend) {
  cookie <- get_session_cookie(backend)
  if (is.null(cookie)) {
    cli::cli_abort(c(
      "No ConnectomeDB session",
      "i" = "Authenticate first with {.code rest_authenticate(backend)}"
    ))
  }

  curl_h <- curl::new_handle()
  curl::handle_setopt(curl_h,
    cookie = paste0("JSESSIONID=", cookie),
    followlocation = TRUE
  )
  curl_h
}

#' List files via REST backend
#'
#' @rdname backend_list
#' @export
#' @method backend_list hcpx_backend_rest
backend_list.hcpx_backend_rest <- function(backend, prefix, ...) {
  backend <- ensure_session(backend)

  # Parse prefix to extract subject and resource type
  # Expected format: HCP_1200/100307/...
  parts <- strsplit(prefix, "/")[[1]]
  parts <- parts[nzchar(parts)]

  if (length(parts) < 2) {
    cli::cli_abort("Invalid prefix for REST backend: {prefix}")
  }

  project <- parts[1]
  subject <- parts[2]

  # XNAT resource listing endpoint
  # /data/projects/{project}/subjects/{subject}/experiments/{session}/resources
  list_url <- sprintf(
    "%s/data/projects/%s/subjects/%s/experiments/%s_3T/resources?format=json",
    backend$base_url, project, subject, subject
  )

  curl_h <- rest_handle(backend)
  response <- tryCatch({
    curl::curl_fetch_memory(list_url, handle = curl_h)
  }, error = function(e) {
    cli::cli_abort("Failed to list resources: {e$message}")
  })

  if (response$status_code == 404) {
    return(tibble::tibble(
      remote_path = character(),
      size_bytes = integer(),
      last_modified = character()
    ))
  }

  if (response$status_code != 200) {
    cli::cli_abort("List failed with status {response$status_code}")
  }

  # Parse JSON response
  content <- rawToChar(response$content)
  data <- jsonlite::fromJSON(content)

  # XNAT returns ResultSet with Result array
  if (!"ResultSet" %in% names(data) || !"Result" %in% names(data$ResultSet)) {
    return(tibble::tibble(
      remote_path = character(),
      size_bytes = integer(),
      last_modified = character()
    ))
  }

  results <- data$ResultSet$Result

  if (length(results) == 0 || nrow(results) == 0) {
    return(tibble::tibble(
      remote_path = character(),
      size_bytes = integer(),
      last_modified = character()
    ))
  }

  tibble::tibble(
    remote_path = results$URI %||% results$label,
    size_bytes = as.integer(results$file_count %||% NA),
    last_modified = NA_character_
  )
}

#' Get file metadata via REST backend
#'
#' @rdname backend_head
#' @export
#' @method backend_head hcpx_backend_rest
backend_head.hcpx_backend_rest <- function(backend, remote_path, ...) {
  backend <- ensure_session(backend)

  # Build URL for the resource
  url <- paste0(backend$base_url, remote_path)

  curl_h <- rest_handle(backend)
  curl::handle_setopt(curl_h, nobody = TRUE)

  response <- tryCatch({
    curl::curl_fetch_memory(url, handle = curl_h)
  }, error = function(e) {
    cli::cli_abort("Failed to HEAD {remote_path}: {e$message}")
  })

  if (response$status_code == 404) {
    cli::cli_abort("File not found: {remote_path}")
  }

  if (response$status_code != 200) {
    cli::cli_abort("HEAD request failed with status {response$status_code}")
  }

  headers <- curl::parse_headers_list(response$headers)

  tibble::tibble(
    size_bytes = as.integer(headers$`content-length` %||% NA),
    etag = headers$etag %||% NA_character_,
    last_modified = headers$`last-modified` %||% NA_character_
  )
}

#' Generate presigned URL via REST backend
#'
#' @rdname backend_presign
#' @export
#' @method backend_presign hcpx_backend_rest
backend_presign.hcpx_backend_rest <- function(backend, remote_path, expires_sec = .DEFAULT_PRESIGN_EXPIRY_SEC) {
  # REST backend doesn't support presigning - requires session
  cli::cli_abort(c(
    "REST backend does not support presigned URLs",
    "i" = "Use {.code backend_download()} with an authenticated session instead"
  ))
}

#' Download file via REST backend
#'
#' @rdname backend_download
#' @export
#' @method backend_download hcpx_backend_rest
backend_download.hcpx_backend_rest <- function(backend, url_or_path, dest, resume = TRUE, ...) {
  backend <- ensure_session(backend)

  # Build full URL if needed
  if (grepl("^https?://", url_or_path)) {
    url <- url_or_path
  } else {
    url <- paste0(backend$base_url, url_or_path)
  }

  curl_h <- rest_handle(backend)
  curl::handle_setopt(curl_h, failonerror = TRUE)

  # Resume support via Range header
  existing_size <- 0L
  if (resume && file.exists(dest)) {
    existing_size <- file.info(dest)$size
    if (!is.na(existing_size) && existing_size > 0) {
      curl::handle_setheaders(curl_h, Range = sprintf("bytes=%d-", existing_size))
    }
  }

  # Download
  tryCatch({
    curl::curl_download(url, dest, handle = curl_h, mode = "ab")
  }, error = function(e) {
    # Check for already complete
    if (grepl("416", e$message) && existing_size > 0) {
      return(invisible(NULL))
    }
    cli::cli_abort("Download failed: {e$message}")
  })

  final_size <- file.info(dest)$size

  tibble::tibble(
    status = "success",
    bytes = as.integer(final_size)
  )
}

#' Store ConnectomeDB credentials in keyring
#'
#' Securely stores ConnectomeDB credentials for later use with the REST backend.
#'
#' @param username ConnectomeDB username
#' @param password ConnectomeDB password
#' @return Invisible TRUE
#' @export
#' @examples
#' \dontrun{
#' # Store credentials (only need to do once)
#' hcpx_connectome_auth("myusername", "mypassword")
#'
#' # Now REST backend will use these automatically
#' h <- hcpx_ya(backend = "rest")
#' }
hcpx_connectome_auth <- function(username, password) {
  if (!has_keyring()) {
    cli::cli_abort(c(
      "Keyring package required",
      "i" = "Install with: {.code install.packages('keyring')}"
    ))
  }

  keyring::key_set_with_value("hcpx_connectome", username = "username", password = username)
  keyring::key_set_with_value("hcpx_connectome", username = "password", password = password)

  cli::cli_alert_success("ConnectomeDB credentials stored in keyring")
  invisible(TRUE)
}

#' Clear ConnectomeDB credentials from keyring
#'
#' Removes stored ConnectomeDB credentials so REST auth will prompt again.
#'
#' @return Invisible TRUE
#' @export
#' @examples
#' \dontrun{
#' hcpx_connectome_clear()
#' }
hcpx_connectome_clear <- function() {
  if (!has_keyring()) {
    cli::cli_warn("Keyring package not installed")
    return(invisible(FALSE))
  }

  tryCatch({
    keyring::key_delete("hcpx_connectome", username = "username")
    keyring::key_delete("hcpx_connectome", username = "password")
    cli::cli_alert_success("ConnectomeDB credentials cleared")
    invisible(TRUE)
  }, error = function(e) {
    cli::cli_alert_info("No ConnectomeDB credentials found in keyring")
    invisible(FALSE)
  })
}

#' Clear REST session
#'
#' @return Invisible NULL
#' @keywords internal
rest_session_clear <- function() {
  rm(list = ls(envir = .rest_session), envir = .rest_session)
  invisible(NULL)
}
