# BALSA / Aspera backend implementation
#
# BALSA (ConnectomeDB powered by BALSA) commonly distributes large datasets via
# IBM Aspera. This backend provides a thin adapter around the `ascp` CLI.

.DEFAULT_ASPERA_PORT <- 33001L
.DEFAULT_ASPERA_RATE <- "300m"

#' BALSA (Aspera) backend
#'
#' Creates a backend that can download files using IBM Aspera `ascp`.
#'
#' This backend does not currently support listing or HEAD metadata; it is
#' intended for downloading known file paths or full Aspera source specs
#' obtained from the BALSA website after authentication.
#'
#' @param ascp Path to `ascp` executable. If NULL, tries `Sys.getenv("HCPX_ASCP")`,
#'   then `Sys.which("ascp")`.
#' @param key Path to Aspera private key file. If NULL, tries
#'   `Sys.getenv("HCPX_ASPERA_KEY")`.
#' @param host Optional Aspera host. Used when `url_or_path` is just a remote
#'   path (e.g. "/path/to/file.zip") and does not include a host.
#' @param user Optional Aspera username. Used when `url_or_path` does not include
#'   a username.
#' @param port Aspera TCP port (default 33001).
#' @param rate Transfer rate limit passed to `ascp -l` (default "300m").
#' @return `hcpx_backend_balsa` object
#' @export
hcpx_backend_balsa <- function(ascp = NULL,
                               key = NULL,
                               host = NULL,
                               user = NULL,
                               port = .DEFAULT_ASPERA_PORT,
                               rate = .DEFAULT_ASPERA_RATE) {
  if (is.null(ascp)) {
    ascp <- Sys.getenv("HCPX_ASCP", unset = "")
    if (!nzchar(ascp)) {
      ascp <- Sys.which("ascp")
    }
  }

  if (is.null(key)) {
    key <- Sys.getenv("HCPX_ASPERA_KEY", unset = "")
    if (!nzchar(key)) key <- NULL
  }

  structure(
    list(
      type = "balsa",
      ascp = ascp,
      key = key,
      host = host,
      user = user,
      port = as.integer(port),
      rate = rate
    ),
    class = c("hcpx_backend_balsa", "hcpx_backend")
  )
}

.ensure_ascp <- function(backend) {
  if (is.null(backend$ascp) || !nzchar(backend$ascp)) {
    cli::cli_abort(c(
      "Aspera CLI not found",
      "i" = "Install IBM Aspera and ensure {.code ascp} is on PATH, or set {.envvar HCPX_ASCP}."
    ))
  }
  backend$ascp
}

.ensure_aspera_key <- function(backend) {
  if (is.null(backend$key) || !nzchar(backend$key)) {
    cli::cli_abort(c(
      "Aspera key file not configured",
      "i" = "Set {.envvar HCPX_ASPERA_KEY} to the Aspera private key file path."
    ))
  }
  backend$key
}

.hcpx_run_system2 <- function(command, args, stdout, stderr) {
  base::system2(command, args = args, stdout = stdout, stderr = stderr)
}

.parse_aspera_source <- function(x, backend = NULL) {
  x <- trimws(x)
  if (!nzchar(x)) {
    cli::cli_abort("Empty Aspera source")
  }

  # Accept fasp://user@host[:port]/path
  if (grepl("^fasp://", x, ignore.case = TRUE)) {
    parsed <- sub("^fasp://", "", x, ignore.case = TRUE)
    at <- regexpr("@", parsed, fixed = TRUE)
    if (at < 2) cli::cli_abort("Invalid fasp URL: {.val {x}}")
    user <- substr(parsed, 1, at - 1)
    rest <- substr(parsed, at + 1, nchar(parsed))
    # host[:port]/path...
    slash <- regexpr("/", rest, fixed = TRUE)
    if (slash < 2) cli::cli_abort("Invalid fasp URL (missing path): {.val {x}}")
    host_port <- substr(rest, 1, slash - 1)
    path <- substr(rest, slash, nchar(rest))

    port <- NA_integer_
    host <- host_port
    if (grepl(":", host_port, fixed = TRUE)) {
      parts <- strsplit(host_port, ":", fixed = TRUE)[[1]]
      if (length(parts) == 2) {
        host <- parts[1]
        port <- suppressWarnings(as.integer(parts[2]))
      }
    }

    return(list(user = user, host = host, port = port, path = path))
  }

  # Accept user@host:/path or host:/path
  if (grepl(":", x, fixed = TRUE)) {
    parts <- strsplit(x, ":", fixed = TRUE)[[1]]
    if (length(parts) >= 2) {
      left <- parts[1]
      path <- paste(parts[-1], collapse = ":")
      user <- NA_character_
      host <- left
      if (grepl("@", left, fixed = TRUE)) {
        up <- strsplit(left, "@", fixed = TRUE)[[1]]
        if (length(up) == 2) {
          user <- up[1]
          host <- up[2]
        }
      }
      return(list(user = user, host = host, port = NA_integer_, path = path))
    }
  }

  # Otherwise treat as a remote path; require host (and ideally user) from backend
  if (is.null(backend) || is.null(backend$host) || !nzchar(backend$host)) {
    cli::cli_abort(c(
      "Aspera source missing host",
      "i" = "Provide a full Aspera spec (e.g. {.val user@host:/path/file}) or configure {.arg host} in hcpx_backend_balsa()."
    ))
  }

  list(
    user = if (!is.null(backend$user) && nzchar(backend$user)) backend$user else NA_character_,
    host = backend$host,
    port = NA_integer_,
    path = x
  )
}

.aspera_download <- function(backend, source, dest, resume = TRUE) {
  ascp <- .ensure_ascp(backend)
  key <- .ensure_aspera_key(backend)

  parsed <- .parse_aspera_source(source, backend = backend)
  user <- parsed$user
  host <- parsed$host
  path <- parsed$path

  if (is.na(user) || !nzchar(user)) {
    cli::cli_abort(c(
      "Aspera source missing username",
      "i" = "Provide source as {.val user@host:/path/file} or set {.arg user} in hcpx_backend_balsa()."
    ))
  }

  port <- backend$port
  if (!is.na(parsed$port) && !is.null(parsed$port)) {
    port <- parsed$port
  }

  # Download to temp dir then move into final dest (ascp destination is a directory)
  dest_dir <- dirname(dest)
  if (!dir.exists(dest_dir)) dir.create(dest_dir, recursive = TRUE)

  tmp_dir <- file.path(dest_dir, paste0(".hcpx-ascp-", as.integer(stats::runif(1, 1e8, 1e9))))
  dir.create(tmp_dir, recursive = TRUE)
  on.exit(unlink(tmp_dir, recursive = TRUE, force = TRUE), add = TRUE)

  expected_name <- basename(path)
  source_arg <- paste0(user, "@", host, ":", path)

  resume_flag <- if (isTRUE(resume)) "2" else "0"
  args <- c(
    "-i", key,
    "-QT",
    "-l", backend$rate,
    "-P", as.character(port),
    "-k", resume_flag,
    source_arg,
    tmp_dir
  )

  status <- .hcpx_run_system2(ascp, args = args, stdout = TRUE, stderr = TRUE)
  exit_status <- attr(status, "status")
  if (!is.null(exit_status) && exit_status != 0) {
    cli::cli_abort(c(
      "Aspera download failed",
      "x" = "{paste(status, collapse = '\\n')}"
    ))
  }

  candidates <- list.files(tmp_dir, recursive = TRUE, full.names = TRUE, all.files = TRUE)
  fi <- file.info(candidates)
  candidates <- candidates[!fi$isdir]

  chosen <- character()
  if (length(candidates) == 1) {
    chosen <- candidates
  } else if (length(candidates) > 1 && nzchar(expected_name)) {
    chosen <- candidates[basename(candidates) == expected_name]
  }

  if (length(chosen) != 1) {
    cli::cli_abort(c(
      "Aspera download completed but output file could not be determined",
      "i" = "Expected filename: {.val {expected_name}}",
      "i" = "Files found: {.val {basename(candidates)}}"
    ))
  }

  if (file.exists(dest)) unlink(dest)
  ok <- file.rename(chosen, dest)
  if (!ok) {
    file.copy(chosen, dest, overwrite = TRUE)
  }

  tibble::tibble(
    status = "success",
    bytes = as.integer(file.info(dest)$size)
  )
}

#' @rdname backend_list
#' @export
#' @method backend_list hcpx_backend_balsa
backend_list.hcpx_backend_balsa <- function(backend, prefix, ...) {
  cli::cli_abort("backend_list() is not supported for BALSA/Aspera backend")
}

#' @rdname backend_head
#' @export
#' @method backend_head hcpx_backend_balsa
backend_head.hcpx_backend_balsa <- function(backend, remote_path, ...) {
  cli::cli_abort("backend_head() is not supported for BALSA/Aspera backend")
}

#' @rdname backend_presign
#' @export
#' @method backend_presign hcpx_backend_balsa
backend_presign.hcpx_backend_balsa <- function(backend, remote_path, expires_sec = .DEFAULT_PRESIGN_EXPIRY_SEC) {
  cli::cli_abort("backend_presign() is not supported for BALSA/Aspera backend")
}

#' @rdname backend_download
#' @export
#' @method backend_download hcpx_backend_balsa
backend_download.hcpx_backend_balsa <- function(backend, url_or_path, dest, resume = TRUE, ...) {
  if (grepl("^https?://", url_or_path, ignore.case = TRUE)) {
    ok <- download_url(url_or_path, dest, resume = resume)
    if (!isTRUE(ok) || !file.exists(dest)) {
      cli::cli_abort("HTTP download failed for BALSA backend")
    }
    return(tibble::tibble(
      status = "success",
      bytes = as.integer(file.info(dest)$size)
    ))
  }

  .aspera_download(backend, url_or_path, dest, resume = resume)
}
