# AWS S3 backend implementation
#
# The HCP open-access bucket (hcp-openaccess) is a "requester-pays" bucket.
# Users need AWS credentials configured to access it. The bucket is "open"
# in that anyone with an AWS account can access it, but it's not public.
#
# For development/testing with demo data, the local backend or seed data
# can be used without AWS credentials.

#' Create AWS backend
#'
#' @param bucket S3 bucket name (default: "hcp-openaccess")
#' @param region AWS region (default: "us-east-1")
#' @param profile AWS profile name for private buckets (optional)
#' @param request_payer Request payer mode for requester-pays buckets. Use
#'   "requester" to send the request-payer flag/header (default: "requester"
#'   for the HCP open-access bucket).
#' @return hcpx_backend_aws object
#' @keywords internal
hcpx_backend_aws <- function(bucket = "hcp-openaccess",
                             region = "us-east-1",
                             profile = NULL,
                             request_payer = if (identical(bucket, "hcp-openaccess")) "requester" else NULL) {
  structure(
    list(
      type = "aws",
      bucket = bucket,
      region = region,
      profile = profile,
      request_payer = request_payer,
      base_url = sprintf("https://%s.s3.amazonaws.com", bucket)
    ),
    class = c("hcpx_backend_aws", "hcpx_backend")
  )
}

#' Build S3 URL for a remote path
#'
#' @param backend AWS backend object
#' @param remote_path Remote path within the bucket
#' @return Character URL
#' @keywords internal
s3_url <- function(backend, remote_path) {
  # Remove leading slash if present
  remote_path <- sub("^/", "", remote_path)
  # URL-encode path components to handle special characters safely
  encoded_path <- utils::URLencode(remote_path, reserved = TRUE)
  paste0(backend$base_url, "/", encoded_path)
}

.aws_cli_path <- function() {
  override <- Sys.getenv("HCPX_AWS_CLI", unset = "")
  if (nzchar(override)) return(override)
  Sys.which("aws")
}

.aws_cli_global_args <- function(backend) {
  args <- c("--no-cli-pager")

  if (!is.null(backend$region) && nzchar(backend$region)) {
    args <- c(args, "--region", backend$region)
  }

  if (!is.null(backend$profile) && nzchar(backend$profile)) {
    args <- c(args, "--profile", backend$profile)
  }

  args
}

.aws_request_payer_args <- function(backend) {
  if (!is.null(backend$request_payer) && nzchar(backend$request_payer)) {
    return(c("--request-payer", backend$request_payer))
  }
  character()
}

.aws_run <- function(backend, args) {
  aws <- .aws_cli_path()
  if (!nzchar(aws)) {
    cli::cli_abort(c(
      "AWS CLI not found",
      "i" = "Install AWS CLI and ensure {.code aws} is on PATH, or set {.envvar HCPX_AWS_CLI}."
    ))
  }

  stdout_file <- tempfile("hcpx_aws_stdout_")
  stderr_file <- tempfile("hcpx_aws_stderr_")
  on.exit(unlink(c(stdout_file, stderr_file)), add = TRUE)

  status <- tryCatch(
    system2(aws, args = args, stdout = stdout_file, stderr = stderr_file),
    error = function(e) 127L
  )

  stdout <- paste(readLines(stdout_file, warn = FALSE), collapse = "\n")
  stderr <- paste(readLines(stderr_file, warn = FALSE), collapse = "\n")

  list(status = status, stdout = stdout, stderr = stderr)
}

.aws_s3api_json <- function(backend, op_args) {
  args <- c(
    .aws_cli_global_args(backend),
    "s3api",
    op_args,
    "--output",
    "json"
  )

  res <- .aws_run(backend, args)
  if (!identical(res$status, 0L)) {
    cli::cli_abort(c(
      "AWS CLI failed",
      "x" = "{res$stderr}"
    ))
  }

  jsonlite::fromJSON(res$stdout)
}

#' List files via AWS backend
#'
#' @rdname backend_list
#' @export
#' @method backend_list hcpx_backend_aws
backend_list.hcpx_backend_aws <- function(backend, prefix, ...) {
  prefix <- sub("^/", "", prefix)

  # Prefer AWS CLI for requester-pays or non-public buckets.
  aws <- .aws_cli_path()
  if (nzchar(aws)) {
    out <- .aws_s3api_json(
      backend,
      c(
        "list-objects-v2",
        "--bucket",
        backend$bucket,
        "--prefix",
        prefix,
        .aws_request_payer_args(backend)
      )
    )

    if (is.null(out$Contents) || length(out$Contents) == 0) {
      return(tibble::tibble(
        remote_path = character(),
        size_bytes = integer(),
        last_modified = character()
      ))
    }

    return(tibble::tibble(
      remote_path = out$Contents$Key,
      size_bytes = as.integer(out$Contents$Size),
      last_modified = as.character(out$Contents$LastModified)
    ))
  }

  if (!is.null(backend$request_payer) && nzchar(backend$request_payer)) {
    cli::cli_abort(c(
      "AWS CLI required for requester-pays buckets",
      "i" = "Install AWS CLI and configure credentials (or use {.code hcpx_auth()}).",
      "i" = "Bucket: {.val {backend$bucket}}"
    ))
  }

  # Fallback: S3 list objects via unsigned REST API (works only for public buckets)
  list_url <- sprintf("%s?list-type=2&prefix=%s",
    backend$base_url,
    utils::URLencode(prefix, reserved = TRUE)
  )

  response <- tryCatch({
    curl_h <- curl::new_handle()
    curl::handle_setopt(curl_h, followlocation = TRUE)
    curl::curl_fetch_memory(list_url, handle = curl_h)
  }, error = function(e) {
    cli::cli_abort("Failed to list S3 bucket: {e$message}")
  })

  if (response$status_code != 200) {
    cli::cli_abort("S3 list failed with status {response$status_code}")
  }

  xml_content <- rawToChar(response$content)

  keys <- unlist(regmatches(
    xml_content,
    gregexpr("(?<=<Key>)[^<]+(?=</Key>)", xml_content, perl = TRUE)
  ))
  sizes <- unlist(regmatches(
    xml_content,
    gregexpr("(?<=<Size>)[^<]+(?=</Size>)", xml_content, perl = TRUE)
  ))

  if (length(keys) == 0) {
    return(tibble::tibble(
      remote_path = character(),
      size_bytes = integer(),
      last_modified = character()
    ))
  }

  tibble::tibble(
    remote_path = keys,
    size_bytes = as.integer(sizes),
    last_modified = NA_character_
  )
}

#' Get file metadata via AWS backend
#'
#' @rdname backend_head
#' @export
#' @method backend_head hcpx_backend_aws
backend_head.hcpx_backend_aws <- function(backend, remote_path, ...) {
  remote_path <- sub("^/", "", remote_path)

  aws <- .aws_cli_path()
  if (nzchar(aws)) {
    res <- .aws_run(
      backend,
      c(
        .aws_cli_global_args(backend),
        "s3api",
        "head-object",
        "--bucket",
        backend$bucket,
        "--key",
        remote_path,
        .aws_request_payer_args(backend),
        "--output",
        "json"
      )
    )

    if (!identical(res$status, 0L)) {
      if (grepl("\\(404\\)", res$stderr, fixed = FALSE) || grepl("Not Found", res$stderr, fixed = TRUE)) {
        cli::cli_abort("File not found: {remote_path}")
      }
      cli::cli_abort(c(
        "HEAD request failed via AWS CLI",
        "x" = "{res$stderr}"
      ))
    }

    out <- jsonlite::fromJSON(res$stdout)
    return(tibble::tibble(
      size_bytes = as.integer(out$ContentLength %||% NA),
      etag = out$ETag %||% NA_character_,
      last_modified = as.character(out$LastModified %||% NA_character_)
    ))
  }

  if (!is.null(backend$request_payer) && nzchar(backend$request_payer)) {
    cli::cli_abort(c(
      "AWS CLI required for requester-pays buckets",
      "i" = "Install AWS CLI and configure credentials (or use {.code hcpx_auth()}).",
      "i" = "Bucket: {.val {backend$bucket}}"
    ))
  }

  url <- s3_url(backend, remote_path)

  curl_h <- curl::new_handle()
  curl::handle_setopt(curl_h,
    nobody = TRUE,
    followlocation = TRUE
  )

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

#' Generate presigned URL via AWS backend
#'
#' @rdname backend_presign
#' @export
#' @method backend_presign hcpx_backend_aws
backend_presign.hcpx_backend_aws <- function(backend, remote_path, expires_sec = .DEFAULT_PRESIGN_EXPIRY_SEC) {
  remote_path <- sub("^/", "", remote_path)
  aws <- .aws_cli_path()

  if (nzchar(aws)) {
    s3_uri <- sprintf("s3://%s/%s", backend$bucket, remote_path)
    res <- .aws_run(
      backend,
      c(
        .aws_cli_global_args(backend),
        "s3",
        "presign",
        s3_uri,
        "--expires-in",
        as.integer(expires_sec)
      )
    )

    url <- trimws(res$stdout)
    if (identical(res$status, 0L) && nzchar(url)) {
      return(url)
    }
  }

  # Fallback: direct HTTPS URL (works only for public buckets)
  s3_url(backend, remote_path)
}

#' Download file via AWS backend
#'
#' @rdname backend_download
#' @export
#' @method backend_download hcpx_backend_aws
backend_download.hcpx_backend_aws <- function(backend, url_or_path, dest, resume = TRUE, ...) {
  # Allow direct download of URLs (e.g., pre-signed links)
  if (grepl("^https?://", url_or_path, ignore.case = TRUE)) {
    ok <- download_url(url_or_path, dest, resume = resume)
    if (!isTRUE(ok) || !file.exists(dest)) {
      cli::cli_abort("Download failed for URL: {url_or_path}")
    }
    return(tibble::tibble(
      status = "success",
      bytes = as.integer(file.info(dest)$size)
    ))
  }

  remote_path <- sub("^/", "", url_or_path)

  dest_dir <- dirname(dest)
  if (!dir.exists(dest_dir)) dir.create(dest_dir, recursive = TRUE)

  aws <- .aws_cli_path()
  if (!nzchar(aws)) {
    if (!is.null(backend$request_payer) && nzchar(backend$request_payer)) {
      cli::cli_abort(c(
        "AWS CLI required for requester-pays buckets",
        "i" = "Install AWS CLI and configure credentials (or use {.code hcpx_auth()}).",
        "i" = "Bucket: {.val {backend$bucket}}"
      ))
    }
    # Fallback: unsigned HTTPS for public buckets
    ok <- download_url(s3_url(backend, remote_path), dest, resume = resume)
    if (!isTRUE(ok) || !file.exists(dest)) {
      cli::cli_abort("Download failed for path: {remote_path}")
    }
    return(tibble::tibble(
      status = "success",
      bytes = as.integer(file.info(dest)$size)
    ))
  }

  meta <- NULL
  if (resume && file.exists(dest)) {
    meta <- tryCatch(backend_head(backend, remote_path), error = function(e) NULL)
    if (!is.null(meta) && nrow(meta) > 0) {
      remote_size <- meta$size_bytes[[1]]
      local_size <- file.info(dest)$size
      if (!is.na(remote_size) && !is.na(local_size) && local_size == remote_size) {
        return(tibble::tibble(status = "success", bytes = as.integer(local_size)))
      }
    }
  }

  # Support partial resume via range requests when we can determine the remote size.
  if (resume && file.exists(dest) && !is.null(meta) && nrow(meta) > 0) {
    remote_size <- meta$size_bytes[[1]]
    local_size <- file.info(dest)$size
    if (!is.na(remote_size) && !is.na(local_size) && local_size > 0 && local_size < remote_size) {
      tmp <- tempfile(pattern = paste0(basename(dest), "_part_"), tmpdir = dest_dir)
      on.exit(unlink(tmp), add = TRUE)

      res <- .aws_run(
        backend,
        c(
          .aws_cli_global_args(backend),
          "s3api",
          "get-object",
          "--bucket",
          backend$bucket,
          "--key",
          remote_path,
          .aws_request_payer_args(backend),
          "--range",
          sprintf("bytes=%d-", as.integer(local_size)),
          tmp
        )
      )

      if (!identical(res$status, 0L)) {
        cli::cli_abort(c(
          "Download resume failed via AWS CLI",
          "x" = "{res$stderr}"
        ))
      }

      ok <- file.append(dest, tmp)
      if (!isTRUE(ok)) {
        cli::cli_abort("Failed to append resumed download chunk to destination file")
      }

      final_size <- file.info(dest)$size
      if (!is.na(final_size) && !is.na(remote_size) && final_size != remote_size) {
        cli::cli_abort(c(
          "Resumed download size mismatch",
          "i" = "Expected {remote_size} bytes, got {final_size} bytes"
        ))
      }

      return(tibble::tibble(status = "success", bytes = as.integer(final_size)))
    }
  }

  # Full download to temp file then move into place.
  tmp <- tempfile(pattern = paste0(basename(dest), "_"), tmpdir = dest_dir)
  on.exit(unlink(tmp), add = TRUE)

  res <- .aws_run(
    backend,
    c(
      .aws_cli_global_args(backend),
      "s3api",
      "get-object",
      "--bucket",
      backend$bucket,
      "--key",
      remote_path,
      .aws_request_payer_args(backend),
      tmp
    )
  )

  if (!identical(res$status, 0L)) {
    cli::cli_abort(c(
      "Download failed via AWS CLI",
      "x" = "{res$stderr}"
    ))
  }

  if (file.exists(dest)) unlink(dest)
  ok <- file.rename(tmp, dest)
  if (!isTRUE(ok)) {
    file.copy(tmp, dest, overwrite = TRUE)
  }

  tibble::tibble(status = "success", bytes = as.integer(file.info(dest)$size))
}
