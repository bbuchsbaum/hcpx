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
#' @return hcpx_backend_aws object
#' @keywords internal
hcpx_backend_aws <- function(bucket = "hcp-openaccess", region = "us-east-1", profile = NULL) {
  structure(
    list(
      type = "aws",
      bucket = bucket,
      region = region,
      profile = profile,
      # Base URL for public S3 access
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

#' List files via AWS backend
#'
#' @rdname backend_list
#' @export
#' @method backend_list hcpx_backend_aws
backend_list.hcpx_backend_aws <- function(backend, prefix, ...) {
  # S3 list objects via REST API
  # For public buckets, we can use the list-type=2 API
  prefix <- sub("^/", "", prefix)

  list_url <- sprintf("%s?list-type=2&prefix=%s",
                      backend$base_url,
                      utils::URLencode(prefix, reserved = TRUE))

  # Fetch XML response
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

  # Parse XML response
  xml_content <- rawToChar(response$content)

  # Extract keys and sizes using regex (avoiding XML dependency)
  keys <- unlist(regmatches(xml_content,
    gregexpr("(?<=<Key>)[^<]+(?=</Key>)", xml_content, perl = TRUE)))
  sizes <- unlist(regmatches(xml_content,
    gregexpr("(?<=<Size>)[^<]+(?=</Size>)", xml_content, perl = TRUE)))

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
  url <- s3_url(backend, remote_path)

  # Use curl to make HEAD request
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

  # Parse headers
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
  # For public buckets like hcp-openaccess, we just return the direct URL
  # No actual presigning needed since the bucket is publicly accessible
  s3_url(backend, remote_path)
}

#' Download file via AWS backend
#'
#' @rdname backend_download
#' @export
#' @method backend_download hcpx_backend_aws
backend_download.hcpx_backend_aws <- function(backend, url_or_path, dest, resume = TRUE, ...) {
  # Determine if we got a URL or a path
  if (grepl("^https?://", url_or_path)) {
    url <- url_or_path
  } else {
    url <- s3_url(backend, url_or_path)
  }

  # Set up curl handle
  curl_h <- curl::new_handle()
  curl::handle_setopt(curl_h,
    followlocation = TRUE,
    failonerror = TRUE
  )

  # Resume support
  existing_size <- 0L
  if (resume && file.exists(dest)) {
    existing_size <- file.info(dest)$size
    if (!is.na(existing_size) && existing_size > 0) {
      curl::handle_setopt(curl_h, resume_from = existing_size)
    }
  }

  # Download
  tryCatch({
    curl::curl_download(url, dest, handle = curl_h, mode = "wb")
  }, error = function(e) {
    # Check if it's a "file already complete" situation
    if (grepl("416", e$message) && existing_size > 0) {
      # File is already complete
      return(invisible(NULL))
    }
    cli::cli_abort("Download failed: {e$message}")
  })
  # Get final file size
  final_size <- file.info(dest)$size

  tibble::tibble(
    status = "success",
    bytes = as.integer(final_size)
  )
}
