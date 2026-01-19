# Tests for hash utilities

# --- asset_id_from_path() tests ---

test_that("asset_id_from_path generates consistent IDs", {
  id1 <- asset_id_from_path("HCP_1200", "path/to/file.nii")
  id2 <- asset_id_from_path("HCP_1200", "path/to/file.nii")
  id3 <- asset_id_from_path("HCP_1200", "different/path.nii")

  expect_type(id1, "character")
  expect_identical(id1, id2)
  expect_false(identical(id1, id3))
})

test_that("asset_id_from_path produces different IDs for different releases", {
  id1 <- asset_id_from_path("HCP_1200", "path/to/file.nii")
  id2 <- asset_id_from_path("HCP_900", "path/to/file.nii")

  expect_false(identical(id1, id2))
})

test_that("asset_id_from_path handles demo asset paths", {
  # Use sample paths from demo_assets.csv
  paths <- c(
    "HCP_1200/100206/MNINonLinear/Results/tfMRI_WM_LR/tfMRI_WM_LR_Atlas.dtseries.nii",
    "HCP_1200/100206/MNINonLinear/Results/tfMRI_WM_RL/tfMRI_WM_RL_Atlas.dtseries.nii",
    "HCP_1200/100307/MNINonLinear/Results/tfMRI_GAMBLING_LR/tfMRI_GAMBLING_LR_Atlas.dtseries.nii"
  )

  ids <- vapply(paths, function(p) asset_id_from_path("HCP_1200", p), character(1))

  # All IDs should be unique
  expect_equal(length(unique(ids)), length(paths))

  # All IDs should be consistent
  ids2 <- vapply(paths, function(p) asset_id_from_path("HCP_1200", p), character(1))
  expect_identical(ids, ids2)
})

# --- hash_md5() tests ---

test_that("hash_md5 generates valid MD5 hashes", {
  skip_if_no_pkg("digest")

  # Create a temporary file with known content
  tmp <- tempfile()
  on.exit(unlink(tmp), add = TRUE)
  writeLines("test content", tmp)

  hash <- hash_md5(tmp)

  expect_type(hash, "character")
  expect_equal(nchar(hash), 32)  # MD5 is 32 hex chars
  expect_match(hash, "^[0-9a-f]{32}$")
})

test_that("hash_md5 is consistent for same content", {
  skip_if_no_pkg("digest")

  tmp1 <- tempfile()
  tmp2 <- tempfile()
  on.exit(unlink(c(tmp1, tmp2)), add = TRUE)

  writeLines("identical content", tmp1)
  writeLines("identical content", tmp2)

  expect_identical(hash_md5(tmp1), hash_md5(tmp2))
})

test_that("hash_md5 differs for different content", {
  skip_if_no_pkg("digest")

  tmp1 <- tempfile()
  tmp2 <- tempfile()
  on.exit(unlink(c(tmp1, tmp2)), add = TRUE)

  writeLines("content A", tmp1)
  writeLines("content B", tmp2)

  expect_false(identical(hash_md5(tmp1), hash_md5(tmp2)))
})

# --- verify_checksum() tests ---

test_that("verify_checksum returns TRUE for matching hash", {
  skip_if_no_pkg("digest")

  tmp <- tempfile()
  on.exit(unlink(tmp), add = TRUE)
  writeLines("checksum test", tmp)

  expected_hash <- hash_md5(tmp)
  expect_true(verify_checksum(tmp, expected_hash))
})

test_that("verify_checksum returns FALSE for mismatched hash", {
  skip_if_no_pkg("digest")

  tmp <- tempfile()
  on.exit(unlink(tmp), add = TRUE)
  writeLines("checksum test", tmp)

  wrong_hash <- "00000000000000000000000000000000"
  expect_false(verify_checksum(tmp, wrong_hash))
})

test_that("verify_checksum supports different algorithms", {
  skip_if_no_pkg("digest")

  tmp <- tempfile()
  on.exit(unlink(tmp), add = TRUE)
  writeLines("algorithm test", tmp)

  # Test SHA256
  sha256_hash <- digest::digest(file = tmp, algo = "sha256")
  expect_true(verify_checksum(tmp, sha256_hash, algo = "sha256"))
})

# --- asset_id_from_path validation tests ---

test_that("asset_id_from_path errors on NULL release", {
  expect_error(
    asset_id_from_path(NULL, "path/to/file.nii"),
    "release must be a non-empty string"
  )
})

test_that("asset_id_from_path errors on NA release", {
  expect_error(
    asset_id_from_path(NA, "path/to/file.nii"),
    "release must be a non-empty string"
  )
})
test_that("asset_id_from_path errors on empty release", {
  expect_error(
    asset_id_from_path("", "path/to/file.nii"),
    "release must be a non-empty string"
  )
})

test_that("asset_id_from_path errors on NULL remote_path", {
  expect_error(
    asset_id_from_path("HCP_1200", NULL),
    "remote_path must be a non-empty string"
  )
})

test_that("asset_id_from_path errors on NA remote_path", {
  expect_error(
    asset_id_from_path("HCP_1200", NA),
    "remote_path must be a non-empty string"
  )
})

test_that("asset_id_from_path errors on empty remote_path", {
  expect_error(
    asset_id_from_path("HCP_1200", ""),
    "remote_path must be a non-empty string"
  )
})

test_that("asset_id_from_path produces valid hash format", {
  id <- asset_id_from_path("HCP_1200", "path/to/file.nii")

  # xxhash32 produces 8 hex chars
  expect_type(id, "character")
  expect_match(id, "^[0-9a-f]+$")
})

# --- hash_md5 validation tests ---

test_that("hash_md5 errors on NULL path", {
  skip_if_no_pkg("digest")

  expect_error(
    hash_md5(NULL),
    "path must be a non-empty string"
  )
})

test_that("hash_md5 errors on NA path", {
  skip_if_no_pkg("digest")

  expect_error(
    hash_md5(NA),
    "path must be a non-empty string"
  )
})

test_that("hash_md5 errors on empty path", {
  skip_if_no_pkg("digest")

  expect_error(
    hash_md5(""),
    "path must be a non-empty string"
  )
})

test_that("hash_md5 errors on non-existent file", {
  skip_if_no_pkg("digest")

  expect_error(
    hash_md5("/nonexistent/path/file.txt"),
    "File not found"
  )
})

test_that("hash_md5 handles binary files", {
  skip_if_no_pkg("digest")

  tmp <- tempfile()
  on.exit(unlink(tmp), add = TRUE)

  # Write binary data
  writeBin(as.raw(0:255), tmp)

  hash <- hash_md5(tmp)
  expect_type(hash, "character")
  expect_equal(nchar(hash), 32)
})

# --- verify_checksum validation tests ---

test_that("verify_checksum errors on NULL path", {
  skip_if_no_pkg("digest")

  expect_error(
    verify_checksum(NULL, "abc123"),
    "path must be a non-empty string"
  )
})

test_that("verify_checksum errors on NA path", {
  skip_if_no_pkg("digest")

  expect_error(
    verify_checksum(NA, "abc123"),
    "path must be a non-empty string"
  )
})

test_that("verify_checksum errors on non-existent file", {
  skip_if_no_pkg("digest")

  expect_error(
    verify_checksum("/nonexistent/path", "abc123"),
    "File not found"
  )
})

test_that("verify_checksum errors on NULL expected", {
  skip_if_no_pkg("digest")

  tmp <- tempfile()
  on.exit(unlink(tmp), add = TRUE)
  writeLines("test", tmp)

  expect_error(
    verify_checksum(tmp, NULL),
    "expected checksum must be provided"
  )
})

test_that("verify_checksum errors on NA expected", {
  skip_if_no_pkg("digest")

  tmp <- tempfile()
  on.exit(unlink(tmp), add = TRUE)
  writeLines("test", tmp)

  expect_error(
    verify_checksum(tmp, NA),
    "expected checksum must be provided"
  )
})

test_that("verify_checksum supports SHA1 algorithm", {
  skip_if_no_pkg("digest")

  tmp <- tempfile()
  on.exit(unlink(tmp), add = TRUE)
  writeLines("sha1 test", tmp)

  sha1_hash <- digest::digest(file = tmp, algo = "sha1")
  expect_true(verify_checksum(tmp, sha1_hash, algo = "sha1"))
})

test_that("verify_checksum supports SHA512 algorithm", {
  skip_if_no_pkg("digest")

  tmp <- tempfile()
  on.exit(unlink(tmp), add = TRUE)
  writeLines("sha512 test", tmp)

  sha512_hash <- digest::digest(file = tmp, algo = "sha512")
  expect_true(verify_checksum(tmp, sha512_hash, algo = "sha512"))
})
