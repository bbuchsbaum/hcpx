# Package-wide constants
#
# Centralizing magic numbers for maintainability

# Cache configuration
.DEFAULT_CACHE_MAX_GB <- 200L

# Download configuration
.DEFAULT_DOWNLOAD_WORKERS <- 8L
.DEFAULT_PRESIGN_EXPIRY_SEC <- 3600L

# File size thresholds
.LARGE_FILE_THRESHOLD_BYTES <- 2e9

# Byte conversion factors
.BYTES_PER_KB <- 1024L
.BYTES_PER_GB <- 1e9

# Asset ID generation
.ASSET_ID_LENGTH <- 12L
