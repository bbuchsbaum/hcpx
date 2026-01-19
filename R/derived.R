# Derived products: recipe registration, derive(), and materialize
#
# The recipe system allows users to define custom data transformations
# that operate on HCP assets or task runs. Recipes are registered with
# a compute function that takes asset paths as input and produces
# derived outputs (e.g., parcellated time series, connectivity matrices).

# Package-level recipe registry (populated at package load time)
.recipe_registry <- new.env(parent = emptyenv())

#' Register a derivation recipe
#'
#' Adds a recipe to the registry that defines how to compute derived
#' products from HCP assets or task runs.
#'
#' @param name Recipe name (must be unique)
#' @param description Human-readable description
#' @param input One of "assets" (single asset) or "tasks" (all assets for a task)
#' @param compute Compute function that takes (paths, ...) and returns result
#' @param requires_bundle Bundle required for tasks-based recipes (optional)
#' @param output_ext Output file extension (default "rds")
#' @return Invisible NULL
#' @seealso [derive()], [recipes()], [unregister_recipe()], [materialize()]
#' @export
#' @examples
#' \dontrun{
#' # Register a simple parcellation recipe
#' register_recipe(
#'   name = "parcellate_schaefer400",
#'   description = "Parcellate CIFTI with Schaefer 400 atlas",
#'   input = "assets",
#'   compute = function(paths, atlas_path) {
#'     # Your parcellation code here
#'     cifti <- read_cifti(paths[1])
#'     parcellate(cifti, atlas_path)
#'   },
#'   output_ext = "rds"
#' )
#' }
register_recipe <- function(name, description, input = c("assets", "tasks"),
                            compute, requires_bundle = NULL, output_ext = "rds") {
  input <- match.arg(input)

  # Validate arguments
  if (!is.character(name) || length(name) != 1 || !nzchar(name)) {
    cli::cli_abort("Recipe name must be a non-empty string")
  }

  if (!is.character(description) || length(description) != 1) {
    cli::cli_abort("Recipe description must be a string")
  }

  if (!is.function(compute)) {
    cli::cli_abort("compute must be a function")
  }

  if (!is.null(requires_bundle) && !is.character(requires_bundle)) {
    cli::cli_abort("requires_bundle must be a character string or NULL")
  }

  # Check for existing recipe
  if (exists(name, envir = .recipe_registry)) {
    cli::cli_warn("Overwriting existing recipe: {.val {name}}")
  }

  # Create recipe definition
  recipe_def <- list(
    name = name,
    description = description,
    input = input,
    compute = compute,
    requires_bundle = requires_bundle,
    output_ext = output_ext,
    registered_at = Sys.time()
  )

  # Store in registry
  assign(name, recipe_def, envir = .recipe_registry)

  cli::cli_alert_success("Registered recipe: {.val {name}}")
  invisible(NULL)
}

#' Unregister a recipe
#'
#' Removes a recipe from the registry.
#'
#' @param name Recipe name
#' @return Invisible NULL
#' @export
#' @examples
#' \dontrun{
#' register_recipe("demo", "Example recipe", input = "assets", compute = function(...) NULL)
#' unregister_recipe("demo")
#' }
unregister_recipe <- function(name) {
  if (!exists(name, envir = .recipe_registry)) {
    cli::cli_warn("Recipe not found: {.val {name}}")
    return(invisible(NULL))
  }

  rm(list = name, envir = .recipe_registry)
  cli::cli_alert_success("Unregistered recipe: {.val {name}}")
  invisible(NULL)
}

#' Get a recipe definition
#'
#' @param name Recipe name
#' @return Recipe definition list
#' @keywords internal
get_recipe <- function(name) {
  if (!exists(name, envir = .recipe_registry)) {
    cli::cli_abort("Recipe not found: {.val {name}}")
  }
  get(name, envir = .recipe_registry)
}

#' List registered recipes
#'
#' Returns a tibble of all registered recipes with their descriptions
#' and input requirements.
#'
#' @param h hcpx handle (optional, not currently used)
#' @return Tibble with columns: name, description, input, requires_bundle, output_ext
#' @export
#' @examples
#' \dontrun{
#' # List all available recipes
#' recipes()
#'
#' # See details
#' recipes() |> print(n = Inf)
#' }
recipes <- function(h = NULL) {
  # Get all recipe names
  recipe_names <- ls(envir = .recipe_registry)

  if (length(recipe_names) == 0) {
    return(tibble::tibble(
      name = character(),
      description = character(),
      input = character(),
      requires_bundle = character(),
      output_ext = character()
    ))
  }

  # Build tibble from registry
  recipe_list <- lapply(recipe_names, function(n) {
    r <- get(n, envir = .recipe_registry)
    tibble::tibble(
      name = r$name,
      description = r$description,
      input = r$input,
      requires_bundle = r$requires_bundle %||% NA_character_,
      output_ext = r$output_ext
    )
  })

  dplyr::bind_rows(recipe_list)
}

#' Create a derivation plan
#'
#' Creates a plan to compute derived products from assets or task runs
#' using a registered recipe.
#'
#' @param x assets table or tasks table (hcpx_tbl)
#' @param recipe Recipe name
#' @param ... Additional parameters passed to the compute function
#' @return An object of class `hcpx_derivation`
#' @seealso [register_recipe()], [recipes()], [materialize()], [derived()]
#' @export
#' @examples
#' \dontrun{
#' h <- hcpx_ya()
#'
#' # Create derivation plan
#' deriv <- subjects(h, gender == "F") |>
#'   tasks("WM") |>
#'   assets(bundle = "tfmri_cifti_min") |>
#'   derive("parcellate_schaefer400", atlas_path = "/path/to/atlas")
#'
#' # Execute the derivation
#' materialize(deriv)
#' }
derive <- function(x, recipe, ...) {
  h <- get_hcpx(x)

  # Get recipe definition
  recipe_def <- get_recipe(recipe)

  # Validate input type
  kind <- attr(x, "kind")
  if (is.null(kind)) {
    cli::cli_abort("Input must be an hcpx_tbl with kind attribute")
  }

  # Check input compatibility
  if (recipe_def$input == "tasks" && kind != "tasks") {
    cli::cli_abort(c(
      "Recipe {.val {recipe}} requires tasks input",
      "i" = "Use {.code tasks()} to create a tasks table"
    ))
  }

  # If recipe requires a bundle, ensure assets are filtered
  if (!is.null(recipe_def$requires_bundle) && kind == "assets") {
    # Could add validation here
  }

  # Collect asset/task info
  if (kind == "assets") {
    input_df <- dplyr::select(x, asset_id, subject_id, task, direction, run,
                               remote_path, local_path = remote_path) |>
      dplyr::collect()
  } else if (kind == "tasks") {
    # For tasks, we need to get the associated assets
    input_df <- assets(x) |>
      dplyr::select(asset_id, subject_id, task, direction, run, remote_path) |>
      dplyr::collect()
    input_df$local_path <- input_df$remote_path
  } else {
    # For subjects, convert to assets first
    input_df <- assets(x) |>
      dplyr::select(asset_id, subject_id, task, direction, run, remote_path) |>
      dplyr::collect()
    input_df$local_path <- input_df$remote_path
  }

  # Capture additional parameters
  params <- list(...)

  # Build derivation object
  deriv <- structure(
    list(
      recipe = recipe_def,
      input_df = input_df,
      params = params,
      created_at = Sys.time(),
      n_inputs = nrow(input_df)
    ),
    class = "hcpx_derivation"
  )

  # Attach hcpx context
  attr(deriv, "hcpx") <- h

  deriv
}

#' Print a derivation plan
#'
#' @param x hcpx_derivation object
#' @param ... Additional arguments (ignored)
#' @return Invisible x
#' @rdname derive
#' @export
#' @method print hcpx_derivation
print.hcpx_derivation <- function(x, ...) {
  cli::cli_h1("hcpx Derivation Plan")

  cli::cli_alert_info("Recipe: {.val {x$recipe$name}}")
  cli::cli_text("  {x$recipe$description}")
  cli::cli_text("")

  cli::cli_alert_info("Input: {.val {x$n_inputs}} {x$recipe$input}")
  cli::cli_text("")

  if (length(x$params) > 0) {
    cli::cli_alert_info("Parameters: {.val {names(x$params)}}")
  }

  cli::cli_text("")
  cli::cli_text("Next: {.code materialize(derivation)} to execute")

  invisible(x)
}

#' Resolve asset local paths for compute function
#'
#' Looks up local cache paths for assets. Assets must be downloaded first.
#'
#' @param h hcpx handle
#' @param asset_ids Character vector of asset IDs
#' @return Tibble with asset_id and local_path
#' @keywords internal
resolve_asset_local_paths <- function(h, asset_ids) {
  h <- get_hcpx(h)

  # Look up in ledger
  cached <- ledger_lookup(h, asset_ids)

  if (nrow(cached) < length(asset_ids)) {
    missing <- setdiff(asset_ids, cached$asset_id)
    cli::cli_abort(c(
      "Some assets are not cached: {.val {head(missing, 5)}}",
      "i" = "Download assets first with {.code download(plan)}"
    ))
  }

  cached[, c("asset_id", "local_path")]
}

#' Clear all registered recipes
#'
#' Removes all recipes from the registry.
#'
#' @return Invisible NULL
#' @keywords internal
clear_recipes <- function() {
  rm(list = ls(envir = .recipe_registry), envir = .recipe_registry)
  invisible(NULL)
}

#' Query derived products
#'
#' Returns a lazy table of derived products from the derived_ledger.
#' Can be filtered by recipe name or other criteria.
#'
#' @param x hcpx handle
#' @param recipe Optional recipe name to filter by
#' @param ... Additional filter expressions
#' @return A lazy table with class `hcpx_tbl` and kind "derived"
#' @export
#' @examples
#' \dontrun{
#' h <- hcpx_ya()
#'
#' # List all derived products
#' derived(h)
#'
#' # Filter by recipe
#' derived(h, recipe = "parcellate_schaefer400")
#'
#' # Count by recipe
#' derived(h) |> count(recipe)
#' }
derived <- function(x, recipe = NULL, ...) {
  h <- get_hcpx(x)
  con <- get_con(h)

  if (!DBI::dbExistsTable(con, "derived_ledger")) {
    cli::cli_warn("Derived ledger table not found")
    # Return empty tibble with expected columns
    empty <- tibble::tibble(
      derived_id = character(),
      recipe = character(),
      source_id = character(),
      local_path = character(),
      created_at = character(),
      params_json = character()
    )
    return(hcpx_tbl(empty, h, "derived"))
  }

  # Create lazy table
  tbl <- dplyr::tbl(con, "derived_ledger")

  # Filter by recipe if specified
  if (!is.null(recipe)) {
    tbl <- dplyr::filter(tbl, recipe == !!recipe)
  }

  # Apply additional filter expressions
  dots <- rlang::enquos(...)
  if (length(dots) > 0) {
    tbl <- dplyr::filter(tbl, !!!dots)
  }

  hcpx_tbl(tbl, h, "derived")
}

#' Record a derived product in the ledger
#'
#' @param h hcpx handle
#' @param derived_id Unique ID for the derived product
#' @param recipe Recipe name
#' @param source_id Source asset/task ID
#' @param local_path Path to derived output file
#' @param params_json JSON string of parameters used
#' @return Invisible h
#' @keywords internal
derived_record <- function(h, derived_id, recipe, source_id, local_path, params_json = NULL) {
  con <- get_con(h)
  now <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")

  # Delete existing entry if present
  DBI::dbExecute(con, "DELETE FROM derived_ledger WHERE derived_id = ?",
                 params = list(derived_id))

  # Insert new entry
  DBI::dbExecute(con, "
    INSERT INTO derived_ledger
      (derived_id, recipe, source_id, local_path, created_at, params_json)
    VALUES (?, ?, ?, ?, ?, ?)
  ", params = list(
    derived_id,
    recipe,
    source_id,
    local_path,
    now,
    params_json %||% NA_character_
  ))

  invisible(h)
}

#' Generate derived product ID
#'
#' @param recipe Recipe name
#' @param source_id Source asset/task ID
#' @param params List of parameters
#' @return Character derived ID
#' @keywords internal
derived_id <- function(recipe, source_id, params = list()) {
  # Create deterministic ID from recipe, source, and params
  input <- paste0(recipe, ":", source_id, ":", jsonlite::toJSON(params, auto_unbox = TRUE))
  digest::digest(input, algo = "xxhash32")
}

#' Materialize a derivation (execute the compute function)
#'
#' @rdname materialize
#' @param x hcpx_derivation object
#' @param progress Show progress (default TRUE)
#' @param ... Additional arguments (ignored)
#' @return Tibble with derived_id, local_path, status
#' @export
materialize.hcpx_derivation <- function(x, progress = TRUE, ...) {
  h <- attr(x, "hcpx")
  if (is.null(h)) {
    cli::cli_abort("Derivation has no attached hcpx handle")
  }

  recipe <- x$recipe
  input_df <- x$input_df
  params <- x$params
  n_inputs <- x$n_inputs

  cli::cli_alert_info("Materializing {.val {recipe$name}} for {.val {n_inputs}} inputs")

  if (n_inputs == 0) {
    cli::cli_alert_info("No inputs to process")
    return(tibble::tibble(
      derived_id = character(),
      local_path = character(),
      status = character()
    ))
  }

  # Resolve local paths for inputs
  cached <- ledger_lookup(h, input_df$asset_id)

  if (nrow(cached) < nrow(input_df)) {
    missing_ids <- setdiff(input_df$asset_id, cached$asset_id)
    cli::cli_abort(c(
      "Some source assets are not cached",
      "i" = "Download them first with {.code download(plan_download(assets))}"
    ))
  }

  # Merge local paths
  input_df <- dplyr::left_join(
    input_df,
    cached[, c("asset_id", "local_path")],
    by = "asset_id",
    suffix = c("", "_cached")
  )
  input_df$local_path <- input_df$local_path_cached

  # Prepare output directory
  derived_dir <- file.path(h$cache$root, "derived", recipe$name)
  if (!dir.exists(derived_dir)) {
    dir.create(derived_dir, recursive = TRUE)
  }

  # Process each input
  results <- vector("list", n_inputs)

  if (progress) {
    cli::cli_progress_bar("Computing", total = n_inputs)
  }

  for (i in seq_len(n_inputs)) {
    row <- input_df[i, ]

    # Generate derived ID
    d_id <- derived_id(recipe$name, row$asset_id, params)

    # Output path
    output_path <- file.path(derived_dir, paste0(d_id, ".", recipe$output_ext))

    # Check if already computed
    if (file.exists(output_path)) {
      results[[i]] <- tibble::tibble(
        derived_id = d_id,
        local_path = output_path,
        status = "cached"
      )
      if (progress) cli::cli_progress_update()
      next
    }

    # Execute compute function
    status <- tryCatch({
      result <- do.call(recipe$compute, c(list(paths = row$local_path), params))

      # Save result
      if (recipe$output_ext == "rds") {
        saveRDS(result, output_path)
      } else {
        # For other formats, assume compute function handles saving
        # or we just save as RDS anyway
        saveRDS(result, output_path)
      }

      # Record in ledger
      derived_record(h, d_id, recipe$name, row$asset_id, output_path,
                     jsonlite::toJSON(params, auto_unbox = TRUE))

      "computed"
    }, error = function(e) {
      cli::cli_alert_danger("Failed for {row$asset_id}: {e$message}")
      "failed"
    })

    results[[i]] <- tibble::tibble(
      derived_id = d_id,
      local_path = output_path,
      status = status
    )

    if (progress) cli::cli_progress_update()
  }

  if (progress) cli::cli_progress_done()

  # Combine results
  result_df <- dplyr::bind_rows(results)

  # Summary
  n_computed <- sum(result_df$status == "computed")
  n_cached <- sum(result_df$status == "cached")
  n_failed <- sum(result_df$status == "failed")

  cli::cli_text("")
  if (n_computed > 0) cli::cli_alert_success("Computed: {.val {n_computed}}")
  if (n_cached > 0) cli::cli_alert_info("Already cached: {.val {n_cached}}")
  if (n_failed > 0) cli::cli_alert_danger("Failed: {.val {n_failed}}")

  result_df
}
