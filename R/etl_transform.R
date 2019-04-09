#' @rdname etl_create
#' @method etl_transform etl_nysatmoschem
#' @export
etl_transform.etl_nysatmoschem = function(obj, sites, years, ...) {
  ## get the Cartesian product of sites and years, then all
  ## corresponding files
  site_years = expand.grid(sites, years)

  ## transform site_year function?

  ## for row in site_years
  ##     1) get associated files
  ##     2) transform files

  
  # load the data and process it if necessary
  src = list.files(attr(obj, "raw_dir"), "\\.csv", full.names = TRUE)
  lcl = file.path(attr(obj, "load_dir"), basename(src))
  file.copy(from = src, to = lcl)
  invisible(obj)
}
