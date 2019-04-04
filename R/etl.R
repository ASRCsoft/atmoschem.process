#' My ETL functions
#' @import etl
#' @inheritParams etl::etl_extract
#' @export
#' @examples
#' \dontrun{
#' if (require(dplyr)) {
#'   obj <- etl("nysatmoschem") %>%
#'     etl_create()
#' }
#' }

etl_extract.etl_nysatmoschem <- function(obj, ...) {
  stop('etl_extract is not yet implemented')
  # Specify the URLs that you want to download
  src <- c("http://www.stat.tamu.edu/~sheather/book/docs/datasets/HoustonChronicle.csv")

  # Use the smart_download() function for convenience
  etl::smart_download(obj, src, ...)

  # Always return obj invisibly to ensure pipeability!
  invisible(obj)
}

#' @export
etl_init.etl_nysatmoschem = function(obj, script = NULL, schema_name = "init",
                                     pkg = attr(obj, "pkg"),
                                     ext = NULL, ...) {
  ## make sure the database connection is good
  obj = etl:::verify_con(obj)
  if (!methods::is(obj$con, "DBIConnection")) {
    stop("Invalid connection to database.")
  }
  pg = obj$con
  
  ## add to postgres' libdir so it can find compiled code
  update_dynamic_library_path(pg)

  ## set up tables and functions
  sql_files = c('utilities', 'setup', 'filtering',
                'calibration', 'flags', 'processing')
  for (sql_file in sql_files) {
    sql_file = etl::find_schema(obj, sql_file, ext = 'sql')
    run_sql_script(pg, sql_file)
  }

  ## add metadata
  measurement_types = read_meta_csv('measurement_types')
  update_measurement_types(pg, measurement_types)
  autocals = read_meta_csv('autocals')
  update_autocals(pg, autocals)
  manual_flags = read_meta_csv('manual_flags')
  update_manual_flags(pg, manual_flags)
  
  invisible(obj)
}


## #' @rdname etl_create
## #' @method etl_transform etl_nysatmoschem
## #' @export

## etl_transform.etl_nysatmoschem = function(obj, sites, years, ...) {
##   ## get the Cartesian product of sites and years, then all
##   ## corresponding files
##   site_years = expand.grid(sites, years)

##   ## transform site_year function?

##   ## for row in site_years
##   ##     1) get associated files
##   ##     2) transform files

  
##   # load the data and process it if necessary
##   src = list.files(attr(obj, "raw_dir"), "\\.csv", full.names = TRUE)
##   lcl = file.path(attr(obj, "load_dir"), basename(src))
##   file.copy(from = src, to = lcl)
##   invisible(obj)
## }
