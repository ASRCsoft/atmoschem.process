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
  metadata_path = system.file('extdata', package = 'nysatmoschem')
  measurement_types_csv =
    file.path(metadata_path, 'measurement_types.csv')
  update_measurement_types(pg, measurement_types_csv)
  autocals_csv = file.path(metadata_path, 'autocals.csv')
  update_autocals(pg, autocals_csv)
  manual_flags_csv = file.path(metadata_path, 'manual_flags.csv')
  update_manual_flags(pg, manual_flags_csv)
  
  invisible(obj)
}
