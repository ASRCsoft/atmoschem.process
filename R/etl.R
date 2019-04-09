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
