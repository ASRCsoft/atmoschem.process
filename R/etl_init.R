## functions used to create the database

#' @import dbx

update_dynamic_library_path = function(pg) {
  ## get the current paths
  cur_paths = DBI::dbGetQuery(pg, 'show dynamic_library_path')
  
  ## add median C library location if needed
  lib_path = system.file('libs',
                         package = 'nysatmoschem')
  if (!grepl(lib_path, cur_paths, fixed = TRUE)) {
    if(.Platform$OS.type == "unix") {
      path_sep = ':'
    } else {
      path_sep = ';'
    }
    new_paths = paste(cur_paths, lib_path, sep = path_sep)
    sql_txt = paste0("set dynamic_library_path to '",
                     new_paths, "'")
    DBI::dbExecute(pg, sql_txt)
  }
}

## this works better than dbRunScript in the default method
run_sql_script = function(pg, f) {
  sql_txt = paste(readLines(f), collapse = "\n")
  suppressWarnings(DBI::dbExecute(pg, sql_txt))
}

read_meta_csv = function(name) {
  metadata_path = system.file('extdata', package = 'nysatmoschem')
  meta_csv = file.path(metadata_path,
                       paste(name, 'csv', sep = '.'))
  read.csv(meta_csv, na.strings=c('', 'NA'))
}

update_metadata_tbl = function(pg, tbl_name, df, action = 'replace') {
  if (action == 'replace') {
    df$measurement_type_id =
      get_measurement_type_id(pg, df$site,
                              df$data_source,
                              df$measurement)
    df$site = NULL
    df$data_source = NULL
    df$measurement = NULL
    ## purge old data then add new data
    dbxDelete(pg, tbl_name)
    dbxInsert(pg, tbl_name, df)
  } else if (action == 'update') {
    df$site_id = get_site_id(pg, df$site)
    df$site = NULL
    ## don't delete any rows, but erase values of unused rows
    pg_meta = dbxSelect(pg, 'select site_id, data_source, measurement from measurement_types')
    merged_meta = merge(pg_meta, df, all = T)
    idx_cols = c('site_id', 'data_source', 'measurement')
    dbxUpsert(pg, tbl_name, merged_meta,
              where_cols = idx_cols)
  }
}

update_measurement_types = function(pg, df) {
  update_metadata_tbl(pg, 'measurement_types', df,
                      action = 'update')
}

update_autocals = function(pg, df) {
  update_metadata_tbl(pg, 'autocals', df)
}

update_manual_flags = function(pg, df) {
  update_metadata_tbl(pg, 'manual_flags', df)
}

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
