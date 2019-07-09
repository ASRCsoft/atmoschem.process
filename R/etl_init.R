## functions used to create the database

#' @import dbx

update_dynamic_library_path = function(pg) {
  ## get the current paths
  cur_paths = DBI::dbGetQuery(pg, 'show dynamic_library_path')
  
  ## add median C library location if needed
  lib_path = system.file('libs',
                         package = 'nysatmoschem')

  if (!dir.exists(lib_path)) {
    ## is this a unit test?
    lib_path = system.file('src',
                           package = 'nysatmoschem')
    if (!dir.exists(lib_path)) {
      stop('Could not find C library path.')
    }
  }
  if (!file.exists(file.path(lib_path, 'median.so'))) {
    stop('Could not find median file.')
  }
  if (!grepl(lib_path, cur_paths, fixed = TRUE)) {
    if(.Platform$OS.type == "unix") {
      path_sep = ':'
    } else {
      path_sep = ';'
    }
    new_paths = paste(cur_paths, lib_path, sep = path_sep)
    ## set dynamic_library_path, but only for the current session
    sql_txt = paste0("set dynamic_library_path to '",
                     new_paths, "'")
    DBI::dbExecute(pg, sql_txt)
    ## set dynamic_library_path for future sessions
    dbname = DBI::dbGetInfo(pg)$dbname
    sql_txt = paste0('alter database "', dbname,
                     "\" set dynamic_library_path to '",
                     new_paths, "'")
    DBI::dbExecute(pg, sql_txt)
  }
  lib_path
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
                              df$measurement_name)
    df$site = NULL
    df$data_source = NULL
    df$measurement_name = NULL
    ## purge old data then add new data
    dbxDelete(pg, tbl_name)
    dbxInsert(pg, tbl_name, df)
  } else if (action == 'update') {
    df$data_source_id = get_data_source_id(pg, df$site, df$data_source)
    df$site = NULL
    df$data_source = NULL
    ## don't delete any rows, but erase values of unused rows
    pg_meta = dbxSelect(pg, 'select data_source_id, name from measurement_types')
    merged_meta = merge(pg_meta, df, all = T)
    idx_cols = c('data_source_id', 'name')
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

update_clock_audits = function(pg, df) {
  df$data_source_id =
    get_data_source_id(pg, df$site,
                       df$data_source)
  df$site = NULL
  df$data_source = NULL
  ## purge old data then add new data
  dbxDelete(pg, 'clock_audits')
  dbxInsert(pg, 'clock_audits', df)
}

## Very weird issue-- this is find_schema from `etl`. But it must be
## defined here to work with devtools, which makes adjustments to
## `system.file` to run unit tests. devtools can't change the
## `system.file` called by etl, so if I use etl's find_schema, my unit
## tests break. *head explodes*
find_schema <- function(obj, schema_name = "init",
                        pkg = attr(obj, "pkg"), ext = NULL, ...) {
  if (is.null(ext)) {
    ext <- db_type(obj)
  }
  sql <- file.path("sql", paste0(schema_name, ".", ext))
  file <- system.file(sql, package = pkg, mustWork = FALSE)
  if (!file.exists(file)) {
    message("Could not find schema initialization script")
    return(NULL)
  }
  return(file)
}

#' My ETL functions
#' @import etl
#' @inheritParams etl::etl_init
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
  dynlib_path = update_dynamic_library_path(pg)

  if ('TRAVIS' %in% names(Sys.getenv())) {
    ## print postgres diagnostic info
    print(paste('dynamic_library_path:',
                DBI::dbGetQuery(pg, 'show dynamic_library_path')))
    print(paste('Found median.so:',
                file.exists(file.path(dynlib_path, 'median.so'))))
    print(paste('data_directory:',
                DBI::dbGetQuery(pg, 'show data_directory')))
    print(system2('pg_lsclusters', stdout = TRUE))
  }

  ## set up tables and functions
  sql_files = c('utilities', 'setup', 'clock_errors',
                'calibration', 'flags', 'processing',
                'derived_measurements')
  for (sql_file in sql_files) {
    ## sql_file = etl::find_schema(obj, sql_file, ext = 'sql')
    sql_file = find_schema(obj, sql_file, ext = 'sql')
    run_sql_script(pg, sql_file)
  }

  ## add metadata
  measurement_types = read_meta_csv('measurement_types')
  update_measurement_types(pg, measurement_types)
  autocals = read_meta_csv('autocals')
  update_autocals(pg, autocals)
  manual_flags = read_meta_csv('manual_flags')
  update_manual_flags(pg, manual_flags)
  clock_audits = read_meta_csv('clock_audits')
  update_clock_audits(pg, clock_audits)
  
  invisible(obj)
}
