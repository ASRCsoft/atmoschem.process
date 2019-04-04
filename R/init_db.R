## functions used to create the database

#' @import dbx

update_dynamic_library_path = function(pg) {
  ## get the current paths
  cur_paths = dbGetQuery(pg, 'show dynamic_library_path')
  
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
    dbExecute(pg, sql_txt)
  }
}

## this works better than dbRunScript in the default method
run_sql_script = function(pg, f) {
  sql_txt = paste(readLines(f), collapse = "\n")
  suppressWarnings(dbExecute(pg, sql_txt))
}

get_site_id = function(pg, x) {
  sites = dbGetQuery(pg, 'select * from sites')
  sites$id[match(x, sites$short_name)]
}

get_measurement_type_id = function(pg, site,
                                   data_source,
                                   measurement) {
  site_id = get_site_id(pg, site)
  sql_txt = 'select * from measurement_types'
  measurement_types = dbxSelect(pg, sql_txt)
  df = data.frame(site_id = site_id,
                  data_source = data_source,
                  measurement = measurement,
                  order = 1:length(measurement))
  df2 = merge(df, measurement_types)
  ## df2 is sorted, have to unsort it
  df2$id[order(df2$order)]
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
