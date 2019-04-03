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

update_metadata_tbl = function(pg, tbl_name, f, action = 'replace') {
  meta = read.csv(f, na.strings=c('', 'NA'))
  if (action == 'replace') {
    meta$measurement_type_id =
      get_measurement_type_id(pg, meta$site,
                              meta$data_source,
                              meta$measurement)
    meta$site = NULL
    meta$data_source = NULL
    meta$measurement = NULL
    ## purge old data then add new data
    dbxDelete(pg, tbl_name)
    dbxInsert(pg, tbl_name, meta)
  } else if (action == 'update') {
    meta$site_id = get_site_id(pg, meta$site)
    meta$site = NULL
    ## don't delete any rows, but erase values of unused rows
    pg_meta = dbxSelect(pg, 'select site_id, data_source, measurement from measurement_types')
    merged_meta = merge(pg_meta, meta, all = T)
    idx_cols = c('site_id', 'data_source', 'measurement')
    dbxUpsert(pg, tbl_name, merged_meta,
              where_cols = idx_cols)
  }
}

update_measurement_types = function(pg, f) {
  update_metadata_tbl(pg, 'measurement_types', f,
                      action = 'update')
}

update_autocals = function(pg, f) {
  update_metadata_tbl(pg, 'autocals', f)
}

update_manual_flags = function(pg, f) {
  update_metadata_tbl(pg, 'manual_flags', f)
}







## ## get the sites and corresponding IDs
## pg = dbxConnect(adapter = 'postgres', dbname = dbname)
## sites = dbxSelect(pg, 'select * from sites')
## dbxDisconnect(pg)

## get_site_id = function(x) sites$id[match(x, sites$short_name)]

## write_metadata = function(f, tbl_name) {
##   ## upsert metadata from a metadata csv file
##   meta = read.csv(f, na.strings=c('', 'NA'))
##   meta$site_id = get_site_id(meta$site)
##   meta$site = NULL
##   ## get the measurement type IDs
##   meta = merge(meta, measurement_types)
##   meta$site_id = NULL
##   meta$data_source = NULL
##   meta$measurement = NULL
##   pg = dbxConnect(adapter = 'postgres', dbname = dbname)
##   ## clear old data first
##   dbxDelete(pg, tbl_name)
##   ## now add new data
##   dbxInsert(pg, tbl_name, meta)
##   dbxDisconnect(pg)
## }

## write_measurement_types = function(f) {
##   measurements_file = file.path(f, 'measurement_types.csv')
##   idx_cols = c('data_source', 'measurement', 'site_id')
##   ## write_metadata(measurements_file,
##   ##                'measurement_types', idx_cols)
##   meta = read.csv(measurements_file, na.strings=c('', 'NA'))
##   meta$site_id = get_site_id(meta$site)
##   meta$site = NULL
##   pg = dbxConnect(adapter = 'postgres', dbname = dbname)
##   dbxUpsert(pg, 'measurement_types', meta, where_cols = idx_cols)
##   ## don't process measurements types that aren't in the metadata
##   ## 1) find measurements in postgres but not in the metadata
##   pg_meta = dbxSelect(pg, 'select * from measurement_types')
##   merged_meta = merge(pg_meta, meta, by = idx_cols, all = T)
##   pg_only = subset(merged_meta, apply_processing.x &
##                                 (is.na(apply_processing.y) | !apply_processing.y))
##   ## 2) turn off processing for those measurements
##   pg_update = cbind(pg_only[, idx_cols], apply_processing = NA)
##   dbxUpdate(pg, 'measurement_types', pg_update, idx_cols)
##   dbxDisconnect(pg)
## }

## write_autocals = function(f) {
##   autocal_file = file.path(f, 'autocals.csv')
##   write_metadata(autocal_file, 'autocals')
## }

## write_manual_flags = function(f) {
##   flags_file = file.path(f, 'manual_flags.csv')
##   write_metadata(flags_file, 'manual_flags')
## }


## f = commandArgs(trailingOnly = T)[-1]
## message('Loading measurements info...')
## write_measurement_types(f)
## ## get the new measurement types
## pg = dbxConnect(adapter = 'postgres', dbname = dbname)
## measurement_types =
##   dbxSelect(pg, 'select id as measurement_type_id, site_id, data_source, measurement from measurement_types')
## dbxDisconnect(pg)
## ## return to loading the metadata
## message('Loading autocalibration schedule...')
## write_autocals(f)
## message('Loading manual flags...')
## write_manual_flags(f)
