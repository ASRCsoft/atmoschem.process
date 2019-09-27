
## only load the data if the file hasn't been loaded already
even_smarter_upload = function(obj, f, site, measurement,
                               ds, clobber = FALSE) {
  for (i in 1:length(f)) {
    f_i = f[i]
    site_i = site[i]
    is_calibration = !measurement[i]
    if (is_calibration) {
      if (site_i %in% c('WFMS', 'WFML')) {
        ds_i = 'campbell'
      } else if (site_i == 'PSP') {
        ds_i = 'envidas'
      }
    } else {
      ds_i = ds[i]
    }
    file_id = get_file_id(obj$con, site_i, ds_i,
                          f_i, is_calibration)
    if (!is.na(file_id)) {
      if (clobber) {
        ## remove the existing file data -- removing the file will
        ## remove all its associated data thanks to cascade options in
        ## postgres
        rmv_file_sql = paste0('delete from files where id=', file_id)
        DBI::dbSendQuery(obj$con, rmv_file_sql)
      } else {
        next()
      }
    }
    add_new_file(obj$con, site_i, ds_i, f_i,
                 is_calibration)
    file_id = get_file_id(obj$con, site_i, ds_i,
                          f_i, is_calibration)
    df = read.csv(f_i)
    if (nrow(df) == 0) next()
    df$measurement_type_id =
      get_measurement_type_id(obj$con, site_i, ds_i,
                              df$measurement_name)
    df$measurement_name = NULL
    if (is_calibration) {
      df$file_id = file_id
      ## put the file ID and measurement type ID first
      ncols = ncol(df)
      df = df[, c((ncols - 1):ncols, 1:(ncols - 2))]
    } else {
      df$observation_id =
        get_obs_id(obj$con, file_id, df$record,
                   df$instrument_time)
      if (any(is.na(df$observation_id)))
        stop('missing observation_ids')
      df$record = NULL
      df$instrument_time = NULL
    }
    ## add measurements (or calibrations)
    if (is_calibration) {
      tbl_name = 'manual_calibrations'
    } else {
      tbl_name = 'measurements'
    }
    ## for now just print a warning for key conflicts
    tryCatch(DBI::dbWriteTable(obj$con, tbl_name, df,
                               row.names = FALSE, append = TRUE),
             error = function(e) {
               if (grepl('conflicting key value violates exclusion constraint', e)) {
                 warning(e)
               } else {
                 stop(e)
               }
             })
  }
}

#' @import etl
#' @inheritParams etl::etl_load
#' @export
etl_load.etl_nysatmoschem = function(obj, sites = NULL, data_sources = NULL,
                                     years = NULL, clobber = FALSE, ...) {
  glob_strs = make_file_globs(attr(obj, 'load_dir'),
                              sites, data_sources, years)
  f_paths = Sys.glob(glob_strs)
  if (length(f_paths) > 0) {
    ## help even_smarter_upload by getting site, file type, and data
    ## source from the file path
    sites = get_site_from_path(attr(obj, 'load_dir'), f_paths)
    is_measurement =
      get_type_from_path(attr(obj, 'load_dir'), f_paths) == 'measurements'
    dss = get_data_source_from_path(attr(obj, 'load_dir'), f_paths)
    ## start a DB transaction and drop a few constraints to make
    ## loading substantially faster
    DBI::dbBegin(obj$con)
    DBI::dbSendQuery(obj$con, 'alter table measurements drop constraint measurements_observation_id_fkey')
    DBI::dbSendQuery(obj$con, 'alter table measurements drop constraint measurements_measurement_type_id_fkey')
    even_smarter_upload(obj, f_paths, sites,
                        is_measurement, dss, clobber)
    ## recreate the constraints and end the transaction
    DBI::dbSendQuery(obj$con, 'alter table measurements add constraint measurements_observation_id_fkey foreign key (observation_id) references observations on delete cascade')
    DBI::dbSendQuery(obj$con, 'alter table measurements add constraint measurements_measurement_type_id_fkey foreign key (measurement_type_id) references measurement_types on delete cascade')
    DBI::dbCommit(obj$con)
  }
  
  invisible(obj)
}
