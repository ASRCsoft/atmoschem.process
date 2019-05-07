
## only load the data if the file hasn't been loaded already
even_smarter_upload = function(obj, f, site, ds,
                               tbl_name) {
  is_calibration = ds == 'calibrations'
  for (f_i in f) {
    file_id = get_file_id(obj$con, site, ds,
                          f_i, is_calibration)
    if (is.na(file_id)) {
      add_new_file(obj$con, site, ds, f_i,
                   is_calibration)
      file_id = get_file_id(obj$con, site, ds,
                            f_i, is_calibration)
    } else {
      next()
    }
    df = read.csv(f_i)
    if (nrow(df) == 0) next()
    ## for calibration files, 'mds' is the data source of the
    ## corresponding measurements
    if (is_calibration) {
      if (site == 'WFMS') {
        mds = 'campbell'
      } else if (site == 'PSP') {
        mds = 'envidas'
      }
    } else {
      mds = ds
    }
    df$measurement_type_id =
      get_measurement_type_id(obj$con, site, mds,
                              df$measurement_name)
    df$measurement_name = NULL
    if (is_calibration) {
      ## put the measurement type ID first
      ncols = ncol(df)
      df = df[, c(ncols, 1:(ncols - 1))]
    } else {
      df$instrument_time = as.POSIXct(df$instrument_time)
      df$observation_id =
        get_obs_id(obj$con, file_id, df$record,
                   df$instrument_time)
      if (any(is.na(df$observation_id)))
        stop('missing observation_ids')
      df$record = NULL
      df$instrument_time = NULL
    }
    ## add measurements (or calibrations)
    
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

load_file = function(obj, f, site, ds) {
  is_calibration = ds == 'calibrations'
  if (is_calibration) {
    tbl_name = 'manual_calibrations'
  } else {
    tbl_name = 'measurements'
  }
  even_smarter_upload(obj, f, site, ds, tbl_name)
}

#' @import etl
#' @inheritParams etl::etl_load
#' @export
etl_load.etl_nysatmoschem = function(obj, sites = NULL, data_sources = NULL, years = NULL, ...) {
  ## if no site is specified, get list of sites from the raw data
  ## files
  if (is.null(sites)) {
    sites = list.files(attr(obj, 'raw_dir'))
  }
  
  for (site in sites) {
    site_data_sources = list.files(file.path(attr(obj, 'load_dir'), site))
    if (!is.null(data_sources)) {
      site_data_sources = site_data_sources[site_data_sources %in% data_sources]
    }
    for (ds in site_data_sources) {
      files = list.files(file.path(attr(obj, 'load_dir'), site, ds))
      try_result = try(file_years <- extract_year(files, site, ds))
      if (class(try_result) == 'try-error') {
        ## this means extract_year isn't implemented for that data
        ## source
        next()
      }
      if (is.null(years)) {
        year_files = files
      } else {
        year_files = files[file_years %in% years]
      }
      f_paths = file.path(attr(obj, 'load_dir'), site,
                          ds, year_files)
      message(paste('Loading', site, '/', ds, 'files...'))
      load_file(obj, f_paths, site, ds)
    }
  }
  
  invisible(obj)
}
