
load_file = function(obj, f, site, ds) {
  if (ds == 'calibrations') {
    tbl_name = 'manual_calibrations'
    ## fix the data_source for calibration files
    if (site == 'WFMS') {
      ds = 'campbell'
    } else if (site == 'PSP') {
      ds = 'envidas'
    }
  } else {
    tbl_name = 'measurements'
  }
  for (f_i in f) {
    df = read.csv(f_i)
    df$measurement_type_id =
      get_measurement_type_id(obj$con, site, ds,
                              df$measurement_name)
    df$measurement_name = NULL
    ncols = ncol(df)
    ## put the measurement type ID first
    df = df[, c(ncols, 1:(ncols - 1))]
    DBI::dbWriteTable(obj$con, tbl_name, df,
                      row.names = FALSE, append = TRUE)
  }
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
