
load_measurements = function(obj, f, ...) {
  smart_upload(obj, f, 'measurements', ...)
}

load_calibrations = function(obj, f, ...) {
  smart_upload(obj, f, 'manual_calibrations', ...)
}

load_file = function(obj, f, ds, ...) {
  if (ds == 'calibrations') {
    load_calibrations(obj, f, ...)
  } else {
    load_measurements(obj, f, ...)
  }
}

#' @import etl
#' @inheritParams etl::etl_load
#' @export
etl_load.etl_nysatmoschem = function(obj, sites = NULL, years = NULL, ...) {
  ## if no site is specified, get list of sites from the raw data
  ## files
  if (is.null(sites)) {
    sites = list.files(attr(obj, 'raw_dir'))
  }
  
  for (site in sites) {
    data_sources = list.files(file.path(attr(obj, 'load_dir'), site))
    for (ds in data_sources) {
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
      load_file(obj, f_paths, ds, header = FALSE,
                row.names = FALSE)
    }
  }
  
  invisible(obj)
}
