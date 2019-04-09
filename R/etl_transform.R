

## transform a raw data file according to its site and data source
transform_file = function(pg, f, site, ds) {
  if (ds == 'campbell') {
    transform_campbell(pg, f)
  }
}

#' @import etl
#' @inheritParams etl::etl_transform
#' @export
etl_transform.etl_nysatmoschem = function(obj, sites, years, ...) {
  ## if no site is specified, get list of sites from the raw data
  ## files
  if (is.null(sites)) {
    sites = list.files(attr(obj, 'raw_dir'))
  }
  
  for (site in sites) {
    data_sources = list.files(file.path(attr(obj, 'raw_dir'), site))
    for (ds in data_sources) {
      files = list.files(file.path(attr(obj, 'raw_dir'), site, ds))
      for (year in years) {
        try_result = try(file_years <- extract_year(files, site, ds))
        if (class(try_result) == 'try-error') {
          ## this means extract_year isn't implemented for that data
          ## source
          next()
        }
        year_files = files[file_years == year]
        for (f in year_files) {
          message(paste('transforming', f, '...'))
          f_path = file.path(attr(obj, 'raw_dir'), site, ds, f)
          df = transform_file(obj$con, f_path, site, ds)
          out_path = file.path(attr(obj, 'load_dir'), site, ds)
          dir.create(out_path, showWarnings = FALSE, recursive = TRUE)
          out_file = file.path(out_path, gsub('\\..*$', '.csv', f))
          write.csv(df, file = out_file, row.names = FALSE)
        }
      }
    }
  }
  
  invisible(obj)
}
