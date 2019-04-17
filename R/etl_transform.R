

## transform a raw data file according to its site and data source
transform_file = function(pg, f, site, ds) {
  if (ds == 'campbell') {
    transform_campbell(pg, f)
  } else if (site == 'PSP' & ds == 'envidas') {
    transform_psp_envidas(pg, f)
  }
}

#' @import etl
#' @inheritParams etl::etl_transform
#' @export
etl_transform.etl_nysatmoschem = function(obj, sites = NULL, years = NULL, ...) {
  ## if no site is specified, get list of sites from the raw data
  ## files
  if (is.null(sites)) {
    sites = list.files(attr(obj, 'raw_dir'))
  }
  
  for (site in sites) {
    data_sources = list.files(file.path(attr(obj, 'raw_dir'), site))
    for (ds in data_sources) {
      files = list.files(file.path(attr(obj, 'raw_dir'), site, ds))
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
      for (f in year_files) {
        f_path = file.path(attr(obj, 'raw_dir'), site, ds, f)
        message(paste('Transforming ', f_path, '...'))
        df = transform_file(obj$con, f_path, site, ds)
        out_path = file.path(attr(obj, 'load_dir'), site, ds)
        dir.create(out_path, showWarnings = FALSE, recursive = TRUE)
        out_file = file.path(out_path, gsub('\\..*$', '.csv', f))
        ## write.csv(df, file = out_file, row.names = FALSE,
        ##           col.names = FALSE)
        ## use write.table to write files without header lines--
        ## dbWriteTable from RPostgreSQL can't handle header lines
        write.table(df, file = out_file, sep = ',', na = '',
                    row.names = FALSE,  col.names = FALSE)
      }
    }
  }
  
  invisible(obj)
}
