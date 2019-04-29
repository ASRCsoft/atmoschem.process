

## transform a raw data file according to its site and data source
transform_file = function(pg, f, out_file, site, ds) {
  df = if (ds == 'campbell') {
         transform_campbell(pg, f)
       } else if (ds == 'ultrafine') {
         transform_ultrafine(pg, f)
       } else if (site == 'PSP' & ds == 'envidas') {
         transform_psp_envidas(pg, f)
       } else if (site == 'PSP' && ds == 'calibrations') {
         transform_psp_calibrations(pg, f)
       } else if (site == 'WFMS' && ds == 'aethelometer') {
         transform_wfms_aethelometer(f)
       }
  write.csv(df, file = out_file, na = '',
            row.names = FALSE)
  ## ## use write.table to write csv files without header lines--
  ## ## dbWriteTable from RPostgreSQL can't handle header lines
  ## write.table(df, file = out_file, sep = ',', na = '',
  ##             row.names = FALSE,  col.names = FALSE)
}

## Only transform new files, similar to `smart_download` from etl
smart_transform = function(obj, raw, cleaned, site, ds, clobber = FALSE) {
  if (length(raw) != length(cleaned)) {
    stop("src and new_filenames must be of the same length")
  }
  if (!clobber) {
    missing = !file.exists(cleaned)
  } else {
    missing = cleaned == cleaned
  }
  message(paste("Transforming", sum(missing), "new files. ",
                sum(!missing), "untouched."))
  if (sum(missing) == 0) return(NULL)
  ## mapply(downloader::download, src[missing], lcl[missing], ... = ...)
  mapply(transform_file, list(obj$con), raw[missing], cleaned[missing],
         site, ds)
}

#' @import etl
#' @inheritParams etl::etl_transform
#' @export
etl_transform.etl_nysatmoschem = function(obj, sites = NULL, data_sources = NULL,
                                          years = NULL, clobber = FALSE) {
  ## if no site is specified, get list of sites from the raw data
  ## files
  if (is.null(sites)) {
    sites = list.files(attr(obj, 'raw_dir'))
  }
  
  for (site in sites) {
    site_data_sources = list.files(file.path(attr(obj, 'raw_dir'), site))
    if (!is.null(data_sources)) {
      site_data_sources = site_data_sources[site_data_sources %in% data_sources]
    }
    for (ds in site_data_sources) {
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
      message(paste('Transforming', site, '/', ds, 'files...', sep = ' '))
      f_paths = file.path(attr(obj, 'raw_dir'), site, ds, year_files)
      out_path = file.path(attr(obj, 'load_dir'), site, ds)
      dir.create(out_path, showWarnings = FALSE, recursive = TRUE)
      out_files = file.path(out_path, gsub('\\..*$', '.csv', year_files))
      smart_transform(obj, f_paths, out_files, site, ds, clobber)
    }
  }
  
  invisible(obj)
}
