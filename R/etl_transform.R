

## transform a raw data file according to its site and data source
transform_calibration = function(f, site, ds) {
  if (site == 'PSP') {
    transform_psp_calibrations(f)
  } else if (site == 'WFML' && ds == 'Thermo_42i') {
    transform_wfm_no_calibration(f, c('NO', 'NOX'))
  } else if (site == 'WFML' && ds == 'Thermo_48C') {
    transform_wfml_48C(f)
  }
}

transform_measurement = function(f, site, ds) {
  if (ds == 'campbell') {
    transform_campbell(f)
  } else if (ds == 'ultrafine') {
    transform_ultrafine(f)
  } else if (ds == 'mesonet') {
    transform_mesonet(f)
  } else if (site == 'PSP' & ds == 'envidas') {
    transform_psp_envidas(f)
  } else if (site == 'WFMS' && ds == 'aethelometer') {
    transform_wfms_aethelometer(f)
  } else if (site == 'WFML' && ds == 'envidas') {
    transform_wfml_envidas(f)
  }
}

transform_file = function(f, out_file, site, measurement, ds) {
  if (measurement) {
    df = transform_measurement(f, site, ds)
  } else {
    df = transform_calibration(f, site, ds)
  }
  if (is.null(df)) {
    message(paste('No transformation for file', f))
  } else {
    write.csv(df, file = out_file, row.names = FALSE)
  }
}

## Only transform new files, similar to `smart_download` from etl
smart_transform = function(raw, cleaned, site, measurement,
                           ds, clobber = FALSE) {
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
  mapply(transform_file, raw[missing], cleaned[missing],
         site[missing], measurement[missing], ds[missing])
}

star_if_null = function(x) {
  if (is.null(x)) {
    '*'
  } else {
    x
  }
}

get_load_path = function(obj, raw_path) {
  raw_path %>%
    tools::file_path_sans_ext() %>%
    gsub(attr(obj, 'raw_dir'), attr(obj, 'load_dir'), .) %>%
    paste0('.csv')
}

transform_glob = function(obj, glob_str, clobber) {
  f_paths = Sys.glob(glob_str)
  out_files = get_load_path(obj, f_paths)
  out_paths = unique(dirname(out_files))
  for (out_path in out_paths) {
    dir.create(out_path, showWarnings = FALSE, recursive = TRUE)
  }
  ## help smart_transform by getting site, file type, and data source
  ## from the file path
  trunc_path = gsub(paste0('^', attr(obj, 'raw_dir')), '', f_paths)
  sites = gsub('/([^/]+)/.*$', '\\1', trunc_path)
  site_regex = paste(paste0('^/', unique(sites)), collapse = '|')
  trunc_path = gsub(site_regex, '', trunc_path)
  is_measurement = grepl('^/measurements', trunc_path)
  trunc_path = gsub('^/measurements|^/calibrations', '', trunc_path)
  dss = gsub('/([^/]+)/.*$', '\\1', trunc_path)
  smart_transform(f_paths, out_files, sites, is_measurement,
                  dss, clobber)
}

#' @import etl
#' @inheritParams etl::etl_transform
#' @export
etl_transform.etl_nysatmoschem = function(obj, sites = NULL, data_sources = NULL,
                                          years = NULL, clobber = FALSE) {
  ## put together a file glob(s)
  site_str = star_if_null(sites)
  ds_str = star_if_null(data_sources)
  year_str = star_if_null(years)

  glob_df_m = expand.grid(site_str, ds_str, year_str)
  m_glob_str = file.path(attr(obj, 'raw_dir'), glob_df_m[, 1], 'measurements',
                         glob_df_m[, 2], glob_df_m[, 3], '*')
  glob_df_c = expand.grid(site_str, year_str)
  c_glob_str = file.path(attr(obj, 'raw_dir'), glob_df_c[, 1], 'calibrations',
                         '*', glob_df_c[, 2], '*')
  glob_strs = c(m_glob_str, c_glob_str)
  transform_glob(obj, glob_strs, clobber)
  
  invisible(obj)
}
