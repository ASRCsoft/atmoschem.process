

## transform a raw data file according to its site and data source
transform_calibration = function(f, site, ds) {
  if (site == 'PSP') {
    transform_psp_calibrations(f)
  } else if (site == 'WFMS' && ds == 'Teledyne_300EU') {
    transform_wfms_300EU(f)
  } else if (site == 'WFMS' && ds == 'Thermo_42C') {
    transform_wfms_42C(f)
  } else if (site == 'WFMS' && ds == 'Thermo_42Cs') {
    transform_wfms_42Cs(f)
  } else if (site == 'WFMS' && ds == 'Thermo_43C') {
    transform_wfms_43C(f)
  } else if (site == 'WFML' && ds == 'Thermo_42i') {
    transform_wfml_42i(f)
  } else if (site == 'WFML' && ds == 'Thermo_48C') {
    transform_wfml_48C(f)
  }
}

transform_measurement = function(f, site, ds) {
  if (ds == 'campbell') {
    return(transform_campbell(f, site))
  } else if (site == 'PSP' & ds == 'envidas') {
    return(transform_psp_envidas(f))
  }
  res = if (ds == 'ultrafine') {
    transform_ultrafine(f)
  } else if (ds == 'mesonet') {
    transform_mesonet(f)
  } else if (site == 'WFMS' && ds == 'aethelometer') {
    transform_wfms_aethelometer(f)
  } else if (site == 'WFML' && ds == 'envidas') {
    transform_wfml_envidas(f)
  }
  res %>%
    transform(time = as.POSIXct(instrument_time, tz = 'EST')) %>%
    reshape(timevar = 'measurement_name', idvar = 'time', direction = 'wide',
            drop = c('record', 'instrument_time'))
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

transform_file2 = function(f, out_file, site, measurement, ds) {
  # keep going in case of errors
  tryCatch(transform_file(f, out_file, site, measurement, ds),
           error = function(e) {
             warning(f, ' transform failed: ', e)
           })
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
  mapply(transform_file2, raw[missing], cleaned[missing],
         site[missing], measurement[missing], ds[missing])
}

get_load_path = function(obj, raw_path) {
  raw_path %>%
    tools::file_path_sans_ext() %>%
    gsub(attr(obj, 'raw_dir'), attr(obj, 'load_dir'), .) %>%
    paste0('.csv')
}

#' @import etl
#' @inheritParams etl::etl_transform
#' @export
etl_transform.etl_atmoschem.process = function(obj, sites = NULL, data_sources = NULL,
                                               years = NULL, clobber = FALSE, ...) {
  glob_strs = make_file_globs(attr(obj, 'raw_dir'),
                              sites, data_sources, years)
  f_paths = Sys.glob(glob_strs)
  if (length(f_paths) > 0) {
    out_files = get_load_path(obj, f_paths)
    out_paths = unique(dirname(out_files))
    for (out_path in out_paths) {
      dir.create(out_path, showWarnings = FALSE, recursive = TRUE)
    }
    ## help smart_transform by getting site, file type, and data source
    ## from the file path
    sites = get_site_from_path(attr(obj, 'raw_dir'), f_paths)
    is_measurement =
      get_type_from_path(attr(obj, 'raw_dir'), f_paths) == 'measurements'
    dss = get_data_source_from_path(attr(obj, 'raw_dir'), f_paths)
    smart_transform(f_paths, out_files, sites, is_measurement,
                    dss, clobber)
  }
  
  invisible(obj)
}
