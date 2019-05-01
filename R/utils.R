## useful functions

is_psp_42C_cal = function(f)
  startsWith(basename(f), 'Pinnacle_42C')
is_psp_API300EU_cal = function(f)
  startsWith(basename(f), 'Pinnacle_API300EU_CO_Weekly')
is_psp_ASRC_TEI42i_Y_NOy_cal = function(f)
  startsWith(basename(f), 'Pinnacle_ASRC_TEI42i_Y_NOy_146i_Weekly') |
    startsWith(basename(f), 'Pinnacle_ASRC_TEI42i_Y_NOy_T700_Weekly')
is_psp_DEC_TEI42i_NOy_cal = function(f)
  startsWith(basename(f), 'Pinnacle DEC TEI42i NOy Weekly') |
    startsWith(basename(f), 'Pinnacle_DEC_TEI42i_NOy_Weekly')
is_psp_TEI43i_SO2_cal = function(f)
  startsWith(basename(f), 'Pinnacle_TEI43i_SO2_Weekly') |
    startsWith(basename(f), 'Pinnacle_TEI43i_SO2_146i_Weekly')
is_psp_TEI49i_O3_49i_cal = function(f)
  startsWith(basename(f), 'Pinnacle_TEI49i_O3_49i_Weekly')

extract_psp_calibrations_year = function(f) {
  if (is_psp_42C_cal(f)) {
    paste0('20', substr(gsub('^.*[ _]', '', f), 1, 2))
  } else if (is_psp_API300EU_cal(f)) {
    gsub('^.*Weekly_|_[^_]*$', '', f)
  } else if (is_psp_ASRC_TEI42i_Y_NOy_cal(f)) {
    substr(gsub('^.*Weekly_', '', f), 1, 4)
  } else if (is_psp_DEC_TEI42i_NOy_cal(f)) {
    gsub('^.*Weekly[ _]|_[^_]*$', '', f)
  } else if (is_psp_TEI43i_SO2_cal(f)) {
    gsub('^.*Weekly_|_[^_]*$', '', f)
  } else if (is_psp_TEI49i_O3_49i_cal(f)) {
    gsub('^.*Weekly_|_[^_]*$', '', f)
  } else {
    warning(paste('extract_year not implemented for PSP calibration', f))
    NA
  }
}

extract_wfms_aethelometer_year = function(f) {
  fbase = basename(f)
  if (startsWith(fbase, 'ae')) {
    paste0('20', substr(fbase, 3, 4))
  } else if (startsWith(fbase, 'BC')) {
    paste0('20', substr(fbase, 7, 8))
  } else {
    warning(paste('extract_year not implemented for WFMS aethelometer file', f))
    NA
  }
}

## get the file year given the filename, site, and data source
extract_year = function(f, site, ds) {
  year_str = if (ds == 'ultrafine') {
               paste0('20', substr(basename(f), 1, 2))
             } else if (site == 'WFMS' && ds == 'campbell') {
               substr(gsub('^.*Table1_', '', f), 1, 4)
             } else if (site == 'WFMS' && ds == 'calibrations') {
               paste0('20', substr(gsub('^.*_', '', f), 1, 2))
             } else if (site == 'PSP' && ds == 'envidas') {
               paste0('20', substr(gsub('^.*-', '', f), 1, 2))
             } else if (site == 'PSP' && ds == 'calibrations') {
               sapply(f, extract_psp_calibrations_year)
             } else if (site == 'WFMS' && ds == 'aethelometer') {
               sapply(f, extract_wfms_aethelometer_year)
             } else {
               stop(paste('extract_year not implemented for',
                          site, 'and', ds))
             }
  as.integer(year_str)
}

get_site_id = function(pg, x) {
  sites = DBI::dbGetQuery(pg, 'select * from sites')
  sites$id[match(x, sites$short_name)]
}

add_new_data_sources = function(pg, site, data_source) {
  df_in = data.frame(site = site,
                     name = data_source)
  uniq_df = unique(df_in)
  ds_ids = get_data_source_id(pg, uniq_df$site, uniq_df$name,
                              add_new = FALSE)
  if (sum(is.na(ds_ids)) > 0) {
    ## insert new measurement types
    site_id = get_site_id(pg, uniq_df$site[is.na(ds_ids)])
    new_dss = data.frame(site_id = site_id,
                         name = uniq_df$name[is.na(ds_ids)])
    DBI::dbWriteTable(pg, 'data_sources', new_dss,
                      row.names = FALSE, append = TRUE)
  }
}

get_data_source_id = function(pg, site, data_source,
                              add_new = TRUE) {
  if (add_new) {
    ## make sure we aren't asking for ID's that don't exist yet
    add_new_data_sources(pg, site, data_source)
  }
  site_ids = get_site_id(pg, site)
  df_in = data.frame(site_id = site_ids,
                     name = data_source,
                     order = 1:length(data_source))
  data_sources = DBI::dbGetQuery(pg, 'select * from data_sources')
  df2 = merge(df_in, data_sources, all.x = TRUE)
  ## df2 is sorted, have to unsort it
  res = df2$id[order(df2$order)]
  if (is.null(res)) return(NA)
  res
}

add_new_file = function(pg, site, data_source, f,
                        calibration) {
  data_source_id = get_data_source_id(pg, site, data_source)
  file_df = data.frame(data_source_id = data_source_id,
                       name = basename(f),
                       calibration = calibration)
  DBI::dbWriteTable(pg, 'files', file_df,
                    row.names = FALSE, append = TRUE)
}

get_file_id = function(pg, site, data_source, f,
                       calibration) {
  ds_id = get_data_source_id(pg, site, data_source)
  df_in = data.frame(data_source_id = ds_id,
                     name = f,
                     calibration = calibration,
                     order = 1:length(f))
  files = DBI::dbGetQuery(pg, 'select * from files')
  df2 = merge(df_in, files, all.x = TRUE)
  ## df2 is sorted, have to unsort it
  res = df2$id[order(df2$order)]
  if (is.null(res)) return(NA)
  res
}

add_new_measurement_types = function(pg, site, data_source,
                                     names) {
  ## since we only run this function for one data source at a time, no
  ## need to have a data source for each row in the data frame
  uniq_names = unique(names)
  m_ids = get_measurement_type_id(pg, site, data_source,
                                  uniq_names,
                                  add_new = FALSE)
  if (sum(is.na(m_ids)) > 0) {
    ## insert new measurement types
    data_source_id = get_data_source_id(pg, site, data_source)
    new_mtypes = data.frame(data_source_id = data_source_id,
                            name = uniq_names[is.na(m_ids)])
    DBI::dbWriteTable(pg, 'measurement_types', new_mtypes,
                      row.names = FALSE, append = TRUE)
  }
}

get_measurement_type_id = function(pg, site,
                                   data_source,
                                   name,
                                   add_new = TRUE) {
  if (length(name) == 0) return(integer(0))
  if (add_new) {
    ## make sure we aren't asking for ID's that don't exist yet
    add_new_measurement_types(pg, site, data_source,
                              name)
  }
  data_source_id = get_data_source_id(pg, site, data_source)
  sql_txt = 'select * from measurement_types'
  measurement_types = DBI::dbGetQuery(pg, sql_txt)
  df = data.frame(data_source_id = data_source_id,
                  name = name,
                  order = 1:length(name))
  df2 = merge(df, measurement_types, all.x = TRUE)
  ## df2 is sorted, have to unsort it
  df2$id[order(df2$order)]
}

#' @import shiny
#' @export
view_processing = function(...) {
  shinyOptions(pg = dplyr::src_postgres(...))
  runApp(system.file('processing_viewer', package = 'nysatmoschem'))
}
