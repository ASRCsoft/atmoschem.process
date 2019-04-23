## useful functions

is_psp_42C_cal = function(f)
  startsWith(basename(f), 'Pinnacle_42C')
is_psp_API300EU_cal = function(f)
  startsWith(basename(f), 'Pinnacle_API300EU_CO_Weekly')
is_psp_ASRC_TEI42i_Y_NOy_146i_cal = function(f)
  startsWith(basename(f), 'Pinnacle_ASRC_TEI42i_Y_NOy_146i_Weekly')
is_psp_DEC_TEI42i_NOy_cal = function(f)
  startsWith(basename(f), 'Pinnacle DEC TEI42i NOy Weekly')

extract_psp_calibrations_year = function(f) {
  if (is_psp_42C_cal(f)) {
    paste0('20', substr(gsub('^.* ', '', f), 1, 2))
  } else if (is_psp_API300EU_cal(f)) {
    gsub('^.*Weekly_|_[^_].*$', '', f)
  } else if (is_psp_ASRC_TEI42i_Y_NOy_146i_cal(f)) {
    gsub('^.*Weekly_|_[^_].*$', '', f)
  } else if (is_psp_DEC_TEI42i_NOy_cal(f)) {
    gsub('^.* |_.*$', '', f)
  } else {
    warning(paste('extract_year not implemented for PSP calibration', f))
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

add_new_measurement_types = function(pg, site, data_source,
                                     measurements) {
  ## since we only run this function for one site and data source at a
  ## time, no need to have a site or data source for each row in the
  ## data frame
  uniq_measurements = unique(measurements)
  m_ids = get_measurement_type_id(pg, site, data_source,
                                  uniq_measurements,
                                  add_new = FALSE)
  if (sum(is.na(m_ids)) > 0) {
    ## insert new measurement types
    new_mtypes = data.frame(site_id = get_site_id(pg, site),
                            data_source = data_source,
                            measurement = uniq_measurements[is.na(m_ids)])
    DBI::dbWriteTable(pg, 'measurement_types', new_mtypes,
                      row.names = FALSE, append = TRUE)
  }
}

get_measurement_type_id = function(pg, site,
                                   data_source,
                                   measurement,
                                   add_new = TRUE) {
  if (length(measurement) == 0) return(integer(0))
  if (add_new) {
    ## make sure we aren't asking for ID's that don't exist yet
    add_new_measurement_types(pg, site, data_source,
                              measurement)
  }
  site_id = get_site_id(pg, site)
  sql_txt = 'select * from measurement_types'
  measurement_types = DBI::dbGetQuery(pg, sql_txt)
  df = data.frame(site_id = site_id,
                  data_source = data_source,
                  measurement = measurement,
                  order = 1:length(measurement))
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
