## useful functions

## get the file year given the filename, site, and data source
extract_year = function(f, site, ds) {
  year_str = if (site == 'WFMS' && ds == 'campbell') {
               substr(gsub('^.*Table1_', '', f), 1, 4)
             } else if (site == 'WFMS' && ds == 'calibrations') {
               paste0('20', substr(gsub('^.*_', '', f), 1, 2))
             } else if (site == 'PSP' && ds == 'envidas') {
               paste0('20', substr(gsub('^.*-', '', f), 1, 2))
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

get_measurement_type_id = function(pg, site,
                                   data_source,
                                   measurement) {
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

add_new_measurement_types = function(pg, site, data_source,
                                     measurements) {
  ## since we only run this function for one site and data source at a
  ## time, no need to have a site or data source for each row in the
  ## data frame
  uniq_measurements = unique(measurements)
  m_ids = get_measurement_type_id(pg, site, data_source,
                                  uniq_measurements)
  if (sum(is.na(m_ids)) > 0) {
    ## insert new measurement types
    new_mtypes = data.frame(site_id = get_site_id(pg, site),
                            data_source = data_source,
                            measurement = uniq_measurements[is.na(m_ids)])
    DBI::dbWriteTable(pg, 'measurement_types', new_mtypes,
                      row.names = FALSE, append = TRUE)
  }
}
