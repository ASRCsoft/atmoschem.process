## useful functions

## get the file year given the filename, site, and data source
extract_year = function(f, site, ds) {
  year_str = if (site == 'WFMS' && ds == 'campbell') {
               substr(gsub('^.*Table1_', '', f), 1, 4)
             } else if (site == 'WFMS' && ds == 'calibrations') {
               paste0('20', substr(gsub('^.*_', '', f), 1, 2))
             } else {
               stop(paste('extract_year not implemented for',
                          site, 'and', ds))
             }
  as.integer(year_str)
}

get_site_id = function(pg, x) {
  sites = dbGetQuery(pg, 'select * from sites')
  sites$id[match(x, sites$short_name)]
}

get_measurement_type_id = function(pg, site,
                                   data_source,
                                   measurement) {
  site_id = get_site_id(pg, site)
  sql_txt = 'select * from measurement_types'
  measurement_types = dbxSelect(pg, sql_txt)
  df = data.frame(site_id = site_id,
                  data_source = data_source,
                  measurement = measurement,
                  order = 1:length(measurement))
  df2 = merge(df, measurement_types, all = TRUE)
  ## df2 is sorted, have to unsort it
  df2$id[order(df2$order)]
}
