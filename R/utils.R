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
