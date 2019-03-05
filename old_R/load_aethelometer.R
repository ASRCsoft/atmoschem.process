## load aethelometer data

library(RPostgreSQL)

## reading a WFMS aethelometer file that starts with 'ae'
read_wfms_aethelometer = function(f) {
  df = read.csv(f, header = F, skip = 1)
  df$instrument_time = strptime(paste(df$V1, df$V2),
                                '%d-%B-%y %H:%M')
  df$source_row = 1:nrow(df) + 1
  ae_col_names = c('concentration', 'sz', 'sb', 'rz', 'rb',
                   'fraction', 'attenuation')
  df1 = df[, c('source_row', 'instrument_time',
               paste0('V', c(3, 6:11)))]
  df2 = df[, c('source_row', 'instrument_time',
               paste0('V', c(4, 12:17)))]
  names(df1)[-(1:2)] = ae_col_names
  names(df2)[-(1:2)] = ae_col_names
  df1$channel = 880
  df2$channel = 370
  rbind(df1, df2)
}

import_aethelometer_file = function(f) {
  path_folders = strsplit(f, '/')[[1]]
  station = path_folders[length(path_folders) - 1]
  if (station == 'WFMS') {
    aedf = read_wfms_aethelometer(f)
    aedf$station_id = 1
    date_str = gsub('^.*ae|\\..*$', '', f)
  } else {
    ## ?
  }
  aedf$source = paste0('(', as.Date(date_str, format='%y%m%d'),
                       ',1,', aedf$source_row, ')')
  aedf$source_row = NULL
  pg = dbConnect(PostgreSQL(), dbname = dbname)
  dbWriteTable(pg, 'wfms_aethelometer', aedf,
               row.names = F, append = T)
  dbDisconnect(pg)
}

dbname = commandArgs(trailingOnly = T)[1]
env_files = commandArgs(trailingOnly = T)[-1]
file_dates = gsub('^.*ae|\\..*$', '', env_files)
env_files = env_files[order(file_dates)]
for (f in env_files) {
  message(paste('Importing', f))
  import_aethelometer_file(f)
}
