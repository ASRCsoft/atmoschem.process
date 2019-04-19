hexToBinStr = function(h)
  format(R.utils::intToBin(strtoi(sprintf('%04d', h), 16L)),
         justify = 'right', width = 16)

## return TRUE for a 1 in any position except 9 and 15, which are
## 'Pulse Height Fault' and 'Service Reminder', respectively
parse_ultrafine_flag = function(f) !grepl('^[0 ]*.[0 ]{5}.0$', hexToBinStr(f))

read_ultrafine = function(f) {
  df = read.csv(f, skip = 5, check.names = F)
  names(df) = trimws(names(df))
  df
}

transform_ultrafine = function(pg, f) {
  uf = read_ultrafine(f)
  uf$flagged = parse_ultrafine_flag(uf$`Status Flags`)
  time_strs = paste(uf$Date, uf$Time)
  uf$instrument_time = as.POSIXct(time_strs,
                                  format = '%Y/%m/%d %T',
                                  tz = 'UTC')
  ## it's not really in UTC but setting the time zone this way keeps R
  ## from trying to convert times
  uf$`Status Flags` = NULL
  uf$Blank = NULL
  uf$Date = NULL
  uf$Time = NULL

  long_uf = tidyr::gather(uf, measurement_name, value,
                          -c(instrument_time, flagged))
  ## add measurement types that don't already exist in postgres
  path_folders = strsplit(f, '/')[[1]]
  ## get the site
  site = path_folders[length(path_folders) - 2]
  add_new_measurement_types(pg, site, 'ultrafine',
                            long_uf$measurement_name)
  long_uf$measurement_type_id =
    get_measurement_type_id(pg, site, 'ultrafine',
                            long_uf$measurement_name)
  long_uf$measurement_name = NULL
  long_uf
}