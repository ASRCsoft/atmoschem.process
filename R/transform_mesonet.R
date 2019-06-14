transform_mesonet = function(f) {
  df = read.csv(f, check.names = F)
  df$instrument_time = as.POSIXct(df$datetime,
                                  format = '%Y%m%dT%H%M%S',
                                  tz = 'UTC')
  attributes(df$instrument_time)$tzone = 'EST'
  df$record = 1:nrow(df) + 1

  ## get data frame of values
  df_vals = df[, -(1:2)]
  long_df = tidyr::gather(df_vals, measurement_name, value,
                          -c(instrument_time, record))
  long_df$flagged = F
  
  long_df[, c('measurement_name', 'instrument_time',
              'record', 'value', 'flagged')]
}
