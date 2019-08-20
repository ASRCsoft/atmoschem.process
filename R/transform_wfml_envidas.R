read_wfml_envidas = function(f, ...) {
  na_strings = c('NA', 'NAN', 'NANN', '-9999')
  ## Sometimes the files have extra commas at the end of the
  ## lines. (Seriously Envidas, get your act together.) We can fix
  ## this by getting values and headers separately, then removing
  ## lines with no header
  headers = strsplit(readLines(f, n = 2)[2], ',')[[1]]
  df1 = read.csv(f, header = F, na.strings = na_strings, skip = 2,
                 fileEncoding = 'UTF-8', stringsAsFactors = F, ...)
  df1 = df1[, 1:length(headers)]
  names(df1) = headers
  df1
}

transform_wfml_envidas = function(f) {
  df = read_wfml_envidas(f)
  ## get timestamps from date and time columns
  df$instrument_time = as.POSIXct(paste(df$Date, df$Time),
                                  format = '%m/%d/%Y %I:%M %p',
                                  tz = 'UTC')
  ## ^ it's not really UTC but this keeps R from mucking with time
  ## zones
  df$record = 1:nrow(df) + 2

  ## get data frame of values
  df_vals = df[, grep(' : Value|instrument_time|record', names(df))]
  names(df_vals) = gsub(' : Value', '', names(df_vals))
  long_vals = tidyr::gather(df_vals, measurement_name, value,
                            -c(instrument_time, record))

  ## get data frame of flags
  df_flags = df[, grep(' : Status|instrument_time|record', names(df))]
  names(df_flags) = gsub(' : Status', '', names(df_flags))
  long_flags = tidyr::gather(df_flags, measurement_name, flag,
                             -c(instrument_time, record))

  long_df = merge(long_vals, long_flags)
  long_df$flagged = long_df$flag != 1
  
  long_df[, c('measurement_name', 'instrument_time',
              'record', 'value', 'flagged')]
}
