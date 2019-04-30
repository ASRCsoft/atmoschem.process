read_psp_envidas = function(f, ...) {
  ## get the headers separately since there's a line of units
  ## separating them from the data
  headers = names(read.csv(f, skip = 2, nrows = 1, check.names = F))
  lines = readLines(f, warn = F, encoding = 'UTF-8')
  ## remove extraneous header lines and lines of summary stats at the
  ## bottom of the file
  csv_lines = lines[-c(1:2, 4,
                       (length(lines) - 8):length(lines))]
  csv_text = paste(csv_lines, collapse = '\n')
  na_strings = c('NA', 'NAN', 'NANN', '-9999', '---')
  df = read.csv(text = csv_text, na.strings = na_strings,
                fileEncoding = 'UTF-8', col.names = headers,
                check.names = F, ...)
  df
}

transform_psp_envidas = function(f) {
  df = read_psp_envidas(f)
  ## check for newer files with the absurd date format
  file_date = gsub('.*envi_rpt-|\\.csv', '', f)
  if (file_date >= '1809') {
    date_format = '%d/%m/%Y %H:%M'
  } else {
    date_format = '%m/%d/%Y %I:%M %p'
  }
  names(df)[1] = 'instrument_time'
  df$instrument_time = as.POSIXct(df$instrument_time,
                                  format = date_format,
                                  tz = 'UTC')
  ## match the row number of the original file
  df$record = 1:nrow(df) + 4
  df = df[, c(1, ncol(df), 2:(ncol(df) - 1))]

  ## get data frame of values
  df_vals = df[, !grepl('Status', names(df))]
  long_vals = tidyr::gather(df_vals, measurement_name, value,
                            -c(instrument_time, record))

  ## get data frame of flags
  df_flags = df[, c(1:2, grep('Status', names(df)))]
  names(df_flags) = names(df_vals)
  long_flags = tidyr::gather(df_flags, measurement_name, flag,
                             -c(instrument_time, record))
  long_flags$flagged = is.na(long_flags$flag) | long_flags$flag != 'Ok'
  long_flags$flag = NULL

  ## long_df = merge(long_vals, long_flags)
  ## safe to assume both data frames are in the same order for now
  long_df = cbind(long_vals, flagged = long_flags$flagged)

  long_df[, c('measurement_name', 'instrument_time',
              'record', 'value', 'flagged')]
}
