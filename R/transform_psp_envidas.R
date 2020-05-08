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

read_envidas_daily = function(f) {
  ## these files were downloaded directly from SQL Server and stored
  ## in a simple csv format
  na_strings = c('NA', 'NAN', 'NANN', '-9999', '---')
  df = read.csv(f, na.strings = na_strings, fileEncoding = 'UTF-8',
                check.names = F)
  df
}

transform_envidas_daily = function(f) {
  df = read_envidas_daily(f)
  df$Date_Time = as.POSIXct(df$Date_Time, format = '%Y-%m-%d %H:%M:%S',
                            tz = 'UTC')
  names(df)[1] = 'instrument_time'
  ## match the row number of the original file
  df$record = 1:nrow(df) + 1
  ## move the new record column to 2nd position
  df = df[, c(1, ncol(df), 2:(ncol(df) - 1))]

  # fix some black carbon data, which is incorrectly recorded as zero when it's
  # missing
  if (all(c('BC1 (ng/m3)', 'BC6 (ng/m3)') %in% names(df))) {
    na_bc = which(df$`BC1 (ng/m3)` == 0 & df$`BC6 (ng/m3)` == 0)
    if (length(na_bc) > 0) df[na_bc, c('BC1 (ng/m3)', 'BC6 (ng/m3)')] = NA
  }

  ## get data frame of values
  df_vals = df[, !grepl(' \\(status\\)$', names(df))]
  ## remove units from name to match column names from the older files
  names(df_vals) = sub(' \\([^(]*\\)$', '', names(df_vals))
  long_vals = tidyr::gather(df_vals, measurement_name, value,
                            -c(instrument_time, record))

  ## get data frame of flags
  df_flags = df[, c(1:2, grep(' \\(status\\)$', names(df)))]
  names(df_flags) = names(df_vals)
  long_flags = tidyr::gather(df_flags, measurement_name, flag,
                             -c(instrument_time, record))
  long_flags$flagged = is.na(long_flags$flag) | long_flags$flag != 1
  long_flags$flag = NULL

  ## long_df = merge(long_vals, long_flags)
  ## safe to assume both data frames are in the same order for now
  long_df = cbind(long_vals, flagged = long_flags$flagged)

  long_df[, c('measurement_name', 'instrument_time',
              'record', 'value', 'flagged')]
}

transform_psp_envidas = function(f) {
  ## check to see if the file is in the simpler daily format
  is_daily_format = grepl('^[0-9]{8}_envidas.csv$', basename(f))
  if (is_daily_format) {
    return(transform_envidas_daily(f))
  }
  
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
  ## move the new record column to 2nd position
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
