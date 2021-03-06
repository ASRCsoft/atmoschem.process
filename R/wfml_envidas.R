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

patch_wfml_envidas = function(df) {
  # flag left on after instrument installed, 2020-01-03 14:02 to 2020-01-07
  # 09:04
  if ('PM25C : Status' %in% names(df)) {
    df$`PM25C : Status`[df$instrument_time >= as.POSIXct('2020-01-03 14:02', tz = 'EST') &
                        df$instrument_time < as.POSIXct('2020-01-07 09:04', tz = 'EST')] = 1
  }
  df
}

transform_wfml_envidas = function(f) {
  df = read_wfml_envidas(f)
  ## get timestamps from date and time columns
  df$instrument_time = as.POSIXct(paste(df$Date, df$Time),
                                  format = '%m/%d/%Y %I:%M %p',
                                  tz = 'EST')
  df = patch_wfml_envidas(df)
  # put the time column first and drop the separate date and time columns
  df = df[, c(ncol(df), 3:(ncol(df) - 1))]
  names(df)[1] = 'time'
  flagcols = grep(' : Status', names(df))
  names(df)[flagcols] =
    sub('(.*) : Status', 'flagged.\\1', names(df)[flagcols])
  valcols = setdiff(2:ncol(df), flagcols)
  names(df)[valcols] =
    paste0('value.', sub(' : Value', '', names(df)[valcols]))
  for (i in flagcols) {
    # convert envidas flags to binary flagged value
    df[, i] = is.na(df[, i]) | df[, i] != 1
  }
  df
}
