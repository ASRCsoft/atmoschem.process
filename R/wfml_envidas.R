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

#' @export
transform_wfml_envidas = function(f, site = 'WFML') {
  df = read_wfml_envidas(f)
  ## get timestamps from date and time columns
  df$instrument_time = as.POSIXct(paste(df$Date, df$Time),
                                  format = '%m/%d/%Y %I:%M %p',
                                  tz = 'EST')
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
