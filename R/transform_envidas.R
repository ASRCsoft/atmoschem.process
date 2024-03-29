read_envidas_daily = function(f) {
  na_strings = c('NA', 'NAN', 'NANN', '-9999', '---')
  df = read.csv(f, na.strings = na_strings, fileEncoding = 'UTF-8',
                check.names = F)
  df
}

#' @describeIn transform Daily Envidas files generated from SQL by a script.
#' @export
transform_envidas_daily = function(f) {
  df = read_envidas_daily(f)
  df$Date_Time = as.POSIXct(df$Date_Time, format = '%Y-%m-%d %H:%M:%S',
                            tz = 'EST')
  # make some corrections
  names(df)[1] = 'instrument_time'
  names(df)[1] = 'time'
  flagcols = grep(' \\(status\\)$', names(df))
  names(df)[flagcols] =
    sub('(.*) \\(status\\)$', 'flagged.\\1', names(df)[flagcols])
  valcols = setdiff(2:ncol(df), flagcols)
  names(df)[valcols] =
    paste0('value.', sub(' \\([^(]*\\)$', '', names(df)[valcols]))
    # sub('(.*) \\([^(]*\\)$', 'value.\\1', names(df)[valcols])
  for (i in flagcols) {
    # convert envidas flags to binary flagged value
    df[, i] = is.na(df[, i]) | df[, i] != 1
  }
  df
}

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

#' @describeIn transform Manually exported Envidas files (WFMS and WFMB).
#' @export
transform_wfm_envidas = function(f) {
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

#' @describeIn transform Manually exported Envidas files (PSP).
#' @export
transform_psp_envidas = function(f) {
  df = read_psp_envidas(f)
  ## check for newer files with the absurd date format
  file_date = gsub('.*envi_rpt-|\\.csv', '', f)
  if (file_date >= '1809' && file_date <= '2007') {
    date_format = '%d/%m/%Y %H:%M'
  } else {
    date_format = '%m/%d/%Y %I:%M %p'
  }
  names(df)[1] = 'time'
  df$time = as.POSIXct(df$time, format = date_format, tz = 'EST')
  flagcols = grep('Status', names(df))
  names(df)[flagcols] = paste0('flagged.', names(df)[flagcols - 1])
  valcols = setdiff(2:ncol(df), flagcols)
  names(df)[valcols] = paste0('value.', names(df)[valcols])
  for (i in flagcols) {
    # convert envidas flags to binary flagged value
    df[, i] = is.na(df[, i]) | df[, i] != 'Ok'
  }
  df
}
