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

patch_psp_envidas_daily = function(df) {
  # fix some black carbon data, which is incorrectly recorded as zero when it's
  # missing
  if (all(c('BC1 (ng/m3)', 'BC6 (ng/m3)') %in% names(df))) {
    na_bc = which(df$`BC1 (ng/m3)` == 0 & df$`BC6 (ng/m3)` == 0)
    if (length(na_bc) > 0) df[na_bc, c('BC1 (ng/m3)', 'BC6 (ng/m3)')] = NA
  }
  # fix some incorrectly set flags
  df$`NOy (status)`[df$instrument_time >= '2020-01-24 12:00' &
                    df$instrument_time < '2020-01-27 11:00'] = 1
  df$`CO (status)`[df$instrument_time >= '2020-08-17 14:10' &
                   df$instrument_time < '2020-08-18 10:00'] = 1
  df$`NOy (status)`[df$instrument_time >= '2020-08-19 14:00' &
                    df$instrument_time < '2020-08-20 09:00'] = 1
  df$`VWS (status)`[df$instrument_time >= '2020-11-04 09:23' &
                    df$instrument_time <= '2020-11-16 09:44'] = 1
  df$`VWD (status)`[df$instrument_time >= '2020-11-04 09:23' &
                    df$instrument_time <= '2020-11-16 09:44'] = 1
  df
}

transform_envidas_daily = function(f, site = 'PSP') {
  df = read_envidas_daily(f)
  df$Date_Time = as.POSIXct(df$Date_Time, format = '%Y-%m-%d %H:%M:%S',
                            tz = 'EST')
  # make some corrections
  names(df)[1] = 'instrument_time'
  if (site == 'PSP') {
    df = patch_psp_envidas_daily(df)
  }
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

#' @export
transform_psp_envidas = function(f, site = 'PSP') {
  ## check to see if the file is in the simpler daily format
  is_daily_format = grepl('^[0-9]{8}_envidas.csv$', basename(f))
  if (is_daily_format) {
    return(transform_envidas_daily(f, site))
  }
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
