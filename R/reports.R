## generate reports

fmt_decimals = function(x, k) format(round(x, k), trim = TRUE, nsmall = k)

## convert strings from csv files to lubridate intervals
as_interval = function(s) {
  formats = c('%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M', '%Y-%m-%d')
  tlow = gsub('^[[(]|,.*$', '', s)
  tlow = lubridate::parse_date_time(tlow, formats, tz = 'EST', exact = T)
  tupp = gsub('^.*, ?|[])]$', '', s)
  tupp = lubridate::parse_date_time(tupp, formats, tz = 'EST', exact = T)
  lubridate::interval(tlow, tupp)
}

## convert lubridate intervals back to csv strings
as_timerange_str = function(tint) {
  ifelse(is.na(tint), NA,
         paste0('[', lubridate::int_start(tint), ',',
                lubridate::int_end(tint), ')'))
}

merge_timerange = function(x, y, tcol.x, tcol.y = tcol.x, ...) {
  ## prevent the time range columns from being used for merging
  new_tcol.x = paste(tcol.x, 'x', sep = '.')
  new_tcol.y = paste(tcol.y, 'y', sep = '.')
  names(x)[names(x) == tcol.x] = new_tcol.x
  names(y)[names(y) == tcol.y] = new_tcol.y
  df2 = merge(x, y, ...)
  ## filter to remove non-overlapping time ranges
  x_int = as_interval(df2[, new_tcol.x])
  y_int = as_interval(df2[, new_tcol.y])
  df2[, tcol.x] = as_timerange_str(lubridate::intersect(x_int, y_int))
  df2 = df2[, !names(df2) %in% c(new_tcol.x, new_tcol.y)]
  df2[!is.na(df2[, tcol.x]), ]
}
