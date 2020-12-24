hexToBinStr = function(h)
  format(R.utils::intToBin(strtoi(h, 16L)),
         justify = 'right', width = 16)

## return TRUE for a 1 in any position except 9 and 15, which are
## 'Pulse Height Fault' and 'Service Reminder', respectively
parse_ultrafine_flag = function(f) !grepl('^[0 ]*.[0 ]{5}.0$', hexToBinStr(f))

# convert character data frame columns to numeric
make_char_numeric = function(x) {
  as.data.frame(lapply(x, function(y) {
    if (inherits(y, 'character')) as.numeric(y) else y
  }), check.names = FALSE)
}

read_ultrafine = function(f) {
  df = read.csv(f, skip = 5, check.names = F)
  names(df) = trimws(names(df))
  # Occasionally (I think if the instrument reboots?) the ultrafine instrument
  # puts another header in the middle of a file, resulting in the numeric rows
  # being read as character
  if (any(sapply(df[, -c(1:2, 11)], inherits, what = 'character'))) {
    # remove nondata rows and convert back to numeric
    df = df[!df$`Status Flags` %in% c('', 'Status Flags'), ]
    df = cbind(df[, 1:2], make_char_numeric(df[, -c(1:2, 11)]), df[, 11])
    names(df)[11] = 'Status Flags'
  }
  df
}

transform_ultrafine = function(f) {
  uf = read_ultrafine(f)
  uf$flagged = parse_ultrafine_flag(as.character(uf$`Status Flags`))
  time_strs = paste(uf$Date, uf$Time)
  uf$instrument_time = as.POSIXct(time_strs,
                                  format = '%Y/%m/%d %T',
                                  tz = 'EST')
  ## it's not really in UTC but setting the time zone this way keeps R
  ## from trying to convert times
  if (nrow(uf) > 0) {
    uf$record = 1:nrow(uf) + 6 # row number in the file
  } else {
    uf$record = integer(0)
  }
  uf$`Status Flags` = NULL
  uf$Blank = NULL
  uf$Date = NULL
  uf$Time = NULL

  long_uf = tidyr::gather(uf, measurement_name, value,
                          -c(instrument_time, record, flagged))
  long_uf[, c('measurement_name', 'instrument_time',
              'record', 'value', 'flagged')]
}
