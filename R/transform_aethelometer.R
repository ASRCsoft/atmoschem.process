#' @describeIn transform Aethelometer files.
#' @export
transform_wfms_aethelometer = function(f) {
  if (startsWith(basename(f), 'ae')) {
    df = read.csv(f, header = FALSE, skip = 1)
    df$record = 1:nrow(df) + 1
  } else if (startsWith(basename(f), 'BC')) {
    df = read.csv(f, header = FALSE)
    df$record = 1:nrow(df)
  } else {
    stop('Aethelometer file not recognized.')
  }
  df$instrument_time =
    as.POSIXct(strptime(paste(df$V1, df$V2),
                        '%d-%B-%y %H:%M', tz = 'EST'))
  ae_col_names = c('concentration', 'sz', 'sb', 'rz', 'rb',
                   'fraction', 'attenuation')
  names(df)[c(3, 6:11)] = paste(ae_col_names, '880', sep = '_')
  names(df)[c(4, 12:17)] = paste(ae_col_names, '370', sep = '_')
  names(df)[5] = 'flow'
  ## set simultaneous zero 370nm and 880nm to NA
  df[which(df[, 3] == 0 & df[, 4] == 0), 3:4] = NA
  long_df = tidyr::gather(df[, -(1:2)], measurement_name, value,
                          -c(instrument_time, record))
  long_df$flagged = is.na(long_df$value)
  long_df[, c('measurement_name', 'instrument_time',
              'record', 'value', 'flagged')]
}
