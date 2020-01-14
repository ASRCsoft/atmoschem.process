## transform measurement files

fast_lookup = function(vals, dict) {
  ## This code to get dictionary (named vector) entries shouldn't need
  ## to be this long but the current version of R seems to have a very
  ## slow lookup function for named vectors, so we need extra code to
  ## work around that.
  res = rep(NA, length(vals))
  has_entry = vals %in% names(dict)
  res[has_entry] = dict[vals[has_entry]]
  res
}

wfms_flags = c(NO = 'NOX', NOx = 'NOX', T = 'TRH',
               RH = 'TRH', NOy = 'NOY', SO2 = 'SO2',
               CO = 'CO')
wfml_flags = c(CO = 'CO', NO = 'NOX', NO2 = 'NOX',
               NGN3 = 'NGN')


read_campbell = function(f) {
  ## headers are on the second line, but data starts on line 4
  headers = read.csv(f, header=F, skip = 1, nrows=1,
                     stringsAsFactors = F)
  na_strings = c('NAN', 'NaN')
  df = read.csv(f, header=F, skip = 4, col.names = headers,
                na.strings = na_strings, stringsAsFactors = F)
  ## replace 'TIMESTAMP' with 'instrument_time' to match other data
  ## tables
  names(df)[1] = 'instrument_time'
  ## replace 'CupA' with 'Cup' for consistency over time
  names(df) = gsub('CupA', 'Cup', names(df))
  ## remove empty columns
  df[, -grep('Spare', names(df), ignore.case = T)]
}

fix_wfms = function(df) {
  ## fix various issues with WFMS data
  
  ## replace mislabeled NO2 column
  names(df)[names(df) == 'NO2_Avg'] = 'NOx_Avg'
  ## adjust miscalculated wind speeds
  df$WS3Cup_Avg[df$instrument_time > '2016-12-14' &
                df$instrument_time < '2019-02-14 15:57'] =
    (.5 / .527) * df$WS3Cup_Avg - (.5 / .527) - .5
  df$WS3Cup_Max[df$instrument_time > '2016-12-14' &
                df$instrument_time < '2019-02-14 15:57'] =
    (.5 / .527) * df$WS3Cup_Max - (.5 / .527) - .5
  ## ignore some incorrect flags
  df$F_TRH_Avg[df$instrument_time > '2018-12-01' &
               df$instrument_time < '2019-01-01'] = 1
  ## replace malfunctioning wind direction values with NA
  df[df$WindDir_SD1_WVT == 0,
     c('WindDir_D1_WVT', 'WindDir_SD1_WVT')] = NA
  df
}

transform_campbell = function(f, site) {
  campbell = read_campbell(f)
  if (site == 'WFMS') {
    campbell = fix_wfms(campbell)
  }
  
  ## clean and reorganize the data
  is_flag = grepl('^F_', names(campbell))
  campbell_long = campbell[, !is_flag] %>%
    tidyr::gather(measurement_name, value, -c(instrument_time, RECORD))
  if (site == 'WFMS') {
    flag_mat = as.matrix(campbell[, is_flag]) != 1
    flag_dict = wfms_flags
  } else if (site == 'WFML') {
    flag_mat = as.matrix(campbell[, is_flag]) != 0
    flag_dict = wfml_flags
  }
  row.names(flag_mat) = campbell$instrument_time
  campbell_long$measurement_name =
    gsub('_Avg$', '', campbell_long$measurement_name)
  flag_rows = match(campbell_long$instrument_time,
                    campbell$instrument_time)
  flag_cols = match(paste('F',
                          fast_lookup(campbell_long$measurement_name,
                                      flag_dict),
                          'Avg', sep = '_'),
                    colnames(flag_mat))
  campbell_long$flagged = flag_mat[cbind(flag_rows, flag_cols)]
  
  names(campbell_long) = tolower(names(campbell_long))
  ## reorder the columns to match the database table
  campbell_long[, c('measurement_name', 'instrument_time',
                    'record', 'value', 'flagged')]
}
