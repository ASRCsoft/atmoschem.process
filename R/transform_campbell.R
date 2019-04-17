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
wfml_flags = c(CO = 'CO', NO = 'NOX', NO2 = 'NOX')


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
  df[, -grep('Spare|Phase', names(df), ignore.case = T)]
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

transform_campbell = function(pg, f) {
  campbell = read_campbell(f)
  path_folders = strsplit(f, '/')[[1]]
  ## get the site
  site = path_folders[length(path_folders) - 2]
  if (site == 'WFMS') {
    campbell = fix_wfms(campbell)
  }
  
  ## clean and reorganize the data
  is_flag = grepl('^F_', names(campbell))
  campbell_long = campbell[, !is_flag] %>%
    tidyr::gather(measurement, value, -c(instrument_time, RECORD))
  if (site == 'WFMS') {
    flag_mat = as.matrix(campbell[, is_flag]) != 1
  } else if (site == 'WFML') {
    flag_mat = as.matrix(campbell[, is_flag]) != 0
  }
  row.names(flag_mat) = campbell$instrument_time
  campbell_long$measurement =
    gsub('_Avg$', '', campbell_long$measurement)
  flag_rows = match(campbell_long$instrument_time,
                    campbell$instrument_time)
  flag_cols = match(paste('F',
                          fast_lookup(campbell_long$measurement,
                                      wfms_flags),
                          'Avg', sep = '_'),
                    colnames(flag_mat))
  campbell_long$flagged = flag_mat[cbind(flag_rows, flag_cols)]
  
  ## add to postgres
  names(campbell_long) = tolower(names(campbell_long))
  ## add measurement types that don't already exist in postgres
  add_new_measurement_types(pg, site, 'campbell',
                            campbell_long$measurement)
  campbell_long$measurement_type_id =
    get_measurement_type_id(pg, site, 'campbell',
                            campbell_long$measurement)
  campbell_long$measurement = NULL
  ## reorder the columns to match the database table
  campbell_long[, c(5, 1:4)]
}