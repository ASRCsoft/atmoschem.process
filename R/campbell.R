## transform measurement files

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
  # ... except for the newer columns that I want to keep for now
  names(df) = gsub('Cup_S', 'CupA_S', names(df))
  ## remove empty columns
  df[, -grep('Spare', names(df), ignore.case = T)]
}

fix_wfms = function(df) {
  ## fix various issues with WFMS data
  
  ## replace mislabeled NO2 column
  names(df)[names(df) == 'NO2_Avg'] = 'NOx_Avg'
  ## adjust miscalculated wind speeds
  wind_miscalculated = df$instrument_time > '2016-12-14' &
    df$instrument_time < '2019-02-14 15:57'
  if (any(wind_miscalculated)) {
    df$WS3Cup_Avg[wind_miscalculated] =
      (.5 / .527) * df$WS3Cup_Avg[wind_miscalculated] - (.5 / .527) - .5
    df$WS3Cup_Max[wind_miscalculated] =
      (.5 / .527) * df$WS3Cup_Max[wind_miscalculated] - (.5 / .527) - .5
  }
  ## ignore some incorrect flags
  df$F_TRH_Avg[df$instrument_time > '2018-12-01' &
               df$instrument_time < '2019-01-01'] = 1
  df$F_CO_Avg[df$instrument_time >= '2019-11-07 04:15' &
              df$instrument_time < '2019-11-15'] = 1
  df$F_NOX_Avg[df$instrument_time >= '2020-02-25 12:01' &
               df$instrument_time < '2020-03-05 10:37'] = 1
  df$F_CO_Avg[df$instrument_time >= '2020-07-15 14:03' &
              df$instrument_time < '2020-07-22 12:00'] = 1
  ## replace malfunctioning wind direction values with NA
  df[df$WindDir_SD1_WVT == 0,
     c('WindDir_D1_WVT', 'WindDir_SD1_WVT')] = NA
  # a few column names got messed up 2020-06-30 when changes were made to wind
  # columns
  if ('WS3CupB_SWS3CupB_S_WVT' %in% names(df)) {
    cupb_col = match('WS3CupB_SWS3CupB_S_WVT', names(df))
    names(df)[cupb_col:(cupb_col + 3)] = 
      c('WS3CupB_S', 'WS3CupB_S_WVT', 'WindDirB_D1_WVT', 'WindDirB_SD1_WVT')
  }
  df
}

transform_campbell = function(f, site) {
  campbell = read_campbell(f)
  if (site == 'WFMS') {
    campbell = fix_wfms(campbell)
  } else if (site == 'WFML') {
    # replace mislabeled NO2 column
    names(campbell)[names(campbell) == 'NO2_Avg'] = 'NOX_Avg'
  }
  
  ## clean and reorganize the data
  is_flag = grepl('^F_', names(campbell))
  # assume 'Avg' by default, so it can be removed
  names(campbell) = sub('_Avg$', '', names(campbell))
  if (site == 'WFMS') {
    flag_mat = as.matrix(campbell[, is_flag]) != 1
    flag_dict = wfms_flags
  } else if (site == 'WFML') {
    flag_mat = as.matrix(campbell[, is_flag]) != 0
    flag_dict = wfml_flags
  }
  campbell2 = campbell[, !is_flag]
  campbell2$RECORD = NULL
  for (i in 2:ncol(campbell2)) {
    varname = names(campbell2)[i]
    flagname = paste0('flagged.', varname)
    if (varname %in% names(flag_dict) &
        paste0('F_', flag_dict[varname]) %in% colnames(flag_mat)) {
      flag_col = paste0('F_', flag_dict[varname])
      campbell2[, flagname] = flag_mat[, flag_col]
    } else {
      campbell2[, flagname] = FALSE
    }
    names(campbell2)[i] = paste0('value.', names(campbell2)[i])
  }
  names(campbell2)[1] = 'time'
  campbell2$time = as.POSIXct(campbell2$time, tz = 'EST')
  campbell2
}
