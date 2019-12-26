## generate reports

fmt_decimals = function(x, k) format(round(x, k), trim = TRUE, nsmall = k)

order_report_cols = function(df, vars, fill = NA) {
  for (v in vars) {
    if (!v %in% names(df)) {
      df[, v] = fill
    }
  }
  df[, c(1, match(vars, names(df)))]
}

get_aqs_flags = function(con, mtype_id, time, narsto_flag) {
  ## default flag value is '' (no flag)
  aqs_flags = rep('', times = length(narsto_flag))
  ## get the flags
  q_ultra = paste0('select lower(times) as start_time, upper(times) as end_time, aqs_flag from manual_flags where measurement_type_id=',
                   mtype_id, " and times&&'[", min(time),
                   ",", max(time), "]'::tsrange order by lower(times) asc")
  conc_flags = DBI::dbGetQuery(con, q_ultra)
  if (nrow(conc_flags) > 0) {
    for (n in 1:nrow(conc_flags)) {
      affected_rows =
        which(time >= conc_flags$start_time[n] &
              time <= conc_flags$end_time[n] &
              narsto_flag == 'M1')
      if (length(affected_rows) > 0) {
        aqs_flags[affected_rows] = conc_flags$aqs_flag[n]
      }
    }
  }
  ## if there are any unflagged M1 NARSTO values left, give them a
  ## miscellaneous void (AM)
  replace(aqs_flags, narsto_flag == 'M1' & aqs_flags == '', 'AM')
}

## #' @export
## generate_wfms_report = function(con, start_time, end_time, freq = 'raw') {
##   if (freq == 'raw') {
##     ws = 'WS'
##     wd = 'WindDir_D1_WVT'
##   } else {
##     ws = 'WS_hourly'
##     wd = 'WD_hourly'
##   }
##   var_dict = c(WindDir_D1_WVT = 'WD_V',
##                WS_hourly = 'WS', WD_hourly = 'WD_V',
##                Concentration = 'Ultrafine PM',
##                concentration_370 = 'BC1_370nm',
##                concentration_880 = 'BC6_880nm',
##                `Wood smoke` = 'Woodsmoke')
##   unit_dict = c(WD = 'degrees')
##   if (freq == 'hourly') {
##     vars = c('NO', 'NO2', 'NOy', 'Ozone', 'CO', 'SO2',
##              'T', 'RH', ws, 'WD', wd, 'WS_Max', 'BP',
##              'SLP', 'Concentration',
##              'concentration_370', 'concentration_880',
##              'Wood smoke')
##     data_sources = c('campbell', 'ultrafine', 'aethelometer')
##     organize_report_data(con, 'WFMS', data_sources, vars, start_time, end_time,
##                          freq = freq, var_dict, unit_dict)
##   } else if (freq == 'raw') {
##     ## return one data frame per data source
##     vars = c('NO', 'NO2', 'NOy', 'Ozone', 'CO', 'SO2',
##              'T', 'RH', ws, 'WD', wd, 'WS_Max', 'BP',
##              'SLP')
##     campbell_df = organize_report_data(con, 'WFMS', 'campbell',
##                                        vars, start_time, end_time,
##                                        freq = freq, var_dict, unit_dict)
##     vars = 'Concentration'
##     ultrafine_df = organize_report_data(con, 'WFMS', 'ultrafine',
##                                         vars, start_time, end_time,
##                                         freq = freq, var_dict, unit_dict)
##     vars = c('concentration_370', 'concentration_880',
##              'Wood smoke')
##     aeth_df = organize_report_data(con, 'WFMS', 'aethelometer',
##                                        vars, start_time, end_time,
##                                    freq = freq, var_dict, unit_dict)
##     list(campbell = campbell_df, ultrafine = ultrafine_df,
##          aethelometer = aeth_df)
##   }
## }

## #' @export
## generate_psp_report = function(con, start_time, end_time, freq = 'raw') {
##   if (freq == 'raw') {
##     ws = 'VWS'
##     wd = 'VWD'
##   } else {
##     ws = 'WS_hourly'
##     wd = 'WD_hourly'
##   }
##   vars = c('Thermo_O3', 'NO', 'NO-DEC', 'NO2', 'NOy', 'NOy-DEC', 'HNO3',
##            'CO', 'SO2', 'WS_raw', ws, 'WD_raw', wd, 'AmbTemp',
##            'AmbRH', 'Precip', 'SR2', 'BP', 'TEOMA(2.5)MC', 'TMD',
##            'NGN3a-BScatt', 'Concentration', 'SLP', 'BC1', 'BC6',
##            'Wood smoke')
##   var_dict = c(Thermo_O3 = 'Ozone', `NO-DEC` = 'DEC_NO',
##                `NOy-DEC` = 'DEC_NOy', VWS = 'WS', VWD = 'WD',
##                WS_hourly = 'WS', WD_hourly = 'WD',
##                AmbTemp = 'Temp', AmbRH = 'RH', SR2 = 'SR',
##                `TEOMA(2.5)MC` = 'TMC', `NGN3a-BScatt` = 'Nephscat',
##                Concentration = 'Ultrafine', BC1 = 'BC1_370nm',
##                BC6 = 'BC6_880nm', `Wood smoke` = 'Woodsmoke')
##   unit_dict = c(WS_raw = 'm/s', WD_raw = 'degrees', TMD = 'ug/m3')
##   organize_report_data(con, 'PSP', 'envidas', vars, start_time, end_time,
##                        freq = freq, var_dict, unit_dict)
## }

## #' @export
## generate_wfml_report = function(con, start_time, end_time, freq = 'raw') {
##   if (freq == 'raw') {
##     ws = 'wind_speed [m/s]'
##     wd = 'wind_direction [degrees]'
##   } else {
##     ws = 'WS_hourly'
##     wd = 'WD_hourly'
##   }
##   unit_dict = c(CH4 = 'ppmv', NMHC = 'ppmv', WS = 'm/s',
##                 WD = 'degrees', WD_V = 'degrees', Bscatt = 'Mm-1')
##   var_dict = c(Ozone_ppbv = 'Ozone', `temperature_2m [degC]` = 'T',
##                `relative_humidity [%]` = 'RH',
##                `wind_speed [m/s]` = 'WS_MESO', WS_hourly = 'WS_MESO',
##                `wind_maximum [m/s]` = 'WS_MAX_MESO',
##                `wind_direction [degrees]` = 'WD_MESO',
##                WD_hourly = 'WD_MESO', BP2 = 'BP', PM25C = 'PM25',
##                NGN3 = 'Bscatt',
##                `BLK-CARBON-1` = 'Black Carbon',
##                `precip_since_00Z [mm]` = 'Precip')
##   if (freq == 'hourly') {
##     vars = c('NO', 'NO2', 'Ozone_ppbv', 'CO', 'SO2', 'CH4', 'NMHC',
##              'temperature_2m [degC]', 'relative_humidity [%]', 'WS',
##              ws, 'wind_maximum [m/s]', 'WD', 'WD_V', wd, 'BP2', 'SLP',
##              'PM25C', 'NGN3', 'BLK-CARBON-1', 'precip_since_00Z [mm]')
##     organize_report_data(con, 'WFML',
##                          c('envidas', 'campbell', 'mesonet'), vars,
##                          start_time, end_time, freq = freq, var_dict,
##                          unit_dict)
##   } else if (freq == 'raw') {
##     ## return one data frame per data source
##     vars = c('NO', 'NO2', 'CO', 'BP2', 'SLP')
##     campbell_df = organize_report_data(con, 'WFML', 'campbell', vars,
##                                        start_time, end_time,
##                                        freq = freq, var_dict,
##                                        unit_dict)
##     vars = c('Ozone_ppbv', 'SO2', 'PM25C', 'BLK-CARBON-1')
##     env_df = organize_report_data(con, 'WFML', 'envidas', vars,
##                                   start_time, end_time, freq = freq,
##                                   var_dict, unit_dict)
##     vars = c('temperature_2m [degC]', 'relative_humidity [%]', ws,
##              'wind_maximum [m/s]', wd, 'precip_since_00Z [mm]')
##     meso_df = organize_report_data(con, 'WFML', 'mesonet', vars,
##                                    start_time, end_time, freq = freq,
##                                    var_dict, unit_dict)
##     list(campbell = campbell_df, envidas = env_df,
##          mesonet = meso_df)
##   }
## }


## format data frame, convert atmoschem data from long format to wide
## format
format_report_data = function(con, columns, mtype_ids, times, 
                              freq = 'hourly', unit_dict = NULL) {
  if (freq == 'raw') {
    tbl_name = 'processed_measurements'
    flag_str = 'Status'
    fill = 0
  } else {
    tbl_name = 'hourly_measurements'
    flag_str = 'NARSTO'
    fill = 'M1'
  }
  if (length(times) == 1) times = rep(times, length(columns))
  mtypes_df = DBI::dbReadTable(con, 'measurement_types')
  ## get the data
  valid_id = !is.na(mtype_ids)
  filters = paste0('measurement_type_id=', mtype_ids[valid_id],
                   ' and time <@ \'', times[valid_id], '\'::tsrange')
  filter_sql = paste(filters, collapse = ') or (')
  sql_str = paste0('select * from ', tbl_name, ' where (', filter_sql, ')')
  ## rearrange (spread)
  dflong = dplyr::tbl(con, dbplyr::sql(sql_str)) %>%
    dplyr::mutate(time = timezone('EST', time)) %>%
    dplyr::rename(`Time (EST)` = time) %>%
    dplyr::collect() %>%
    dplyr::mutate(column = columns[match(measurement_type_id, mtype_ids)]) %>%
    dplyr::select(-measurement_type_id)
  if (freq == 'raw') {
    dflong = dflong %>%
      dplyr::mutate(flag = as.integer(!flagged)) %>%
      dplyr::select(-flagged)
  }
  value_df = dflong %>%
    dplyr::select(-flag) %>%
    tidyr::spread(column, value) %>%
    order_report_cols(columns)
  flag_df = dflong %>%
    dplyr::select(-value) %>%
    tidyr::spread(column, flag, fill = fill) %>%
    order_report_cols(columns, fill = fill)
  
  ## add units to columns
  names(flag_df)[2:ncol(flag_df)] = paste0(columns, ' (', flag_str, ')')
  var_units = mtypes_df$units[match(mtype_ids, mtypes_df$id)]
  if (!is.null(unit_dict) && any(names(unit_dict) %in% columns)) {
    unit_dict2 = unit_dict[names(unit_dict) %in% columns]
    var_units[match(names(unit_dict2), columns)] = unit_dict2
  }
  names(value_df)[2:ncol(value_df)] = paste0(columns, ' (', var_units, ')')

  ## reorder columns, alternating between values and the corresponding
  ## flag
  dfwide = merge(value_df, flag_df)
  nvars = length(columns)
  dfwide = dfwide[, c(1, rep(1:nvars, each=2) + 1 + rep(c(0, nvars), nvars))]
  var_decimals = mtypes_df$report_decimals[match(mtype_ids, mtypes_df$id)]
  for (n in 1:nvars) {
    col_n = n * 2
    ## format number decimals
    if (!is.na(var_decimals[n])) {
      dfwide[, col_n] = fmt_decimals(dfwide[, col_n], var_decimals[n])
      ## replace NA strings with real NA's
      dfwide[dfwide[, col_n] == 'NA', col_n] = NA
    }
    ## remove values with M1 flag
    dfwide[dfwide[, col_n + 1] == 'M1', col_n] = NA
  }

  ## if the report includes ultrafine data, need to include an AQS
  ## flag column
  if (freq == 'hourly' && 'Ultrafine' %in% columns) {
    ## get the relevant column names
    aqs_col = 'Ultrafine (AQS)'
    narsto_col = 'Ultrafine (NARSTO)'
    ultra_n = which(columns == 'Ultrafine')
    ## get the flags
    ultra_id = mtype_ids[ultra_n]
    dfwide[, aqs_col] =
      get_aqs_flags(con, ultra_id, dfwide$`Time (EST)`, dfwide[, narsto_col])
    ## fix the column ordering
    dfwide = dfwide[, c(1:(1 + ultra_n * 2), ncol(dfwide),
                        (2 + ultra_n * 2):(ncol(dfwide) - 1))]
  }
  
  dfwide
}


## report generating function

## returns list of: hourly values, minute values (separated by data
## source), and instruments
generate_report = function(obj, columns, times, site, data_source,
                           measurement, hmeasurement = NULL,
                           units = NULL) {
  res = list()
  if (!is.null(units)) {
    unit_dict = setNames(na.omit(units), columns[!is.na(units)])
  } else {
    unit_dict = NULL
  }
  if (length(times) == 1) times = rep(times, length(columns))
  ## hourly data
  ## if an hourly measurement name isn't specified, use the value from
  ## `measurement`
  hmeasurement = replace(measurement, !is.na(hmeasurement),
                         na.omit(hmeasurement))
  mtype_ids = get_measurement_type_id(obj$con, site, data_source,
                                      hmeasurement)
  res$hourly = format_report_data(obj$con, columns, mtype_ids, times,
                                  freq = 'hourly', unit_dict)
  ## raw data
  res$raw = list()
  for (ds in unique(na.omit(data_source))) {
    ind = which(data_source == ds)
    mtype_ids = get_measurement_type_id(obj$con, site[ind], ds,
                                        measurement[ind])
    res$raw[[ds]] = format_report_data(obj$con, columns[ind],
                                       mtype_ids, times[ind],
                                       freq = 'raw', unit_dict)
  }
  ## instrument info
  ## match column measurements to measurement instruments
  ## ...
  class(res) = 'atmoschem_report'
  res
}
