## generate reports

order_report_cols = function(df, vars, fill = NA) {
  for (v in vars) {
    if (!v %in% names(df)) {
      df[, v] = fill
    }
  }
  df[, c(1, match(vars, names(df)))]
}

organize_report_data = function(con, site_name, dsname, vars,
                                start_time, end_time, freq = 'raw',
                                var_dict = NULL, unit_dict = NULL) {
  ## check for:
  ## - valid connection
  ## - only one site
  ## - only one data source
  if (!freq %in% c('raw', 'hourly')) stop("freq should be 'raw' or 'hourly'")

  ## get the measurement type ID's
  mtype_ids = get_measurement_type_id(con, site_name,
                                      dsname, vars,
                                      add_new = FALSE)
  ## also look in the derived values at the site
  mtype_derived_ids = get_measurement_type_id(con, site_name,
                                              'derived', vars,
                                              add_new = FALSE)
  mtype_ids[is.na(mtype_ids)] = mtype_derived_ids[is.na(mtype_ids)]

  ## get measurement units
  mtypes_df = DBI::dbReadTable(con, 'measurement_types')
  mtypes_df = subset(mtypes_df, id %in% mtype_ids)

  ## get values from start_time to end_time
  sql_mtypes = paste(na.omit(mtype_ids), collapse = ', ')
  if (freq == 'raw') {
    q1 = paste0('select * from processed_measurements where measurement_type_id in (',
                sql_mtypes, ") and measurement_time>='",
                start_time, "' and measurement_time<'", end_time, "'")
  } else {
    q1 = paste0('select * from hourly_measurements where measurement_type_id in (',
                sql_mtypes, ") and measurement_time>='",
                start_time, "' and measurement_time<'", end_time, "'")
  }

  df1 = DBI::dbGetQuery(con, q1)
  df1$measurement_name =
    mtypes_df$name[match(df1$measurement_type_id, mtypes_df$id)]
  df1$measurement_type_id = NULL
  names(df1)[1] = 'Time (EST)'

  ## rearrange
  value_df = tidyr::spread(df1[, -3], measurement_name, value)
  value_df = order_report_cols(value_df, vars)
  if (freq == 'raw') {
    df1$status = as.integer(!df1$flagged)
    df1$flagged = NULL
    flag_df = tidyr::spread(df1[, -2], measurement_name, status,
                            fill = 0)
    flag_df = order_report_cols(flag_df, vars, fill = 0)
  } else {
    flag_df = tidyr::spread(df1[, -2], measurement_name, flag,
                            fill = 'M1')
    flag_df = order_report_cols(flag_df, vars, fill = 'M1')
  }
  
  ## rename columns as needed
  measurement_col_names = names(value_df)[2:ncol(value_df)]
  if (is.null(var_dict)) {
    renamed_col_names = measurement_col_names
  } else {
    renamed_col_names = 
      ifelse(measurement_col_names %in% names(var_dict),
             var_dict[measurement_col_names],
             measurement_col_names)
  }
  
  ## add units to columns
  var_units = mtypes_df$units[match(measurement_col_names, mtypes_df$name)]
  if (!is.null(var_units) && sum(is.na(var_units)) > 0) {
    var_units[is.na(var_units)] = unit_dict[measurement_col_names[is.na(var_units)]]
  }
  renamed_val_col_names =
    paste0(renamed_col_names, ' (', var_units, ')')
  if (freq == 'raw') {
    renamed_flag_col_names = paste(renamed_col_names, '(Status)')
  } else {
    renamed_flag_col_names = paste(renamed_col_names, '(NARSTO)')
  }
  names(value_df)[2:ncol(value_df)] = renamed_val_col_names
  names(flag_df)[2:ncol(flag_df)] = renamed_flag_col_names

  ## return data frame
  df2 = merge(value_df, flag_df)
  ## reorder columns
  nvars = length(vars)
  df2 = df2[, c(1, rep(1:nvars, each=2) + 1 + rep(c(0, nvars), nvars))]
  
  df2
}

#' @export
generate_psp_report = function(con, start_time, end_time, freq = 'raw') {
  if (freq == 'raw') {
    ws = 'VWS'
    wd = 'VWD'
  } else {
    ws = 'WS_hourly'
    wd = 'WD_hourly'
  }
  vars = c('Thermo_O3', 'NO', 'NO-DEC', 'NO2', 'NOy', 'NOy-DEC', 'HNO3',
           'CO', 'SO2', 'WS_raw', ws, 'WD_raw', wd, 'AmbTemp',
           'AmbRH', 'Precip', 'SR', 'BP', 'TEOMA(2.5)MC', 'TMD',
           'NGN3a-BScatt', 'Concentration', 'SLP', 'BC1', 'BC6',
           'Wood smoke')
  var_dict = c(Thermo_O3 = 'Ozone', `NO-DEC` = 'DEC_NO',
               `NOy-DEC` = 'DEC_NOy', VWS = 'WS', VWD = 'WD',
               WS_hourly = 'WS', WD_hourly = 'WD',
               AmbTemp = 'Temp', AmbRH = 'RH', `TEOMA(2.5)MC` = 'TMC',
               `NGN3a-BScatt` = 'Nephscat', Concentration = 'Ultrafine',
               BC1 = 'BC1_370nm', BC6 = 'BC6_880nm',
               `Wood smoke` = 'Woodsmoke')
  unit_dict = c(WS_raw = 'm/s', WD_raw = 'degrees', TMD = 'ug/m3')
  organize_report_data(con, 'PSP', 'envidas', vars, start_time, end_time,
                       freq = freq, var_dict, unit_dict)
}
