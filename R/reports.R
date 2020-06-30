## generate reports

fmt_decimals = function(x, k) format(round(x, k), trim = TRUE, nsmall = k)

order_report_cols = function(df, vars, fill = NA) {
  for (v in vars) {
    if (!v %in% names(df)) {
      df[, v] = fill
    }
  }
  df[, c(1:2, match(vars, names(df)))]
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

## convert strings from csv files to lubridate intervals
as_interval = function(s) {
  tlow = as.POSIXct(gsub('^[[(]|,.*$', '', s), tz='EST')
  tupp = as.POSIXct(gsub('^.*,|[])]$', '', s), tz='EST')
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

## format data frame, convert atmoschem data from long format to wide
## format
format_report_data = function(con, columns, mtype_ids, times, 
                              unit_dict = NULL) {
  tbl_name = 'hourly_measurements'
  flag_str = 'flag'
  fill = 'M1'
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
    dplyr::select(-measurement_type_id) %>%
    ## occasionally in the raw data a few minutes are repeated after a
    ## clock adjustment, so an additional row identifier is needed to
    ## pivot the data frame
    dplyr::group_by(`Time (EST)`, column) %>% 
    dplyr::mutate(group_id = row_number()) %>%
    dplyr::ungroup()
  # make sure the report time zone is EST
  attributes(dflong$`Time (EST)`)$tzone = 'EST'
  value_df = dflong %>%
    dplyr::select(-flag) %>%
    tidyr::spread(column, value) %>%
    order_report_cols(columns)
  flag_df = dflong %>%
    dplyr::select(-value) %>%
    tidyr::spread(column, flag, fill = fill) %>%
    order_report_cols(columns, fill = fill)
  
  ## add units to columns
  names(flag_df)[3:ncol(flag_df)] = paste0(columns, ' (', flag_str, ')')
  var_units = mtypes_df$units[match(mtype_ids, mtypes_df$id)]
  if (!is.null(unit_dict) && any(names(unit_dict) %in% columns)) {
    unit_dict2 = unit_dict[names(unit_dict) %in% columns]
    var_units[match(names(unit_dict2), columns)] = unit_dict2
  }
  names(value_df)[3:ncol(value_df)] = paste0(columns, ' (', var_units, ')')

  ## reorder columns, alternating between values and the corresponding
  ## flag
  dfwide = merge(value_df, flag_df)
  dfwide$group_id = NULL
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
  if (any(grepl('Ultrafine', columns))) {
    ## get the relevant column names
    ultra_n = grep('Ultrafine', columns)[1]
    ultra_col = columns[ultra_n]
    aqs_col = paste(ultra_col, '(AQS flag)')
    narsto_col = paste0(ultra_col, ' (', flag_str, ')')
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
                                  unit_dict)
  class(res) = 'atmoschem_report'
  res
}

write_report_files = function(report, name = 'report', dir = '.',
                              version = NULL, ...) {
  if (is.null(version)) {
    version_str = ''
  } else {
    version_str = paste0('_v', version)
  }
  ## write the hour file
  message('Writing hourly data file...')
  hour_file = paste0(name, version_str, '.csv')
  hour_path = file.path(dir, hour_file)
  write.csv(report$hourly, file = hour_path, ...)
}
