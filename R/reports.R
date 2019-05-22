## generate reports

## to make hourly data report, I need:
## - database connection
## - data source
## - list of variable names
## - time range
## - dictionary of variable name -> column name

organize_data = function(con, tbl_name, site_name, dsname,
                         vars, start_time, end_time, var_dict,
                         freq = 'raw') {
  ## check for:
  ## - valid connection
  ## - only one site
  ## - only one data source

  ## get the measurement type ID's
  mtype_ids = get_measurement_type_id(con, site_name,
                                      dsname, vars,
                                      add_new = FALSE)

  ## get measurement units
  mtypes_df = DBI::dbReadTable(con, 'measurement_types')
  mtypes_df = subset(mtypes_df, id %in% mtype_ids)

  ## get values from start_time to end_time
  sql_table = ifelse()
  sql_mtypes = paste(mtype_ids, collapse = ', ')
  q1 = paste('select * from ', tbl_name,
             ' where measurement_type_id in (',
             sql_mtypes, ") and time between '",
             start_time, "' and '", end_time, "'")
  df1 = DBI::dbGetQuery(con, q1)
  df1$measurement_name =
    mtypes_df$name[match(df1$measurement_type_id, mtypes_df$measurement_type_id)]
  df1$measurement_type_id = NULL

  ## rearrange
  value_df = tidyr::spread(df, measurement_name, value)
  flag_df = tidyr::spread(df, measurement_name, flagged)
  ## rename columns as needed
  names(value_df)[2:ncol(value_df)] =
    ifelse(names(value_df)[2:ncol(value_df)] %in% var_dict,
           var_dict[names(value_df)[2:ncol(value_df)]],
           names(value_df)[2:ncol(value_df)])
  names(flag_df)[2:ncol(flag_df)] =
    ifelse(names(flag_df)[2:ncol(flag_df)] %in% var_dict,
           var_dict[names(flag_df)[2:ncol(flag_df)]],
           names(flag_df)[2:ncol(flag_df)])
  ## add units to columns
  names(value_df)[2:ncol(value_df)] =
    paste(names(value_df)[2:ncol(value_df)], '(',
          mtypes_df$units[match(names(value_df)[2:ncol(value_df)], mtypes_df$name)],
          ')')
  names(flag_df)[2:ncol(flag_df)] =
    paste(names(flag_df)[2:ncol(flag_df)], '(NARSTO)')

  ## return data frame
  df2 = merge(value_df, flag_df)
  ## reorder columns
  ## ...
  df2
}
