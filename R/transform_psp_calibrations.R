## get PSP calibrations

read_excel_cell = function(f, cell, ...) {
  as.data.frame(readxl::read_excel(f, range = cell, ...))[1, 1]
}

parse_psp_cal_time = function(d, time) {
  ## occasionally a formatting error (I think?) causes us to get the
  ## time as a string
  if (class(time)[1] == 'character') {
    as.POSIXct(paste(format(d, '%F'), time))
  } else if (startsWith(class(time)[1], 'POSIX')) {
    as.POSIXct(paste(format(d, '%F'), format(time, '%T')))
  } else {
    NA
  }
}

## get times from the header section
read_psp_cal_times = function(f, date_cell = 'L11', start_cell = 'Z11',
                              end_cell = 'AE11') {
  cal_date = read_excel_cell(f, date_cell, col_names = F)
  start_time = read_excel_cell(f, start_cell, col_names = F)
  end_time = read_excel_cell(f, end_cell, col_names = F)
  ## occasionally a formatting error (I think?) causes us to get the
  ## time as a string
  cal_start = parse_psp_cal_time(cal_date, start_time)
  cal_end = parse_psp_cal_time(cal_date, end_time)
  ## errors with end time render the calibration unusable
  if (is.na(cal_end)) {
    warning(paste('Unable to retrieve calibration time from file', f))
    return(list(start_time = NULL, end_time = NULL))
  }
  list(start_time = cal_start, end_time = cal_end)
}

empty_measurements = function() {
  data.frame(measurement_type_id = integer(0),
             type = character(0),
             cal_time = vector(),
             provided_value = numeric(0),
             measured_value = numeric(0),
             corrected = logical(0))
}

transform_psp_42C_calibrations = function(pg, f) {
  ## need to get "zero and span checks" section
  df1 = readxl::read_xls(f, range = 'F33:AD35', col_names = F)
  cals = as.data.frame(df1[c(1, 3), c(1, 8, 14)])
  names(cals) = c('cert_span', 'zero', 'span')
  row.names(cals) = c('NO', 'NOx')
  
  cal_end = read_psp_cal_times(f, 'K10', 'Z10', 'AF10')$end_time
  if (is.null(cal_end)) {
    return(empty_measurements())
  }

  cal_df = expand.grid(c('NO', 'NOx'), c('zero', 'span'))
  names(cal_df) = c('measurement_name', 'type')
  add_new_measurement_types(pg, 'PSP', 'envidas',
                            cal_df$measurement_name)
  cal_df$measurement_type_id =
    get_measurement_type_id(pg, 'PSP', 'envidas',
                            cal_df$measurement_name)
  data.frame(measurement_type_id = cal_df$measurement_type_id,
             type = cal_df$type,
             cal_time = cal_end,
             provided_value = c(rep(0, 2), cals$cert_span),
             measured_value = cals[as.matrix(cal_df[, 1:2])],
             corrected = FALSE)
}

transform_psp_API300EU_calibrations = function(pg, f) {
  ## need to get "zero and span checks" section
  df = readxl::read_xlsx(f, range = 'U36:AA38', col_names = F)
  cals = as.data.frame(df[c(1, 3), c(1, ncol(df))])
  names(cals) = c('provided_value', 'measured_value')
  row.names(cals) = c('zero', 'span')
  
  cal_end = read_psp_cal_times(f)$end_time
  if (is.null(cal_end)) {
    return(empty_measurements())
  }

  add_new_measurement_types(pg, 'PSP', 'envidas', 'CO')
  data.frame(measurement_type_id = get_measurement_type_id(pg, 'PSP', 'envidas', 'CO'),
             type = row.names(cals),
             cal_time = cal_end,
             provided_value = cals$provided_value,
             measured_value = cals$measured_value,
             corrected = FALSE)
}

transform_psp_ASRC_TEI42i_Y_NOy_146i_calibrations = function(pg, f) {
  ## need to get "zero and span checks" section
  ## df1 = readxl::read_excel(f, range = 'F33:AD35', col_names = F)
  df1 = readxl::read_xlsx(f, range = 'F33:AD35', col_names = F)
  cals = as.data.frame(df1[c(1, 3), c(1, 8, 14)])
  names(cals) = c('cert_span', 'zero', 'span')
  row.names(cals) = c('NO-DEC', 'NOy-DEC')
  
  cal_end = read_psp_cal_times(f)$end_time
  if (is.null(cal_end)) {
    return(empty_measurements())
  }

  cal_df = expand.grid(c('NO-DEC', 'NOy-DEC'), c('zero', 'span'))
  names(cal_df) = c('measurement_name', 'type')
  add_new_measurement_types(pg, 'PSP', 'envidas',
                            cal_df$measurement_name)
  cal_df$measurement_type_id =
    get_measurement_type_id(pg, 'PSP', 'envidas',
                            cal_df$measurement_name)
  data.frame(measurement_type_id = cal_df$measurement_type_id,
             type = cal_df$type,
             cal_time = cal_end,
             provided_value = c(rep(0, 2), cals$cert_span),
             measured_value = cals[as.matrix(cal_df[, 1:2])],
             corrected = FALSE)
}

transform_psp_calibrations = function(pg, f) {
  ## figure out which function to send this file to
  if (is_psp_42C_cal(f)) {
    transform_psp_42C_calibrations(pg, f)
  } else if (is_psp_API300EU_cal(f)) {
    transform_psp_API300EU_calibrations(pg, f)
  } else if (is_psp_ASRC_TEI42i_Y_NOy_146i_cal(f)) {
    transform_psp_ASRC_TEI42i_Y_NOy_146i_calibrations(pg, f)
  } else {
    warning(paste('Transform not implemented for', f))
  }
}
