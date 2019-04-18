## get PSP calibrations

transform_psp_42C_calibrations = function(pg, f) {
  ## need to get "zero and span checks" section
  df1 = readxl::read_xls(f, range = 'F33:AD35', col_names = F)
  cals = as.data.frame(df1[c(1, 3), c(1, 8, 14)])
  names(cals) = c('cert_span', 'zero', 'span')
  row.names(cals) = c('NO', 'NOx')
  
  ## get times from the header section
  df2 = as.data.frame(readxl::read_xls(f, range = 'K10:AF10', col_names = F))
  cal_date = df2[1, 1]
  start_time = df2[1, 16]
  end_time = df2[1, 22]
  ## occasionally a formatting error (I think?) causes us to get the
  ## time as a string
  if (class(start_time)[1] == 'character') {
    cal_start = as.POSIXct(paste(format(cal_date, '%F'), start_time))
  } else {
    cal_start = as.POSIXct(paste(format(cal_date, '%F'), format(start_time, '%T')))
  }
  ## errors with end time render the calibration unusable
  if (is.na(end_time)) {
    warning(paste('Unable to retrieve calibration time from file', f))
    return(NULL)
  }
  cal_end = as.POSIXct(paste(format(cal_date, '%F'), format(end_time, '%T')))

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

transform_psp_calibrations = function(pg, f) {
  ## figure out which function to send this file to
  if (substr(basename(f), 1, 12) == 'Pinnacle_42C') {
    transform_psp_42C_calibrations(pg, f)
  } else {
    warning(paste('Transform not implemented for', f))
  }
}
