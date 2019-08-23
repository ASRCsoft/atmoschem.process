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
  if (is.na(cal_end) || cal_end < cal_start) {
    warning(paste('Unable to retrieve calibration time from file', f))
    return(NULL)
  }
  time_strs = format(c(cal_start, cal_end), '%Y-%m-%d %H:%M:%S')
  paste0('[', time_strs[1], ',', time_strs[2], ')')
}

## get "zero and span checks" section
read_psp_NO_cal_table = function(f, trange = 'F33:AD35',
                                 chem_names = c('NO', 'NOx'),
                                 cal_times, ce_row = 42) {
  ## gotta help out readxl a bit with the macro files
  if (endsWith(f, 'm')) {
    readf = readxl::read_xlsx
  } else {
    readf = readxl::read_xls
  }
  ## first the zero and span
  df = readf(f, range = trange, col_names = F)
  cals = as.data.frame(df[c(1, 3), c(1, 8, 14)])
  names(cals) = c('cert_span', 'zero', 'span')
  row.names(cals) = chem_names
  cal_df = expand.grid(chem_names, c('zero', 'span'))
  names(cal_df) = c('measurement_name', 'type')
  provided_vals = as.numeric(c(rep(0, 2), cals$cert_span))
  measured_vals = as.numeric(cals[as.matrix(cal_df[, 1:2])])
  res1 = data.frame(measurement_name = cal_df$measurement_name,
                    type = cal_df$type,
                    times = cal_times,
                    provided_value = provided_vals,
                    measured_value = measured_vals,
                    corrected = FALSE)
  ## and now the conversion efficiencies
  ce_range = paste0('O', ce_row, ':W', ce_row)
  df2 = readf(f, range = ce_range, col_names = F)
  ces = as.data.frame(df2[, c(1, 9)])
  names(ces) = c('calibrator', 'response')
  responses = strsplit(as.character(ces$response), ' +')[[1]]
  if (length(responses) == 1) {
    res2 = data.frame(measurement_name = cal_df$measurement_name[2],
                      type = 'CE',
                      times = cal_times,
                      provided_value = ces$calibrator,
                      measured_value = ces$response,
                      corrected = FALSE)
  } else if (length(responses) == 2) {
    res2 = data.frame(measurement_name = cal_df$measurement_name[1:2],
                      type = 'CE',
                      times = cal_times,
                      provided_value = ces$calibrator,
                      measured_value = responses,
                      corrected = FALSE)
  } else {
    stop("Can't parse conversion efficiencies.")
  }
  rbind(res1, res2)
}

## get "zero, span and PC checks" section
read_psp_cal_table = function(f, trange = 'U36:AA40',
                              chem_name = 'CO', cal_times) {
  df = readxl::read_xlsx(f, range = trange, col_names = F)
  cals = as.data.frame(df[c(1, nrow(df)), c(1, ncol(df))])
  names(cals) = c('provided_value', 'measured_value')
  row.names(cals) = c('span', 'zero')
  data.frame(measurement_name = chem_name,
             type = row.names(cals),
             times = cal_times,
             provided_value = cals$provided_value,
             measured_value = cals$measured_value,
             corrected = FALSE)
}

empty_measurements = function() {
  data.frame(measurement_name = character(0),
             type = character(0),
             times = character(0),
             provided_value = numeric(0),
             measured_value = numeric(0),
             corrected = logical(0))
}

transform_psp_NO_calibrations = function(f, date_cell = 'L11',
                                         start_cell = 'Z11', end_cell = 'AE11',
                                         trange = 'F33:AD35',
                                         chem_names = c('NO', 'NOx'),
                                         ce_row = 42) {
  cal_times = read_psp_cal_times(f, date_cell, start_cell,
                                 end_cell)
  if (is.null(cal_times)) {
    return(empty_measurements())
  }
  read_psp_NO_cal_table(f, trange, chem_names,
                        cal_times, ce_row)
}

transform_psp_single_calibrations = function(f, date_cell = 'L11',
                                             start_cell = 'Z11', end_cell = 'AE11',
                                             trange = 'U36:AA40',
                                             chem_name = 'CO') {  
  cal_times = read_psp_cal_times(f, date_cell, start_cell,
                                 end_cell)
  if (is.null(cal_times)) {
    return(empty_measurements())
  }
  read_psp_cal_table(f, trange, chem_name, cal_times)
}

transform_psp_42C_calibrations = function(f) {
  transform_psp_NO_calibrations(f, 'K10', 'Z10', 'AF10',
                                trange = 'F33:AD35',
                                chem_names = c('NO', 'NOx'))
}

transform_psp_API300EU_calibrations = function(f) {  
  transform_psp_single_calibrations(f, trange = 'U38:AA42',
                                    chem_name = 'CO')
}

transform_psp_ASRC_TEI42i_Y_NOy_calibrations = function(f) {
  transform_psp_NO_calibrations(f,
                                chem_names = c('NOy-HNO3', 'NOy'),
                                ce_row = 45)
}

transform_psp_DEC_TEI42i_NOy_calibrations = function(f) {
  transform_psp_NO_calibrations(f, 'K11', trange = 'F34:AD36',
                                chem_names = c('NO-DEC', 'NOy-DEC'),
                                ce_row = 46)
}

transform_psp_TEI43i_SO2_calibrations = function(f) {  
  transform_psp_single_calibrations(f, chem_name = 'SO2')
}

transform_psp_TEI49i_O3_49i_calibrations = function(f) {  
  transform_psp_single_calibrations(f, 'L10', 'Z10', 'AE10',
                                    trange = 'U35:AA39',
                                    chem_name = 'Thermo_O3')
}

transform_psp_calibrations = function(f) {
  ## figure out which function to send this file to
  if (is_psp_42C_cal(f)) {
    transform_psp_42C_calibrations(f)
  } else if (is_psp_API300EU_cal(f)) {
    transform_psp_API300EU_calibrations(f)
  } else if (is_psp_ASRC_TEI42i_Y_NOy_cal(f)) {
    transform_psp_ASRC_TEI42i_Y_NOy_calibrations(f)
  } else if (is_psp_DEC_TEI42i_NOy_cal(f)) {
    transform_psp_DEC_TEI42i_NOy_calibrations(f)
  } else if (is_psp_TEI43i_SO2_cal(f)) {
    transform_psp_TEI43i_SO2_calibrations(f)
  } else if (is_psp_TEI49i_O3_49i_cal(f)) {
    transform_psp_TEI49i_O3_49i_calibrations(f)
  } else {
    warning(paste('Transform not implemented for', f))
    NULL
  }
}
