## get manual calibrations from pdf cal sheets

## Get the fields from a calibration pdf form.
read_pdf_form = function(f) {
  f1 = staplr::get_fields(f)
  ## simplify the list
  lapply(f1, function(x) {
    if (is.na(x$value) || x$value == '') {
      NA
    } else {
      x$value
    }
  })
}

## return True if the box is checked, otherwise False
box_checked = function(box) !is.na(box) & box == 'On'

format_pdf_time = function(pdf, time_label)
  strptime(paste(pdf$date, unlist(pdf[time_label])), '%d-%b-%y %H:%M', tz = 'EST')

get_cal_start_time = function(start_time, offline_time, measured_times) {
  ## get the start time, guessing if needed
  prev_measured_times = measured_times[1:(length(measured_times) - 1)]
  cur_measured_time = tail(measured_times, 1)
  if (!is.na(start_time)) {
    start_time
  } else if (length(measured_times) > 1 &&
             !all(is.na(prev_measured_times))) {
    ## get the previous measured time if one is available
    tail(na.omit(prev_measured_times), 1)
  } else if (!is.na(offline_time)) {
    ## get the switch flag offline time
    offline_time
  } else if (!is.na(cur_measured_time)) {
    ## get the measured time - 40 minutes
    measured_time - as.difftime(40, units = 'mins')
  }
}

get_cal_end_time = function(start_times, online_time, measured_time) {
  ## get the end time, guessing if needed
  next_start_times = start_times[2:length(start_times)]
  if (length(start_times) > 1 && !all(is.na(next_start_times))) {
    ## get the next start time if one is available
    na.omit(next_start_times)[1]
  } else if (!is.na(online_time)) {
    ## get the switch flag online time
    online_time
  } else if (!is.na(measured_time)) {
    ## get the measured time + 10 minutes
    measured_time + as.difftime(10, units = 'mins')
  } else if (!is.na(start_times[1])) {
    ## get the start time + 40 minutes
    start_times[1] + as.difftime(40, units = 'mins')
  }
}

transform_wfm_cal_list = function(pdf, measurement_name,
                                  offline_time = 'time_log_1',
                                  calibrated = c('zero_cal_mode_2',
                                                 'span_cal_mode_4',
                                                 'zero_mode_6'),
                                  start_time = c('time_log_2',
                                                 'time_log_4',
                                                 'time_log_6'),
                                  measured_time = c('time_log_3',
                                                    'time_log_5',
                                                    'time_log_7'),
                                  corrected = c('set_42ctls_to_zero_3',
                                                'set_span_noy_a_5',
                                                NA),
                                  provided = c(0, 14, 0),
                                  measured = c('measured_zero_3',
                                               'measured_span_5',
                                               'zero_check_7'),
                                  online_time = 'time_log_8') {
  offline_time = format_pdf_time(pdf, offline_time)
  online_time = format_pdf_time(pdf, online_time)
  types = c('zero', 'span', 'zero check')
  start_times = format_pdf_time(pdf, start_time)
  measured_times = format_pdf_time(pdf, measured_time)
  res = data.frame()
  for (n in 1:3) {
    performed_cal = box_checked(pdf[[calibrated[n]]])
    if (performed_cal) {
      start_time = get_cal_start_time(start_times[n], offline_time,
                                      measured_times[1:n])
      end_time = get_cal_end_time(start_times[n:3], online_time,
                                  measured_times[n])
      if (!is.null(start_time) && !is.null(end_time)) {
        times_str = paste0('[', start_time, ', ', end_time, ']')
        bool_corrected = !is.na(corrected[n]) &&
          corrected[n] %in% names(pdf) &&
          box_checked(pdf[[corrected[n]]])
        res1 = data.frame(measurement_name, type = types[n],
                          times = times_str,
                          provided_value = provided[n],
                          measured_value = pdf[[measured[n]]],
                          corrected = bool_corrected)
        res = rbind(res, res1)
      }
    }
  }
  res
}

transform_wfm_single_cal = function(f, ...) {
  pdf = read_pdf_form(f)
  transform_wfm_cal_list(pdf, ...)
}

transform_wfm_no_cal = function(f, measurement_names, provided_span) {
  ## this is based on the "42Cs_calsheet_v04" format
  pdf = read_pdf_form(f)
  ## this one has different cal checkbox labels
  provided = c(0, provided_span, 0)
  res1 = transform_wfm_cal_list(pdf, measurement_names[1],
                                measured = c('measured_zero_noy_a_3',
                                             'measured_span_noy_a_5',
                                             '42ctls_zero_noy_a_7'),
                                corrected = c('set_42ctls_to_zero_3',
                                              'set_span_noy_a_5',
                                              NA),
                                provided = provided)
  res2 = transform_wfm_cal_list(pdf, measurement_names[2],
                                measured = c('measured_zero_noy_b_3',
                                             'measured_span_noy_b_5',
                                             '42ctls_zero_noy_b_7'),
                                corrected = c('set_42ctls_to_zero_3',
                                              'set_span_noy_b_5',
                                              NA),
                                provided = provided)
  rbind(res1, res2)
}

transform_wfml_42i = function(f) {
  transform_wfm_no_cal(f, c('NO', 'NOX'), 14)
}

transform_wfml_48C = function(f) {
  df48C = transform_wfm_single_cal(f, 'CO',
                                   calibrated = c('zero_cal_mode_2',
                                                  'span_cal_mode_4',
                                                  'zero_cal_check_6'),
                                   corrected = c('set_48C_zero_offset_ 3',
                                                 'set_48C_span_5',
                                                 NA),
                                   provided = c(0, 786, 0))
  ## need to multiply by 1000 because cal values are recorded in
  ## different units than the measurements!
  df48C$measured_value = as.numeric(df48C$measured_value) * 1000
  df48C
}

transform_wfms_300EU = function(f) {
  transform_wfm_single_cal(f, 'CO',
                           calibrated = c('zero_cal_mode_2',
                                          'span cal mode 4',
                                          'zero cal check 6'),
                           start_time = c('time_log_2',
                                          'time log 4',
                                          'time log 6'),
                           measured_time = c('time log 3',
                                             'time log 5',
                                             'time log 7'),
                           corrected = c('set 300EU to zero 3',
                                         'set 300EU span 5',
                                         NA),
                           provided = c(0, 452, 0),
                           measured = c('measured zero 3',
                                        'measured span 5',
                                        'zero check 300EU ppb'),
                           online_time = 'time log 8')
}

transform_wfms_42C = function(f) {
  transform_wfm_no_cal(f, c('NO', 'NOx'), 4)
}

transform_wfms_42Cs = function(f) {
  transform_wfm_no_cal(f, c('NO', 'NOy'), 4)
}

transform_wfms_43C = function(f) {
  transform_wfm_single_cal(f, 'SO2',
                           corrected = c('set_43c_to_zero_3',
                                         'set_span_so2_5',
                                         NA),
                           measured = c('measured_zero_so2_3',
                                        'measured_span_so2_5',
                                        '43c_zero_7'),
                           provided = c(0, 5.9, 0))
}
