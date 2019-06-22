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
                                  calibrated = c('zero_cal_mode_2',
                                                 'span_cal_mode_4',
                                                 'zero_cal_check_6'),
                                  corrected = c('set_42ctls_to_zero_3',
                                                'set_span_noy_a_5',
                                                NA),
                                  provided = c(0, 14, 0),
                                  measured = c('measured_zero_3',
                                               'measured_span_5',
                                               'zero_check_7')) {
  ## the time labels don't change
  offline_time = format_pdf_time(pdf, 'time_log_1')
  online_time = format_pdf_time(pdf, 'time_log_8')
  types = c('zero', 'span', 'zero check')
  start_times =
    format_pdf_time(pdf, c('time_log_2', 'time_log_4', 'time_log_6'))
  measured_times =
    format_pdf_time(pdf, c('time_log_3', 'time_log_5', 'time_log_7'))
  res = data.frame()
  for (n in 1:3) {
    performed_cal = box_checked(pdf[[calibrated[n]]])
    if (performed_cal) {
      end_time = get_cal_end_time(start_times[n:3], online_time,
                                  measured_times[n])
      if (!is.null(end_time)) {
        times_str = paste0('[', start_times[n], ', ', end_time, ']')
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

transform_wfm_no_calibration = function(f, measurement_names) {
  ## this is based on the "42Cs_calsheet_v04" format
  pdf = read_pdf_form(f)
  ## this one has different cal checkbox labels
  calibrated = c('zero_cal_mode_2', 'span_cal_mode_4',
                 'zero_mode_6')
  res1 = transform_wfm_cal_list(pdf, measurement_names[1],
                                calibrated = calibrated,
                                measured = c('measured_zero_noy_a_3',
                                             'measured_span_noy_a_5',
                                             '42ctls_zero_noy_a_7'),
                                corrected = c('set_42ctls_to_zero_3',
                                              'set_span_noy_a_5',
                                              NA),
                                provided = c(0, 14, 0))
  res2 = transform_wfm_cal_list(pdf, measurement_names[2],
                                calibrated = calibrated,
                                measured = c('measured_zero_noy_b_3',
                                             'measured_span_noy_b_5',
                                             '42ctls_zero_noy_b_7'),
                                corrected = c('set_42ctls_to_zero_3',
                                              'set_span_noy_b_5',
                                              NA),
                                provided = c(0, 14, 0))
  rbind(res1, res2)
}

transform_wfml_48C = function(f) {
  transform_wfm_single_cal(f, 'CO',
                           corrected = c('set_48C_zero_offset_ 3',
                                         'set_48C_span_5',
                                         NA),
                           provided = c(0, .786, 0))
}
