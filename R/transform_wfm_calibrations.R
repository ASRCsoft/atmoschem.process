## get manual calibrations from pdf cal sheets

## Get the fields from a calibration pdf form.
read_pdf_form = function(f) {
  f1 = staplr::get_fields(f)
  ## simplify the list
  lapply(f1, function(x) {
    if (x$value == '') {
      NA
    } else {
      x$value
    }
  })
}

## return the first non-NA value
first_non_na = function(x) x[!is.na(x)][1]

## return True if the box is checked, otherwise False
box_checked = function(box) !is.na(box) & box == 'On'

format_pdf_time = function(date_str, time_str)
  strptime(paste(date_str, time_str), '%d-%b-%y %H:%M', tz = 'EST')

## return a list of calibration information
organize_cal = function(pdf, type, calibrated,
                        start_time, measured_time,
                        provided, measured, corrected) {
  list(type = type,
       calibrated = box_checked(pdf[[calibrated]]),
       start_time = pdf[[start_time]],
       measured_time = pdf[[measured_time]],
       provided = provided,
       measured = pdf[[measured]],
       corrected = corrected %in% names(pdf) &&
         box_checked(pdf[[corrected]]))
}

transform_cal_section = function(measurement_name, cal_type,
                                 start_time, end_time, provided_value,
                                 measured_value, corrected) {
  times_str = paste0('[', start_time, ', ', end_time, ']')
  data.frame(measurement_name = measurement_name,
             type = cal_type,
             times = times_str,
             provided_value = provided_value,
             measured_value = measured_value,
             corrected = corrected)
}

transform_wfm_single_calibration = function(f, measurement_name,
                                            date = 'date',
                                            calibrated = c('zero_cal_mode_2',
                                                           'span_cal_mode_4',
                                                           'zero_check_7'),
                                            start_time = c('time_log_2',
                                                           'time_log_4',
                                                           'time_log_4'),
                                            measured_time = c('time_log_3',
                                                              'time_log_5',
                                                              'time_log_5'),
                                            corrected = c('set_42ctls_to_zero_3',
                                                          'set_span_noy_a_5',
                                                          'set_42ctls_to_zero_7'),
                                            provided = c(0, 14, 0),
                                            measured = c('measured_zero_noy_a_3',
                                                         'measured_span_noy_a_5',
                                                         'zero_check_7')) {
  pdf = read_pdf_form(f)
  res = data.frame()
  caldf = data.frame(calibrated, start_time,
                     measured_time, corrected,
                     provided, measured,
                     end_time = NA,
                     row.names = c('zero', 'span', 'zero check'))
  for (n in 1:nrow(caldf)) {
    if (calibrated[n]) {
      cal = organize_cal(pdf, row.names(caldf[n]),
                         calibrated[n], start_time[n],
                         measured_time[n], provided[n],
                         measured[n], corrected[n])
      ## get the end time, guessing if needed
      if (n < 3 && !all(is.na(caldf[(n + 1):3, 'start_time']))) {
        ## get the next start time if one is available
        cal$end_time =
          format_pdf_time(pdf[[date]],
                          first_non_na(caldf[(n + 1):3, 'start_time']))
      } else if (!is.na(pdf[[flag_online]])) {
        ## get the switch flag online time
        cal$end_time = format_pdf_time(pdf[[date]], pdf[[flag_online]])
      } else if (!is.na(cal$measured_time)) {
        ## get the measured time + 10 minutes
        cal$end_time = format_pdf_time(pdf[[date]], cal$measured_time) +
          as.difftime(10, units = 'minutes')
      } else if (!is.na(cal$start_time)) {
        ## get the start time + 40 minutes
        cal$end_time = format_pdf_time(pdf[[date]], cal$start_time) +
          as.difftime(40, units = 'minutes')
      }
      if ('end_time' %in% names(cal)) {
        start_time = format_pdf_time(pdf[[date]], cal$start_time)
        res1 = transform_cal_section(measurement_name, cal$type,
                                     start_time, cal$end_time,
                                     cal$provided, cal$measured,
                                     cal$corrected)
        res = rbind(res, res1)
      }
    }
  }
  res
}

transform_wfm_no_calibration = function(f, measurement_names) {
  ## this is based on the "42Cs_calsheet_v04" format
  pdf = read_pdf_form(f)
  res = data.frame()
  if (box_checked(pdf$zero_cal_mode_2) && !is.na(pdf$time_log_3)) {
    format_pdf_time(pdf$date, pdf$time_log_3)
    start_time = format_pdf_time(pdf$date, pdf$time_log_2)
    end_time = format_pdf_time(pdf$date, pdf$time_log_3)
    corrected = box_checked(pdf$set_42ctls_to_zero_3)
    measured_values = c(pdf$measured_zero_noy_a_3,
                        pdf$measured_zero_noy_b_3)
    res1 = transform_cal_section(measurement_names, 'zero',
                                 start_time, end_time,
                                 0, measured_values,
                                 corrected)
    res = rbind(res, res1)
  }
  if (box_checked(pdf$span_cal_mode_4) && !is.na(pdf$time_log_5)) {
    start_time = format_pdf_time(pdf$date, pdf$time_log_4)
    end_time = format_pdf_time(pdf$date, pdf$time_log_5)
    corrected = c(box_checked(pdf$set_span_noy_a_5),
                  box_checked(pdf$set_span_noy_b_5))
    measured_values = c(pdf$measured_span_noy_a_5,
                        pdf$measured_span_noy_b_5)
    res1 = transform_cal_section(measurement_names, 'span',
                                 start_time, end_time,
                                 14, measured_values,
                                 corrected)
    res = rbind(res, res1)
  }
  if (box_checked(pdf$zero_check_7) && !is.na(pdf$time_log_7)) {
    start_time = format_pdf_time(pdf$date, pdf$time_log_6)
    end_time = format_pdf_time(pdf$date, pdf$time_log_7)
    measured_values = c(pdf$`42ctls_zero_noy_a_7`,
                        pdf$`42ctls_zero_noy_b_7`)
    res1 = transform_cal_section(measurement_names, 'span',
                                 start_time, end_time,
                                 0, measured_values,
                                 FALSE)
    res = rbind(res, res1)
  }
  res
}

transform_wfml_48C = function(f) {
  transform_wfm_single_calibration(f, 'CO',
                                   zero_corrected = 'set_48C_zero_offset_ 3',
                                   zero_measured = 'measured_zero_3',
                                   span_corrected = 'set_48C_span_5',
                                   span_provided = .786,
                                   span_measured = 'measured_span_5',
                                   zero_check_corrected = NA)
}
