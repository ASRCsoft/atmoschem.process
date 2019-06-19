## get manual calibrations from pdf cal sheets

## Get the fields from a calibration pdf form.
read_pdf_form = function(f) {
  f1 = staplr::get_fields(f)
  ## simplify the list
  lapply(f1, function(x) x$value)
}

## return True if the box is checked, otherwise False
box_checked = function(box) !is.na(box) & box == 'On'

format_pdf_time = function(date_str, time_str)
  strptime(paste(date_str, time_str), '%d-%b-%y %H:%M', tz = 'EST')

## return a list of calibration information
organize_cal = function(pdf, type, calibrated,
                        start_time, end_time,
                        provided, measured, corrected) {
  list(type = type,
       calibrated = box_checked(pdf[[calibrated]]),
       start_time = pdf[[start_time]],
       end_time = pdf[[end_time]],
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
                                            zero_cal = 'zero_cal_mode_2',
                                            zero_start_time = 'time_log_2',
                                            zero_end_time = 'time_log_3',
                                            zero_corrected = 'set_42ctls_to_zero_3',
                                            zero_provided = 0,
                                            zero_measured = 'measured_zero_noy_a_3',
                                            span_cal = 'span_cal_mode_4',
                                            span_start_time = 'time_log_4',
                                            span_end_time = 'time_log_5',
                                            span_corrected = 'set_span_noy_a_5',
                                            span_provided = 14,
                                            span_measured = 'measured_span_noy_a_5',
                                            zero_check_cal = 'zero_check_7',
                                            zero_check_start_time = 'time_log_4',
                                            zero_check_end_time = 'time_log_5',
                                            zero_check_corrected = 'set_42ctls_to_zero_7',
                                            zero_check_provided = 0, 
                                            zero_check_measured = 'zero_check_7') {
  pdf = read_pdf_form(f)
  res = data.frame()
  zero_vals = organize_cal(pdf, 'zero', zero_cal,
                           zero_start_time,
                           zero_end_time, zero_provided,
                           zero_measured, zero_corrected)
  span_vals = organize_cal(pdf, 'span', span_cal,
                           span_start_time,
                           span_end_time, span_provided,
                           span_measured, span_corrected)
  zero_check_vals = organize_cal(pdf, 'zero check', zero_check_cal,
                                 zero_check_start_time,
                                 zero_check_end_time, zero_check_provided,
                                 zero_check_measured, zero_check_corrected)
  pdf_cals = list(zero = zero_vals, span = span_vals,
                  zero_check = zero_check_vals)
  for (cal in pdf_cals) {
    if (cal$calibrated && !is.na(cal$end_time)) {
      start_time = format_pdf_time(pdf[[date]], cal$start_time)
      end_time = format_pdf_time(pdf[[date]], cal$end_time)
      res1 = transform_cal_section(measurement_name, cal$type,
                                   start_time, end_time,
                                   cal$provided, cal$measured,
                                   cal$corrected)
      res = rbind(res, res1)
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
