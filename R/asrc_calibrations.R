# get manual calibrations from pdf cal sheets

#' Get fields from a pdf form
#'
#' Get the field values from a pdf form (AcroForms). This function is a
#' streamlined alternative to \code{staplr::get_fields} that's about twice as
#' fast.
#'
#' @param f Path to pdf file.
#' @return A list of the field values.
#' @examples
#' # read the staplr example file
#' pdf = system.file('testForm.pdf', package = 'staplr')
#' head(read_pdf_form(pdf))
#'
#' @seealso \link[staplr]{get_fields}
#' @importFrom magrittr %>%
#' @export
read_pdf_form = function(f) {
  out = tempfile()
  cmd = paste(staplr:::pdftk_cmd(), shQuote(f), "dump_data_fields", "output",
              shQuote(out))
  system(cmd)
  on.exit(file.remove(out))
  data.frame(lines = readLines(out)) %>%
    transform(field_id = cumsum(lines == '---')) %>%
    subset(lines != '---') %>%
    transform(attr = sub('Field([^:]*):.*', '\\1', lines),
              val = sub('Field[^:]*: +(.*)', '\\1', lines)) %>%
    subset(attr != 'StateOption') %>%
    reshape(timevar = 'attr', idvar = 'field_id', direction = 'wide',
            drop = 'lines') %>%
    with(setNames(val.Value, val.Name)) %>%
    as.list
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
    cur_measured_time - as.difftime(40, units = 'mins')
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

# Return the labels for the pdf time entries, ordered 1 to 8. `x` is a vector of
# labels
get_time_labels = function(x) {
  labels = x[grepl('time[ _]log[ _][0-9]+', x)]
  stopifnot(length(labels) == 8)
  n = as.integer(sub('[^0-9]*', '', labels))
  stopifnot(all(sort(n) == 1:8))
  labels[order(n)]
}

# Return the labels for the didcheck check boxes, ordered 2, 4, 6 (zero, span,
# second zero). `x` is a vector of labels
get_didcheck_labels = function(x) {
  labels = x[grepl('(zero|span)[ _](cal[ _])?(mode|check)[ _][246]', x)]
  stopifnot(length(labels) == 3)
  n = as.integer(sub('[^0-9]*', '', labels))
  stopifnot(all(sort(n) == c(2, 4, 6)))
  labels[order(n)]
}

# Return the labels for the corrected check boxes, ordered 3, 5 (zero,
# span). `x` is a vector of labels. Some files contain 2 parameters, and in that
# case the function will return different values depending on which parameter (1
# or 2) is selected.
get_corrected_labels = function(x, param = NULL) {
  if (is.null(param)) {
    regex = 'set.*zero.*3|set.*span.*5'
  } else {
    if (param == 1) {
      regex = 'set.*zero.*3|set.*span.*a_5'
    } else if (param == 2) {
      regex = 'set.*zero.*3|set.*span.*b_5'
    }
  }
  labels = x[grepl(regex, x)]
  stopifnot(length(labels) == 2)
  n = as.integer(sub('.*[^0-9]*', '', labels))
  stopifnot(all(sort(n) == c(3, 5)))
  labels[order(n)]
}

transform_wfm_cal_list = function(pdf, measurement_name,
                                  provided = c(0, 14, 0),
                                  measured = c('measured_zero_3',
                                               'measured_span_5',
                                               'zero_check_7'),
                                  param = NULL) {
  time_entries = format_pdf_time(pdf, get_time_labels(names(pdf)))
  offline_time = time_entries[1]
  start_times = time_entries[c(2, 4, 6)]
  measured_times = time_entries[c(3, 5, 7)]
  online_time = time_entries[8]
  didcheck = get_didcheck_labels(names(pdf))
  corrected = box_checked(unlist(pdf[get_corrected_labels(names(pdf), param)]))
  types = c('zero', 'span', 'zero check')
  res = data.frame()
  for (n in 1:3) {
    ## Hey! need to look at more info here to determine if the
    ## calibration happened-- occasionally people forget to check the
    ## box
    performed_cal = box_checked(pdf[[didcheck[n]]])
    if (performed_cal) {
      start_time = get_cal_start_time(start_times[n], offline_time,
                                      measured_times[1:n])
      end_time = get_cal_end_time(start_times[n:3], online_time,
                                  measured_times[n])
      # This is an embarrassing workaround to get the correct span values for
      # the WFMS NO autocals. The span numbers really shouldn't be hardcoded in
      # the first place and I should just rewrite how this whole thing works so
      # that it gets the values from the autocal schedule.
      if (end_time > '2020-08-12 06:00' & end_time < '2020-08-19 06:00' &
          measurement_name %in% c('NO', 'NOx', 'NO 42Cs', 'NOy')) {
        # make sure it's the WFMS and not WFML instrument
        if (provided[2] == 4) provided[2] = 4.2
      }

      if (!is.null(start_time) && !is.null(end_time)) {
        times_str = paste0('[', start_time, ', ', end_time, ']')
        res1 = data.frame(measurement_name, type = types[n],
                          times = times_str,
                          provided_value = provided[n],
                          measured_value = pdf[[measured[n]]],
                          corrected = n < 3 && corrected[n])
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
                                provided = provided, param = 1)
  res2 = transform_wfm_cal_list(pdf, measurement_names[2],
                                measured = c('measured_zero_noy_b_3',
                                             'measured_span_noy_b_5',
                                             '42ctls_zero_noy_b_7'),
                                provided = provided, param = 2)
  rbind(res1, res2)
}

#' @export
transform_wfml_42i = function(f) {
  transform_wfm_no_cal(f, c('NO', 'NOX'), 14)
}

#' @export
transform_wfml_48C = function(f) {
  df48C = transform_wfm_single_cal(f, 'CO', provided = c(0, 450, 0))
  ## need to multiply by 1000 because cal values are recorded in
  ## different units than the measurements!
  df48C$measured_value =
    as.numeric(as.character(df48C$measured_value)) * 1000
  df48C
}

#' @export
transform_wfms_300EU = function(f) {
  df300EU = transform_wfm_single_cal(f, 'CO',
                                     provided = c(0, 452, 0),
                                     measured = c('measured zero 3',
                                                  'measured span 5',
                                                  'zero check 300EU ppb'))
  ## for whatever reason these cal values are being recorded as an
  ## offset from 200 rather than from zero
  df300EU$measured_value =
    as.numeric(as.character(df300EU$measured_value)) + 200
  df300EU
}

#' @export
transform_wfms_42C = function(f) {
  transform_wfm_no_cal(f, c('NO', 'NOx'), 4)
}

#' @export
transform_wfms_42Cs = function(f) {
  transform_wfm_no_cal(f, c('NO 42Cs', 'NOy'), 4)
}

#' @export
transform_wfms_43C = function(f) {
  transform_wfm_single_cal(f, 'SO2',
                           measured = c('measured_zero_so2_3',
                                        'measured_span_so2_5',
                                        '43c_zero_7'),
                           provided = c(0, 5.9, 0))
}
