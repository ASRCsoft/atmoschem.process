## organize calibration

get_cal_zeros = function(obj, m_id, times) {
  if (!is(times, 'POSIXct')) stop("'times' must be of class POSIXct.")
  zeros = obj %>%
    tbl('calibration_zeros') %>%
    filter(measurement_type_id == m_id) %>%
    select(time, value) %>%
    arrange(time) %>%
    collect()
  if (nrow(zeros) == 0) {
    warning('No zeros found.')
    return(rep(NA, length(times)))
  }
  m_params = get_mtype_params(obj, m_id)
  zeros$smoothed_value = runmed(zeros$value, m_params$zero_smooth_window)
  approx(zeros$time, zeros$smoothed_value,
         times, rule = 2)$y
}

get_cal_spans = function(obj, m_id, times) {
  if (!is(times, 'POSIXct')) stop("'times' must be of class POSIXct.")
  spans = obj %>%
    tbl('calibration_spans') %>%
    filter(measurement_type_id == m_id) %>%
    select(time, value, provided_value) %>%
    arrange(time) %>%
    collect()
  if (nrow(spans) == 0) {
    warning('No spans found.')
    return(rep(NA, length(times)))
  }
  ## get the estimated zero values at the corresponding times
  spans$zero = get_cal_zeros(obj, m_id, spans$time)
  spans$ratio = (spans$value - spans$zero) / spans$provided_value
  m_params = get_mtype_params(obj, m_id)
  spans$smoothed_value = runmed(spans$ratio, m_params$span_smooth_window)
  approx(spans$time, spans$smoothed_value,
         times, rule = 2)$y
}

apply_cal = function(obj, m_id, times, x) {
  zeros = get_cal_zeros(obj, m_id, times)
  spans = get_cal_spans(obj, m_id, times)
  ifelse(is.na(spans[0]), x - zeros, (x - zeros) / spans)
}

get_ces = function(obj, m_id, times) {
  if (!is(times, 'POSIXct')) stop("'times' must be of class POSIXct.")
  ces = obj %>%
    tbl('conversion_efficiency_inputs') %>%
    filter(measurement_type_id == m_id) %>%
    select(time, measured_value, provided_value) %>%
    arrange(time) %>%
    collect()
  if (nrow(ces) == 0) {
    warning('No conversion efficiencies found.')
    return(rep(NA, length(times)))
  }
  m_params = get_mtype_params(obj, m_id)
  if (!is.na(m_params$has_calibration) && m_params$has_calibration) {
    ces$efficiency = apply_cal(obj, m_id, ces$time, ces$measured_value) /
      ces$provided_value
  } else {
    ces$efficiency = ces$measured_value / ces$provided_value
  }
  ces$smoothed_value = runmed(ces$efficiency, 31)
  approx(ces$time, ces$smoothed_value,
         times, rule = 2)$y
}
