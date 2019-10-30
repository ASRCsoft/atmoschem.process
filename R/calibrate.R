## organize calibration

## see if a dbplyr result exists
dbplyr_exists = function(x) {
  as.logical(nrow(as.data.frame(head(x, 1))))
}

## interpolate a function with discontinuities at `breaks`
piecewise_approx = function(x, y, xout, breaks, ...) {
  x_segments = findInterval(x, breaks)
  x_list = split(x, x_segments)
  y_list = split(y, x_segments)
  xout_segments = findInterval(xout, breaks)
  xout_list = split(xout, xout_segments)
  ## match x and y lists with xout
  x_list = x_list[names(xout_list)]
  y_list = y_list[names(xout_list)]
  res_list = mapply(approx, x_list, y_list, xout_list,
                    MoreArgs = list(...), SIMPLIFY = FALSE)
  unlist(lapply(res_list, function(x) x$y), use.names = FALSE)
}

## running median with observations split into independent groups
## given by `segments`
piecewise_runmed = function(x, k, segments, ...) {
  res_list = by(x, segments, runmed, k = k, ...)
  unlist(res_list, use.names = FALSE)
}

get_cal_breaks = function(obj, m_id, cal_type) {
  breaks = obj %>%
    tbl('manual_calibrations') %>%
    filter(measurement_type_id == m_id,
           type == cal_type,
           corrected)
  if (!dbplyr_exists(breaks)) return(NULL)
  breaks %>%
    mutate(time = timezone('EST', upper(times))) %>%
    arrange(time) %>%
    pull(time)
}

estimate_cals = function(x, y, k, xout, breaks) {
  has_smoothing = !is.na(k) && k > 1
  has_breaks = length(breaks) > 0 &&
    !(length(breaks) == 1 && is.na(breaks))
  if (!has_smoothing) {
    ## no smoothing
    smoothed_y = y
  } else if (has_breaks) {
    ## smoothing separated by breaks
    segment = findInterval(x, breaks)
    smoothed_y = piecewise_runmed(y, k, segment)
  } else {
    ## regular smoothing
    smoothed_y = runmed(y, k)
  }
  if (has_breaks) {
    piecewise_approx(x, smoothed_y, xout, breaks, rule = 2)
  } else {
    approx(x, smoothed_y, xout, rule = 2)$y
  }
}

get_cal_zeros = function(obj, m_id) {
  obj %>%
    tbl('calibration_results') %>%
    filter(measurement_type_id == m_id,
           type == 'zero',
           !is.na(measured_value)) %>%
    mutate(time = timezone('EST', time)) %>%
    select(time, measured_value, flagged) %>%
    arrange(time) %>%
    collect()
}

estimate_zeros = function(obj, m_id, times) {
  if (!is(times, 'POSIXct')) stop("'times' must be of class POSIXct.")
  zeros = get_cal_zeros(obj, m_id) %>%
    filter(!flagged)
  if (nrow(zeros) == 0) {
    warning('No zeros found.')
    return(rep(NA, length(times)))
  }
  breaks = get_cal_breaks(obj, m_id, 'zero')
  m_params = get_mtype_params(obj, m_id)
  estimate_cals(zeros$time, zeros$measured_value,
                m_params$zero_smooth_window, times, breaks)
}

get_cal_spans = function(obj, m_id) {
  spans = obj %>%
    tbl('calibration_results') %>%
    filter(measurement_type_id == m_id,
           type == 'span',
           !is.na(measured_value),
           !is.na(provided_value))
  if (!dbplyr_exists(spans)) return(spans)
  spans = spans %>%
    mutate(time = timezone('EST', time)) %>%
    select(time, measured_value, provided_value, flagged) %>%
    arrange(time) %>%
    collect() %>%
    ## get the estimated zero values at the corresponding times
    mutate(zero = estimate_zeros(obj, m_id, time),
           ratio = (measured_value - zero) / provided_value)
}

estimate_spans = function(obj, m_id, times) {
  if (!is(times, 'POSIXct')) stop("'times' must be of class POSIXct.")
  spans = get_cal_spans(obj, m_id) %>%
    filter(!flagged)
  if (nrow(spans) == 0) {
    warning('No spans found.')
    return(rep(NA, length(times)))
  }
  breaks = get_cal_breaks(obj, m_id, 'span')
  m_params = get_mtype_params(obj, m_id)
  estimate_cals(spans$time, spans$ratio,
                m_params$span_smooth_window, times, breaks)
}

apply_cal = function(obj, m_id, times, x) {
  zeros = estimate_zeros(obj, m_id, times)
  spans = estimate_spans(obj, m_id, times)
  if (!is.na(spans[1])) {
    ## (spans depend on zeros so this is actually a check that both
    ## exist)
    (x - zeros) / spans
  } else if (!is.na(zeros[1])) {
    x - zeros
  } else {
    warning('No calibration data found.')
    x
  }
}

wfms_no2_ce_inputs = function(obj) {
  wfms_nox_id = get_measurement_type_id(obj$con, 'WFMS', 'campbell',
                                        'NOx')
  obj %>%
    tbl('calibration_results') %>%
    filter(measurement_type_id == wfms_nox_id,
           type == 'CE',
           !is.na(measured_value),
           !is.na(provided_value)) %>%
    mutate(measurement_type_id =
             get_measurement_type_id(obj$con, 'WFMS', 'campbell',
                                     'NO2'),
           time = timezone('EST', time),
           measured_value =
             apply_cal(obj$con, wfms_nox_id, time, measured_value)) %>%
    select(time, measured_value, provided_value, flagged) %>%
    arrange(time) %>%
    collect()
}

psp_no2_ce_inputs = function(obj) {
  psp_no_id = get_measurement_type_id(obj$con, 'PSP', 'envidas', 'NO')
  psp_nox_id = get_measurement_type_id(obj$con, 'PSP', 'envidas',
                                       'NOx')
  no_tbl = obj %>%
    tbl('calibration_results') %>%
    filter(measurement_type_id == psp_no_id,
           type == 'CE',
           !flagged,
           !is.na(measured_value),
           !is.na(provided_value)) %>%
    mutate(time = timezone('EST', time)) %>%
    collect() %>%
    mutate(measured_no =
             apply_cal(obj$con, psp_no_id, time, measured_value),
           no_flagged = flagged) %>%
    select(time, measured_no, no_flagged)
  nox_tbl = obj %>%
    tbl('calibration_results') %>%
    filter(measurement_type_id == psp_nox_id,
           type == 'CE',
           !is.na(measured_value),
           !is.na(provided_value)) %>%
    mutate(time = timezone('EST', time)) %>%
    collect() %>%
    mutate(measured_nox =
             apply_cal(obj$con, psp_nox_id, time, measured_value),
           nox_flagged = flagged) %>%
    select(time, measured_nox, provided_value, nox_flagged)
  no_tbl %>%
    inner_join(nox_tbl) %>%
    mutate(measurement_type_id =
             get_measurement_type_id(obj$con, 'PSP', 'envidas', 'NO2'),
           measured_value = measured_nox - measured_no,
           flagged = no_flagged | nox_flagged) %>%
    select(time, measured_value, provided_value, flagged) %>%
    arrange(time) %>%
    collect()
}

get_ces = function(obj, m_id) {
  ces = obj %>%
    tbl('calibration_results') %>%
    filter(measurement_type_id == m_id,
           type == 'CE',
           !is.na(measured_value),
           !is.na(provided_value)) %>%
    mutate(time = timezone('EST', time)) %>%
    select(time, measured_value, provided_value, flagged) %>%
    arrange(time) %>%
    collect()
  if (nrow(ces) == 0) {
    wfms_no2_id = get_measurement_type_id(obj$con, 'WFMS', 'campbell',
                                          'NO2')
    psp_no2_id = get_measurement_type_id(obj$con, 'PSP', 'envidas',
                                         'NO2')
    if (m_id == wfms_no2_id) {
      ces = wfms_no2_ce_inputs(obj)
    } else if (m_id == psp_no2_id) {
      ces = psp_no2_ce_inputs(obj)
    }
  }
  if (nrow(ces) == 0) {
    return(ces)
  }
  m_params = get_mtype_params(obj, m_id)
  if (!is.na(m_params$has_calibration) && m_params$has_calibration) {
    ces$efficiency = apply_cal(obj, m_id, ces$time, ces$measured_value) /
      ces$provided_value
  } else {
    ces$efficiency = ces$measured_value / ces$provided_value
  }
  ces
}

estimate_ces = function(obj, m_id, times) {
  if (!is(times, 'POSIXct')) stop("'times' must be of class POSIXct.")
  ces = get_ces(obj, m_id) %>%
    filter(!flagged)
  if (nrow(ces) == 0) {
    warning('No conversion efficiencies found.')
    return(rep(NA, length(times)))
  }
  breaks = numeric() # currently no way to adjust CE values
  m_params = get_mtype_params(obj, m_id)
  estimate_cals(ces$time, ces$efficiency, m_params$ce_smooth_window,
                times, breaks)
}
