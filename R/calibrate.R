## organize calibration

get_cal_zeros = function(obj, m_id) {
  obj %>%
    tbl('calibration_results') %>%
    filter(measurement_type_id == m_id,
           type == 'zero',
           !is.na(measured_value)) %>%
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
  m_params = get_mtype_params(obj, m_id)
  if (is.na(m_params$zero_smooth_window)) {
    zeros$smoothed_value = zeros$measured_value
  } else {
    zeros$smoothed_value = runmed(zeros$measured_value,
                                  m_params$zero_smooth_window)
  }
  approx(zeros$time, zeros$smoothed_value,
         times, rule = 2)$y
}

get_cal_spans = function(obj, m_id) {
  spans = obj %>%
    tbl('calibration_results') %>%
    filter(measurement_type_id == m_id,
           type == 'span',
           !is.na(measured_value),
           !is.na(provided_value)) %>%
    select(time, measured_value, provided_value, flagged) %>%
    arrange(time) %>%
    collect()
  if (nrow(spans) == 0) {
    return(spans)
  }
  ## get the estimated zero values at the corresponding times
  spans %>%
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
  m_params = get_mtype_params(obj, m_id)
  if (is.na(m_params$zero_smooth_window)) {
    spans$smoothed_value = spans$ratio
  } else {
    spans$smoothed_value = runmed(spans$ratio,
                                  m_params$span_smooth_window)
  }
  approx(spans$time, spans$smoothed_value,
         times, rule = 2)$y
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
             get_measurement_type_id(obj$con, 'WFMS', 'derived',
                                     'NO2'),
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
    mutate(measured_nox =
             apply_cal(obj$con, psp_nox_id, time, measured_value),
           nox_flagged = flagged) %>%
    select(time, measured_nox, provided_value, flagged)
  no_tbl %>%
    inner_join(nox_tbl) %>%
    mutate(measurement_type_id =
             get_measurement_type_id(obj$con, 'PSP', 'derived', 'NO2'),
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
    select(time, measured_value, provided_value, flagged) %>%
    arrange(time) %>%
    collect()
  if (nrow(ces) == 0) {
    wfms_no2_id = get_measurement_type_id(obj$con, 'WFMS', 'derived',
                                          'NO2')
    psp_no2_id = get_measurement_type_id(obj$con, 'PSP', 'derived',
                                         'NO2')
    if (m_id == wfms_no2_id) {
      ces = wfms_no2_ce_inputs(obj)
    } else if (m_id == psp_no2_id) {
      ces = psp_no2_ce_inputs(obj)
    }
  }
  if (nrow(ces) == 0) {
    return(spans)
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
  m_params = get_mtype_params(obj, m_id)
  ces$smoothed_value = runmed(ces$efficiency, 31)
  approx(ces$time, ces$smoothed_value, times, rule = 2)$y
}
