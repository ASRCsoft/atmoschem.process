## organize calibration

get_cal_zeros = function(obj, m_id, times) {
  if (!is(times, 'POSIXct')) stop("'times' must be of class POSIXct.")
  zeros = obj %>%
    tbl('calibration_results') %>%
    filter(measurement_type_id == m_id,
           type == 'zero',
           !flagged,
           !is.na(measured_value)) %>%
    select(time, measured_value) %>%
    arrange(time) %>%
    collect()
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

get_cal_spans = function(obj, m_id, times) {
  if (!is(times, 'POSIXct')) stop("'times' must be of class POSIXct.")
  spans = obj %>%
    tbl('calibration_results') %>%
    filter(measurement_type_id == m_id,
           type == 'span',
           !flagged,
           !is.na(measured_value),
           !is.na(provided_value)) %>%
    select(time, measured_value, provided_value) %>%
    arrange(time) %>%
    collect()
  if (nrow(spans) == 0) {
    warning('No spans found.')
    return(rep(NA, length(times)))
  }
  ## get the estimated zero values at the corresponding times
  spans$zero = get_cal_zeros(obj, m_id, spans$time)
  spans$ratio = (spans$measured_value - spans$zero) /
    spans$provided_value
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
  zeros = get_cal_zeros(obj, m_id, times)
  spans = get_cal_spans(obj, m_id, times)
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
           !flagged,
           !is.na(measured_value),
           !is.na(provided_value)) %>%
    mutate(measurement_type_id =
             get_measurement_type_id(obj$con, 'WFMS', 'derived',
                                     'NO2'),
           measured_value =
             apply_cal(obj$con, wfms_nox_id, time, measured_value)) %>%
    select(time, measured_value, provided_value) %>%
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
             apply_cal(obj$con, psp_no_id, time, measured_value)) %>%
    select(time, measured_no)
  nox_tbl = obj %>%
    tbl('calibration_results') %>%
    filter(measurement_type_id == psp_nox_id,
           type == 'CE',
           !flagged,
           !is.na(measured_value),
           !is.na(provided_value)) %>%
    mutate(measured_nox =
             apply_cal(obj$con, psp_nox_id, time, measured_value)) %>%
    select(time, measured_nox, provided_value)
  no_tbl %>%
    inner_join(nox_tbl) %>%
    mutate(measurement_type_id =
             get_measurement_type_id(obj$con, 'PSP', 'derived', 'NO2'),
           measured_value = measured_nox - measured_no) %>%
    select(time, measured_value, provided_value) %>%
    arrange(time) %>%
    collect()
}

get_ces = function(obj, m_id, times) {
  if (!is(times, 'POSIXct')) stop("'times' must be of class POSIXct.")
  ces = obj %>%
    tbl('calibration_results') %>%
    filter(measurement_type_id == m_id,
           type == 'CE',
           !flagged,
           !is.na(measured_value),
           !is.na(provided_value)) %>%
    select(time, measured_value, provided_value) %>%
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
  approx(ces$time, ces$smoothed_value, times, rule = 2)$y
}
