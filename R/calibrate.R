## organize calibration

## see if a dbplyr result exists
dbplyr_exists = function(x) {
  as.logical(nrow(as.data.frame(head(x, 1))))
}

## interpolate a function with discontinuities at `breaks`. `...` is
## passed to `approx`. `int_missing` will interpolate values from the
## previous and next segment for segments that don't contain values.
piecewise_approx = function(x, y, xout, breaks, ..., interp_missing = TRUE) {
  ## remove missing values
  val_missing = is.na(x) | is.na(y)
  x = x[!val_missing]
  y = y[!val_missing]
  ## split into segments
  x_segments = findInterval(x, breaks)
  x_list = split(x, x_segments)
  y_list = split(y, x_segments)
  xout_segments = findInterval(xout, breaks)
  xout_list = split(xout, xout_segments)
  ## match x and y lists with xout
  x_list = x_list[names(xout_list)]
  y_list = y_list[names(xout_list)]
  if (interp_missing) {
    ## see if any sections have no values
    empty_inds = which(lengths(x_list) == 0)
    ## fill in missing segments
    if (length(empty_inds)) {
      if (length(x_list) == 1) stop('No non-missing y data')
      ## the xout_list names are the break numbers
      break_inds = as.integer(names(xout_list)[empty_inds])
      for (n in empty_inds) {
        xs = vector(mode = 'numeric')
        ys = vector(mode = 'numeric')
        ## first and last segments are special cases
        if (n > 1) {
          ## get the last value from the previous segment, place it at
          ## the beginning of this segment
          xs = breaks[n]
          ys = tail(y_list[[n - 1]], 1)
        }
        if (n < length(x_list)) {
          ## get the first value from the next segment, place it at
          ## the end of this segment
          xs = c(xs, breaks[n + 1])
          ys = c(ys, y_list[[n + 1]][1])
        }
        x_list[[n]] = xs
        y_list[[n]] = ys
      }
    }
  }
  method_list = as.list(ifelse(lengths(x_list) >= 2, 'linear',
                               'constant'))
  res_list = mapply(approx, x_list, y_list, xout_list, method_list,
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

drift_correct = function(t, v, z = NULL, s = NULL, f = NULL, config) {
  # zeros
  if (!is.null(z) && nrow(z)) {
    # zero values estimated at the measurement times
    z_breaks = z$time[z$corrected]
    zeros = estimate_cals(z$time, z$measured_value, config$zero_smooth_window,
                          t, z_breaks)
  } else {
    zeros = NULL
  }
  # spans
  if (!is.null(s) && nrow(s)) {
    if (!is.null(f) && nrow(f)) {
      # flow values estimated at the span times
      f_breaks = f$time[f$corrected]
      provided_value = estimate_cals(f$time, f$measured_value, NA, s$time,
                                     f_breaks)
    } else {
      provided_value = s$provided_value
    }
    if (!is.null(zeros)) {
      # apply zero corrections to measured spans
      s_zeros = estimate_cals(z$time, z$measured_value,
                              config$zero_smooth_window, s$time, z_breaks)
      measured_value = s$measured_value - s_zeros
    } else {
      measured_value = s$measured_value
    }
    # convert to ratio
    ratio = measured_value / provided_value
    # span ratios estimated at the measurement times
    s_breaks = s$time[s$corrected]
    spans = estimate_cals(s$time, ratio, config$span_smooth_window, t, s_breaks)
  } else {
    spans = NULL
  }
  # adjust the measurement values
  res = v
  if (!is.null(zeros)) res = res - zeros
  if (!is.null(spans)) res = res / spans
  res
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

estimate_flows = function(obj, m_name, times) {
  if (!is(times, 'POSIXct')) stop("'times' must be of class POSIXct.")
  gil_tbl = obj %>%
    tbl('gilibrator') %>%
    filter(measurement_name == m_name) %>%
    mutate(time = timezone('EST', time)) %>%
    arrange(time)
  spans = gil_tbl %>%
    filter(!is.na(measured_value)) %>%
    collect()
  if (nrow(spans) == 0) {
    warning('No gilibrator spans found')
    return(rep(NA, length(times)))
  }
  breaks = gil_tbl %>%
    filter(!is.na(changed) & changed) %>%
    pull(time)
  estimate_cals(spans$time, spans$measured_value,
                NA, times, breaks)
}

get_cal_spans = function(obj, m_id) {
  m_params = get_mtype_params(obj, m_id)
  has_gilibrator = !is.na(m_params$gilibrator_span)
  spans = obj %>%
    tbl('calibration_results') %>%
    filter(measurement_type_id == m_id,
           type == 'span',
           !is.na(measured_value))
  if (!has_gilibrator) {
    ## if there are no gilibrator measurements then the provided value
    ## must come from the calibration table
    spans = spans %>%
      filter(!is.na(provided_value))
  }
  if (!dbplyr_exists(spans)) return(spans)
  spans = spans %>%
    mutate(time = timezone('EST', time)) %>%
    select(time, measured_value, provided_value, flagged) %>%
    arrange(time) %>%
    collect()
  if (has_gilibrator) {
    ## get the provided value from gilibrator measurements
    spans = spans %>%
      mutate(provided_value =
               estimate_flows(obj, m_params$gilibrator_span, time))
  }
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
    return(ces)
  }
  m_params = get_mtype_params(obj, m_id)
  if (!is.na(m_params$gilibrator_ce)) {
    ces$provided_value = estimate_flows(obj, m_params$gilibrator_ce,
                                        ces$time)
  }
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
  smoothed_ces = estimate_cals(ces$time, ces$efficiency,
                               m_params$ce_smooth_window, times,
                               breaks)
  ## conversion efficiencies can't go above 1
  pmin(smoothed_ces, 1)
}
