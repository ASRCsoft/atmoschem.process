## organize calibration

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
    z_breaks = z$time[is_true(z$corrected)]
    zeros = estimate_cals(z$time, z$measured_value, config$zero_smooth_window,
                          t, z_breaks)
  } else {
    zeros = NULL
  }
  # spans
  if (!is.null(s) && nrow(s)) {
    if (!is.null(f) && nrow(f)) {
      # flow values estimated at the span times
      f_breaks = f$time[is_true(f$changed)]
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
    s_breaks = s$time[is_true(s$corrected)]
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

# apply conversion efficiency corrections to measured values
ceff_correct = function(t, v, ceff, f = NULL, config) {
  if (!is.null(f) && nrow(f)) {
    # flow values estimated at the ceff times
    f_breaks = f$time[is_true(f$changed)]
    provided_value = estimate_cals(f$time, f$measured_value, NA, ceff$time,
                                   f_breaks)
  } else {
    provided_value = ceff$provided_value
  }
  raw_efficiency = ceff$measured_value / provided_value
  # get smoothed conversion efficiencies estimated at the measurement times
  ceff_breaks = ceff$time[is_true(ceff$corrected)]
  smoothed_efficiency = estimate_cals(ceff$time, raw_efficiency,
                                      config$ce_smooth_window, t, ceff_breaks)
  # conversion efficiencies can't go above 1
  v / pmin(smoothed_efficiency, 1)
}
