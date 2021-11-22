## organize calibration

# try `approx`, return NAs if it fails
try_approx = function(x, y, xout, method, ...) {
  if (!length(y) || all(is.na(y))) {
    rep(NA, times = length(xout))
  } else {
    approx(x, y, xout, method, ...)
  }
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
  x_counts = sapply(x_list, function(x) length(unique(x)))
  y_counts = sapply(y_list, function(x) sum(!is.na(x)))
  method_list =
    as.list(ifelse(x_counts >= 2 & y_counts >= 2, 'linear', 'constant'))
  res_list = mapply(try_approx, x_list, y_list, xout_list, method_list,
                    MoreArgs = list(...), SIMPLIFY = FALSE)
  unlist(lapply(res_list, function(x) x$y), use.names = FALSE)
}

## running median with observations split into independent groups
## given by `segments`
piecewise_runmed = function(x, k, segments, ...) {
  res_list =
    by(x, segments, function(x, ...) suppressWarnings(runmed(x, k, ...)), ...)
  unlist(res_list, use.names = FALSE)
}

#' Estimate calibration values
#'
#' Estimate calibration values by applying a median filter and interpolating
#' between the filtered values.
#'
#' @param x Calibration check times (POSIXct).
#' @param y Calibration check values.
#' @param k Median filter window width. If \code{NA} no smoothing is applied.
#' @param xout Times at which to estimate the calibration value.
#' @param breaks Times of discontinuities caused by instrument adjustments.
#' @return Estimated calibration values at times \code{xout}.
#' @export
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
    yout = piecewise_approx(x, smoothed_y, xout, breaks, rule = 2)
    # Some segments may not have any values, resulting in NAs. In this case
    # interpolate between the nearest values from neighboring segments.
    if (any(is.na(yout))) {
      # get the values from the beginning and end of each segment (immediately
      # before and immediately after each break)
      x2 = rep(breaks, each = 2) -
        rep(as.difftime(1:0, units = "secs"), length(breaks))
      y2 = piecewise_approx(x, smoothed_y, x2, breaks, rule = 2)
      # fill the missing y's by interpolating between nearest segment end values
      yout[is.na(yout)] = approx(x2, y2, xout[is.na(yout)], rule = 2)$y
    }
    yout
  } else {
    approx(x, smoothed_y, xout, rule = 2)$y
  }
}

#' Instrument drift corrections
#'
#' Apply instrument drift corrections based on calibration check results.
#'
#' Zero and span values are estimated using \code{\link{estimate_cals}}. If not
#' provided, zeros and spans are assumed to be 0 and 1, respectively. The
#' corrected values are calculated as \eqn{(measurement - zero) / span}.
#'
#' @param t Measurement times (POSIXct).
#' @param v Measurement values.
#' @param z Data frame of zero calibration checks. Should contain columns
#'   \code{time}, \code{value}, and \code{corrected} (logical, whether the
#'   instrument was adjusted).
#' @param s Data frame of span calibration checks. Should contain columns
#'   \code{time}, \code{value}, and \code{corrected} (logical, whether the
#'   instrument was adjusted).
#' @param config list of Configuration options from the \code{measurement_types}
#'   config table.
#' @return Drift-corrected measurements.
#' @seealso \code{\link{estimate_cals}}
#' @export
drift_correct = function(t, v, z = NULL, s = NULL, config) {
  # zeros
  if (!is.null(z) && nrow(z)) {
    # zero values estimated at the measurement times
    z_breaks = z$time[is_true(z$corrected)]
    zeros = estimate_cals(z$time, z$value, config$zero_smooth_window, t,
                          z_breaks)
  } else {
    zeros = NULL
  }
  # spans
  if (!is.null(s) && nrow(s)) {
    # span ratios estimated at the measurement times
    s_breaks = s$time[is_true(s$corrected)]
    spans = estimate_cals(s$time, s$value, config$span_smooth_window, t,
                          s_breaks)
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
ceff_correct = function(t, v, ceff, config) {
  # get smoothed conversion efficiencies estimated at the measurement times
  ceff_breaks = ceff$time[is_true(ceff$corrected)]
  smoothed_efficiency = estimate_cals(ceff$time, ceff$value,
                                      config$ce_smooth_window, t, ceff_breaks)
  # conversion efficiencies can't go above 1
  v / pmin(smoothed_efficiency, 1)
}
