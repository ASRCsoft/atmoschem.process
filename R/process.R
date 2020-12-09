## process data

#' @importFrom lubridate %within%
#' @importFrom Rdpack reprompt
NA

#' Flag aggregated data
#'
#' Return the NARSTO flag for an aggregated measurement (for example, hourly
#' average or maximum). See
#' \insertCite{@ @narsto_consensus_2001, p. 10;textual}{atmoschem.process}.
#'
#' @param p Proportion of valid measurements.
#' @param below_mdl Whether the aggregated value is below the method detection
#'   limit. MDLs for aggregates are generally lower than the limits for
#'   individual measurements.
#' @return A NARSTO flag.
#' @examples
#' narsto_agg_flag(.7, below_mdl = FALSE)
#'
#' @references \insertAllCited{}
#' @export
narsto_agg_flag = function(p, below_mdl = FALSE) {
  flag = rep('V0', length(p))
  flag[below_mdl] = 'V1'
  flag[p < .75] = 'V4'
  flag[p < .5] = 'M1'
  if (inherits(p, 'matrix')) {
    matrix(flag, nrow = nrow(p), ncol = ncol(p), dimnames = dimnames(p))
  } else {
    flag
  }
}

get_mtype_params = function(obj, m_id) {
  obj %>% tbl('measurement_types') %>%
    filter(id == m_id) %>%
    mutate(lower_range = lower(valid_range),
           upper_range = upper(valid_range),
           lower_inc = lower_inc(valid_range),
           upper_inc = upper_inc(valid_range)) %>%
    collect() %>%
    as.list()
}

get_flagged_periods = function(obj, m_id) {
  fp = obj %>% tbl('flagged_periods') %>%
    filter(measurement_type_id == m_id) %>%
    mutate(start_time = timezone('EST', lower(times)),
           end_time = timezone('EST', upper(times))) %>%
    select(start_time, end_time) %>%
    arrange(start_time) %>%
    collect()
  if (nrow(fp) == 0) return(vector())
  fp %>%
    mutate(interval = lubridate::interval(start_time, end_time)) %>%
    pull(interval)
}

in_interval = function(x, l, u, l_inc, u_inc) {
  res = TRUE
  if (!is.na(l)) {
    if (l_inc) {
      res = x >= l
    } else {
      res = x > l
    }
  }
  if (!is.na(u)) {
    if (u_inc) {
      res = res & x <= u
    } else {
      res = res & x < u
    }
  }
  res
}

#' Rolling median absolute deviation (MAD)
#'
#' Calculate the median absolute deviation (MAD) of a rolling window of values.
#'
#' The median absolute deviation (MAD) is a median-based alternative to the
#' standard deviation often preferred for its robustness to outliers
#' \insertCite{leys_detecting_2013}{atmoschem.process}. The MAD for a collection
#' of values \eqn{X} is defined as
#'
#' \deqn{1.4826 \times \textrm{med}(\left| X - \textrm{med}(X) \right|)}{1.4826 * med(|X - med(X)|)}
#'
#' where the constant 1.4826 scales the result to estimate the standard
#' deviation.
#'
#' @param x A vector of numbers.
#' @param k Width of the rolling window (an odd integer).
#' @return A vector of MADs.
#' @examples
#' rolling_mad(rnorm(20), 5)
#'
#' @references \insertAllCited{}
#' @export
rolling_mad = function(x, k) {
  n = length(x)
  k2 = floor(k / 2)
  ## surprisingly this seems to be the most practical way to calculate
  ## running medians with NA values in R
  m = caTools::runquantile(x, k, .5)
  ## for each segment of sequential equal medians, get rolling median
  ## of |x - median|
  seg_inds = c(1, which(m[2:n] != m[1:(n - 1)]) + 1, n + 1)
  seg_m = m[seg_inds]
  mads = lapply(2:length(seg_inds), function(y) {
    ## index bounds, padding the range with k/2 values on each side
    ## (if available)
    lpad = max(1, seg_inds[y - 1] - k2)
    upad = min(n, seg_inds[y] - 1 + k2)
    ## the bounds, removing the padding
    lbound = seg_inds[y - 1] - lpad + 1
    ubound = seg_inds[y] - lpad
    caTools::runquantile(abs(x[lpad:upad] - seg_m[y - 1]), k, .5)[lbound:ubound]
  })
  ## multiply by 1.4826 to estimate standard deviation
  unlist(mads) * 1.4826
}

#' Rolling outlier detection using the Hampel identifier
#'
#' Detect outliers in a rolling window using the Hampel identifier.
#'
#' The Hampel identifier uses the median absolute deviation (MAD) and a
#' threshold to identify outliers based on their distance from the median
#' \insertCite{davies_identification_1993}{atmoschem.process}. This is a robust
#' alternative to the commonly used thresholds \eqn{mean \pm 3 \sigma}{mean
#' +/-3sd} to identify outliers
#' \insertCite{leys_detecting_2013}{atmoschem.process}.
#'
#' Values are classified as outliers when
#' 
#' \deqn{\frac{\left| X_i - \textrm{med}(X) \right|}{\textrm{MAD}(X)} >
#' threshold}{|X_i - med(X)| / MAD(X) > threshold}
#'
#' When the MAD is zero this equation is undefined. In this case the function
#' returns FALSE.
#'
#' @param x A vector of numbers.
#' @param k Width of the rolling window (an odd integer).
#' @param threshold Threshold for labeling outliers. For normally distributed
#'   data this is equivalent to standard deviations.
#' @return A vector of boolean values, TRUE if the value is an outlier.
#' @examples
#' # test a dataset with an outlier
#' x <- rnorm(20)
#' x[3] <- 10
#' hampel_outlier(x, 5)
#'
#' @references \insertAllCited{}
#' @export
hampel_outlier = function(x, k, threshold = 3.5) {
  medians = caTools::runquantile(x, k, probs = .5)
  mads = rolling_mad(x, k)
  (mads != 0) & (abs(x - medians) / mads > threshold)
}

is_flagged = function(obj, m_id, times, x, flagged = FALSE) {
  if (length(times) == 0) {
    return(logical(0))
  }
  ## treat missing flagged values as false
  flagged = is_true(flagged)
  ## get the mtype attributes
  mtype = get_mtype_params(obj, m_id)

  flagged_periods = as.list(get_flagged_periods(obj, m_id))
  in_flagged_period = times %within% flagged_periods

  ## check for outliers
  if (!is.na(mtype$remove_outliers) && mtype$remove_outliers) {
    is_outlier = x %>%
      ## don't use previously flagged data (often indicating
      ## calibrations) during outlier detection
      replace(flagged | in_flagged_period, NA) %>%
      { if (is_true(mtype$spike_log_transform)) log(.) else . } %>%
      hampel_outlier(mtype$spike_window) %>%
      replace(., is.na(.), FALSE)
  } else {
    is_outlier = FALSE
  }

  ## check for invalid numbers
  if (!is.na(mtype$valid_range)) {
    is_valid = in_interval(x, mtype$lower_range, mtype$upper_range,
                           mtype$lower_inc, mtype$upper_inc)
    is_valid[is.na(is_valid)] = FALSE
  } else {
    is_valid = TRUE
  }

  ## check for abrupt jumps
  if (!is.na(mtype$max_jump)) {
    is_jump = c(FALSE, abs(diff(x)) > mtype$max_jump)
    is_jump[is.na(is_jump)] = FALSE
  } else {
    is_jump = FALSE
  }
  flagged | in_flagged_period | is_outlier | !is_valid | is_jump
}

process = function(obj, msmts, m_id) {
  m_params = get_mtype_params(obj, m_id)
  ## if (nrow(msmts) == 0) {
  ##   warning('No measurements found.')
  ##   return(data.frame())
  ## }
  if (is_true(m_params$has_calibration)) {
    msmts$value = apply_cal(obj, m_id, msmts$time, msmts$value)
  }
  if (is_true(m_params$apply_ce)) {
    msmts$value = msmts$value / estimate_ces(obj, m_id, msmts$time)
  }
  msmts$flagged = is_flagged(obj, m_id, msmts$time, msmts$value,
                             msmts$flagged)
  msmts = msmts[, c('measurement_type_id', 'time', 'value', 'flagged')]
  attributes(msmts$time)$tzone = 'EST'
  msmts
}
