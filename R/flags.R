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
  medians = caTools::runquantile(x, k, .5)
  mads = caTools::runmad(x, k, medians)
  (mads != 0) & (abs(x - medians) / mads > threshold)
}

is_flagged = function(x, config, flagged = FALSE) {
  if (length(x) == 0) {
    return(logical(0))
  }
  ## treat missing flagged values as false
  flagged = is_true(flagged)

  ## check for outliers
  if (!is.na(config$remove_outliers) && config$remove_outliers) {
    is_outlier = x %>%
      ## don't use previously flagged data (often indicating
      ## calibrations) during outlier detection
      replace(flagged, NA) %>%
      { if (is_true(config$spike_log_transform)) log(.) else . } %>%
      hampel_outlier(config$spike_window) %>%
      replace(., is.na(.), FALSE)
  } else {
    is_outlier = FALSE
  }

  ## check for invalid numbers
  if (!is.na(config$valid_range)) {
    is_valid = in_interval(x, config$lower_range, config$upper_range,
                           config$lower_inc, config$upper_inc)
    is_valid[is.na(is_valid)] = FALSE
  } else {
    is_valid = TRUE
  }

  ## check for abrupt jumps
  if (!is.na(config$max_jump)) {
    is_jump = c(FALSE, abs(diff(x)) > config$max_jump)
    is_jump[is.na(is_jump)] = FALSE
  } else {
    is_jump = FALSE
  }
  flagged | is_outlier | !is_valid | is_jump
}
