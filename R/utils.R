## useful functions

#' @export
is_true = function(x) !is.na(x) & x

# R's lead and lag functions aren't so useful. These are better.
#' @export
lag2 = function(x, k = 1) c(rep(NA, k), head(x, -k))
#' @export
lead2 = function(x, k = 1) c(tail(x, -k), rep(NA, k))

# add time around a lubridate interval
#' @export
pad_interval = function(interval, start, end) {
  interval(int_start(interval) - start, int_end(interval) + end)
}

#' @export
read_csv_dir = function(f, ...) {
  csvs = list.files(f, '\\.csv$', full.names = TRUE)
  res = lapply(csvs, read.csv, ...)
  names(res) = sub('\\.csv$', '', basename(csvs))
  res
}
