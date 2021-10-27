## useful functions

is_true = function(x) !is.na(x) & x

#' @export
read_csv_dir = function(f, ...) {
  csvs = list.files(f, '\\.csv$', full.names = TRUE)
  res = lapply(csvs, read.csv, ...)
  names(res) = sub('\\.csv$', '', basename(csvs))
  res
}
