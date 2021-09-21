## useful functions

is_true = function(x) !is.na(x) & x

is_psp_42C_cal = function(f)
  startsWith(basename(f), 'Pinnacle_42C')
is_psp_API300EU_cal = function(f)
  startsWith(basename(f), 'Pinnacle_API300EU_CO_Weekly')
is_psp_ASRC_TEI42i_Y_NOy_cal = function(f)
  startsWith(basename(f), 'Pinnacle_ASRC_TEI42i_Y_NOy_146i_Weekly') |
    startsWith(basename(f), 'Pinnacle_ASRC_TEI42i_Y_NOy_146i_WEEKLY') |
    startsWith(basename(f), 'Pinnacle_ASRC_TEI42i_Y_NOy_T700_Weekly')
is_psp_DEC_TEI42i_NOy_cal = function(f)
  startsWith(basename(f), 'Pinnacle DEC TEI42i NOy Weekly') |
    startsWith(basename(f), 'Pinnacle_DEC_TEI42i_NOy_Weekly')
is_psp_TEI43i_SO2_cal = function(f)
  startsWith(basename(f), 'Pinnacle_TEI43i_SO2_Weekly') |
    startsWith(basename(f), 'Pinnacle_TEI43i_SO2_146i_Weekly')
is_psp_TEI49i_O3_49i_cal = function(f)
  startsWith(basename(f), 'Pinnacle_TEI49i_O3_49i_Weekly') |
    startsWith(basename(f), 'Pinnacle_TEI49i_O3_Weekly')

#' @export
read_csv_dir = function(f, ...) {
  csvs = list.files(f, '\\.csv$', full.names = TRUE)
  res = lapply(csvs, read.csv, ...)
  names(res) = sub('\\.csv$', '', basename(csvs))
  res
}
