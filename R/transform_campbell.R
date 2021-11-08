## transform measurement files

wfms_flags = c(NO = 'NOX', NO2 = 'NOX', NOx = 'NOX', T = 'TRH', RH = 'TRH',
               NOy = 'NOY', SO2 = 'SO2', CO = 'CO')
wfml_flags = c(CO = 'CO', NO = 'NOX', NO2 = 'NOX', NGN3 = 'NGN')

read_campbell = function(f) {
  ## headers are on the second line, but data starts on line 4
  headers = read.csv(f, header=F, skip = 1, nrows=1,
                     stringsAsFactors = F)
  na_strings = c('NAN', 'NaN')
  df = read.csv(f, header=F, skip = 4, col.names = headers,
                na.strings = na_strings, stringsAsFactors = F)
  ## replace 'TIMESTAMP' with 'instrument_time' to match other data
  ## tables
  names(df)[1] = 'instrument_time'
  ## replace 'CupA' with 'Cup' for consistency over time
  names(df) = gsub('CupA', 'Cup', names(df))
  # ... except for the newer columns that I want to keep for now
  names(df) = gsub('Cup_S', 'CupA_S', names(df))
  ## remove empty columns
  df[, -grep('Spare', names(df), ignore.case = T)]
}

#' Transform raw data into a common format
#'
#' Read a raw data file and return a data frame of measurements in a
#' standardized format.
#'
#' @param f Filepath of the raw data file.
#' @param site Site abbreviation (only needed when format varies by site).
#' @return A data frame of measurements. The first column is "time" (POSIXct),
#'   followed by two columns for each parameter-- "value.param_name" and
#'   "flagged.param_name". "flagged.param_name" columns are all logical vectors,
#'   where flagged=TRUE indicates that the measurement shouldn't be used.
#' @describeIn transform Campbell datalogger files (WFMS and WFMB).
#' @export
transform_campbell = function(f, site) {
  campbell = read_campbell(f)
  # clean and reorganize the data
  is_flag = grepl('^F_', names(campbell))
  if (site == 'WFMS') {
    flag_mat = as.matrix(campbell[, is_flag]) != 1
    flag_dict = wfms_flags
  } else if (site == 'WFMB') {
    flag_mat = as.matrix(campbell[, is_flag]) != 0
    flag_dict = wfml_flags
  }
  campbell2 = campbell[, !is_flag]
  campbell2$RECORD = NULL
  for (i in 2:ncol(campbell2)) {
    varname = names(campbell2)[i]
    flagname = paste0('flagged.', varname)
    varname2 = sub('_Avg', '', varname)
    if (varname2 %in% names(flag_dict)) {
      flag_col = paste0('F_', flag_dict[varname2], '_Avg')
      if (flag_col %in% colnames(flag_mat)) {
        campbell2[, flagname] = flag_mat[, flag_col]
      }
    } else {
      campbell2[, flagname] = FALSE
    }
    names(campbell2)[i] = paste0('value.', names(campbell2)[i])
  }
  names(campbell2)[1] = 'time'
  campbell2$time = as.POSIXct(campbell2$time, tz = 'EST')
  campbell2
}
