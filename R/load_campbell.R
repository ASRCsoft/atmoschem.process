#!/usr/bin/env Rscript
## getting campbell data into postgres

## run this script from the command line like so:
## Rscript load_campbell.R dbname ~/data/campbell/WFMS/WFMFS_SUMMIT_Table1_2018_06_29_2300.dat

library(dbx)
library(tidyr)

wfms_flags = c(NO = 'NOX', NO2 = 'NOX', T = 'TRH',
               RH = 'TRH', NOy = 'NOY', SO2 = 'SO2')
wfml_flags = c(CO = 'CO', NO = 'NOX', NO2 = 'NOX')

fast_lookup = function(vals, dict) {
  ## This code to get dictionary (named vector) entries shouldn't need
  ## to be this long but the current version of R seems to have a very
  ## slow lookup function for named vectors, so we need extra code to
  ## work around that.
  res = rep(NA, length(vals))
  has_entry = vals %in% names(dict)
  res[has_entry] = dict[vals[has_entry]]
  res
}

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
  ## remove empty columns
  df[, -grep('Spare|Phase', names(df), ignore.case = T)]
}

write_campbell = function(f) {
  campbell = read_campbell(f)
  path_folders = strsplit(f, '/')[[1]]
  ## campbell$file = tail(path_folders, 1)
  ## get the station
  station = path_folders[length(path_folders) - 1]
  if (station == 'WFMS') {
    table = 'campbell_wfms'
    ## replace mislabeled NO2 column
    names(campbell)[names(campbell) == 'NO2_Avg'] = 'NOX_Avg'
  } else if (station == 'WFML') {
    table = 'campbell_wfml'
  } else {
    stop('Station not recognized.')
  }
  
  ## clean and reorganize the data
  is_flag = grepl('^F_', names(campbell))
  campbell_long = campbell[, !is_flag] %>%
    gather(measurement, value, -c(instrument_time, RECORD))
  if (station == 'WFMS') {
    flag_mat = as.matrix(campbell[, is_flag]) != 1
  } else if (station == 'WFML') {
    flag_mat = as.matrix(campbell[, is_flag]) != 0
  }
  row.names(flag_mat) = campbell$instrument_time
  campbell_long$measurement =
    gsub('_Avg$', '', campbell_long$measurement)
  flag_rows = match(campbell_long$instrument_time,
                    campbell$instrument_time)
  flag_cols = match(paste('F',
                          fast_lookup(campbell_long$measurement,
                                      wfms_flags),
                          'Avg', sep = '_'),
                    colnames(flag_mat))
  campbell_long$flagged = flag_mat[cbind(flag_rows, flag_cols)]
  
  ## add to postgres
  names(campbell_long) = tolower(names(campbell_long))
  pg = dbxConnect(adapter = 'postgres', dbname = dbname)
  ## Some files contain duplicate times due to a datalogger
  ## restart. However, all the duplicated times appear to have
  ## matching values, so we can skip updates for those pre-existing
  ## rows.

  ## It would be prudent to add a check here, though, to verify that
  ## duplicated times have matching values.
  dbxUpsert(pg, table, campbell_long,
            where_cols = c('instrument_time', 'measurement'),
            batch_size = 10000, skip_existing = T)
  dbxDisconnect(pg)
}

dbname = commandArgs(trailingOnly = T)[1]
files = commandArgs(trailingOnly = T)[-1]
file_date_strs = gsub('^.*Table1_|\\.dat$', '', files)
files = files[order(file_date_strs)]
for (f in files) {
  message(paste('Importing', f))
  write_campbell(f)
}
