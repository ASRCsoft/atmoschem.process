#!/usr/bin/env Rscript
## getting campbell data into postgres

## run this script from the command line like so:
## Rscript load_campbell.R dbname ~/data/campbell/WFMS/WFMFS_SUMMIT_Table1_2018_06_29_2300.dat

library(dbx)

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
  campbell$file = tail(path_folders, 1)
  ## get the station
  station = path_folders[length(path_folders) - 1]
  if (station == 'WFMS') {
    table = 'campbell_wfms'
  } else if (station == 'WFML') {
    table = 'campbell_wfml'
  } else {
    stop('Station not recognized.')
  }
  ## add to postgres
  names(campbell) = tolower(names(campbell))
  pg = dbxConnect(adapter = 'postgres', dbname = dbname)
  ## Some files contain duplicate times due to a datalogger
  ## restart. However, all the duplicated times appear to have
  ## matching values, so we can skip updates for those pre-existing
  ## rows.

  ## It would be prudent to add a check here, though, to verify that
  ## duplicated times have matching values.
  dbxUpsert(pg, table, campbell, where_cols = 'instrument_time',
            skip_existing = T)
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
