#!/usr/bin/env Rscript
## getting envidas data into postgres

## run this script from the command line like so:
## Rscript load_envidas.R ~/data/envidas/WFMS/envi-rpt-1101.csv

library(lubridate)
library(RPostgreSQL)

read_envidas = function(f, ...) {
  lines = readLines(f)
  csv_lines = lines[c(2, 5:length(lines))]
  csv_text = paste(csv_lines, collapse = '\n')
  na_strings = c('NA', 'NAN', 'NANN')
  read.csv(text = csv_text, na.strings = na_strings, ...)
}

process_envidas = function(df) {
  ## tidy column names
  names(df) = gsub('_Avg$', '', names(df))
  names(df) = gsub('^F_(.*)$', '\\1_flag', names(df))
  names(df) = gsub('CupA', 'Cup', names(df))
  ## remove useless columns
  df = df[, !grepl('^Spare|^Phase', names(df))]
  ## format columns
  time_formats = c('%m/%d/%Y %H:%M', '%m/%d/%y %H:%M')
  df$TIMESTAMP = parse_date_time(df$TIMESTAMP,
                                 orders = time_formats)
  ## rename a few columns
  col_dict = c(TIMESTAMP = 'instrument_time',
               T = 'temperature', TRH_flag = 'temp_rh_flag',
               Rain_mm_Tot = 'rain', PTemp_C = 'ptemp')
  names(df) = ifelse(names(df) %in% names(col_dict),
                     col_dict[names(df)], names(df))
  df
}

write_envidas_table = function(df, f) {
  df$row = as.integer(row.names(df))
  df$station_id = 1
  df$file = gsub('\\.csv', '', tail(strsplit(f, '/')[[1]], 1))
  names(df) = tolower(names(df))
  pg = dbConnect(PostgreSQL(), dbname = 'chemtest')
  dbWriteTable(pg, 'envidas_wfms', df, append = T, row.names = F)
  dbDisconnect(pg)
}

import_envidas_file = function(f) {
  env = process_envidas(read_envidas(f))
  write_envidas_table(env, f)
}

env_files = commandArgs(trailingOnly = T)
for (f in env_files) {
  message(paste('Importing', f))
  import_envidas_file(f)
}
