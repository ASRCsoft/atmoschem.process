#!/usr/bin/env Rscript
## getting envidas data into postgres

## run this script from the command line like so:
## Rscript load_envidas.R ~/data/envidas/WFMS/envi-rpt-1101.csv

library(lubridate)
library(RPostgreSQL)

read_envidas = function(f, ...) {
  lines = readLines(f, warn = F, encoding = 'UTF-8')
  n_header = grep('^Date|^TIME', lines)[1]
  headers = strsplit(lines[n_header], ',')[[1]]
  ## find the first line that starts with a date
  n_start = grep('^[0-9]', lines)[1]
  ## ignore summary statistics lines, which typically start with a row
  ## of 'Min' values
  min_lines = grep('^Min', lines)
  if (length(min_lines) > 0) {
    n_end = min_lines[1] - 1
  } else {
    n_end = length(lines)
  }
  csv_lines = lines[c(n_header, n_start:n_end)]
  csv_text = paste(csv_lines, collapse = '\n')
  na_strings = c('NA', 'NAN', 'NANN', '-9999')
  ## NA values are also sometimes represented with text values
  ## 'InVld', '---', '<Samp', 'OffScan', but right now these are
  ## removed in the write_envidas_table function
  df = read.csv(text = csv_text, na.strings = na_strings,
                row.names = NULL, fileEncoding = 'UTF-8', ...)
  names(df) = headers # don't let R muck up header names
  ## replace non-numeric values with NA in some columns
  df$CO = as.numeric(df$CO)
  df
}

format_envidas_headers = function(h) {
  df = data.frame(orig = h, indicator = F, flag = F,
                  new = NA)
  ## remove unneeded suffixes
  df$new = trimws(df$orig)
  df$new = gsub('_Avg$| : Value$|^Amb', '', df$new)
  ## replace various status name formats with a consistent '_flag'
  ## suffix format
  flag_regex = '^S_|^S\\.| : Status$|^Status$'
  indicator_regex = '^F_'
  df$flag[grep(flag_regex, df$new)] = T
  df$indicator[grep(indicator_regex, df$new)] = T
  df$new = gsub(flag_regex, '', df$new)
  df$new = gsub(indicator_regex, '', df$new)
  ## add subject to blank headers
  df$new[is.na(df$new)] = ''
  is_blank = df$new == ''
  follows_previous = is_blank &
    (df$flag | df$indicator)
  df$new[follows_previous] =
    df$new[which(follows_previous) - 1]
  ## name things consistently
  df$new = gsub('^T$|^Temp$', 'Temperature', df$new)
  time_regex = '^TIMESTAMP$|^Date Time$|^Date&Time$'
  df$new = gsub(time_regex, 'Timestamp', df$new)
  df$new = gsub('CupA', 'Cup', df$new)
  df$new = paste0(df$new,
                  ifelse(df$flag, '_flag', ''),
                  ifelse(df$indicator, '_indicator', ''))
  df$new
}

process_envidas = function(df) {
  ## tidy column names
  names(df) = format_envidas_headers(names(df))
  ## remove useless columns
  blank_regex = '^Spare|^Phase|^_flag$|^_indicator$|^$'
  df = df[, !grepl(blank_regex, names(df))]
  ## remove empty columns and empty status columns
  ## df = df[df$new != '', ]
  if ('Time' %in% names(df)) {
    ## if there's a time column add it to the timestamp
    df$Timestamp = as.POSIXct(paste(df$Timestamp,
                                    df$Time),
                              format = "%m/%d/%Y %H:%M")
    df$Time = NULL
  } else {
    ## parse times, trying the full year format first, then falling
    ## back to the last two numbers of the year if that doesn't work
    time_formats = c('%m/%d/%Y %H:%M', '%m/%d/%y %H:%M')
    df$Timestamp = parse_date_time(df$Timestamp,
                                   orders = time_formats)
  }

  ## rename a few columns
  col_dict = c(Timestamp = 'instrument_time',
               Rain_mm_Tot = 'rain', PTemp_C = 'ptemp')
  names(df) = ifelse(names(df) %in% names(col_dict),
                     col_dict[names(df)], names(df))
  df
}

format_hstore = function(x)
  ## this assumes no names have quotation marks in them
  paste(paste0('"', names(x), '"=>"', x, '"'), collapse=',')

write_envidas_table = function(df, f) {
  envidas_cols = c('instrument_time', 'temperature', 'rh',
                   'bp', 'no', 'co', 'so2')
  names(df) = tolower(names(df))
  ## replace 'zero' text values with 0 in numeric columns, and replace
  ## other text values with NA
  for (h in envidas_cols[-1]) {
    df[, h] = ifelse(df[, h] == 'zero', 0, as.numeric(df[, h]))
  }
  dict_df = df[, !names(df) %in% envidas_cols]
  df = df[, names(df) %in% envidas_cols]
  ## add the source info columns
  df$row = as.integer(row.names(df))
  ## get the station id
  path_folders = strsplit(f, '/')[[1]]
  station = path_folders[length(path_folders) - 1]
  if (station == 'WFMS') {
    df$station_id = 1
  } else if (station == 'WFML') {
    df$station_id = 2
  } else if (station == 'PSP') {
    df$station_id = 3
  }
  df$file = gsub('\\.csv', '', tail(strsplit(f, '/')[[1]], 1))
  ## add the hstore column
  df$data_dict = apply(dict_df, 1, format_hstore)
  pg = dbConnect(PostgreSQL(), dbname = 'chemtest')
  dbWriteTable(pg, 'envidas', df, append = T, row.names = F)
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
