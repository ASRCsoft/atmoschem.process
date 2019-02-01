#!/usr/bin/env Rscript
## load metadata into postgres

## run this script from the command line like so:
## Rscript load_metadata.R /path/to/metadata/files

library(dbx)

## get the sites and corresponding IDs
pg = dbxConnect(adapter = 'postgres', dbname = 'chemtest')
sites = dbxSelect(pg, 'select * from stations')
dbxDisconnect(pg)

write_measurements = function(f) {
  measurements_file = file.path(f, 'measurements.csv')
  measurements = read.csv(measurements_file, na.strings=c('', 'NA'))
  measurements$station_id =
    sites$id[match(measurements$site, sites$short_name)]
  measurements$site = NULL
  measurements$valid_range[measurements$valid_range == ''] = NA
  idx_cols = c('measurement', 'station_id')
  pg = dbxConnect(adapter = 'postgres', dbname = 'chemtest')
  dbxUpsert(pg, 'measurements', measurements, where_cols = idx_cols)
  dbxDisconnect(pg)
}

write_autocals = function(f) {
  autocal_file = file.path(f, 'autocals.csv')
  autocals = read.csv(autocal_file, na.strings=c('', 'NA'))
  autocals$station_id =
    sites$id[match(autocals$station, sites$short_name)]
  autocals$station = NULL
  idx_cols = c('instrument', 'dates', 'times', 'station_id')
  pg = dbxConnect(adapter = 'postgres', dbname = 'chemtest')
  dbxUpsert(pg, 'autocals', autocals, where_cols = idx_cols)
  dbxDisconnect(pg)
}


f = commandArgs(trailingOnly = T)
message('Loading measurements info...')
write_measurements(f)
message('Loading autocalibration schedule...')
write_autocals(f)
