#!/usr/bin/env Rscript
## load metadata into postgres

## run this script from the command line like so:
## Rscript load_metadata.R dbname /path/to/metadata/files

library(dbx)

dbname = commandArgs(trailingOnly = T)[1]

## get the sites and corresponding IDs
pg = dbxConnect(adapter = 'postgres', dbname = dbname)
sites = dbxSelect(pg, 'select * from stations')
dbxDisconnect(pg)

write_metadata = function(f, tbl_name, idx_cols) {
  ## upsert metadata from a metadata csv file
  meta = read.csv(f, na.strings=c('', 'NA'))
  meta$station_id = sites$id[match(meta$site, sites$short_name)]
  meta$site = NULL
  pg = dbxConnect(adapter = 'postgres', dbname = dbname)
  dbxUpsert(pg, tbl_name, meta, where_cols = idx_cols)
  dbxDisconnect(pg)
}

write_measurements = function(f) {
  measurements_file = file.path(f, 'measurements.csv')
  idx_cols = c('measurement', 'station_id')
  write_metadata(measurements_file,
                 'measurements', idx_cols)
}

write_autocals = function(f) {
  autocal_file = file.path(f, 'autocals.csv')
  idx_cols = c('instrument', 'dates', 'times', 'station_id')
  write_metadata(autocal_file, 'autocals', idx_cols)
}

write_manual_flags = function(f) {
  flags_file = file.path(f, 'manual_flags.csv')
  idx_cols = c('measurement', 'times', 'station_id')
  write_metadata(flags_file, 'manual_flags', idx_cols)
}


f = commandArgs(trailingOnly = T)
message('Loading measurements info...')
write_measurements(f)
message('Loading autocalibration schedule...')
write_autocals(f)
message('Loading manual flags...')
write_manual_flags(f)
