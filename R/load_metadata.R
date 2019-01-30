#!/usr/bin/env Rscript
## load metadata into postgres

## run this script from the command line like so:
## Rscript load_metadata.R /path/to/metadata/files

library(dbx)

## get the sites and corresponding IDs
pg = dbxConnect(adapter = 'postgres', dbname = 'chemtest')
sites = dbxSelect(pg, 'select * from stations')
dbxDisconnect(pg)


write_autocals = function(f) {
  autocal_file = file.path(f, 'autocals.csv')
  autocals = read.csv(autocal_file)
  autocals$station_id =
    sites$id[match(autocals$station, sites$short_name)]
  autocals$station = NULL
  idx_cols = c('instrument', 'dates', 'times', 'station_id')
  pg = dbxConnect(adapter = 'postgres', dbname = 'chemtest')
  dbxUpsert(pg, 'autocals', autocals, where_cols = idx_cols)
  dbxDisconnect(pg)
}


f = commandArgs(trailingOnly = T)
message('Loading autocalibration schedule...')
write_autocals(f)
