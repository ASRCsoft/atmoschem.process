# load raw data into SQLite

# run like so:
# Rscript analysis/load_raw.R <site> <data_source>

# produces file analysis/intermediate/raw_<site>_<data_source>.sqlite

library(atmoschem.process)
library(magrittr)
library(DBI)
library(RSQLite)

site = commandArgs(trailingOnly = T)[1]
data_source = commandArgs(trailingOnly = T)[2]
raw_folder = paste0('raw_data_v', Sys.getenv('raw_version'))

# read the file and add it to the sqlite database
load_file = function(f, db) {
  dat = tryCatch(atmoschem.process:::transform_measurement(f, site, data_source),
                 error = function(e) {
                   stop(f, ' loading failed: ', e)
                 })
  if (!nrow(dat)) return()
  # add columns to the db table if needed
  cur_fields = dbListFields(db, 'measurements')
  if (!all(names(dat) %in% cur_fields)) {
    new_fields = setdiff(names(dat), cur_fields)
    for (fname in new_fields) {
      sql_text = 'alter table measurements add column ?'
      sql = sqlInterpolate(db, sql_text, dbQuoteIdentifier(db, fname))
      dbExecute(db, sql)
    }
  }
  dat$time = format(dat$time, '%Y-%m-%d %H:%M:%S', tz = 'EST')
  dbWriteTable(db, 'measurements', dat, append = T)
}

# set up the sqlite database
interm_dir = file.path('analysis', 'intermediate')
dir.create(interm_dir, F, T)
dbpath = paste0('raw_', site, '_', data_source, '.sqlite') %>%
  file.path(interm_dir, .)
if (file.exists(dbpath)) file.remove(dbpath)
db = dbConnect(SQLite(), dbpath)
dbExecute(db, 'create table measurements(time text)')

# load data into sqlite
files = file.path('analysis', 'raw', raw_folder, site, 'measurements',
                  data_source, '*', '*') %>%
  Sys.glob
message('Loading ', length(files), ' files into ', basename(dbpath), '...')
for (f in files) load_file(f, db)

# speed up future queries, especially queries that filter by time of day
dbExecute(db, 'create index time_index on measurements(time, substr(time, 12, 5))')
dbDisconnect(db)
