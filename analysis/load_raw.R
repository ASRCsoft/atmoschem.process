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

# organize raw data from an `etl_transform`ed file
read_raw = function(f) {
  atmoschem.process:::transform_measurement(f, site, data_source)
}

# get the manual raw
raw_list = file.path('analysis', 'raw', 'raw_data_v0.3', site, 'measurements',
                     data_source, '*', '*') %>%
  Sys.glob %>%
  lapply(read_raw)
# make sure columns match
all_names = unique(unlist(lapply(raw_list, names)))
fill_df = function(x) {
  setdiff(all_names, names(x)) %>%
    sapply(function(y) NA) %>%
    c(x, .) %>%
    data.frame
}
rawdf = raw_list %>%
  lapply(fill_df) %>%
  do.call(rbind, .) %>%
  transform(time = as.POSIXct(time, tz = 'EST', origin = '1970-01-01'))

# write to sqlite file
# convert time to character for compatibility with sqlite
rawdf$time = format(rawdf$time, '%Y-%m-%d %H:%M:%S', tz = 'EST')
interm_dir = file.path('analysis', 'intermediate')
dir.create(interm_dir, F, T)
dbpath = paste0('raw_', site, '_', data_source, '.sqlite') %>%
  file.path(interm_dir, .)
db = dbConnect(SQLite(), dbpath)
dbWriteTable(db, 'measurements', rawdf, overwrite = T)
dbDisconnect(db)
