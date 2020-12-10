# load calibration data into SQLite

# run like so:
# Rscript analysis/load_calibration.R <site>

# produces file analysis/intermediate/cals_<site>.sqlite

# library(atmoschem.process)
library(DBI)
library(RSQLite)

site = commandArgs(trailingOnly = T)[1]
data_source = commandArgs(trailingOnly = T)[2]

# get the manual cals
cal_path = file.path('analysis/cleaned/raw_data', site, 'calibrations')
cal_glob = file.path(cal_path, '*', '*', '*')
cal_paths = Sys.glob(cal_glob)
cals = do.call(rbind, lapply(cal_paths, read.csv))
# split up awkwardly formatted times column
cals$start_time = gsub('\\[|,.*', '', cals$times)
cals$end_time = gsub('.*, |\\]', '', cals$times)
cals$times = NULL

# get the automated cals
# ...

# write to sqlite file
interm_dir = file.path('analysis', 'intermediate')
dir.create(interm_dir, F, T)
dbpath = paste0('cals_', site, '.sqlite') %>%
  file.path(interm_dir, .)
db = dbConnect(SQLite(), dbpath)
dbWriteTable(db, 'calibrations', cals, overwrite = T)
dbDisconnect(db)
