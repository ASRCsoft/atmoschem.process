# load calibration data into SQLite

# run like so:
# Rscript analysis/load_calibration.R <site>

# produces file analysis/intermediate/cals_<site>.sqlite

library(atmoschem.process)
library(magrittr)
library(lubridate)
library(caTools)
library(DBI)
library(RSQLite)

site = commandArgs(trailingOnly = T)[1]

# calibration-related functions
min_ma = function(x, k, ...) min(runmean(x, k, ...), na.rm = TRUE)
max_ma = function(x, k, ...) max(runmean(x, k, ...), na.rm = TRUE)
get_param = function(p, d, t1, t2) {
  dbpath = file.path('analysis', 'intermediate',
                     paste0('raw_', site, '_', d, '.sqlite'))
  db = dbConnect(SQLite(), dbpath)
  sql_str = 'select time, ? as value from measurements where time>=? and time<=? order by time asc'
  valname = dbQuoteIdentifier(db, paste0('value.', p))
  sql = sqlInterpolate(db, sql_str, valname, format(t1, tz = 'EST'),
                       format(t2, tz = 'EST'))
  meas = try(dbGetQuery(db, sql))
  dbDisconnect(db)
  meas$time = as.POSIXct(meas$time, tz = 'EST')
  meas
}
# start and end times from interval notation
as_interval = atmoschem.process:::as_interval
# add time around a lubridate interval
pad_interval = function(interval, start, end) {
  interval(int_start(interval) - start, int_end(interval) - end)
}
# get a set of calibration results for a scheduled autocal
get_cal_results = function(dt_int, t_int, p, d, agg_f) {
  meas = get_param(p, d, int_start(dt_int), int_end(dt_int))
  if (nrow(meas) == 0) {
    warning('No measurements found for ', p, ' / ', d)
    return(data.frame())
  }
  stime = hm(gsub('^[[(]|,.*$', '', t_int))
  etime = hm(gsub('^.*, ?|[])]$', '', t_int))
  meas %>%
    transform(hms = hms(strftime(time, format = '%H:%M:%S'))) %>%
    subset(hms >= stime & hms <= etime) %>%
    transform(date = as.Date(time, tz = 'EST')) %>%
    aggregate(value ~ date, FUN = agg_f, data = .)
}

# get the manual cals
mcals = file.path('analysis', 'cleaned', 'raw_data', site, 'calibrations', '*',
                  '*', '*') %>%
  Sys.glob %>%
  lapply(read.csv) %>%
  do.call(rbind, .)
# split up awkwardly formatted times column
mcals$start_time = int_start(as_interval(mcals$times))
mcals$end_time = int_end(as_interval(mcals$times))
mcals$times = NULL
mcals$manual = T

# get PSP NO conversion efficiencies
# ...
# guess_42C_ce_time, psp_no_ces

# get the automated cals
# first let's get a data frame of the cal times
acals0 = autocals %>%
  transform(dt_int = as_interval(dates))
acals0 = acals0[acals0$site == site, ]
# for ongoing schedules use the current time as the end
int_end(acals0$dt_int[is.na(int_end(acals0$dt_int))]) = Sys.time()
acals_list = list()
for (i in 1:nrow(acals0)) {
  acalsi = acals0[i, ]
  agg_f = switch(acalsi$type, zero = function(x) min_ma(x, 5),
                 span = function(x) max_ma(x, 5),
                 CE = function(x) max_ma(x, 5), function(x) NA)
  dt_intp3 = pad_interval(acalsi$dt_int, as.difftime(3, units = 'mins'),
                          as.difftime(3, units = 'mins'))
  # spans calibrations tend to spike at the beginning, so remove the first 15
  # minutes
  if (acalsi$type == 'span') {
    dt_intp3 = pad_interval(dt_intp3, as.difftime(-15, units = 'mins'), 0)
  }
  c1 = with(acalsi, get_cal_results(dt_intp3, times, measurement_name,
                                    data_source, agg_f))
  if (nrow(c1) == 0) next()
  names(c1)[2] = 'measured_value'
  stime = hm(gsub('^[[(]|,.*$', '', acalsi$times))
  etime = hm(gsub('^.*, ?|[])]$', '', acalsi$times))
  c1$start_time = as.POSIXct(as.character(c1$date), tz = 'EST') + stime
  c1$end_time = as.POSIXct(as.character(c1$date), tz = 'EST') + etime
  c1$date = NULL
  c1$measurement_name = acalsi$measurement_name
  c1$data_source = acalsi$data_source
  c1$type = acalsi$type
  c1$provided_value = acalsi$value
  acals_list[[i]] = c1
}
acals = do.call(rbind, acals_list)
acals$corrected = F
acals$manual = F

# combine!
all_cals = rbind(mcals, acals[, names(mcals)])

# write to sqlite file
interm_dir = file.path('analysis', 'intermediate')
dir.create(interm_dir, F, T)
dbpath = paste0('cals_', site, '.sqlite') %>%
  file.path(interm_dir, .)
db = dbConnect(SQLite(), dbpath)
dbWriteTable(db, 'calibrations', all_cals, overwrite = T)
dbDisconnect(db)

# also write to postgres. This keeps the code running until I get a chance to
# rewrite the R code so it doesn't look for calibrations in postgres.
# ...
