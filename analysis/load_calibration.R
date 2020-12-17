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
# R's lead and lag functions aren't so useful. These are better.
lag2 = function(x, k = 1) c(rep(NA, k), head(x, -k))
lead2 = function(x, k = 1) c(tail(x, -k), rep(NA, k))
# Guess the conversion efficiency calibration time period using the recorded NOx
# value from the cal sheet (noxval), the range of calibration times, and the
# NO/NOx divergence
guess_42C_ce = function(meas, noxval) {
  candidates = meas %>%
    transform(no2 = nox - no) %>%
    # split measurements by discontinuities and give each group an ID
    transform(dnox = nox - lag2(nox), dno2 = no2 - lag2(no2)) %>%
    transform(discontinuous = abs(dnox) > 1 | abs(dno2) > 1) %>%
    transform(new_group = discontinuous & !lag2(discontinuous)) %>%
    transform(group = cumsum(!is.na(new_group) & new_group)) %>%
    # remove short-lived groups
    subset(!discontinuous) %>%
    transform(stime = ave(time, group, FUN = min),
              etime = ave(time, group, FUN = max)) %>%
    subset(etime - stime >= as.difftime(10, units = 'mins')) %>%
    # get calibration results for each group 
    transform(nocal = ave(no, group, FUN = function(x) min_ma(x, 5)),
              noxcal = ave(nox, group, FUN = function(x) max_ma(x, 5))) %>%
    subset(select = c('stime', 'etime', 'nocal', 'noxcal')) %>%
    unique %>%
    transform(no2cal = noxcal - nocal) %>%
    # remove zero calibrations
    subset(nocal > .3) %>%
    # make sure measured NOx value is within +/- 30% of the recorded NOx value
    subset(abs(100 * noxcal / noxval - 100) < 30)
  if (!nrow(candidates)) return(NA)
  candidates[order(candidates$no2cal, decreasing = T), ] %>%
    head(1) %>%
    with(list(start_time = stime, end_time = etime, no = nocal))
}

# get the manual cals
mcals = file.path('analysis', 'raw', 'raw_data_v0.3', site, 'calibrations', '*',
                  '*', '*') %>%
  Sys.glob %>%
  data.frame(f = .) %>%
  transform(ds = gsub('^.*calibrations/|/[0-9]{4}/[^/].*$', '', f)) %>%
  with(mapply(atmoschem.process:::transform_calibration, f = f, site = site,
              ds = ds, SIMPLIFY = F)) %>%
  do.call(rbind, .)
# ideally, the data source would be assigned via the config files. But for now,
# hardcoded
mcals$data_source = switch(site, WFMS = 'campbell', WFML = 'campbell',
                           PSP = 'envidas')

# add manual cals to postgres so we don't require `etl_load`
# also write to postgres. This keeps the code running until I get a chance to
# rewrite the R code so it doesn't look for calibrations in postgres.
pg = src_postgres(dbname = 'nysatmoschemdb')
pgmcals = mcals
pgmcals$measurement_type_id =
  atmoschem.process:::get_measurement_type_id(pg$con, site, pgmcals$data_source,
                                              pgmcals$measurement_name,
                                              add_new = F)
pgmcals = pgmcals[!is.na(pgmcals$measurement_type_id), ]
pgmcals = pgmcals[, c('measurement_type_id', 'type', 'times', 'provided_value',
                      'measured_value', 'corrected')]
site_id = switch(site, WFMS = 1, WFML = 2, PSP = 3, QC = 4)
del_sql = paste0('delete from manual_calibrations where measurement_type_id=any(get_data_source_ids(',
                site_id, ", '", mcals$data_source[1], "'))")
dbExecute(pg$con, del_sql)
dbWriteTable(pg$con, 'manual_calibrations', pgmcals, row.names = F, append = T)
dbDisconnect(pg$con)

mcals$times = as_interval(mcals$times)


if (site == 'PSP') {
  # find NO conversion efficiency results, which weren't recorded in the PSP cal
  # sheets

  # get NO and NOx from sqlite
  dbpath = file.path('analysis', 'intermediate', 'raw_PSP_envidas.sqlite')
  db = dbConnect(SQLite(), dbpath)
  sql_str = 'select time, ? as no, ? as nox from measurements order by time asc'
  sql = sqlInterpolate(db, sql_str, dbQuoteIdentifier(db, 'value.NO'),
                       dbQuoteIdentifier(db, 'value.NOx'))
  meas = dbGetQuery(db, sql)
  dbDisconnect(db)
  meas$time = as.POSIXct(meas$time, tz = 'EST')

  # make a plausible guess about the 42C CE results, since they aren't recorded
  # in the PSP cal sheets
  ce_mcals = subset(mcals, measurement_name == 'NOx' & type == 'CE')
  no_ces = meas %>%
    transform(is_cal = time %within% as.list(ce_mcals$times)) %>%
    transform(new_cal = is_cal & !lag2(is_cal)) %>%
    transform(cal_id = cumsum(new_cal)) %>%
    subset(is_cal) %>%
    # guess 42C CE times for each calibration
    split(., .$cal_id) %>%
    lapply(function(x) {
      # find the matching interval in ce_mcals
      mcal_i = ce_mcals[x$time[1] %within% ce_mcals$times, ]
      guess = guess_42C_ce(x, mcal_i$measured_value)
      if (!is.list(guess)) {
        data.frame(measurement_name = 'NO', type = 'CE',
                   start_time = int_start(mcal_i$times),
                   end_time = int_end(mcal_i$times),
                   provided_value = mcal_i$provided_value, measured_value = NA)
      } else {
        data.frame(measurement_name = 'NO', type = 'CE',
                   start_time = guess$start_time, end_time = guess$end_time,
                   provided_value = mcal_i$provided_value,
                   measured_value = guess$no)
      }
    }) %>%
    do.call(rbind, .) %>%
    # fix time formatting messed up by rbind
    transform(start_time = as.POSIXct(start_time, tz = 'EST', origin = '1970-01-01'),
              end_time = as.POSIXct(end_time, tz = 'EST', origin = '1970-01-01')) %>%
    transform(times = interval(start_time, end_time))
  no_ces = no_ces[, !names(no_ces) %in% c('start_time', 'end_time')]
  no_ces = no_ces[, c(1:2, 5, 3:4)]
  no_ces$corrected = F
  no_ces$data_source = 'envidas'
  
  # add NO CE values to the other manual cals
  mcals = rbind(mcals, no_ces[, names(mcals)])
}

# fix up mcal formatting
mcals = mcals %>%
  transform(start_time = int_start(times),
            end_time = int_end(times))
mcals$times = NULL
mcals$manual = T

# get the automated cals
# first let's get a data frame of the cal times
acals0 = autocals %>%
  subset(type %in% c('zero', 'span', 'CE')) %>%
  transform(dt_int = as_interval(dates))
acals0 = acals0[acals0$site == site, ]
if (nrow(acals0)) {
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

  # add acals to the mcals
  acals$corrected = F
  acals$manual = F
  all_cals = rbind(mcals, acals[, names(mcals)])
} else {
  all_cals = mcals
}

# apply calibration flags
all_cals$flagged = F
cal_flags2 = cal_flags[cal_flags$site == site, ] %>%
  transform(times = as_interval(times))
for (n in unique(all_cals$measurement_name)) {
  cal_flags_n = subset(cal_flags2, measurement_name == n)
  if (!nrow(cal_flags_n)) next()
  for (i in 1:nrow(cal_flags_n)) {
    matches = with(all_cals, measurement_name == n &
                             type == cal_flags_n$type[i])
    all_cals[matches, 'flagged'] =
      all_cals[matches, 'end_time'] %within% cal_flags_n$times[i]
  }
}

# write to sqlite file
# fix time formatting for sqlite compatibility
all_cals$start_time = format(all_cals$start_time, '%Y-%m-%d %H:%M:%S', tz = 'EST')
all_cals$end_time = format(all_cals$end_time, '%Y-%m-%d %H:%M:%S', tz = 'EST')
interm_dir = file.path('analysis', 'intermediate')
dir.create(interm_dir, F, T)
dbpath = paste0('cals_', site, '.sqlite') %>%
  file.path(interm_dir, .)
db = dbConnect(SQLite(), dbpath)
dbWriteTable(db, 'calibrations', all_cals, overwrite = T)
dbDisconnect(db)

# also write to postgres. This keeps the code running until I get a chance to
# rewrite the R code so it doesn't look for calibrations in postgres.
pg = src_postgres(dbname = 'nysatmoschemdb')
data_source = switch(site, WFMS = 'campbell', WFML = 'campbell',
                     PSP = 'envidas')
all_cals$measurement_type_id =
  atmoschem.process:::get_measurement_type_id(pg$con, site, data_source,
                                              all_cals$measurement_name,
                                              add_new = F)
all_cals$time = as.POSIXct(all_cals$end_time, tz = 'EST')
all_cals = all_cals[!is.na(all_cals$time), ]
all_cals = all_cals[!is.na(all_cals$measurement_type_id), ]
all_cals = all_cals[, c('measurement_type_id', 'type', 'time', 'provided_value',
                        'measured_value', 'flagged')]
tbl_name = paste0('calibrations_', tolower(site))
dbSendQuery(pg$con, paste('delete from', tbl_name))
dbWriteTable(pg$con, tbl_name, all_cals, row.names = F, append = T)
dbDisconnect(pg$con)
