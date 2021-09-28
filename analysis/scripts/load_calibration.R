# load calibration data into SQLite

# run like so:
# Rscript analysis/load_calibration.R <site>

# produces file analysis/intermediate/cals_<site>.sqlite

library(atmoschem.process)
library(magrittr)
suppressPackageStartupMessages(library(lubridate))
library(DBI)
library(RSQLite)

site = commandArgs(trailingOnly = T)[1]
raw_folder = paste0('raw_data_v', Sys.getenv('raw_version'))
config = read_csv_dir('analysis/config')

# calibration-related functions
min_ma = function(x, k) min(suppressWarnings(runmed(x, k, 'constant')), na.rm = TRUE)
max_ma = function(x, k) max(suppressWarnings(runmed(x, k, 'constant')), na.rm = TRUE)
# start and end times from interval notation
as_interval = atmoschem.process:::as_interval
# add time around a lubridate interval
pad_interval = function(interval, start, end) {
  interval(int_start(interval) - start, int_end(interval) - end)
}
# add time around a character clock time interval
pad_time_interval = function(interval, start, end) {
  stime = as.POSIXct(gsub('^[[(]|,.*$', '', interval), tz = 'EST',
                     format = '%H:%M')
  etime = as.POSIXct(gsub('^.*, ?|[])]$', '', interval), tz = 'EST',
                     format = '%H:%M')
  stime_min = as.POSIXct('00:00', tz = 'EST', format = '%H:%M')
  etime_max = as.POSIXct('23:59', tz = 'EST', format = '%H:%M')
  stime = max(stime - start, stime_min)
  etime = min(etime + end, etime_max)
  c(stime, etime) %>%
    strftime(format = '%H:%M', tz = 'EST') %>%
    paste(collapse = ',') %>%
    paste0('[', ., ')')
}
# get a set of calibration results for a scheduled autocal
get_cal_results = function(dt_int, t_int, p, d, agg_f) {
  stime = gsub('^[[(]|,.*$', '', t_int)
  etime = gsub('^.*, ?|[])]$', '', t_int)
  dbpath = file.path('analysis', 'intermediate',
                     paste0('raw_', site, '_', d, '.sqlite'))
  db = dbConnect(SQLite(), dbpath)
  sql_str = '
select time,
       ? as value
  from measurements
 where time>=?
   and time<=?
   and substr(time, 12, 5)>=?
   and substr(time, 12, 5)<=?
 order by time asc'
  valname = dbQuoteIdentifier(db, paste0('value.', p))
  sql = sqlInterpolate(db, sql_str, valname,
                       format(int_start(dt_int), tz = 'EST'),
                       format(int_end(dt_int), tz = 'EST'), stime, etime)
  meas = try(dbGetQuery(db, sql))
  dbDisconnect(db)
  meas$time = as.POSIXct(meas$time, tz = 'EST')
  meas
  if (nrow(meas) == 0) {
    warning('No measurements found for ', p, ' / ', d)
    return(data.frame())
  }
  meas %>%
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
    # make sure measured NOx value is in the ballpark of the recorded NOx value
    subset(100 * noxcal / noxval - 100 > -30 & 
           100 * noxcal / noxval - 100 < 50 )
  if (!nrow(candidates)) return(NA)
  candidates[order(candidates$no2cal, decreasing = T), ] %>%
    head(1) %>%
    with(list(start_time = stime, end_time = etime, no = nocal))
}

# get the manual cals
files = file.path('analysis', 'raw', raw_folder, site, 'calibrations', '*', '*',
                  '*') %>%
  Sys.glob
message('Loading ', length(files), ' files into cals_', site, '.sqlite...')
mcals = data.frame(f = files) %>%
  transform(ds = gsub('^.*calibrations/|/[0-9]{4}/[^/].*$', '', f)) %>%
  with(mapply(atmoschem.process:::transform_calibration, f = f, site = site,
              ds = ds, SIMPLIFY = F)) %>%
  do.call(rbind, .)
# ideally, the data source would be assigned via the config files. But for now,
# hardcoded
mcals$data_source = switch(site, WFMS = 'campbell', WFML = 'campbell',
                           PSP = 'envidas')
mcals$times = as_interval(mcals$times)
mcals$measured_value = as.numeric(mcals$measured_value)

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
acals0 = config$autocals %>%
  subset(type %in% c('zero', 'span', 'CE')) %>%
  transform(dt_int = as_interval(dates))
acals0 = acals0[acals0$site == site, ]
if (nrow(acals0)) {
  message('Calculating autocal results...')
  # for ongoing schedules use the current time as the end
  int_end(acals0$dt_int[is.na(int_end(acals0$dt_int))]) = Sys.time()
  acals_list = list()
  for (i in 1:nrow(acals0)) {
    acalsi = acals0[i, ]
    agg_f = switch(acalsi$type, zero = function(x) min_ma(x, 5),
                   span = function(x) max_ma(x, 5),
                   CE = function(x) max_ma(x, 5), function(x) NA)
    # add a few minutes on either end to account for possible clock drift
    timesp3 = pad_time_interval(acalsi$times, as.difftime(3, units = 'mins'),
                                 as.difftime(3, units = 'mins'))
    # spans calibrations tend to spike at the beginning, so remove the first 15
    # minutes
    if (acalsi$type == 'span') {
      timesp3 = pad_time_interval(timesp3, as.difftime(-15, units = 'mins'), 0)
    }
    c1 = with(acalsi, get_cal_results(dt_int, timesp3, measurement_name,
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
cal_flags2 = config$cal_flags[config$cal_flags$site == site, ] %>%
  transform(times = as_interval(times))
for (n in unique(all_cals$measurement_name)) {
  cal_flags_n = subset(cal_flags2, measurement_name == n)
  if (!nrow(cal_flags_n)) next()
  for (ntype in unique(cal_flags_n$type)) {
    ntype_flags = subset(cal_flags_n, type == ntype)
    matches = with(all_cals, measurement_name == n & type == ntype)
    all_cals[matches, 'flagged'] =
      all_cals[matches, 'end_time'] %within% as.list(ntype_flags$times)
  }
}

# Calculate NO2 conversion efficiencies (derived from NOx and NO results)
if (site == 'WFMS') {
  # WFMS NO2 conversion efficiencies are recorded on the NOx channel
  no2_ce = all_cals$measurement_name == 'NOx' & all_cals$type == 'CE'
  all_cals$measurement_name[no2_ce] = 'NO2'
} else if (site == 'PSP') {
  # PSP NO2 conversion efficiencies require subtracting NO (because the CE air
  # includes both NO2 and NO I think?)
  all_cals2 = all_cals
  all_cals2$time = all_cals2$end_time
  all_cals2$date = as.Date(all_cals2$time)
  ces_list = list()
  for (mname in c('NO', 'NOx')) {
    m_cals = subset(all_cals2, measurement_name == mname)
    m_ces = subset(m_cals, type == 'CE')
    m_conf = subset(config$channels, site == 'PSP' & name == mname)
    m_ces[, mname] =
      drift_correct(m_ces$time, m_ces$measured_value,
                    m_cals[m_cals$type == 'zero', ],
                    m_cals[m_cals$type == 'span', ], config = m_conf)
    ces_list[[mname]] = m_ces
  }
  # the NO and NOx start and end times aren't exactly the same, so match by date
  # instead
  same_cols = c('date', 'type', 'provided_value', 'data_source', 'manual',
                'corrected')
  both_ces = merge(ces_list[['NO']], ces_list[['NOx']], by = same_cols)
  no2_ces = both_ces %>%
    transform(measurement_name = 'NO2',
              measured_value = NOx - NO,
              flagged = flagged.x | flagged.y,
              start_time = start_time.x, end_time = end_time.x) %>%
    subset(select = c(same_cols[-1], 'measurement_name', 'start_time',
                      'end_time', 'measured_value', 'flagged'))
  all_cals = rbind(all_cals, no2_ces[, names(all_cals)])
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
