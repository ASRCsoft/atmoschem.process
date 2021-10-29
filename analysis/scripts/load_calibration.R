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

is_psp_42C_cal = function(f)
  startsWith(basename(f), 'Pinnacle_42C')
is_psp_API300EU_cal = function(f)
  startsWith(basename(f), 'Pinnacle_API300EU_CO_Weekly')
is_psp_ASRC_TEI42i_Y_NOy_cal = function(f)
  startsWith(basename(f), 'Pinnacle_ASRC_TEI42i_Y_NOy_146i_Weekly') |
    startsWith(basename(f), 'Pinnacle_ASRC_TEI42i_Y_NOy_146i_WEEKLY') |
    startsWith(basename(f), 'Pinnacle_ASRC_TEI42i_Y_NOy_T700_Weekly')
is_psp_DEC_TEI42i_NOy_cal = function(f)
  startsWith(basename(f), 'Pinnacle DEC TEI42i NOy Weekly') |
    startsWith(basename(f), 'Pinnacle_DEC_TEI42i_NOy_Weekly')
is_psp_TEI43i_SO2_cal = function(f)
  startsWith(basename(f), 'Pinnacle_TEI43i_SO2_Weekly') |
    startsWith(basename(f), 'Pinnacle_TEI43i_SO2_146i_Weekly')
is_psp_TEI49i_O3_49i_cal = function(f)
  startsWith(basename(f), 'Pinnacle_TEI49i_O3_49i_Weekly') |
    startsWith(basename(f), 'Pinnacle_TEI49i_O3_Weekly')

transform_psp_calibrations = function(f) {
  # figure out which function to use for a Pinnacle calibration file
  if (is_psp_42C_cal(f)) {
    transform_psp_42C_calibrations(f)
  } else if (is_psp_API300EU_cal(f)) {
    transform_psp_API300EU_calibrations(f)
  } else if (is_psp_ASRC_TEI42i_Y_NOy_cal(f)) {
    transform_psp_ASRC_TEI42i_Y_NOy_calibrations(f)
  } else if (is_psp_DEC_TEI42i_NOy_cal(f)) {
    transform_psp_DEC_TEI42i_NOy_calibrations(f)
  } else if (is_psp_TEI43i_SO2_cal(f)) {
    transform_psp_TEI43i_SO2_calibrations(f)
  } else if (is_psp_TEI49i_O3_49i_cal(f)) {
    transform_psp_TEI49i_O3_49i_calibrations(f)
  } else {
    warning(paste('Transform not implemented for', f))
    NULL
  }
}

# decide what to do with a calibration file using its site and instrument
transform_calibration = function(f, site, ds) {
  if (site == 'PSP') {
    transform_psp_calibrations(f)
  } else if (site == 'WFMS' && ds == 'Teledyne_300EU') {
    transform_wfms_300EU(f)
  } else if (site == 'WFMS' && ds == 'Thermo_42C') {
    transform_wfms_42C(f)
  } else if (site == 'WFMS' && ds == 'Thermo_42Cs') {
    transform_wfms_42Cs(f)
  } else if (site == 'WFMS' && ds == 'Thermo_43C') {
    transform_wfms_43C(f)
  } else if (site == 'WFMB' && ds == 'Thermo_42i') {
    transform_wfml_42i(f)
  } else if (site == 'WFMB' && ds == 'Thermo_48C') {
    transform_wfml_48C(f)
  }
}

# calibration-related functions
min_ma = function(x, k) min(suppressWarnings(runmed(x, k, 'constant')), na.rm = TRUE)
max_ma = function(x, k) max(suppressWarnings(runmed(x, k, 'constant')), na.rm = TRUE)
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

# Find the PSP conversion efficiency check time period using measurements from
# the overall calibration check period (meas) and the recorded NO2 "instrument
# response" value from the cal sheet (no2val)
find_42C_ce = function(meas, no2val) {
  # get the calibrator info
  calibrators = config$psp_no2_calibrators
  calibrator = calibrators[findInterval(meas$time[1], as.POSIXct(calibrators$start_time, tz = 'EST')), ]
  candidates = meas %>%
    transform(no2 = nox - no) %>%
    # split measurements by discontinuities and give each group an ID
    transform(dnox = nox - lag2(nox), dno2 = no2 - lag2(no2)) %>%
    transform(discontinuous = abs(dnox) > 1 | abs(dno2) > 1) %>%
    transform(new_group = discontinuous & !lag2(discontinuous)) %>%
    transform(group = cumsum(!is.na(new_group) & new_group)) %>%
    subset(!discontinuous) %>%
    # remove short-lived groups
    transform(stime = ave(time, group, FUN = min),
              etime = ave(time, group, FUN = max)) %>%
    subset(etime - stime >= as.difftime(10, units = 'mins')) %>%
    # get calibration results for each group 
    transform(nocal = ave(no, group, FUN = function(x) min_ma(x, 5)),
              noxcal = ave(nox, group, FUN = function(x) max_ma(x, 5))) %>%
    subset(select = c('stime', 'etime', 'nocal', 'noxcal')) %>%
    unique %>%
    transform(no2cal = noxcal - nocal)
  if (calibrator$model == '146i') {
    candidates = candidates %>%
      # no2cal, no2val are both measured NOx - measured NO, should be close
      # (within 10%)
      transform(pctdiff = abs(100 * no2cal / no2val - 100)) %>%
      subset(pctdiff < 10)
  } else if (calibrator$model == 'T700U') {
    candidates = candidates %>%
      # in this case no2val is certified NOx - measured NO
      transform(noval = calibrator$certified_nox - no2val) %>%
      # look for NO values within 10% of the implied cal sheet NO value
      transform(pctdiff = abs(100 * nocal / noval - 100)) %>%
      subset(pctdiff < 10)
  } else {
    stop('Calibrator', calibrator$model, 'is not even possible. What??')
  }
  if (!nrow(candidates)) return(NA)
  candidates[order(candidates$pctdiff), ] %>%
    head(1) %>%
    with(list(start_time = stime, end_time = etime, no = nocal, nox = noxcal))
}

# get the manual cals
files = file.path('analysis', 'raw', raw_folder, site, 'calibrations', '*', '*',
                  '*') %>%
  Sys.glob
# ignore Pinnacle audit and calibration files-- currently only care about the
# weekly check files
files = files[!grepl('audit|calibration', basename(files), ignore.case = T)]
message('Loading ', length(files), ' files into cals_', site, '.sqlite...')
mcals = data.frame(f = files) %>%
  transform(ds = gsub('^.*calibrations/|/[0-9]{4}/[^/].*$', '', f)) %>%
  with(mapply(transform_calibration, f = f, site = site, ds = ds,
              SIMPLIFY = F)) %>%
  do.call(rbind, .)
# ideally, the data source would be assigned via the config files. But for now,
# hardcoded
mcals$data_source = switch(site, WFMS = 'campbell', WFMB = 'campbell',
                           PSP = 'envidas')
mcals$times = as_interval(mcals$times)
mcals$measured_value = as.numeric(mcals$measured_value)
# WFMS calibrations apply to both campbell and envidas data sources, so they
# need to be duplicated
if (site == 'WFMS') {
  envidas_mcals = mcals %>%
    subset(int_start(times) >= as.POSIXct('2019-01-01', tz = 'EST')) %>%
    transform(data_source = 'envidas') %>%
    # NOx is NOX, NOy is NOY in envidas (all uppercase)
    transform(measurement_name = toupper(measurement_name))
  mcals = rbind(mcals, envidas_mcals)
}

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

  # find the 42C CE results in the raw data since they weren't recorded in the
  # PSP cal sheets
  ce_mcals = subset(mcals, measurement_name == 'NOx' & type == 'CE')
  nox_ces = meas %>%
    transform(is_cal = time %within% as.list(ce_mcals$times)) %>%
    transform(new_cal = is_cal & !lag2(is_cal)) %>%
    transform(cal_id = cumsum(new_cal)) %>%
    subset(is_cal) %>%
    # guess 42C CE times for each calibration
    split(., .$cal_id) %>%
    lapply(function(x) {
      # find the matching interval in ce_mcals
      mcal_i = ce_mcals[x$time[1] %within% ce_mcals$times, ]
      guess = find_42C_ce(x, mcal_i$measured_value)
      if (!is.list(guess)) {
        data.frame(measurement_name = 'NO', type = 'CE',
                   start_time = int_start(mcal_i$times),
                   end_time = int_end(mcal_i$times),
                   provided_value = mcal_i$provided_value, measured_value = NA,
                   nox = NA)
      } else {
        data.frame(measurement_name = 'NO', type = 'CE',
                   start_time = guess$start_time, end_time = guess$end_time,
                   provided_value = mcal_i$provided_value,
                   measured_value = guess$no, nox = guess$nox)
      }
    }) %>%
    do.call(rbind, .) %>%
    # fix time formatting messed up by rbind
    transform(start_time = as.POSIXct(start_time, tz = 'EST', origin = '1970-01-01'),
              end_time = as.POSIXct(end_time, tz = 'EST', origin = '1970-01-01')) %>%
    transform(times = interval(start_time, end_time)) %>%
    subset(select = -c(measurement_name, start_time, end_time))
  names(nox_ces)[3:4] = c('measured_value.NO', 'measured_value.NOx')
  nox_ces = reshape(nox_ces, varying = 3:4, timevar = 'measurement_name',
                    idvar = names(nox_ces)[-(3:4)], direction = 'long')
  nox_ces$corrected = F
  nox_ces$data_source = 'envidas'
  
  # add NO CE values to the other manual cals
  mcals = mcals %>%
    # remove the values that came from the cal sheet
    subset(!(measurement_name == 'NOx' & type == 'CE')) %>%
    # append the new values
    rbind(nox_ces[, names(mcals)])
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
# WFMS autocalibrations apply to both campbell and envidas data sources, so they
# need to be duplicated
if (site == 'WFMS') {
  envidas_acals = acals0 %>%
    transform(data_source = 'envidas') %>%
    # NOx is NOX, NOy is NOY in envidas (all uppercase)
    transform(measurement_name = toupper(measurement_name))
  # envidas data starts in 2019
  int_start(envidas_acals$dt_int) =
    pmax(int_start(envidas_acals$dt_int), as.POSIXct('2019-01-01', tz = 'EST'))
  acals0 = rbind(acals0, envidas_acals)
}
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
for (ds in unique(cal_flags2$data_source)) {
  cal_flags_ds = subset(cal_flags2, data_source == ds)
  for (name in unique(cal_flags_ds$measurement_name)) {
    cal_flags_name = subset(cal_flags_ds, measurement_name == name)
    for (ntype in unique(cal_flags_name$type)) {
      ntype_flags = subset(cal_flags_name, type == ntype)
      matches = with(all_cals, data_source == ds & measurement_name == name &
                               type == ntype)
      all_cals[matches, 'flagged'] =
        all_cals[matches, 'end_time'] %within% as.list(ntype_flags$times)
    }
  }
}

# Calculate NO2 conversion efficiencies (derived from NOx and NO results)
if (site == 'WFMS') {
  # WFMS NO2 conversion efficiencies are recorded on the NOx channel
  no2_ce = all_cals$measurement_name %in% c('NOx', 'NOX') &
    all_cals$type == 'CE'
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
