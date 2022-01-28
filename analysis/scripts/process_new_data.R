# Process the data in its original time resolution

# run this script from the project root directory with
# Rscript analysis/process_new_data.R <site> <data_source>

# produces file analysis/intermediate/processed_<site>_<data_source>.sqlite

library(atmoschem.process)
suppressPackageStartupMessages(library(lubridate))
library(magrittr)
library(DBI)
library(RSQLite)

site = commandArgs(trailingOnly = T)[1]
data_source = commandArgs(trailingOnly = T)[2]
start_time = as.POSIXct('2018-10-01', tz = 'EST')
end_time = as.POSIXct(Sys.getenv('processing_end'), tz = 'EST')
config = read_csv_dir('analysis/config')

# get delta precip from bucket instrument's cycling cumulative values
# Open question-- how should this function deal with data gaps?
bucket_precip = function(val, val.max = .5, err.min = -.02) {
  # Carefully deal with value resets from 0.5 back to 0, which usually happens
  # in only one minute, but sometimes takes two minutes with an average
  # measurement (of the previous high value and next low value) in between
  out = c(NA, diff(val))
  resetting = out < err.min
  # if resetting in one step
  reset1 = which(resetting & !lag2(resetting) & !lead2(resetting))
  out[reset1] = out[reset1] + val.max
  # if 1st step, resetting in 2 steps-- split the difference over the two steps
  reset2_1 = which(resetting & lead2(resetting))
  out[reset2_1] = (lead2(out)[reset2_1] - lag2(out)[reset2_1] + val.max) / 2
  # if 2nd step, resetting in 2 steps-- this should be the same as above
  reset2_2 = which(resetting & lag2(resetting))
  out[reset2_2] = (out[reset2_2] - lag2(out, 2)[reset2_2] + val.max) / 2
  out
}

# for a vector of binary values, find contiguous clusters of `TRUE` values and
# return a list of lubridate time intervals containing each cluster
interval_list = function(t, val) {
  data.frame(time = t, true = val) %>%
    transform(group = cumsum(true & c(F, !lag2(true)[-1]))) %>%
    subset(true) %>%
    split(., .$group) %>%
    lapply(function(x) interval(min(x$time), max(x$time)))
}

# add parameters used by `is_flagged` to a list of parameter values
add_flag_params = function(l) {
  l$lower_range = as.numeric(gsub('^[[(]|,.*$', '', l$valid_range))
  l$upper_range = as.numeric(gsub('^.*, ?|[])]$', '', l$valid_range))
  l$lower_inc = substr(l$valid_range, 1, 1) == '['
  len = nchar(l$valid_range)
  l$upper_inc = substr(l$valid_range, len, len) == ']'
  l
}

# organize data from processed_measurements

# get all the measurement IDs for a data source
mtypes = config$channels[config$channels$site == site &
                         config$channels$data_source == data_source, ] %>%
  subset(is.na(derived) | !derived)

# make processed data

# 1) get measurements
dbpath = file.path('analysis', 'intermediate',
                   paste0('raw_', site, '_', data_source, '.sqlite'))
db = dbConnect(SQLite(), dbpath)
sql_text = 'select * from measurements where time >= ? and time <= ? order by time asc'
sql = sqlInterpolate(db, sql_text, format(start_time, '%Y-%m-%d %H:%M:%S', tz = 'EST'),
                     format(end_time, '%Y-%m-%d %H:%M:%S', tz = 'EST'))
meas = dbGetQuery(db, sql, check.names = F)
dbDisconnect(db)
meas$time = as.POSIXct(meas$time, tz = 'EST')
# keep track of the non-derived measurements, for later
nonderived = sub('^value\\.', '', names(meas)[grep('^value', names(meas))])

# also get calibrations
dbpath = file.path('analysis', 'intermediate',
                   paste0('processedcals_', site, '.sqlite'))
db = dbConnect(SQLite(), dbpath)
cals = dbGetQuery(db, 'select * from calibrations where not flagged')
dbDisconnect(db)
cals = cals[cals$data_source == data_source, ]
cals$time = as.POSIXct(cals$time, tz = 'EST')
cals$manual = as.logical(cals$manual)
cals = cals[order(cals$time), ]

# 2) add miscellaneous flags that aren't included in
# `atmoschem.process:::is_flagged`
# manual calibrations (better to get them from the unprocessed calibration file)
dbpath = file.path('analysis', 'intermediate',
                   paste0('cals_', site, '.sqlite'))
db = dbConnect(SQLite(), dbpath)
sql_text = '
select *
  from calibrations
 where data_source=?
   and manual=1
 order by end_time asc'
sql = sqlInterpolate(db, sql_text, data_source)
mcals = dbGetQuery(db, sql)
dbDisconnect(db)
if (nrow(mcals)) {
  mcals$start_time = as.POSIXct(mcals$start_time, tz = 'EST')
  mcals$end_time = as.POSIXct(mcals$end_time, tz = 'EST')
  if (any(is.na(mcals$end_time))) {
    # Why am I doing this again after I already did it in process_calibrations.R?
    # Should rearrange this a bit
    mcals$end_time[is.na(mcals$end_time)] =
      mcals$start_time[is.na(mcals$end_time)] + as.difftime(1, units = 'hours')
  }
  for (mname in unique(mcals$measurement_name)) {
    flagname = paste0('flagged.', mname)
    mcal_periods = mcals %>%
      subset(measurement_name == mname) %>%
      with(interval(start_time, end_time))
    meas[meas$time %within% as.list(mcal_periods), flagname] = T
  }
}
# autocals
if (site == 'WFMS' && data_source == 'envidas') {
  # envidas autocalibrations are the same as campbell autocalibrations
  acals_ds = config$autocals[config$autocals$site == site &
                             config$autocals$data_source == 'campbell', ]
  # remove '_Avg' suffix and capitalize the names
  acals_ds$measurement_name =
    toupper(sub('_Avg', '', acals_ds$measurement_name))
} else {
  acals_ds = config$autocals[config$autocals$site == site &
                             config$autocals$data_source == data_source, ]
}

if (nrow(acals_ds)) {
  time_hms = hms(strftime(meas$time, format = '%H:%M:%S', tz = 'EST'))
  for (i in 1:nrow(acals_ds)) {
    flagname = paste0('flagged.', acals_ds$measurement_name[i])
    sched_times = atmoschem.process:::as_interval(acals_ds$dates[i])
    stime = hm(gsub('^[[(]|,.*$', '', acals_ds$times[i]))
    etime = hm(gsub('^.*, ?|[])]$', '', acals_ds$times[i]))
    if (is.na(int_end(sched_times))) int_end(sched_times) = Sys.time()
    meas[meas$time %within% sched_times &
         time_hms >= stime & time_hms <= etime, flagname] = T
  }
}
# manual flags
mflags_ds = config$manual_flags[config$manual_flags$site == site &
                                config$manual_flags$data_source == data_source, ]
manual_unflags = config$manual_unflag[config$manual_unflag$site == site &
                                      config$manual_unflag$data_source == data_source, ]
manual_unflags$times = atmoschem.process:::as_interval(manual_unflags$times)
for (mname in unique(mflags_ds$measurement_name)) {
  meas_flags = subset(mflags_ds, measurement_name == mname)
  meas_flag_periods = atmoschem.process:::as_interval(meas_flags$times)
  flagname = paste0('flagged.', mname)
  meas[meas$time %within% as.list(meas_flag_periods), flagname] = T
}
# power outage flags
if (site %in% c('WFMS', 'WFMB') & data_source == 'campbell') {
  if (site == 'WFMS') {
    is_outage = with(meas, value.Phase1_Avg < 105 | value.Phase3_Avg < 100)
    padding_list = list(CO = 50, NO = 4, NOx = 4, NOy = 4, Ozone = 10, SO2 = 4)
  } else if (site == 'WFMB') {
    is_outage = meas$value.Phase1_Avg < 105
    padding_list = list(CO = 10, NO = 45, NOX = 45)
  }
  outage_periods = interval_list(meas$time, is_outage)
  for (i in 1:length(padding_list)) {
    varname = paste0('value.', names(padding_list)[i])
    flagname = paste0('flagged.', names(padding_list)[i])
    # pad the flagging period by 2 minutes on the front end for all values
    padded_periods = lapply(outage_periods, pad_interval,
                            start = as.difftime(2, units='mins'),
                            end = as.difftime(padding_list[[i]], units='mins'))
    meas[meas$time %within% padded_periods, flagname] = T
  }
}
# freezing anemometers
if (site == 'WFMS' & data_source == 'campbell') {
  windcols = c('WS3Cup_Avg', 'WS3Cup_Max', 'WS3CupB_Avg', 'WS3CupB_Max',
               'WS3CupA_S_WVT', 'WS3CupB_S_WVT')
  # has the wind been slow for at least 30 minutes?
  looks_frozen = function(x) {
    int_end(x) - int_start(x) > as.difftime(30, units = 'mins')
  }
  # is it (at least close to) freezing?
  is_freezing = function(x) {
    meas[match(int_start(x), meas$time), 'value.T_Avg'] < 5
  }
  for (w in windcols) {
    varname = paste0('value.', w)
    flagname = paste0('flagged.', w)
    slow_wind = !is.na(meas[, varname]) & meas[, varname] < .2
    slow_periods = interval_list(meas$time, slow_wind)
    frozen_periods = slow_periods %>%
      subset(., sapply(., looks_frozen)) %>%
      subset(., sapply(., is_freezing)) %>%
      lapply(pad_interval, start = as.difftime(1, units = 'hours'),
             end = as.difftime(40, units = 'mins'))
    meas[meas$time %within% frozen_periods, flagname] = T
  }
}

# 3) process measurements
pr_meas = data.frame(time = meas$time)
# update mtypes with only processed measurement types
mtypes = config$channels[config$channels$site == site &
                         config$channels$data_source == data_source, ] %>%
  subset(!is.na(apply_processing) & apply_processing) %>%
  subset(is.na(derived) | !derived)
for (n in 1:nrow(mtypes)) {
  mname = mtypes$name[n]
  message('Processing ', mname, '...')
  if (paste0('value.', mname) %in% names(meas)) {
    msmt_cols = c('time', paste0('value.', mname), paste0('flagged.', mname))
    msmts = meas[, msmt_cols]
    names(msmts) = c('time', 'value', 'flagged')
    m_cals = subset(cals, measurement_name == mname)
    m_zeros = subset(m_cals, type == 'zero')
    m_spans = subset(m_cals, type == 'span')
    m_ces = subset(m_cals, type == 'CE')
    m_conf = add_flag_params(subset(mtypes, name == mname))
    unflags = subset(manual_unflags, measurement_name == mname)
    tryCatch({
      if (is_true(m_conf$has_calibration)) {
        msmts$value = drift_correct(msmts$time, msmts$value, m_zeros, m_spans,
                                    m_conf)
      }
      if (is_true(m_conf$apply_ce)) {
        msmts$value =
          atmoschem.process:::ceff_correct(msmts$time, msmts$value, m_ces,
                                           m_conf)
      }
      if (nrow(unflags)) {
        unflagged = msmts$time %within% as.list(unflags$times)
      } else {
        unflagged = F
      }
      autoflagged = atmoschem.process:::is_flagged(msmts$value, m_conf,
                                                   msmts$flagged) & !unflagged
      msmts$flagged = msmts$flagged | autoflagged
      # write to pr_meas
      pr_meas[, paste0('value.', mname)] = msmts$value
      pr_meas[, paste0('flagged.', mname)] = msmts$flagged
    },
    error = function(e) {
      warning(mname, ' processing failed: ', e)
    })
  } else {
    warning('No measurements found.')
  }
}

# 4) add derived measurements
message('Calculating derived values...')
if (site == 'WFMS') {
  if (data_source == 'campbell') {
    # NO2
    meas$value.NO2 = with(pr_meas, value.NOx_Avg - value.NO_Avg)
    meas$flagged.NO2 = with(pr_meas, flagged.NOx_Avg | flagged.NO_Avg)
    # SLP
    meas$value.SLP =
      with(pr_meas, sea_level_pressure(value.BP_Avg, value.T_Avg, 1483.5))
    meas$flagged.SLP = with(pr_meas, flagged.BP_Avg | flagged.T_Avg)
    # WS/WD (wind shadow corrected)
    meas$value.WS =
      with(pr_meas, pmax(value.WS3Cup_Avg, value.WS3CupB_Avg, na.rm = T))
    meas$flagged.WS = with(pr_meas, flagged.WS3Cup_Avg & flagged.WS3CupB_Avg)
    meas$value.WD = pr_meas$value.WindDir_D1_WVT
    meas$flagged.WD = pr_meas$flagged.WindDir_D1_WVT
    # WS/WD continued (new columns starting 2020-06-30)
    new_winds = pr_meas$time >= as.POSIXct('2020-06-30 13:57', tz = 'EST')
    a_valid = with(pr_meas[new_winds, ],
                   !is.na(value.WS3CupA_S_WVT) & !flagged.WS3CupA_S_WVT)
    a_ge_b = with(pr_meas[new_winds, ],
                  a_valid & value.WS3CupA_S_WVT >= value.WS3CupB_S_WVT)
    meas$value.WS[new_winds] =
      with(pr_meas[new_winds, ],
           ifelse(a_ge_b, value.WS3CupA_S_WVT, value.WS3CupB_S_WVT))
    meas$flagged.WS[new_winds] =
      with(pr_meas[new_winds, ],
           ifelse(a_ge_b, flagged.WS3CupA_S_WVT, flagged.WS3CupB_S_WVT))
    meas$value.WD[new_winds] =
      with(pr_meas[new_winds, ],
           ifelse(a_ge_b, value.WindDirA_D1_WVT, value.WindDirB_D1_WVT))
    meas$flagged.WD[new_winds] =
      with(pr_meas[new_winds, ],
           ifelse(a_ge_b, flagged.WindDirA_D1_WVT, flagged.WindDirB_D1_WVT))
    # WS_max (wind shadow corrected)
    meas$value.WS_Max =
      with(pr_meas, pmax(value.WS3Cup_Max, value.WS3CupB_Max, na.rm = T))
    meas$flagged.WS_Max =
      with(pr_meas, flagged.WS3Cup_Max & flagged.WS3CupB_Max)
  } else if (data_source == 'envidas') {
    # NO2
    meas$value.NO2 = with(pr_meas, value.NOX - value.NO)
    meas$flagged.NO2 = with(pr_meas, flagged.NOX | flagged.NO)
  } else if (data_source == 'aethelometer') {
    # Wood smoke
    meas$`value.Wood smoke` =
      with(pr_meas,
           wood_smoke(value.concentration_370, value.concentration_880))
    meas$`flagged.Wood smoke` =
      with(pr_meas, flagged.concentration_370 | flagged.concentration_880)
  } else if (data_source == 'DEC_envidas') {
    # Ozone (ppbv)
    meas$value.O3_ppbv = pr_meas$value.O3 * 1000
    meas$flagged.O3_ppbv = pr_meas$flagged.O3
  }
} else if (site == 'WFMB') {
  if (data_source == 'campbell') {
    # NO2
    meas$value.NO2 = with(pr_meas, value.NOX_Avg - value.NO_Avg)
    meas$flagged.NO2 = with(pr_meas, flagged.NOX_Avg | flagged.NO_Avg)
    # SLP. This is a bit awkward because outside temps are only measured by the
    # Mesonet, at a different frequency than Campbell's barometric
    # pressure. Solution is to interpolate Mesonet outside temps at Campbell
    # measurement times.
    dbpath = file.path('analysis', 'intermediate',
                       'processed_WFMB_mesonet.sqlite')
    db = dbConnect(SQLite(), dbpath)
    sql_text =
'select time,
        "value.temperature_2m [degC]",
        "flagged.temperature_2m [degC]"
   from measurements
  where time >= ? and time <= ?
  order by time asc'
    sql = sqlInterpolate(db, sql_text, format(start_time, '%Y-%m-%d %H:%M:%S', tz = 'EST'),
                         format(end_time, '%Y-%m-%d %H:%M:%S', tz = 'EST'))
    meso_temps = dbGetQuery(db, sql, check.names = F)
    dbDisconnect(db)
    meso_temps$time = as.POSIXct(meso_temps$time, tz = 'EST')
    meso_temps$`value.temperature_2m [degC]`[meso_temps$`flagged.temperature_2m [degC]`] = NA
    interp_temps = approx(meso_temps$time,
                          meso_temps$`value.temperature_2m [degC]`, meas$time,
                          na.rm = F)$y
    meas$value.SLP =
      sea_level_pressure(pr_meas$value.BP2_Avg, interp_temps, 604)
    meas$flagged.SLP = with(pr_meas, flagged.BP2_Avg | is.na(interp_temps))
  } else if (data_source == 'envidas') {
    # Ozone_ppbv
    meas$value.Ozone_ppbv = pr_meas$`value.API-T400-OZONE` * 1000
    meas$flagged.Ozone_ppbv = pr_meas$`flagged.API-T400-OZONE`
  }
} else if (site == 'PSP') {
  if (data_source == 'envidas') {
    # NO2
    meas$value.NO2 = with(pr_meas, value.NOx - value.NO)
    meas$flagged.NO2 = with(pr_meas, flagged.NOx | flagged.NO)
    # HNO3
    meas$value.HNO3 = with(pr_meas, value.NOy - `value.NOy-HNO3`)
    meas$flagged.HNO3 = with(pr_meas, flagged.NOy | `flagged.NOy-HNO3`)
    # Precip (get changes from cumulative value and convert to mm)
    meas$value.Precip = bucket_precip(pr_meas$value.Rain, .5, -.02) * 25.4
    meas$flagged.Precip = pr_meas$flagged.Rain
    # TEOMA(2.5)BaseMC
    meas$`value.TEOMA(2.5)BaseMC` =
      with(pr_meas, `value.TEOMA(2.5)MC` + `value.TEOMA(2.5)RefMC`)
    meas$`flagged.TEOMA(2.5)BaseMC` =
      with(pr_meas, `flagged.TEOMA(2.5)MC` | `flagged.TEOMA(2.5)RefMC`)
    # TEOMB(crs)BaseMC
    meas$`value.TEOMB(crs)BaseMC` =
      with(pr_meas, `value.TEOMB(crs)MC` + `value.TEOMB(crs)RefMC`)
    meas$`flagged.TEOMB(crs)BaseMC` =
      with(pr_meas, `flagged.TEOMB(crs)MC` | `flagged.TEOMB(crs)RefMC`)
    # Dichot(10)BaseMC
    meas$`value.Dichot(10)BaseMC` =
      with(pr_meas, `value.Dichot(10)MC` + `value.Dichot(10)RefMC`)
    meas$`flagged.Dichot(10)BaseMC` =
      with(pr_meas, `flagged.Dichot(10)MC` | `flagged.Dichot(10)RefMC`)
    # Wood smoke
    meas$`value.Wood smoke` = with(pr_meas, wood_smoke(value.BC1, value.BC6))
    meas$`flagged.Wood smoke` = with(pr_meas, flagged.BC1 | flagged.BC6)
    # SLP
    meas$value.SLP =
      with(pr_meas, sea_level_pressure(value.BP, value.AmbTemp, 504))
    meas$flagged.SLP = with(pr_meas, flagged.BP | flagged.AmbTemp)
    # SR2 (simple zero-correction)
    meas$value.SR2 = pr_meas$value.SR + 17.7
    meas$flagged.SR2 = pr_meas$flagged.SR
  }
}

# process the derived values
# what was derived?
all_meas = sub('^value\\.', '', names(meas)[grep('^value', names(meas))])
derived = setdiff(all_meas, nonderived)
# update mtypes, including derived values
mtypes = config$channels[config$channels$site == site &
                         config$channels$data_source == data_source, ] %>%
  subset(!is.na(apply_processing) & apply_processing)
for (mname in derived) {
  n = match(mname, mtypes$name)
  message('Processing ', mname, '...')
  msmt_cols = c('time', paste0('value.', mname), paste0('flagged.', mname))
  msmts = meas[, msmt_cols]
  names(msmts) = c('time', 'value', 'flagged')
  m_cals = subset(cals, measurement_name == mname)
  m_zeros = subset(m_cals, type == 'zero')
  m_spans = subset(m_cals, type == 'span')
  m_ces = subset(m_cals, type == 'CE')
  m_conf = add_flag_params(subset(mtypes, name == mname))
  unflags = subset(manual_unflags, measurement_name == mname)
  tryCatch({
    if (is_true(m_conf$has_calibration)) {
      msmts$value = drift_correct(msmts$time, msmts$value, m_zeros, m_spans,
                                  m_conf)
    }
    if (is_true(m_conf$apply_ce)) {
      msmts$value =
        atmoschem.process:::ceff_correct(msmts$time, msmts$value, m_ces, m_conf)
    }
    if (nrow(unflags)) {
      unflagged = msmts$time %within% as.list(unflags$times)
    } else {
      unflagged = F
    }
    autoflagged = atmoschem.process:::is_flagged(msmts$value, m_conf,
                                                 msmts$flagged) & !unflagged
    msmts$flagged = msmts$flagged | autoflagged
    # write to pr_meas
    pr_meas[, paste0('value.', mname)] = msmts$value
    pr_meas[, paste0('flagged.', mname)] = msmts$flagged
  },
  error = function(e) {
    warning('derived value (', mname, ') processing failed: ', e)
  })
}

# write to sqlite file
# convert time to character for compatibility with sqlite
pr_meas$time = format(pr_meas$time, '%Y-%m-%d %H:%M:%S', tz = 'EST')
interm_dir = file.path('analysis', 'intermediate')
dir.create(interm_dir, F, T)
dbpath = paste0('processed_', site, '_', data_source, '.sqlite') %>%
  file.path(interm_dir, .)
db = dbConnect(SQLite(), dbpath)
dbWriteTable(db, 'measurements', pr_meas, overwrite = T)
invisible(dbExecute(db, 'create index time_index on measurements(time)'))
dbDisconnect(db)
