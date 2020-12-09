# Process the data in its original time resolution

# run this script from the project root directory with
# Rscript analysis/process_new_data.R <site> <data_source>

# produces file analysis/intermediate/processed_<site>_<data_source>.sqlite

library(atmoschem.process)
library(lubridate)
library(DBI)
library(RSQLite)

site = commandArgs(trailingOnly = T)[1]
data_source = commandArgs(trailingOnly = T)[2]

start_time = as.POSIXct('2018-10-01', tz = 'EST')
end_time = as.POSIXct('2020-07-01', tz = 'EST')

# organize the ETL object
dbcon = src_postgres(dbname = 'nysatmoschemdb')
nysac = etl('atmoschem.process', db = dbcon)

# R's lead and lag functions aren't so useful. These are better.
lag2 = function(x, k = 1) c(rep(NA, k), head(x, -k))
lead2 = function(x, k = 1) c(tail(x, -k), rep(NA, k))

# get delta precip from bucket instrument's cycling cumulative values
# Open question-- how should this function deal with data gaps?
bucket_precip = function(val, val.max = .5, err.min = -.02) {
  # Carefully deal with value resets from 0.5 back to 0, which usually happens
  # in only one minute, but sometimes takes two minutes with an average
  # measurement (of the previous high value and next low value) in between
  out = c(NA, diff(val))
  resetting = out <= err.min
  # if resetting in one step
  out[resetting & !lag2(resetting) & !lead2(resetting)] = out + val.max
  # if 1st step, resetting in 2 steps-- split the difference over the two steps
  out[resetting & lead2(resetting)] = (lead2(out) - lag2(out) + val.max) / 2
  # if 2nd step, resetting in 2 steps-- this should be the same as above
  out[resetting & lag2(resetting)] = (out - lag2(out, 2) + val.max) / 2
  out
}

# for a vector of binary values, find contiguous clusters of `TRUE` values and
# return a list of lubridate time intervals containing each cluster
interval_list = function(t, val) {
  data.frame(time = t, true = val) %>%
    transform(group = cumsum(true & c(F, !lag(true)[-1]))) %>%
    subset(true) %>%
    split(., .$group) %>%
    lapply(function(x) interval(min(x$time), max(x$time)))
}

# add time around a lubridate interval
pad_interval = function(interval, start, end) {
  interval(int_start(interval) - start, int_end(interval) - end)
}

# organize data from processed_measurements

# get all the measurement IDs for a data source
site_id = switch(site, WFMS = 1, WFML = 2, PSP = 3, QC = 4)
data_sources = nysac %>%
  tbl('data_sources') %>%
  filter(site_id == local(site_id),
         name == data_source) %>%
  collect()
mtypes = nysac %>%
  tbl('measurement_types') %>%
  filter(data_source_id == local(data_sources$id),
         is.na(derived) | !derived) %>%
  collect()

# make processed data

# 1) update database stuff
message('Updating processing inputs ...')
q_in = 'select update_processing_inputs(?site, ?ds, ?start, ?end)'
sql_in = sqlInterpolate(nysac$con, q_in, site = site_id, ds = data_source,
                        start = format(start_time, tz = 'EST'),
                        end = format(end_time, tz = 'EST'))
dbExecute(nysac$con, sql_in)

# 2) get measurements
obs = tbl(nysac, 'processed_observations')
meas = tbl(nysac, 'measurements') %>%
  filter(measurement_type_id %in% local(mtypes$id)) %>%
  left_join(obs, c('observation_id' = 'id')) %>%
  filter(timezone('EST', time) >= local(start_time),
         timezone('EST', time) < local(end_time)) %>%
  arrange(time) %>%
  mutate(time = timezone('EST', time)) %>%
  collect() %>%
  as.data.frame %>%
  transform(param = mtypes$name[match(measurement_type_id, mtypes$id)]) %>%
  reshape(timevar = 'param', idvar = 'time', direction = 'wide',
          drop = c('observation_id', 'measurement_type_id'))
attr(meas$time, 'tzone') = 'EST'
# keep track of the non-derived measurements, for later
nonderived = sub('^value\\.', '', names(meas)[grep('^value', names(meas))])

# 2.5) add miscellaneous flags that aren't included in
# `atmoschem.process:::is_flagged`
# power outage flags
if (site %in% c('WFMS', 'WFML') & data_source == 'campbell') {
  if (site == 'WFMS') {
    is_outage = with(meas, value.Phase1 < 105 | value.Phase3 < 100)
    padding_list = list(CO = 50, NO = 4, NOx = 4, NOy = 4, Ozone = 10, SO2 = 4)
  } else if (site == 'WFML') {
    is_outage = meas$value.Phase1 < 105
    padding_list = list(CO = 10, NO = 45, NOX = 45)
  }
  outage_periods = interval_list(meas$time, is_outage)
  for (i in 1:length(padding_list)) {
    varname = paste0('value.', names(padding_list)[i])
    # pad the flagging period by 2 minutes on the front end for all values
    padded_periods = lapply(outage_periods, pad_interval,
                            start = as.difftime(2, units='mins'),
                            end = as.difftime(padding_list[[i]], units='mins'))
    meas[meas$time %within% padded_periods, varname] = T
  }
}

# 3) process measurements
pr_meas = data.frame(time = meas$time)
# update mtypes with only processed measurement types
mtypes = nysac %>%
  tbl('measurement_types') %>%
  filter(data_source_id == local(data_sources$id),
         !is.na(apply_processing) & apply_processing,
         is.na(derived) | !derived) %>%
  collect()
for (n in 1:nrow(mtypes)) {
  mname = mtypes$name[n]
  message('Processing ', mname, '...')
  if (paste0('value.', mname) %in% names(meas)) {
    msmt_cols = c('time', paste0('value.', mname), paste0('flagged.', mname))
    msmts = meas[, msmt_cols]
    names(msmts) = c('time', 'value', 'flagged')
    msmts$measurement_type_id = mtypes$id[n]
    tryCatch({
      pr_msmts = atmoschem.process:::process(nysac, msmts, mtypes$id[n])
      # write to pr_meas
      pr_meas[, paste0('value.', mname)] = pr_msmts$value
      pr_meas[, paste0('flagged.', mname)] = pr_msmts$flagged
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
    meas$value.NO2 = with(pr_meas, value.NOx - value.NO)
    meas$flagged.NO2 = with(pr_meas, flagged.NOx | flagged.NO)
    # SLP
    meas$value.SLP =
      with(pr_meas, sea_level_pressure(value.BP, value.PTemp_C, 1483.5))
    meas$flagged.SLP = with(pr_meas, flagged.BP | flagged.PTemp_C)
    # WS (wind shadow corrected)
    meas$value.WS = with(pr_meas, pmax(value.WS3Cup, value.WS3CupB, na.rm = T))
    meas$flagged.WS = with(pr_meas, flagged.WS3Cup & flagged.WS3CupB)
    # WS_max (wind shadow corrected)
    meas$value.WS_Max =
      with(pr_meas, pmax(value.WS3Cup_Max, value.WS3CupB_Max, na.rm = T))
    meas$flagged.WS_Max =
      with(pr_meas, flagged.WS3Cup_Max & flagged.WS3CupB_Max)
  } else if (data_source == 'aethelometer') {
    # Wood smoke
    meas$`value.Wood smoke` =
      with(pr_meas,
           wood_smoke(value.concentration_370, value.concentration_880))
    meas$`flagged.Wood smoke` =
      with(pr_meas, flagged.concentration_370 | flagged.concentration_880)
  }
} else if (site == 'WFML') {
  if (data_source == 'campbell') {
    # NO2
    meas$value.NO2 = with(pr_meas, value.NOX - value.NO)
    meas$flagged.NO2 = with(pr_meas, flagged.NOX | flagged.NO)
    # SLP
    meas$value.SLP =
      with(pr_meas, sea_level_pressure(value.BP2, value.PTemp_C, 604))
    meas$flagged.SLP = with(pr_meas, flagged.BP2 | flagged.PTemp_C)
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
    meas$flagged.Precip = with(pr_meas, flagged.Rain | is.na(value.Precip))
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
mtypes = nysac %>%
  tbl('measurement_types') %>%
  filter(data_source_id == local(data_sources$id),
         !is.na(apply_processing) & apply_processing) %>%
  collect()
for (mname in derived) {
  n = match(mname, mtypes$name)
  message('Processing ', mname, '...')
  msmt_cols = c('time', paste0('value.', mname), paste0('flagged.', mname))
  msmts = meas[, msmt_cols]
  names(msmts) = c('time', 'value', 'flagged')
  msmts$measurement_type_id = mtypes$id[n]
  tryCatch({
    pr_msmts = atmoschem.process:::process(nysac, msmts, mtypes$id[n])
    # write to pr_meas
    pr_meas[, paste0('value.', mname)] = pr_msmts$value
    pr_meas[, paste0('flagged.', mname)] = pr_msmts$flagged
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
dbDisconnect(db)
