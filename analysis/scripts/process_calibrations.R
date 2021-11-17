# apply calibration adjustments and flags to cal check results

# run like so:
# Rscript analysis/scripts/process_calibrations.R <site>

# produces file analysis/intermediate/processedcals_<site>.sqlite

library(atmoschem.process)
suppressPackageStartupMessages(library(lubridate))
library(magrittr)
library(DBI)
library(RSQLite)

site = commandArgs(trailingOnly = T)[1]
config = read_csv_dir('analysis/config')

# get some site-specific config info
site_channels = config$channels[config$channels$site == site, ]
cal_flags = config$cal_flags[config$cal_flags$site == site, ] %>%
  transform(times = as_interval(times))
site_flows = config$cal_flows[config$cal_flows$site == site, ]
site_flows$time = as.Date(site_flows$time, tz = 'EST')

# set up database connections
dbinpath = file.path('analysis', 'intermediate',
                     paste0('cals_', site, '.sqlite'))
dbin = dbConnect(SQLite(), dbinpath)
dboutpath = file.path('analysis', 'intermediate',
                      paste0('processedcals_', site, '.sqlite'))
if (file.exists(dboutpath)) invisible(file.remove(dboutpath))
dbout = dbConnect(SQLite(), dboutpath)

# determine whether a cal check result is flagged based on the end time of the
# check (t)
cal_flagged = function(t, datalogger, param, type) {
  if (!length(t)) return(logical(0))
  flag_times = cal_flags[cal_flags$data_source == datalogger &
                         cal_flags$measurement_name == param &
                         cal_flags$type == type, 'times']
  if (!length(flag_times)) return(FALSE)
  t %within% as.list(flag_times)
}

# get the raw cal check results
get_cals = function(datalogger, param, type) {
  sql_str = "
select *
  from calibrations
 where data_source=?
   and measurement_name=?
   and type=?
 order by end_time asc"
  q = sqlInterpolate(dbin, sql_str, datalogger, param, type)
  res = dbGetQuery(dbin, q)
  res$start_time = as.POSIXct(res$start_time, tz = 'EST')
  res$end_time = as.POSIXct(res$end_time, tz = 'EST')
  if (any(is.na(res$end_time))) {
    nmiss = sum(is.na(res$end_time))
    warning('Missing ', nmiss,
            ' calibration end times. Guessing the end times.')
    res$end_time[is.na(res$end_time)] =
      res$start_time[is.na(res$end_time)] + as.difftime(1, units = 'hours')
    res = res[order(res$end_time), ]
  }
  res$manual = as.logical(res$manual)
  res$flagged = cal_flagged(res$end_time, datalogger, measurement, type)
  res
}

append_cals = function(cals) {
  # write time, value, corrected columns
  transform(cals, time = format(end_time, '%Y-%m-%d %H:%M:%S', tz = 'EST')) %>%
    subset(select = c(data_source, measurement_name, type, time, value, corrected)) %>%
    dbWriteTable(dbout, 'calibrations', ., append = TRUE)
}

# get the processed cal check results
get_processed_cals = function(datalogger, param, type) {
  sql_str = "
select *
  from calibrations
 where data_source=?
   and measurement_name=?
   and type=?
 order by time asc"
  q = sqlInterpolate(dbout, sql_str, datalogger, param, type)
  res = dbGetQuery(dbout, q)
  res$time = as.POSIXct(res$time, tz = 'EST')
  res
}

get_flow_values = function(param, t) {
  flows = site_flows[site_flows$measurement_name == param, ]
  f_breaks = flows$time[is_true(flows$changed)]
  estimate_cals(flows$time, flows$measured_value, NA, t, f_breaks)
}

parameters = dbGetQuery(dbin, 'select distinct data_source, measurement_name from calibrations')

for (i in seq_len(nrow(parameters))) {
  datalogger = parameters$data_source[i]
  measurement = parameters$measurement_name[i]
  if (measurement == 'NO2') next()
  meas_conf = site_channels[site_channels$data_source == datalogger &
                            site_channels$name == measurement, ]
  # skip if there's no processing instructions
  if (!nrow(meas_conf)) next()
  # zero results don't need to be corrected
  zeros = get_cals(datalogger, measurement, 'zero')
  zeros$value = zeros$measured_value
  append_cals(zeros)
  spans = get_cals(datalogger, measurement, 'span')
  if (nrow(spans)) {
    # zero values estimated at the span times
    z_breaks = zeros$end_time[is_true(zeros$corrected)]
    s_zeros = estimate_cals(zeros$end_time, zeros$measured_value,
                            meas_conf$zero_smooth_window, spans$end_time,
                            z_breaks)
    spans$value = spans$measured_value - s_zeros
    # convert spans to ratio, possibly using flow cals
    if (site == 'WFMS') {
      provided_value = get_flow_values(meas_conf$gilibrator_span,
                                       spans$end_time)
    } else {
      provided_value = spans$provided_value
    }
    spans$value = spans$value / provided_value
    append_cals(spans)
  }
  # get CE cals
  ceffs = get_cals(datalogger, measurement, 'CE')
  if (nrow(ceffs)) {
    # zero and span values estimated at the ceff times
    z_breaks = zeros$end_time[is_true(zeros$corrected)]
    ce_zeros = estimate_cals(zeros$end_time, zeros$measured_value,
                             meas_conf$zero_smooth_window, ceffs$end_time,
                             z_breaks)
    s_breaks = spans$end_time[is_true(spans$corrected)]
    ce_spans = estimate_cals(spans$end_time, spans$value,
                             meas_conf$span_smooth_window, ceffs$end_time,
                             s_breaks)
    ceffs$value = (ceffs$measured_value - ce_zeros) / ce_spans
    # Convert ceffs to ratio, possibly using flow cals. But only if it applies
    # to this parameter-- otherwise need to save the non-ratio value, and the
    # ratio is calculated further down
    if (is_true(meas_conf$apply_ce)) {
      if (site == 'WFMS') {
        provided_value = get_flow_values(meas_conf$gilibrator_ce,
                                         ceffs$end_time)
      } else {
        provided_value = ceffs$provided_value
      }
      ceffs$value = ceffs$value / provided_value
    }
    append_cals(ceffs)
  }
}

# Calculate NO2 conversion efficiencies (derived from NOx and NO results)
if (site == 'WFMS') {
  # WFMS NO2 conversion efficiencies are recorded on the NOx channel
  no2_ceffs = rbind(get_processed_cals('campbell', 'NOx_Avg', 'CE'),
                    get_processed_cals('envidas', 'NOX', 'CE'))
  no2_ceffs$measurement_name = 'NO2'
  # convert to ratio
  no2_ceffs$value = no2_ceffs$value / get_flow_values('NO2', no2_ceffs$time)
  no2_ceffs$time = format(no2_ceffs$time, '%Y-%m-%d %H:%M:%S', tz = 'EST')
  dbWriteTable(dbout, 'calibrations', no2_ceffs, append = TRUE)
} else if (site == 'PSP') {
  # PSP NO2 conversion efficiencies require subtracting NO, because the CE air
  # includes both NO2 and NO
  no2_ceffs = rbind(get_processed_cals('envidas', 'NO', 'CE'),
                    get_processed_cals('envidas', 'NOx', 'CE'))
  no2_ceffs = reshape(no2_ceffs, timevar = 'measurement_name', idvar = 'time',
                      v.names = 'value', direction = 'wide')
  # also need to get the raw values to get the provided_value
  nox_ceffs = get_cals('envidas', 'NOx', 'CE')
  provided_values =
    nox_ceffs$provided_value[match(no2_ceffs$time, nox_ceffs$end_time)]
  no2_ceffs$value = with(no2_ceffs, value.NOx - value.NO) / provided_values
  no2_ceffs$measurement_name = 'NO2'
  no2_ceffs[, c('value.NO', 'value.NOx')] = NULL
  no2_ceffs$time = format(no2_ceffs$time, '%Y-%m-%d %H:%M:%S', tz = 'EST')
  dbWriteTable(dbout, 'calibrations', no2_ceffs, append = TRUE)
}

dbDisconnect(dbin)
dbDisconnect(dbout)
