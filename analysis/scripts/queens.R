# collect and organize Queens College data

# run this script from the project root directory with
# Rscript analysis/scripts/queens.R

# produces file analysis/intermediate/hourly_QC_AQS.sqlite

library(magrittr)
library(atmoschem.process)
library(DBI)
library(RSQLite)

years = 2001:2021
site = '36-081-0124'
email = Sys.getenv('aqs_email')
key = Sys.getenv('aqs_key')
aqs_params = read.csv('analysis/config/aqs_queens.csv')
aqs_params$aqs_code = as.character(aqs_params$aqs_code)

aqs_dict = aqs_flags %>%
  subset(!is.na(narsto)) %>%
  with(setNames(narsto, code))

# get NARSTO flag from AQS flag and data value
narsto_from_aqs = function(val, aqs) {
  if (!all(na.omit(unique(aqs)) %in% names(aqs_dict))) {
    distinct_aqs = na.omit(unique(aqs))
    missing = distinct_aqs[!distinct_aqs %in% names(aqs_dict)]
    missing_str = paste(missing, collapse = ', ')
    stop('Untranslated flags: ', missing_str)
  }
  flag = aqs_dict[aqs]
  # if there's no data (flag or value), NARSTO says M1
  flag[is.na(val) & is.na(flag)] = 'M1'
  # if AQS has no flag, meaning no problems, NARSTO says V0
  flag[!is.na(val) & is.na(flag)] = 'V0'
  # use M2 for data that doesn't need to be missing according to NARSTO but has
  # been removed anyway
  flag[is.na(val) & !startsWith(flag, 'M')] = 'M2'
  flag
}

# NARSTO flags, ordered by importance
narsto_priority = c('H1', 'M1', 'M2', paste0('V', 7:0))
prioritize_narsto_flag = function(x, y) {
  x_priority = match(x, narsto_priority)
  y_priority = match(y, narsto_priority)
  ifelse(x_priority < y_priority, x, y)
}

# Make method table (with method time ranges) from sample table. For each
# parameter, prefer the newest available method
get_methods = function(samples) {
  # get method date ranges
  starts = aggregate(time ~ parameter_code + method, FUN = min, data = samples)
  names(starts)[3] = 'method_start'
  starts = starts[with(starts, order(parameter_code, method_start)), ]
  starts %>%
    transform(method_end = c(tail(method_start, -1), NA),
              param_changes = c(diff(as.integer(parameter_code)) != 0, F)) %>%
    transform(method_end = replace(method_end, param_changes, NA)) %>%
    subset(select = c('parameter_code', 'method', 'method_start', 'method_end'))
}


samples = aqs_api_samples(aqs_params$aqs_code, site, years, email, key)

# save(samples, file = 'tmp.RData')
# load('tmp.RData')

samples %<>%
  subset(sample_duration == '1 HOUR') %>%
  transform(time = as.POSIXct(paste(date_gmt, time_gmt), tz = 'UTC'))
names(samples) = sub('sample_measurement', 'value', names(samples))
# print the times in EST
attr(samples$time, 'tzone') = 'EST'

# when multiple values are collected simultaneously, use only the newest method
methods = get_methods(samples)
samples %<>%
  merge(methods) %>%
  subset(is.na(method_end) | time < method_end,
         select = -c(method_start, method_end))

# get NARSTO flags
samples %<>%
  transform(qualifier = sub(' -.*', '', qualifier)) %>%
  transform(flag = narsto_from_aqs(value, qualifier))

# organize into wide format
wide = samples %>%
  subset(select = c('time', 'parameter_code', 'value', 'flag')) %>%
  reshape(v.names = c('value', 'flag'), timevar = 'parameter_code',
          idvar = 'time', direction = 'wide')
wide = wide[order(wide$time), ]
param_code = sub('.*\\.', '', names(wide)[-1])
parameter = aqs_params$column[match(param_code, aqs_params$aqs_code)]
names(wide)[-1] = paste0(sub('[0-9]+', '', names(wide)[-1]), parameter)

# get derived values

# NMHC
wide$value.NMHC = with(wide, `value.Total hydrocarbons` - value.CH4)
wide$flag.NMHC =
  with(wide, prioritize_narsto_flag(flag.CH4, `flag.Total hydrocarbons`))
# should impossible/improbable NMHC values be flagged?
# unit conversions
wide$value.T_C = (wide$value.T - 32) * 5 / 9
wide$flag.T_C = wide$flag.T
wide$value.WS_ms = wide$value.WS / 1.944
wide$flag.WS_ms = wide$flag.WS
wide$value.Ozone_ppbv = wide$value.Ozone * 1000
wide$flag.Ozone_ppbv = wide$flag.Ozone

# write to sqlite
wide$time = format(wide$time, '%Y-%m-%d %H:%M:%S', tz = 'EST')
interm_dir = file.path('analysis', 'intermediate')
dir.create(interm_dir, F, T)
dbpath = paste0('hourly_QC_AQS.sqlite') %>%
  file.path(interm_dir, .)
db = dbConnect(SQLite(), dbpath)
dbWriteTable(db, 'measurements', wide, overwrite = T)
dbExecute(db, 'create index time_index on measurements(time)')
dbDisconnect(db)
