# make hourly aggregated data

# run this script from the project root directory with
# Rscript analysis/aggregate_hourly.R <site> <data_source>

# produces file analysis/intermediate/hourly_<site>_<data_source>.sqlite

library(atmoschem.process)
library(magrittr)
library(DBI)
library(RSQLite)

site = commandArgs(trailingOnly = T)[1]
data_source = commandArgs(trailingOnly = T)[2]

# the wind speed and direction variables-- this should be made into a config
# option somehow
winds = switch(site, WFMS = c('WS', 'WindDir_D1_WVT'), 
               WFML = c('wind_speed [m/s]', 'wind_direction [degrees]'),
               PSP = c('VWS', 'VWD'))
if (data_source == 'mesonet')
  winds = c('wind_speed [m/s]', 'wind_direction [degrees]')

# aggregate function for each measurement type. It's a bit awkward to organize
# things this way. This should be specified in a config file instead
get_agg_fun = function(name) {
  fun = rep('mean', length(name))
  fun[grepl('^v?w[ds]|^wind', name, ignore.case = T)] = 'wind'
  fun[grepl('_Max$|_maximum', name)] = 'max'
  fun[name == 'Precip'] = 'sum'
  fun[name == 'precip_since_00Z [mm]'] = 'head'
  # if we have unused wind columns just get the mean
  fun[!name %in% winds & fun == 'wind'] = 'mean'
  fun
}

# similar to `stats::aggregate`, but for resultant wind speeds. ws and wd are
# the column names of the wind speed and directions, respectively, inside data
# frame x
aggregate_wind = function(x, f, ws, wd) {
  res = x %>%
    split(f) %>%
    sapply(function(x) {
      c(res_wind_speed(x[[ws]], x[[wd]]), res_wind_dir(x[[ws]], x[[wd]]))
    }) %>%
    t %>%
    as.data.frame %>%
    transform(., time = as.POSIXct(row.names(.), tz = 'EST')) %>%
    subset(select = c(3, 1:2))
  names(res)[2:3] = c(ws, wd)
  res
}

# get measurement info
mtypes = measurement_types[measurement_types$site == site &
                           measurement_types$data_source == data_source, ] %>%
  subset(!is.na(apply_processing) & apply_processing)

# get the processed data
dbpath = file.path('analysis', 'intermediate',
                   paste0('processed_', site, '_', data_source, '.sqlite'))
db = dbConnect(SQLite(), dbpath)
site_data = dbReadTable(db, 'measurements', check.names = F)
dbDisconnect(db)
site_data$time = as.POSIXct(site_data$time, tz = 'EST')

# aggregate
time_hours = as.POSIXct(trunc(site_data$time, 'hour'))
val_mat = site_data[, grep('value', names(site_data)), drop = F]
flag_mat = site_data[, grep('flagged', names(site_data)), drop = F]
# remove flagged data
val_mat[!is.na(flag_mat) & flag_mat] = NA
agg_funs = names(val_mat) %>%
  sub('^value\\.', '', .) %>%
  get_agg_fun
hourly_vals = agg_funs %>%
  unique %>%
  # create a list of aggregated data frames, one per aggregation method
  lapply(function(x) {
    xmat = val_mat[, agg_funs == x, drop = F]
    if (x == 'mean') {
      aggregate(xmat, by = list(time = time_hours), mean, na.rm = T)
    } else if (x == 'max') {
      aggregate(xmat, by = list(time = time_hours),
                function(x) if (all(is.na(x))) NA else max(x, na.rm = T))
    } else if (x == 'sum') {
      aggregate(xmat, by = list(time = time_hours), sum, na.rm = T)
    } else if (x == 'head') {
      aggregate(xmat, by = list(time = time_hours), head, n = 1)
    } else if (x == 'wind') {
      windcols = paste0('value.', winds)
      aggregate_wind(xmat, time_hours, windcols[1], windcols[2])
    } else {
      stop(paste('aggregate function', x, 'not recognized'))
    }
  }) %>%
  # merge all the aggregated data frames
  Reduce(function(x, y) merge(x, y, all = T), .) %>%
  # put the columns back in the same order they started in
  subset(select = c('time', names(val_mat)))

# for flags, need count and below_mdl
count_mat = flag_mat %>%
  aggregate(by = list(time = time_hours),
            FUN = function(x) sum(!is.na(x) & !x)) %>%
  subset(select = -1) %>%
  as.matrix
mdls = names(hourly_vals)[-1] %>%
  sub('^value\\.', '', .) %>%
  match(mtypes$name) %>%
  mtypes$mdl[.]
below_mdl = hourly_vals %>%
  subset(select = -1) %>%
  scale(mdls, scale = F) %>%
  {!is.na(.) & . < 0}
# put this info in a data_sources config file
hourly_freq = switch(data_source, aethelometer = 4, mesonet = 12, 60)
flags = narsto_agg_flag(count_mat / hourly_freq, below_mdl)

# combine vals and flags
ntypes = ncol(flags)
hourly_ds = cbind(hourly_vals, flags) %>%
  # intersperse the flags with the values
  subset(select = c(1, rep(1:ntypes + 1, each = 2) + c(0, ntypes))) %>%
  # convert time to character for compatibility with sqlite
  # transform(time = format(time)) # this one removes spaces from names :(
  within(time <- format(time, '%Y-%m-%d %H:%M:%S', tz = 'EST'))
# hourly flag columns are different than the "flagged" binary value in the
# high-resolution data, so I think it deserves a different column name to
# distinguish it
names(hourly_ds) = names(hourly_ds) %>%
  sub('^flagged', 'flag', .)

# write to sqlite file
interm_dir = file.path('analysis', 'intermediate')
dir.create(interm_dir, F, T)
dbpath = paste0('hourly_', site, '_', data_source, '.sqlite') %>%
  file.path(interm_dir, .)
db = dbConnect(SQLite(), dbpath)
dbWriteTable(db, 'measurements', hourly_ds, overwrite = T)
dbDisconnect(db)
