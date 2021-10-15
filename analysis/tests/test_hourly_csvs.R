# make sure the hourly csv files are formatted correctly

library(atmoschem.process)

csv_dir = file.path('..', '..', Sys.getenv('routine_out'))
config = read_csv_dir(file.path('..', '..', 'analysis', 'config'))

# check that data with an M* flag is NA
check_M_data = function(dat, site) {
  flag_cols = grep('\\(flag\\)', names(dat))
  for (i in flag_cols) {
    m_flags = which(startsWith(dat[, i], 'M'))
    if (length(m_flags)) {
      m_data = dat[m_flags, i - 1]
      info = paste(site, names(dat)[i - 1], 'M* values are NA')
      expect_true(all(is.na(m_data)), info)
    }
  }
}

# read hourly files for testing
hourly = list()
for (site in config$sites$abbreviation) {
  site_csv = file.path(csv_dir, paste0('hourly_', site, '.csv'))
  if (file.exists(site_csv))
    hourly[[site]] = read.csv(site_csv, check.names = F)
}

# check numeric formatting (just WFML NO to make sure it's running)
wfml = hourly[['WFML']]
no_config = subset(config$channels, site == 'WFML' & data_source == 'campbell' &
                                    name == 'NO')
no_report_decimals = no_config$report_decimals
new_no = wfml[wfml$`Time (EST)` >= '2018-10-01', 'NO (ppbv)']
max_no_decimals =
  max(nchar(sub('^[^.]*\\.?', '', as.character(new_no))), na.rm = T)
expect_true(max_no_decimals <= no_report_decimals, 'correct number of decimals')

# check M* flag data
for (site in names(hourly)) check_M_data(hourly[[site]], site)

# check for missing hours
for (site in names(hourly)) {
  info = paste(site, 'includes all hours')
  csv_dat = hourly[[site]]
  time_range = as.POSIXct(range(csv_dat$`Time (EST)`), tz = 'EST')
  all_hours = seq(time_range[1], time_range[2], by = 'hour')
  csv_hours = as.POSIXct(csv_dat$`Time (EST)`, tz = 'EST')
  same_hour_numbers = length(all_hours) == length(csv_hours)
  expect_true(same_hour_numbers && all(all_hours == csv_hours), info)
}
