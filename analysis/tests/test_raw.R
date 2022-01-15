library(DBI)
library(RSQLite)

dbpath = function(site, datalogger) {
  file.path('..', 'intermediate',
            paste0('raw_', site, '_', datalogger, '.sqlite'))
}

# test that raw data values are recorded with correct times in EST
check_time_zone = function(db, param, time, value) {
  sql_text = "
select ?
  from measurements
 where time=?"
  sql = sqlInterpolate(db, sql_text,
                       dbQuoteIdentifier(db, paste0('value.', param)), time)
  res = dbGetQuery(db, sql)
  info_str = paste(basename(db@dbname), 'has correct time zone')
  expect_equal(res[1, 1], value, info = info_str)
}

time_zone_checks = read.csv('raw_time_zone_checks.csv')

for (i in seq_len(nrow(time_zone_checks))) {
  file = dbpath(time_zone_checks$site[i], time_zone_checks$datalogger[i])
  if (file.exists(file)) {
    db = dbConnect(SQLite(), file)
    check_time_zone(db, time_zone_checks$parameter[i], time_zone_checks$time[i],
                    time_zone_checks$value[i])
    dbDisconnect(db)
  }
}
