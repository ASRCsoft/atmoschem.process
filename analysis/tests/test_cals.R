library(DBI)
library(RSQLite)

dbpath = function(site) {
  file.path('..', 'intermediate', paste0('cals_', site, '.sqlite'))
}

if (file.exists(dbpath('WFMS'))) {
  db = dbConnect(SQLite(), dbpath('WFMS'))
  
  sql_txt = "
select measured_value/provided_value as ratio
  from calibrations
 where measurement_name='NO'
   and type='span'
   and end_time>'2018-11-15'
   and end_time<'2018-11-26'"
  res = dbGetQuery(db, sql_txt)
  expect_true(max(res$ratio) < 1.1,
              "beginning spikes don't break span estimates")
  
  sql_txt = "
select measured_value/provided_value as ratio
  from calibrations
 where measurement_name='NO'
   and type='span'
   and end_time>'2019-08-08'
   and end_time<'2019-08-14'"
  res = dbGetQuery(db, sql_txt)
  expect_true(max(res$ratio) < 1.07, "ending spikes don't break span estimates")
  
  dbDisconnect(db)
}
