library(atmoschem.process)

dbname = 'nysatmoschem_unit_test_tmp'
setup({
  db_res = system2('createdb', dbname)
  tmp_db_available <<- db_res == 0
})
teardown({
  if (tmp_db_available) system2('dropdb', dbname)
})

test_that('etl_init works', {
  skip_if_not(tmp_db_available)
  dbcon = src_postgres(dbname = dbname)
  on.exit(DBI::dbDisconnect(dbcon$con))
  nysac = etl('atmoschem.process', db = dbcon)
  ## expect no error
  expect_error(etl_init(nysac), NA)
})
