context('DB')
library(nysatmoschem)

## create temp database
travis_ci = 'TRAVIS' %in% names(Sys.getenv())
if (travis_ci) {
  dbname = 'travis_ci_test'
  has_temp_db = TRUE
} else {
  dbname = basename(tempfile(pattern=''))
  db_res = system2('createdb', dbname)
  if (db_res == 0) {
    has_temp_db = TRUE
    dbcon = src_postgres(dbname = dbname)
  } else {
    has_temp_db = FALSE
  }
}

## make sure the temporary database is dropped even if there's an
## error
if (!travis_ci && has_temp_db) {
  on.exit({
    try(DBI::dbDisconnect(dbcon$con))
    system2('dropdb', dbname)
  })
}

check_db = function() {
  if (!has_temp_db) {
    skip('Temporary database not available')
  }
}

test_that('etl_init works', {
  check_db()
  nysac = etl('nysatmoschem', db = dbcon)
  ## expect no error
  expect_error(etl_init(nysac), NA)
})
