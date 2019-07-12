context('DB')
library(nysatmoschem)

## create temp database
dbname = basename(tempfile(pattern=''))
db_res = try(system2('createdb', dbname))
if (class(db_res) == 'try-error') {
  has_temp_db = FALSE
} else {
  has_temp_db = TRUE
  dbcon = src_postgres(dbname = dbname)
}

## make sure the temporary database is dropped even if there's an
## error
if (has_temp_db) {
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
