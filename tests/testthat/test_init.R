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

as_sql_array = function(x) {
  gsub('NA', 'null',
       paste0("'{", paste(x, collapse = ','),
              "}'::double precision[]"))
}

## get the running median from postgres
sql_runmed = function(pg, x) {
  q1 = paste0('select runmed(val) over (rows between 120 preceding and 120 following) from unnest(',
              as_sql_array(x),
              ') as val')
  DBI::dbGetQuery(pg, q1)$runmed
}

test_that('postgres running median works', {
  check_db()
  ## this depends on etl_init, so hopefully that worked earlier

  ## does it work in normal cases?
  x1 = 1:5
  r_runmed = rep(median(x1), length(x1))
  pg_runmed = sql_runmed(dbcon$con, x1)
  expect_equal(r_runmed, pg_runmed)

  ## does it work at the end of the sequence?
  x2 = 1:241
  r_runmed_end = zoo::rollapply(x2, 241, median, partial = TRUE)[121:241]
  pg_runmed_end = sql_runmed(dbcon$con, x2)[121:241]
  expect_equal(r_runmed_end, pg_runmed_end)

  ## does it correctly handle nulls?
  x3 = c(1:2, NA, 4:5)
  r_runmed_null = rep(median(x3, na.rm = TRUE), length(x3))
  pg_runmed_null = sql_runmed(dbcon$con, x3)
  expect_equal(r_runmed_null, pg_runmed_null)
})
