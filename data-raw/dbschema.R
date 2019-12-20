## save the database schema for use in a vignette

library(DBI)
library(datamodelr)
library(nysatmoschem)

## create an empty temporary postgres database
dbname = basename(tempfile(pattern=''))
system2('createdb', dbname)

## make sure the temporary database is dropped even if there's an
## error
on.exit({
  try(dbDisconnect(dbcon$con))
  system2('dropdb', dbname)
})

## set up the nysatmoschem schema
dbcon = src_postgres(dbname = dbname)
nysac = etl('nysatmoschem', db = dbcon, dir = 'data')
nysac %>% etl_init()

## get the schema info
sQuery = dm_re_query('postgres')
nysacdb_schema = dbGetQuery(dbcon$con, sQuery)
## don't include intermediate processing tables in this schema
nysacdb_schema = subset(nysacdb_schema, !startsWith(table, '_'))
save(nysacdb_schema, file = 'vignettes/nysacdb_schema.rda')
