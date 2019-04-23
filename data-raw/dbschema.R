## save the database schema for use in a vignette

library(DBI)
library(datamodelr)
library(nysatmoschem)

## create an empty temporary postgres database
dbname = basename(tempfile(pattern=''))
system2('createdb', dbname)

## set up the nysatmoschem schema
dbcon = src_postgres(dbname = dbname)
nysac = etl('nysatmoschem', db = dbcon, dir = 'data')
nysac %>% etl_init()

## get the schema info
sQuery = dm_re_query('postgres')
nysacdb_schema = dbGetQuery(dbcon$con, sQuery)
save(nysacdb_schema, file = 'inst/extdata/nysacdb_schema.rda')

## delete the temporary database
dbDisconnect(dbcon$con)
system2('dropdb', dbname)
