# Set up the processing database

library(atmoschem.process)

dbname = 'nysatmoschemdb'
# delete database if it already exists, then create it
try(system2('dropdb', args = dbname))
system2('createdb', args = dbname)

# organize the ETL object
dbcon = src_postgres(dbname = dbname)
nysac = etl('atmoschem.process', db = dbcon)
attr(nysac, 'raw_dir') = 'analysis/raw/raw_data_v0.3'
attr(nysac, 'load_dir') = 'analysis/cleaned/raw_data'
etl_init(nysac)
