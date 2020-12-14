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

# load the data
years = 2018:2020
for (site in c('WFMS', 'WFML', 'PSP')) {
  message('Starting ', site, '...')
  nysac %>%
    # ignore the measurement files and only transform/load the calibration files
    etl_transform(sites = site, data_sources = 'nothing',
                  years = years) %>%
    etl_load(sites = site, data_sources = 'nothing',
             years = years)
}
