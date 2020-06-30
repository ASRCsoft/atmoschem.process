# Set up the processing database

library(nysatmoschem)

dbname = 'nysatmoschemdb'
# delete database if it already exists, then create it
try(system2('dropdb', args = dbname))
system2('createdb', args = dbname)

# organize the ETL object
dbcon = src_postgres(dbname = dbname)
nysac = etl('nysatmoschem', db = dbcon)
attr(nysac, 'raw_dir') = 'datasets/raw/raw_data_v0.1'
attr(nysac, 'load_dir') = 'datasets/cleaned/raw_data'
etl_init(nysac)

# load the data
sources_list = list(
    WFMS = c('campbell', 'ultrafine', 'aethelometer'),
    WFML = c('envidas', 'campbell', 'mesonet'),
    PSP = c('envidas', 'mesonet')
)
years = 2018:2020
for (site in names(sources_list)) {
  message('Starting ', site, '...')
  site_sources = sources_list[[site]]
  nysac %>%
    etl_transform(sites = site, data_sources = site_sources,
                  years = years) %>%
    etl_load(sites = site, data_sources = site_sources,
             years = years)
}
