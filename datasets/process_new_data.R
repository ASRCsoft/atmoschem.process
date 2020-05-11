# Generate the official processed dataset files
# add environment variable for database name?
library(nysatmoschem)

dbname = 'nysatmoschemdb'
outdir = 'datasets/cleaned/processed_data'
# delete database if it already exists, then create it
try(system2('dropdb', args = dbname))
system2('createdb', args = dbname)

# organize the ETL object
dbcon = src_postgres(dbname = dbname)
nysac = etl('nysatmoschem', db = dbcon)
attr(nysac, 'raw_dir') = 'datasets/raw/raw_data_v0.1'
attr(nysac, 'load_dir') = 'datasets/cleaned/raw_data'
etl_init(nysac)

sources_list = list(
    WFMS = c('campbell', 'ultrafine', 'aethelometer'),
    WFML = c('envidas', 'campbell', 'mesonet'),
    PSP = 'envidas'
)
years = 2018:2020
for (site in names(sources_list)) {
  message('Starting ', site, '...')
  site_sources = sources_list[[site]]
  # add the raw data to postgres
  nysac %>%
    etl_transform(sites = site, data_sources = site_sources,
                  years = years) %>%
    etl_load(sites = site, data_sources = site_sources,
             years = years)
  # process
  for (s in site_sources) {
    update_processing(nysac, site, s, '2018-10-01', '2020-01-01')
  }
  # generate the processed dataset files
  rcols = report_columns[report_columns$site == site, ]
  rep = nysatmoschem:::generate_report(nysac, rcols$column,
                                       rcols$times, rcols$site,
                                       rcols$data_source,
                                       rcols$measurement,
                                       rcols$hourly_measurement,
                                       rcols$units)
  dir.create(outdir, showWarnings = FALSE, recursive = TRUE)
  nysatmoschem:::write_report_files(rep, name = site, dir = outdir,
                                    row.names = F, na = '-999')
}
