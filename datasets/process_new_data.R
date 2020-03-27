## Generate the official processed dataset files

## need to add command line arguments for atmoschem username and
## password, database name, dataset version -- probably should use
## optparse package
u = '<atmoschem username>'
p = '<atmoschem password>'
dbname = '<database name>'
v = '<version string>'
outdir = 'cleaned/processed_data'

library(nysatmoschem)

sources_list = list(
    WFMS = c('campbell', 'ultrafine', 'aethelometer'),
    WFML = c('envidas', 'campbell', 'mesonet'),
    PSP = 'envidas'
)
years = 2018:2019

dbcon = src_postgres(dbname = dbname)
nysac = etl('nysatmoschem', db = dbcon)
etl_init(nysac)

for (site in names(sources_list)) {
  message('Starting ', site, '...')
  site_sources = sources_list[[site]]
  ## get the raw data
  nysac %>%
    etl_extract(user = u, password = p, sites = site,
                data_sources = site_sources, years = years) %>%
    etl_transform(sites = site, data_sources = site_sources,
                  years = years) %>%
    etl_load(sites = site, data_sources = site_sources,
             years = years)
  ## process
  for (s in site_sources) {
    update_processing(nysac, site, s, '2018-10-01', '2019-10-01')
  }
  ## generate the processed dataset files
  rcols = report_columns[report_columns$site == site, ]
  rep = nysatmoschem:::generate_report(nysac, rcols$column,
                                       rcols$times, rcols$site,
                                       rcols$data_source,
                                       rcols$measurement,
                                       rcols$hourly_measurement,
                                       rcols$units)
  dir.create(outdir, showWarnings = FALSE, recursive = TRUE)
  nysatmoschem:::write_report_files(rep, name = site,
                                    dir = outdir, version = v,
                                    row.names = F, na = '-999')
}
