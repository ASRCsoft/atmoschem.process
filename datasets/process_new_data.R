# Generate the official processed dataset files

library(nysatmoschem)

site = commandArgs(trailingOnly = TRUE)[1]
dbname = 'nysatmoschemdb'
outdir = 'datasets/cleaned/processed_data'

# organize the ETL object
dbcon = src_postgres(dbname = dbname)
nysac = etl('nysatmoschem', db = dbcon)

sources_list = list(
    WFMS = c('campbell', 'ultrafine', 'aethelometer'),
    WFML = c('envidas', 'campbell', 'mesonet'),
    PSP = 'envidas'
)
years = 2018:2020
site_sources = sources_list[[site]]
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
