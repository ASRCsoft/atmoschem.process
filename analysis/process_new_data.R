# Process the data in its original time resolution

# run this script from the project root directory with
# Rscript analysis/process_new_data.R <site> <data_source>

# produces file analysis/intermediate/processed_<site>_<data_source>.sqlite

library(atmoschem.process)
library(DBI)
library(RSQLite)

site = commandArgs(trailingOnly = T)[1]
data_source = commandArgs(trailingOnly = T)[2]

# organize the ETL object
dbcon = src_postgres(dbname = 'nysatmoschemdb')
nysac = etl('atmoschem.process', db = dbcon)

# process
update_processing(nysac, site, s, '2018-10-01', '2020-07-01')

# organize data from processed_measurements

# get all the measurement IDs for a data source
site_id = switch(site, WFMS = 1, WFML = 2, PSP = 3, QC = 4)
data_sources = nysac %>%
  tbl('data_sources') %>%
  filter(site_id == local(site_id),
         name == data_source) %>%
  collect()
mtypes = nysac %>%
  tbl('measurement_types') %>%
  filter(data_source_id %in% local(data_sources$id),
         !is.na(apply_processing) & apply_processing) %>%
  collect()

# collect and organize all data from the data source
site_data = nysac %>%
  tbl('processed_measurements') %>%
  filter(measurement_type_id %in% local(mtypes$id),
         !is.na(value),
         !flagged) %>%
  # add time zone so R interprets the time correctly
  mutate(time = timezone('EST', time)) %>%
  collect() %>%
  as.data.frame %>%
  transform(param = mtypes$name[match(measurement_type_id, mtypes$id)]) %>%
  reshape(timevar = 'param', idvar = 'time', direction = 'wide',
          drop = 'measurement_type_id') %>%
  # convert time to character for compatibility with sqlite
  within(time <- format(time, '%Y-%m-%d %H:%M:%S', tz = 'EST'))

# write to sqlite file
interm_dir = file.path('analysis', 'intermediate')
dir.create(interm_dir, F, T)
dbpath = paste0('processed_', site, '_', data_source, '.sqlite') %>%
  file.path(interm_dir, .)
db = dbConnect(SQLite(), dbpath)
dbWriteTable(db, 'measurements', site_data, overwrite = T)
dbDisconnect(db)
