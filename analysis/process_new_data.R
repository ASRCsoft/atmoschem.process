# Process the data in its original time resolution

# run this script from the project root directory with
# Rscript analysis/process_new_data.R <site> <data_source>

# produces file analysis/intermediate/processed_<site>_<data_source>.sqlite

library(atmoschem.process)
library(DBI)
library(RSQLite)

site = commandArgs(trailingOnly = T)[1]
data_source = commandArgs(trailingOnly = T)[2]

start_time = as.POSIXct('2018-10-01', tz = 'EST')
end_time = as.POSIXct('2020-07-01', tz = 'EST')

# organize the ETL object
dbcon = src_postgres(dbname = 'nysatmoschemdb')
nysac = etl('atmoschem.process', db = dbcon)

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
  filter(data_source_id == local(data_sources$id),
         !is.na(apply_processing) & apply_processing,
         is.na(derived) | !derived) %>%
  collect()

# make processed data

# 1) update database stuff
message('Updating processing inputs ...')
q_in = 'select update_processing_inputs(?site, ?ds, ?start, ?end)'
sql_in = sqlInterpolate(nysac$con, q_in, site = site_id, ds = data_source,
                        start = format(start_time, tz = 'EST'),
                        end = format(end_time, tz = 'EST'))
dbExecute(nysac$con, sql_in)

# 2) get measurements
obs = tbl(nysac, 'processed_observations')
meas = tbl(nysac, 'measurements') %>%
  filter(measurement_type_id %in% local(mtypes$id)) %>%
  left_join(obs, c('observation_id' = 'id')) %>%
  filter(timezone('EST', time) >= local(start_time),
         timezone('EST', time) < local(end_time)) %>%
  arrange(time) %>%
  mutate(time = timezone('EST', time)) %>%
  collect() %>%
  as.data.frame %>%
  transform(param = mtypes$name[match(measurement_type_id, mtypes$id)]) %>%
  reshape(timevar = 'param', idvar = 'time', direction = 'wide',
          drop = c('observation_id', 'measurement_type_id'))
attr(meas$time, 'tzone') = 'EST'

# 3) process measurements
pr_meas = data.frame(time = meas$time)
for (n in 1:nrow(mtypes)) {
  mname = mtypes$name[n]
  message('Processing ', mname, '...')
  if (paste0('value.', mname) %in% names(meas)) {
    msmt_cols = c('time', paste0('value.', mname), paste0('flagged.', mname))
    msmts = meas[, msmt_cols]
    names(msmts) = c('time', 'value', 'flagged')
    msmts$measurement_type_id = mtypes$id[n]
    tryCatch({
      pr_msmts = atmoschem.process:::process(nysac, msmts, mtypes$id[n])
      # write to pr_meas
      pr_meas[, paste0('value.', mname)] = pr_msmts$value
      pr_meas[, paste0('flagged.', mname)] = pr_msmts$flagged
    },
    error = function(e) {
      warning(mname, ' processing failed: ', e)
    })
  } else {
    warning('No measurements found.')
  }
}

# 4) add derived measurements
derived_vals = atmoschem.process:::derived_vals
# monkey patch `combine_measures` until I can rewrite the deriving functions
combine_measures2 = function(obj, site, data_source, m1, m2, start_time,
                             end_time) {
  col_names0 = expand.grid(c('value','flagged'), c(m1, m2))
  col_names = paste(col_names0[[1]], col_names0[[2]], sep = '.')
  outdf = pr_meas[, c('time', col_names)]
  names(outdf)[-1] = c('value1', 'flagged1', 'value2', 'flagged2')
  outdf
}
acp = getNamespace('atmoschem.process')
unlockBinding('combine_measures', acp)
acp$combine_measures = combine_measures2
lockBinding('combine_measures', acp)
if (site %in% names(derived_vals) &&
    data_source %in% names(derived_vals[[site]]) &&
    length(derived_vals[[site]][[data_source]]) > 0) {
  derive_list = derived_vals[[site]][[data_source]]
  # update mtypes
  mtypes = nysac %>%
    tbl('measurement_types') %>%
    filter(data_source_id == local(data_sources$id),
           !is.na(apply_processing) & apply_processing) %>%
    collect()
  for (n in 1:length(derive_list)) {
    tryCatch({
      f_n = derive_list[[n]]
      msmts = f_n(nysac, start_time, end_time)
      if (nrow(msmts) > 0) {
        ## some functions return multiple derived measurements
        un_id = unique(msmts$measurement_type_id)
        for (id in un_id) {
          name = mtypes$name[match(id, mtypes$id)]
          message('Processing ', name, '...')
          id_msmts = subset(msmts, measurement_type_id == id)
          pr_msmts = atmoschem.process:::process(nysac, id_msmts, id)
          # write to pr_meas
          pr_meas[, paste0('value.', name)] = pr_msmts$value
          pr_meas[, paste0('flagged.', name)] = pr_msmts$flagged
        }
      }
    },
    error = function(e) {
      warning('derived value (', n, ') processing failed: ', e)
    })
  }
}

# write to sqlite file
# convert time to character for compatibility with sqlite
pr_meas$time = format(pr_meas$time, '%Y-%m-%d %H:%M:%S', tz = 'EST')
interm_dir = file.path('analysis', 'intermediate')
dir.create(interm_dir, F, T)
dbpath = paste0('processed_', site, '_', data_source, '.sqlite') %>%
  file.path(interm_dir, .)
db = dbConnect(SQLite(), dbpath)
dbWriteTable(db, 'measurements', pr_meas, overwrite = T)
dbDisconnect(db)
