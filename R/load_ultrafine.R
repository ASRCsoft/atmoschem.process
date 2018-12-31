## load ultrafine data

library(RPostgreSQL)

import_ultrafine_file = function(f) {
  path_folders = strsplit(f, '/')[[1]]
  station = path_folders[length(path_folders) - 1]
  q = paste0("select load_ultrafine('", station, "', '", f, "')")
  pg = dbConnect(PostgreSQL(), dbname='chemtest')
  dbGetQuery(pg, q)
  dbDisconnect(pg)
}

env_files = commandArgs(trailingOnly = T)
file_dates = gsub('^[^0-9]*([0-9]{8}).*', '\\1', env_files)
env_files = env_files[order(file_dates)]
for (f in env_files) {
  message(paste('Importing', f))
  import_ultrafine_file(f)
}
