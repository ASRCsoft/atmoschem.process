## load ultrafine data

library(RPostgreSQL)

ultrafine_path = '/home/wmay/data/ultrafine/'
sites = c('WFMS', 'PSP', 'QC')
pg = dbConnect(PostgreSQL(), dbname='chemtest')

for (site in sites) {
  site_path = paste0(ultrafine_path, site, '/')
  files = list.files(site_path, full.names = T)
  for (f in files) {
    ## print(f)
    q = paste0("select load_ultrafine('", site, "', '", f, "')")
    dbGetQuery(pg, q)
  }
}
dbDisconnect(pg)
