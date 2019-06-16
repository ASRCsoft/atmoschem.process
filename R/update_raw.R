## update the raw data repository

download_mesonet = function(obj, mesonet_api_url, d) {
  d = as.Date(d)
  date_str = format(d, '%Y%m%d')
  mes_url = paste0(mesonet_api_url, '/WFMB/',
                   date_str, 'T0000/',
                   date_str, 'T2355')
  file_name = paste(date_str, 'mesonet.csv', sep = '_')
  out_folder = file.path('WFML', 'measurements', 'mesonet', format(d, '%Y'))
  out_path = file.path(out_folder, file_name)
  ## create directories if needed
  full_out_folder_paths = unique(file.path(attr(obj, "raw_dir"), out_folder))
  for (f in full_out_folder_paths) {
    dir.create(f, showWarnings = FALSE, recursive = TRUE)
  }
  etl::smart_download(obj, mes_url, out_path)
}

#' @export
update_raw_data = function(obj, mesonet_api_url) {
  ## get NYS Mesonet data from the first recorded data to yesterday
  tnow = Sys.time()
  attributes(tnow)$tzone = 'UTC'
  dnow = as.Date(tnow) - 1
  d = seq(as.Date('2016-01-29'), dnow)
  message('Downloading NYS Mesonet files...')
  download_mesonet(obj, mesonet_api_url, d)
}
