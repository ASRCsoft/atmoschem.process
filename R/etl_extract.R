## download data files

list_html_files = function(url, user, password) {
  ## due to the password need to convert regular URL string to this
  ## fancy rvest session
  pw_config = httr::authenticate(user, password)
  session = rvest::html_session(url, pw_config)
  df = xml2::read_html(session) %>%
    rvest::html_table() %>%
    `[[`(1)
  df$Name[-c(1:2, nrow(df))]
}

#' My ETL functions
#' @import etl
#' @inheritParams etl::etl_extract
#' @export
#' @examples
#' \dontrun{
#' if (require(dplyr)) {
#'   obj <- etl("nysatmoschem") %>%
#'     etl_create()
#' }
#' }
etl_extract.etl_nysatmoschem = function(obj, user, password, sites = NULL,
                                        data_sources = NULL, years = NULL, ...) {
  if (is.null(user) || is.null(password)) {
    stop('Username and password required to access data.')
  }
  base_url = paste0('http://', user, ':', password,
                    '@atmoschem.asrc.cestm.albany.edu/~aqm/AQM_Products/raw')

  ## if no site is specified, visit the website to get a list of sites
  if (is.null(sites)) {
    sites = gsub('/$', '', list_html_files(base_url, user, password))
  }

  for (site in sites) {
    site_url = paste(base_url, site, sep = '/')
    measurements_url = paste(site_url, 'measurements', sep = '/')
    site_data_sources = gsub('/$', '', list_html_files(measurements_url, user, password))
    if (!is.null(data_sources)) {
      site_data_sources = site_data_sources[site_data_sources %in% data_sources]
    }
    for (ds in site_data_sources) {
      ds_url = paste(measurements_url, ds, sep = '/')
      data_source_years = gsub('/$', '', list_html_files(ds_url, user, password))
      if (!is.null(years)) {
        data_source_years = data_source_years[as.integer(data_source_years) %in% years]
      }
      for (y in data_source_years) {
        y_url = paste(ds_url, y, sep = '/')
        message(paste('Downloading', site, '/', ds, '/', y, 'files...', sep = ' '))
        year_files = list_html_files(y_url, user, password)
        file_urls = paste(y_url, year_files, sep = '/')
        rel_paths = gsub('.*/raw/', '', file_urls)
        ## make sure local directory exists
        dir.create(file.path(attr(obj, 'raw_dir'), site, 'measurements', ds, y),
                   showWarnings = FALSE, recursive = TRUE)
        etl::smart_download(obj, sapply(file_urls, URLencode),
                            rel_paths, ...)
      }
    }
    ## also get the calibrations for each site
    calibrations_url = paste(site_url, 'calibrations', sep = '/')
    site_instruments = gsub('/$', '', list_html_files(calibrations_url, user, password))
    for (instr in site_instruments) {
      instr_url = paste(calibrations_url, instr, sep = '/')
      instrument_years = gsub('/$', '', list_html_files(instr_url, user, password))
      if (!is.null(years)) {
        instrument_years = instrument_years[as.integer(instrument_years) %in% years]
      }
      for (y in instrument_years) {
        y_url = paste(instr_url, y, sep = '/')
        message(paste('Downloading', site, '/', instr, '/', y, 'calibration files...', sep = ' '))
        year_files = list_html_files(y_url, user, password)
        file_urls = paste(y_url, year_files, sep = '/')
        rel_paths = gsub('.*/raw/', '', file_urls)
        ## make sure local directory exists
        dir.create(file.path(attr(obj, 'raw_dir'), site, 'calibrations', instr, y),
                   showWarnings = FALSE, recursive = TRUE)
        etl::smart_download(obj, sapply(file_urls, URLencode),
                            rel_paths, ...)
      }
    }
  }
  invisible(obj)
}
