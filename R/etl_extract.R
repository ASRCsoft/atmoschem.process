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
etl_extract.etl_nysatmoschem <- function(obj, user, password, sites = NULL,
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

  ## download_urls = character(0)
  for (site in sites) {
    site_url = paste(base_url, site, sep = '/')
    site_data_sources = gsub('/$', '', list_html_files(site_url, user, password))
    if (!is.null(data_sources)) {
      site_data_sources = site_data_sources[site_data_sources %in% data_sources]
    }
    for (s in site_data_sources) {
      s_url = paste(site_url, s, sep = '/')
      files = list_html_files(s_url, user, password)
      try_result = try(file_years <- extract_year(files, site, s))
      if (class(try_result) == 'try-error') {
        ## this means extract_year isn't implemented for that data
        ## source
        next()
      }
      if (is.null(years)) {
        year_files = files[!is.na(file_years)]
      } else {
        year_files = files[file_years %in% years]
      }
      if (length(year_files) > 0) {
        message(paste('Extracting', site, '/', s, 'files...', sep = ' '))
        ## add urls to download list
        file_urls = paste(base_url, site, s, year_files, sep = '/')
        rel_paths = gsub('.*/raw/', '', file_urls)
        ## make sure local directory exists
        dir.create(file.path(attr(obj, 'raw_dir'), site, s),
                   showWarnings = FALSE, recursive = TRUE)
        etl::smart_download(obj, sapply(file_urls, URLencode),
                            rel_paths, ...)
      }
    }
  }

  invisible(obj)
}
