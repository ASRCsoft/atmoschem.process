## download data files

list_html_files = function(url) {
  df = xml2::read_html(url) %>%
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
etl_extract.etl_nysatmoschem <- function(obj, user, password, years, sites = NULL) {
  ## stop('etl_extract is not yet implemented')
  if (is.null(user) || is.null(password)) {
    stop('Username and password required to access data.')
  }
  base_url = paste0('http://', user, ':', password,
                    '@atmoschem.asrc.cestm.albany.edu/~aqm/AQM_Products/raw/')

  ## if no site is specified, visit the website to get a list of sites
  if (is.null(sites)) {
    sites = gsub('/$', '', list_html_files(base_url))
  }

  month_format = '%Y/%m'
  date_format = '/%Y/%m/%Y%m%d_'
  for (site in sites) {
    site_url = paste(base_url, site, sep = '/')
    ## get data sources
    data_sources = gsub('/$', '', list_html_files(site_url))

    ## for source in data sources
    for (s in data_sources) {
      s_url = paste(site_url, s, sep = '/')
      ## get files
      files = list_html_files(s_url)

      ## do something dependent on file type
      ## ...
    }


    
    for (n in 1:length(dates)) {
      d = dates[n]
      month_path = file.path(attr(obj, 'raw_dir'), site,
                             format(d, format = month_format))
      print(month_path)
      dir.create(month_path, showWarnings = T, recursive = T)
      date_path = format(d, format = date_format)
      for (f in file_types) {
        rel_path = paste0(site, date_path, f)
        src = paste0(base_url, rel_path)
        etl::smart_download(obj, src, rel_path)
      }
    }
  }

  
  ## # Specify the URLs that you want to download
  ## src <- c("http://www.stat.tamu.edu/~sheather/book/docs/datasets/HoustonChronicle.csv")

  ## # Use the smart_download() function for convenience
  ## etl::smart_download(obj, src, ...)

  # Always return obj invisibly to ensure pipeability!
  invisible(obj)
}
