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

etl_extract.etl_nysatmoschem <- function(obj, ...) {
  # Specify the URLs that you want to download
  src <- c("http://www.stat.tamu.edu/~sheather/book/docs/datasets/HoustonChronicle.csv")

  # Use the smart_download() function for convenience
  etl::smart_download(obj, src, ...)

  # Always return obj invisibly to ensure pipeability!
  invisible(obj)
}

etl_init.nysatmoschem = function(obj, script = NULL,
                                 schema_name = "init",
                                 pkg = attr(obj, "pkg"),
                                 ext = NULL, ...) {
  ## make sure the database connection is good
  obj = etl:::verify_con(obj)
  if (!methods::is(obj$con, "DBIConnection")) {
    stop("Invalid connection to database.")
  }
  
  ## add to postgres' libdir so it can find compiled code
  pg_src_path = system.file('libs',
                            package = 'nysatmoschem')
  ## (think I need to change the colon to semi-colon on windows)
  dynamic_library_path = paste(pg_src_path, '$libdir', sep = ':')
  sql_txt = paste0("set dynamic_library_path to '",
                   dynamic_library_path, "'")
  DBI::dbClearResult(DBI::dbSendStatement(obj$con, sql_txt))

  ## set up tables and functions
  sql_files = c('utilities', 'setup', 'calibration',
                'filtering', 'flags', 'processing')
  for (sql_file in sql_files) {
    ## this works a lot better than dbRunScript in the default
    ## method
    schema = find_schema(obj, sql_file, 'nysatmoschem', 'sql')
    sql_txt = paste(readLines(schema), collapse = "\n")
    suppressWarnings(DBI::dbClearResult(DBI::dbSendStatement(obj$con, sql_txt)))
  }

  ## add metadata
  ## ...
  invisible(obj)
}
