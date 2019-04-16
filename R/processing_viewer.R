#' @import shiny
#' @export
view_processing = function(...) {
  shinyOptions(pg = src_dbi(dbConnect(PostgreSQL(), ...)))
  runApp(system.file('processing_viewer', package = 'nysatmoschem'))
}
