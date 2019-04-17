#' @import shiny
#' @export
view_processing = function(...) {
  shinyOptions(pg = dplyr::src_postgres(...))
  runApp(system.file('processing_viewer', package = 'nysatmoschem'))
}
