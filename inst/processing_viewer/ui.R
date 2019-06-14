library(shiny)
library(shinyWidgets)
library(dplyr)
library(DBI)

pg = getShinyOption('pg')

## get sites and measurements
measurements = as.data.frame(tbl(pg, 'measurement_types'))
data_sources = as.data.frame(tbl(pg, 'data_sources'))
sites = as.data.frame(tbl(pg, 'sites'))
measurements$data_source_name =
  data_sources$name[match(measurements$data_source_id, data_sources$id)]
measurements$site_id =
  data_sources$site_id[match(measurements$data_source_id, data_sources$id)]
measurements$site_name =
  sites$short_name[match(measurements$site_id, sites$id)]
measurements$label = paste(measurements$site_name,
                           measurements$data_source_name,
                           measurements$name,
                           sep = ' / ')
measurements = measurements[order(measurements$label), ]
measurements_dict = setNames(measurements$id,
                             measurements$label)

plotTypes = c(Raw = 'raw', `Calibration Zero` = 'zero',
              `Calibration Span` = 'span',
              `Conversion Efficiency` = 'ce',
              Processed = 'processed',
              `Hourly Processed` = 'hourly')

shinyUI(fluidPage(
    fluidRow(
        ## 4 columns
        column(3,
               h3('Data Processing Viewer')
               ),
        column(2,
               airDatepickerInput('dateRange',
                                  label = 'Time range:',
                                  value = as.POSIXct(c('2018-12-01 00:00',
                                                       '2019-01-01 00:00')),
                                  range = T,
                                  timepicker = T,
                                  addon = 'none',
                                  update_on = 'close')
               ),
        column(3,
               selectInput('measure', 'Measure:',
                           measurements_dict)
               ),
        column(2,
               checkboxInput("log", "Log", FALSE),
               checkboxInput("showFlagged", "Show Flagged Data",
                             TRUE)
               ),
        column(2,
               pickerInput('plotTypes', 'Plots:',
                           choices = plotTypes,
                           selected = plotTypes,
                           multiple = TRUE)
               )
    ),
    hr(),
    plotOutput("plots")
))
