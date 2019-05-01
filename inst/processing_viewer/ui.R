library(shiny)
library(shinyWidgets)
library(dplyr)
library(DBI)

pg = getShinyOption('pg')
print('got pg')

## get sites and measurements
measurements = as.data.frame(tbl(pg, 'measurement_types'))
print(names(measurements))
print(measurements)
data_sources = as.data.frame(tbl(pg, 'data_sources'))
print(data_sources)
sites = as.data.frame(tbl(pg, 'sites'))
print('got tables')
measurements$data_source_name =
  data_sources$name[match(measurements$data_source_id, data_sources$id)]
print(measurements$data_source_id)
print('got data source names')
print(measurements$data_source_name)
measurements$site_id =
  data_sources$site_id[match(measurements$data_source_id, data_sources$id)]
print('got site ids')
print(measurements$site_id)
measurements$site_name =
  sites$short_name[match(measurements$site_id, sites$id)]
print('got site names')
print(measurements$site_name)
measurements$label = paste(measurements$site_name,
                           measurements$data_source_name,
                           measurements$name,
                           sep = ' / ')
print('got labels')
measurements = measurements[order(measurements$label), ]
print('reordered measurements')
measurements_dict = setNames(measurements$id,
                             measurements$label)
print('created dict')
print(measurements_dict)

shinyUI(fluidPage(
    fluidRow(
        ## 4 columns
        column(3,
               h3('Data Processing Viewer')
               ),
        column(3,
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
        column(3,
               checkboxInput("log", "Log", FALSE),
               checkboxInput("showFlagged", "Show Flagged Data",
                             TRUE)
               )
    ),
    hr(),
    plotOutput("plots")
))
