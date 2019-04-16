library(shiny)
library(shinyWidgets)
library(dplyr)
library(DBI)

## dbname = getShinyOption('dbname')
pg = getShinyOption('pg')

measurements = c(NO = 'NO', NO2 = 'NO2', NOx = 'NOx',
                 NOy = 'NOy', Ozone = 'Ozone',
                 CO = 'CO', SO2 = 'SO2',
                 Temperature = 'Temp', RH = 'RH',
                 'Wind speed' = 'WS',
                 'Wind direction' = 'WD',
                 Gust = 'WS_MAX', BP = 'BP')

## get sites and measurements
## pg = dbConnect(RPostgreSQL::PostgreSQL(),
##                dbname = dbname)
sites_tab = tbl(pg, 'sites')
measurements_tab = tbl(pg, 'measurement_types')
measurements_df = merge(measurements_tab, sites_tab,
                        by.x = 'site_id', by.y = 'id')
measurements_df$label = paste(measurements_df$short_name,
                              measurements_df$data_source,
                              measurements_df$measurement,
                              sep = ' / ')
measurements_df = measurements_df[order(measurements_df$label), ]
measurements_dict = setNames(measurements_df$id,
                             measurements_df$label)
## dbDisconnect(pg)

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
