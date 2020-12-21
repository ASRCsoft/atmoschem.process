library(shiny)
library(shinyWidgets)
library(dplyr)
library(DBI)

pg = getShinyOption('pg')

## get sites and measurements
sites = as.data.frame(tbl(pg, 'sites'))
site_dict = setNames(sites$id, sites$long_name)
plotTypes = c(Raw = 'raw', `Calibration Zero` = 'zero',
              `Calibration Span` = 'span',
              `Conversion Efficiency` = 'ce',
              Processed = 'processed',
              `Hourly Processed` = 'hourly')


shinyUI(fluidPage(
    titlePanel('Data Processing Viewer'),
    sidebarLayout(
        sidebarPanel(
            airDatepickerInput('dateRange',
                               label = 'Time range:',
                               value = as.POSIXct(c('2018-12-01 00:00',
                                                    '2019-01-01 00:00')),
                               range = T,
                               timepicker = T,
                               addon = 'none',
                               update_on = 'close'),
            selectInput('site', 'Site:', site_dict),
            uiOutput('data_sources'),
            uiOutput('measurements'),
            pickerInput('plotTypes', 'Plots:',
                        choices = plotTypes,
                        selected = plotTypes,
                        multiple = TRUE),
            checkboxInput("log", "Log", FALSE),
            checkboxInput("showFlagged", "Show Flagged Data",
                          TRUE),
            width = 3
        ),
        mainPanel(
            plotOutput('plots'),
            width = 9
        )
    )
))
