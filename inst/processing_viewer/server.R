library(shiny)
library(dplyr)
library(tidyr)
library(ggplot2)
library(DBI)

pg = getShinyOption('pg')

## get measurement info
measurements_df = tbl(pg, 'measurement_types') %>%
  collect()
data_sources = tbl(pg, 'data_sources') %>%
  collect()

# get only true values
is_true = function(x) !is.na(x) & x

## retrieve data from table based on timerange and measure
get_raw = function(measure, t1, t2) {
  pgtbl = tbl(pg, 'measurements2')
  results = pgtbl %>%
    filter(measurement_type_id == measure &
           time >= t1 &
           time <= t2) %>%
    select(time, value, flagged) %>%
    mutate(flagged = !is.na(flagged) & flagged,
           ## prevent RPostgreSQL from mucking with the time zones
           time = as.character(time)) %>%
    arrange(time) %>%
    collect()
  if (nrow(results) > 0) {
    results = results %>%
      ## get the times back
      mutate(time = as.POSIXct(time, tz = 'GMT'))
  }
  results
}

get_processed = function(measure, t1, t2) {
  pgtbl = tbl(pg, 'processed_measurements')
  results = pgtbl %>%
    filter(measurement_type_id == measure &
           time >= t1 &
           time <= t2) %>%
    select(time, value, flagged) %>%
    ## prevent RPostgreSQL from mucking with the time zones
    mutate(time = as.character(time)) %>%
    arrange(time) %>%
    collect()
  if (nrow(results) > 0) {
    results = results %>%
      ## get the times back
      mutate(time = as.POSIXct(time, tz = 'GMT'))
  }
  results
}

get_cals = function(measure, t1, t2) {
  pgtbl = tbl(pg, 'calibration_values')
  results = pgtbl %>%
    filter(measurement_type_id == measure &
           time >= t1 &
           time <= t2) %>%
    mutate(label = type) %>%
    select(time, value, label) %>%
    ## prevent RPostgreSQL from mucking with the time zones
    mutate(time = as.character(time)) %>%
    arrange(time) %>%
    collect()
  if (nrow(results) > 0) {
    results = results %>%
      ## get the times back
      mutate(time = as.POSIXct(time, tz = 'GMT'))
  }
  results
}

get_ces = function(measure, t1, t2) {
  m_info = measurements_df[measurements_df$id == measure, ]
  m_name = m_info$name
  m_ds_id = m_info$data_source_id
  m_site = data_sources$site_id[data_sources$id == m_ds_id]
  pgtbl = tbl(pg, 'conversion_efficiencies')
  results = pgtbl %>%
    filter(measurement_type_id == measure &
           time >= t1 &
           time <= t2) %>%
    select(time, efficiency, filtered_efficiency) %>%
    ## prevent RPostgreSQL from mucking with the time zones
    mutate(time = as.character(time)) %>%
    arrange(time) %>%
    collect()
  if (nrow(results) == 0) {
    return(NULL)
  }
  results = results %>%
    ## get the times back
    mutate(time = as.POSIXct(time, tz = 'GMT')) %>%
    gather(filtered, efficiency, -time) %>%
    mutate(filtered = filtered == 'filtered_efficiency')
}

get_hourly = function(measure, t1, t2) {
  pgtbl = tbl(pg, 'hourly_measurements')
  results = pgtbl %>%
    filter(measurement_type_id == measure &
           time >= t1 &
           time <= t2) %>%
    select(time, value) %>%
    ## prevent RPostgreSQL from mucking with the time zones
    mutate(time = as.character(time)) %>%
    arrange(time) %>%
    collect()
  if (nrow(results) > 0) {
    results = results %>%
      ## get the times back
      mutate(time = as.POSIXct(time, tz = 'GMT'))
  }
  results
}

make_processing_plot = function(m, t1, t2, logt = F,
                                show_flagged = T) {
  ## get measurement info
  m_info = subset(measurements_df, id == m)
  has_processing = is_true(m_info$apply_processing)
  has_cal = is_true(m_info$has_calibration)
  has_ce = is_true(m_info$apply_ce)

  ## get the data
  raw = get_raw(m, t1, t2)
  ## has_processing = F
  if (has_processing) processed = get_processed(m, t1, t2)
  if (has_cal) cals = get_cals(m, t1, t2)
  if (has_ce) ces = get_ces(m, t1, t2)
  if (has_processing) hourly = get_hourly(m, t1, t2)

  ## organize for ggplot2
  df_list = list()
  if (nrow(raw) > 0) {
    raw$label = 'Raw'
    raw = raw[, c(1:2, 4, 3)]
    raw$filtered = F
    df_list$raw = raw
  }
  if (has_cal && nrow(cals) > 0) {
    cals$flagged = F
    cals$filtered = F
    df_list$cals = cals
  }
  if (has_ce && !is.null(ces) > 0) {
    ces$label = 'Conversion Efficiency'
    ces$flagged = F
    ces = ces[, c(1, 3:5, 2)]
    df_list$ces = ces
  }
  if (has_processing && nrow(processed) > 0) {
    processed$label = 'Processed'
    processed = processed[, c(1:2, 4, 3)]
    processed$filtered = F
    df_list$processed = processed
  }
  if (has_processing && nrow(hourly) > 0) {
    hourly$label = 'Hourly'
    hourly$flag = FALSE
    hourly$filtered = FALSE
    df_list$hourly = hourly
  }
  df_names = c('Time', 'Value', 'Label', 'Flag', 'Filtered')
  for (df_name in names(df_list)) {
    names(df_list[[df_name]]) = df_names
  }
  df = do.call(rbind, df_list)
  if (nrow(df) == 0) return(NULL)
  df$Label = factor(df$Label,
                    levels=c('Raw', 'zero', 'span',
                             'Conversion Efficiency',
                             'Processed', 'Hourly'))
  if (logt) {
    is_cal = df$Label %in% c('zero', 'span')
    df[!is_cal, 'Value'] = log(df$Value[!is_cal])
  }
  if (!show_flagged) {
    # remove flagged data, but only from the processed data
    df$Value[df$Flag] = NA
  }

  ## plot
  ggplot(df, aes(Time, Value, color = Flag,
                 linetype = Filtered)) +
    geom_line(size = .2) +
    xlim(as.POSIXct(as.character(c(t1, t2)))) +
    scale_color_manual(values = c('black', 'red')) +
    scale_linetype_manual(values = c('solid', 'twodash')) +
    facet_grid(Label ~ ., scales = 'free_y')
}

shinyServer(function(input, output) {
  output$plots = renderPlot({
    ## to make sure the time zone is handled correctly
    date_range = as.POSIXct(as.character(input$dateRange),
                            tz = 'GMT')
    measure = as.integer(input$measure)
    make_processing_plot(measure, date_range[1],
                         date_range[2],
                         input$log, input$showFlagged)
  },
  height = 700, res = 100)
})
