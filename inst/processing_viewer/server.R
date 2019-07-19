library(shiny)
library(dplyr)
library(tidyr)
library(ggplot2)
library(DBI)
library(nysatmoschem)

obj = getShinyOption('obj')
pg = getShinyOption('pg')

## get measurement info
measurements = tbl(pg, 'measurement_types') %>%
  collect()
data_sources = tbl(pg, 'data_sources') %>%
  collect()

get_site_data_sources = function(x) {
  site_dss = subset(data_sources, site_id == x)
  setNames(site_dss$id, site_dss$name)
}

get_data_source_measurements = function(x) {
  ds_measurements = subset(measurements, data_source_id == x)
  setNames(ds_measurements$id, ds_measurements$name)
}

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
  zeros = obj %>%
    nysatmoschem:::get_cal_zeros(measure)
  if (nrow(zeros) > 0) {
    zeros = zeros %>%
      mutate(filtered_value =
               nysatmoschem:::estimate_zeros(obj, measure, time)) %>%
      ## If we keep the values before and after the time range
      ## [t1,t2], they will influence ggplot2's y-axis bounds
      ## calculations, which we don't want. But we also want the
      ## preceding and succeeding points so that the zero/span lines
      ## can be drawn to the edges of the graph.
      filter(ifelse(is.na(lead(time)), time > t1, lead(time) > t1),
             ifelse(is.na(lag(time)), time < t2, lag(time) < t2)) %>%
      gather(filtered, value, -time, -flagged) %>%
      mutate(filtered = filtered == 'filtered_value',
             flagged = ifelse(filtered, FALSE, flagged),
             label = 'zero')
  }
  ## now the spans
  spans = obj %>%
    nysatmoschem:::get_cal_spans(measure) %>%
    select(time, ratio, flagged)
  if (nrow(spans) > 0) {
    spans = spans %>%
      mutate(filtered_value =
               nysatmoschem:::estimate_spans(obj, measure, time)) %>%
      filter(ifelse(is.na(lead(time)), time > t1, lead(time) > t1),
             ifelse(is.na(lag(time)), time < t2, lag(time) < t2)) %>%
      gather(filtered, value, -time, -flagged) %>%
      mutate(filtered = filtered == 'filtered_value',
             flagged = ifelse(filtered, FALSE, flagged),
             label = 'span')
  }
  rbind(zeros, spans)
}

get_ces = function(measure, t1, t2) {
  ces = obj %>%
    nysatmoschem:::get_ces(measure) %>%
    select(time, efficiency, flagged)
  if (nrow(ces) > 0) {
    ces = ces %>%
      mutate(filtered_value =
               nysatmoschem:::estimate_ces(obj, measure, time)) %>%
      filter(ifelse(is.na(lead(time)), time > t1, lead(time) > t1),
             ifelse(is.na(lag(time)), time < t2, lag(time) < t2)) %>%
      gather(filtered, value, -time, -flagged) %>%
      mutate(filtered = filtered == 'filtered_value',
             flagged = ifelse(filtered, FALSE, flagged))
  }
  ces
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

make_processing_plot = function(m, t1, t2, plot_types,
                                logt = F, show_flagged = T) {
  ## get measurement info
  m_info = subset(measurements, id == m)
  has_raw = 'raw' %in% plot_types
  has_processing = is_true(m_info$apply_processing) &
    'processed' %in% plot_types
  has_cal = is_true(m_info$has_calibration) &
    any(c('zero','span') %in% plot_types)
  has_ce = is_true(m_info$apply_ce) &
    'ce' %in% plot_types
  has_hourly = is_true(m_info$apply_processing) &
    'hourly' %in% plot_types

  ## get the data
  if (has_raw) raw = get_raw(m, t1, t2)
  if (has_processing) processed = get_processed(m, t1, t2)
  if (has_cal) cals = get_cals(m, t1, t2)
  if (has_ce) ces = get_ces(m, t1, t2)
  if (has_hourly) hourly = get_hourly(m, t1, t2)

  ## organize for ggplot2
  df_list = list()
  if (has_raw && nrow(raw) > 0) {
    raw$label = 'Raw'
    raw = raw[, c(1:2, 4, 3)]
    raw$filtered = F
    df_list$raw = raw
  }
  if (has_cal && nrow(cals) > 0) {
    cals = cals[, c('time', 'value', 'label', 'flagged', 'filtered')]
    if (!'zero' %in% plot_types) {
      cals = subset(cals, label != 'zero')
    } else if (!'span' %in% plot_types) {
      cals = subset(cals, label != 'span')
    }
    df_list$cals = cals
  }
  if (has_ce && !is.null(ces) > 0) {
    ces$label = 'Conversion Efficiency'
    ces = ces[, c('time', 'value', 'label', 'flagged', 'filtered')]
    df_list$ces = ces
  }
  if (has_processing && nrow(processed) > 0) {
    processed$label = 'Processed'
    processed = processed[, c(1:2, 4, 3)]
    processed$filtered = F
    df_list$processed = processed
  }
  if (has_hourly && nrow(hourly) > 0) {
    hourly$label = 'Hourly'
    hourly$flag = FALSE
    hourly$filtered = FALSE
    df_list$hourly = hourly
  }
  df_names = c('Time', 'Value', 'Label', 'Flagged', 'Filtered')
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
    df$Value[df$Flagged] = NA
  }

  if (any(df$Filtered)) {
    raw_df = subset(df, !Filtered)
    filtered_df = subset(df, Filtered)
    ## getting ggplot2 to plot different linetypes along with
    ## different colors, and create correct legends, requires an
    ## elaborate ruse with a fake dataset
    df_fake = df[1:2, ]
    df_fake$Filtered = factor(c('Original', 'Filtered'),
                              c('Original', 'Filtered'))
    ltype_values = c('Original' = 'solid', 'Filtered' = 'twodash')
    ggplot(raw_df, aes(Time, Value)) +
      geom_line(aes(color = Flagged, group = 1), size = .2) +
      geom_line(data = filtered_df,
                linetype = 'twodash', size = .2) +
      ## I'm adding this invisible line to trick ggplot2 into showing
      ## the legend for the linetype
      geom_line(aes(linetype = Filtered), df_fake,
                color = NA, size = .2) +
      scale_x_datetime(expand = expand_scale(mult = .01)) +
      scale_color_manual(values = c('black', 'red')) +
      scale_linetype_manual('Data', values = ltype_values) +
      coord_cartesian(xlim = as.POSIXct(as.character(c(t1, t2)))) +
      facet_grid(Label ~ ., scales = 'free_y') +
      guides(linetype = guide_legend(override.aes = list(color = 'black')))
  } else {
    ggplot(df, aes(Time, Value, color = Flagged, group = 1)) +
      geom_line(size = .2) +
      scale_x_datetime(expand = expand_scale(mult = .01)) +
      scale_color_manual(values = c('black', 'red')) +
      coord_cartesian(xlim = as.POSIXct(as.character(c(t1, t2)))) +
      facet_grid(Label ~ ., scales = 'free_y')
  }
}

shinyServer(function(input, output) {
  output$data_sources = renderUI({
    selectInput('data_source', 'Data Source:',
                get_site_data_sources(input$site))
  })
  output$measurements = renderUI({
    selectInput('measurement', 'Measurement:',
                get_data_source_measurements(input$data_source))
  })
  output$plots = renderPlot({
    ## to make sure the time zone is handled correctly
    date_range = as.POSIXct(as.character(input$dateRange),
                            tz = 'GMT')
    make_processing_plot(input$measurement,
                         date_range[1], date_range[2],
                         input$plotTypes,
                         input$log, input$showFlagged)
  },
  height = 700, res = 100)
})
