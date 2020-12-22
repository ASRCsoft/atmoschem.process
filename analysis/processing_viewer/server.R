library(atmoschem.process)
library(shiny)
library(magrittr)
library(DBI)
library(RSQLite)
library(ggplot2)

# relative path to the sqlite files from the app directory
interm_dir = file.path('..', 'intermediate')

# get only true values
is_true = function(x) !is.na(x) & x

## retrieve data from table based on timerange and measure
get_raw = function(s, ds, m, t1, t2) {
  dbpath = file.path(interm_dir, paste0('raw_', s, '_', ds, '.sqlite'))
  param_col = paste0('value.', m)
  param_fcol = paste0('flagged.', m)
  db = dbConnect(SQLite(), dbpath)
  q = paste0('select time, ?, ? from measurements where time >= ? and time <= ? order by time asc')
  sql = sqlInterpolate(db, q, dbQuoteIdentifier(db, param_col),
                       dbQuoteIdentifier(db, param_fcol),
                       format(t1, tz = 'EST'), format(t2, tz = 'EST'))
  res = dbGetQuery(db, sql)
  dbDisconnect(db)
  res[, 1] = as.POSIXct(res[, 1], tz = 'EST')
  # booleans are stored as numbers in SQLite so they need to be converted
  res[, 3] = !is.na(res[, 3]) & as.logical(res[, 3])
  res
}

get_processed = function(s, ds, m, t1, t2) {
  dbpath = file.path(interm_dir, paste0('processed_', s, '_', ds, '.sqlite'))
  param_col = paste0('value.', m)
  param_fcol = paste0('flagged.', m)
  db = dbConnect(SQLite(), dbpath)
  q = paste0('select time, ?, ? from measurements where time >= ? and time <= ? order by time asc')
  sql = sqlInterpolate(db, q, dbQuoteIdentifier(db, param_col),
                       dbQuoteIdentifier(db, param_fcol),
                       format(t1, tz = 'EST'), format(t2, tz = 'EST'))
  res = dbGetQuery(db, sql)
  dbDisconnect(db)
  res[, 1] = as.POSIXct(res[, 1], tz = 'EST')
  # booleans are stored as numbers in SQLite so they need to be converted
  res[, 3] = !is.na(res[, 3]) & as.logical(res[, 3])
  res
}

get_cals = function(s, ds, m, t1, t2) {
  dbpath = file.path(interm_dir, paste0('cals_', s, '.sqlite'))
  db = dbConnect(SQLite(), dbpath)
  q = "
select *,
       end_time as time,
       measured_value as value
  from calibrations
 where data_source = ?
   and measurement_name = ?
   and type = ?
 order by start_time asc"
  sql = sqlInterpolate(db, q, ds, m, 'zero')
  zeros = dbGetQuery(db, sql)
  sql = sqlInterpolate(db, q, ds, m, 'span')
  spans = dbGetQuery(db, sql)
  dbDisconnect(db)
  zeros$time = as.POSIXct(zeros$time, tz = 'EST')
  spans$time = as.POSIXct(spans$time, tz = 'EST')
  zeros$flagged = as.logical(zeros$flagged)
  spans$flagged = as.logical(spans$flagged)
  zeros$filtered = F
  spans$filtered = F
  zeros$label = 'zero'
  spans$label = 'span'

  # Get the estimated (median filtered) zeros from the raw values. These
  # calculations are the same as in `drift_correct`
  m_conf = subset(measurement_types, site == s & data_source == ds & name == m)
  z_breaks = zeros$time[is_true(zeros$corrected)]
  # If we keep the values before and after the time range [t1,t2], they will
  # influence ggplot2's y-axis bounds calculations, which we don't want. But we
  # also want the preceding and succeeding points so that the zero/span lines
  # can be drawn to the edges of the graph.
  zeros0 = zeros
  zeros = zeros %>%
    transform(lead_time = c(tail(time, -1), NA),
              lag_time = c(NA, head(time, -1))) %>%
    subset(ifelse(is.na(lead_time), time > t1, lead_time > t1) &
           ifelse(is.na(lag_time), time < t2, lag_time < t2))
  fzeros = zeros
  good_zeros = replace(zeros0$value, zeros0$flagged, NA)
  fzeros$value =
    atmoschem.process:::estimate_cals(zeros0$time, good_zeros,
                                      m_conf$zero_smooth_window, zeros$time,
                                      z_breaks)
  fzeros$filtered = T
  zeros = rbind(zeros, fzeros)

  # convert spans to ratios
  if (nrow(spans) > 0) {
    szeros = atmoschem.process:::estimate_cals(zeros0$time, good_zeros,
                                               m_conf$zero_smooth_window,
                                               spans$time, z_breaks)
    measured_value = spans$value - szeros
    # convert to ratio
    spans$measured_value = measured_value / spans$provided_value
    s_breaks = spans$time[is_true(spans$corrected)]
    spans0 = spans
    spans = spans %>%
      transform(lead_time = c(tail(time, -1), NA),
                lag_time = c(NA, head(time, -1))) %>%
      subset(ifelse(is.na(lead_time), time > t1, lead_time > t1) &
             ifelse(is.na(lag_time), time < t2, lag_time < t2))
    fspans = spans
    good_spans = replace(spans0$value, spans0$flagged, NA)
    fspans$value =
      atmoschem.process:::estimate_cals(spans0$time, good_spans,
                                        m_conf$span_smooth_window, spans$time,
                                        s_breaks)
    fspans$filtered = T
    spans = rbind(spans, fspans)
  }

  cols = c('time', 'value', 'flagged', 'filtered', 'label')
  rbind(zeros, spans)[, cols]
}

# get_ces = function(measure, t1, t2) {
#   ces = obj %>%
#     atmoschem.process:::get_ces(measure)
#   if (nrow(ces) > 0) {
#     ces = ces %>%
#       select(time, efficiency, flagged) %>%
#       mutate(filtered_value =
#                atmoschem.process:::estimate_ces(obj, measure, time)) %>%
#       filter(ifelse(is.na(lead(time)), time > t1, lead(time) > t1),
#              ifelse(is.na(lag(time)), time < t2, lag(time) < t2)) %>%
#       gather(filtered, value, -time, -flagged) %>%
#       mutate(filtered = filtered == 'filtered_value',
#              flagged = ifelse(filtered, FALSE, flagged))
#   }
#   ces
# }

get_hourly = function(s, ds, m, t1, t2) {
  dbpath = file.path(interm_dir, paste0('hourly_', s, '_', ds, '.sqlite'))
  param_col = paste0('value.', m)
  param_fcol = paste0('flag.', m)
  db = dbConnect(SQLite(), dbpath)
  q = paste0('select time, ?, ? from measurements where time >= ? and time <= ? order by time asc')
  sql = sqlInterpolate(db, q, dbQuoteIdentifier(db, param_col),
                       dbQuoteIdentifier(db, param_fcol),
                       format(t1, tz = 'EST'), format(t2, tz = 'EST'))
  res = dbGetQuery(db, sql)
  dbDisconnect(db)
  res[, 1] = as.POSIXct(res[, 1], tz = 'EST')
  # convert hourly NARSTO flag to boolean
  res[, 3] = !is.na(res[, 3]) & startsWith(res[, 3], 'M')
  res
}

make_processing_plot = function(s, ds, m, t1, t2, plot_types, logt = F,
                                show_flagged = T) {
  ## get measurement info
  m_info = subset(measurement_types, site == s & data_source == ds & name == m)
  has_raw = 'raw' %in% plot_types
  has_processing = is_true(m_info$apply_processing) &
    'processed' %in% plot_types
  has_cal = is_true(m_info$has_calibration) &
    any(c('zero','span') %in% plot_types)
  has_ce = is_true(m_info$apply_ce) &
    'ce' %in% plot_types
  has_hourly = is_true(m_info$apply_processing) &
    'hourly' %in% plot_types

  # temporarily, until I fix the functions
  has_ce = F

  ## get the data
  if (has_raw) raw = get_raw(s, ds, m, t1, t2)
  if (has_processing) processed = get_processed(s, ds, m, t1, t2)
  if (has_cal) cals = get_cals(s, ds, m, t1, t2)
  if (has_ce) ces = get_ces(m, t1, t2)
  if (has_hourly) hourly = get_hourly(s, ds, m, t1, t2)

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
  if (has_ce && nrow(ces) > 0) {
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
    hourly = hourly[, c(1:2, 4, 3)]
    hourly$filtered = F
    df_list$hourly = hourly
  }
  df_names = c('Time', 'Value', 'Label', 'Flagged', 'Filtered')
  for (df_name in names(df_list)) {
    names(df_list[[df_name]]) = df_names
  }
  df = do.call(rbind, df_list)
  if (nrow(df) == 0) return(NULL)
  attributes(df$Time)$tzone = 'EST'
  df$Label = factor(df$Label,
                    levels=c('Raw', 'zero', 'span',
                             'Conversion Efficiency',
                             'Processed', 'Hourly'))
  if (logt) {
    is_cal = df$Label %in% c('zero', 'span')
    df[!is_cal, 'Value'] = log(df$Value[!is_cal])
  }
  if (!show_flagged && any(df$Flagged)) {
    # remove flagged data, but only from the processed data
    df$Value[df$Flagged] = NA
  }
  
  if (any(df$Filtered)) {
    raw_df = subset(df, !Filtered)
    filtered_df = subset(df, Filtered)
    # zero_breaks = atmoschem.process:::get_cal_breaks(obj, m, 'zero')
    # span_breaks = atmoschem.process:::get_cal_breaks(obj, m, 'span')
    zero_breaks = numeric(0)
    span_breaks = numeric(0)
    combined_breaks = c(zero_breaks, span_breaks)
    if (!is.null(combined_breaks)) {
      breaks_df = data.frame(breaks = c(zero_breaks, span_breaks),
                             Label = c(rep('zero', length(zero_breaks)),
                                       rep('span', length(span_breaks))))
    } else {
      breaks_df = data.frame(breaks = numeric(0))
    }

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
      geom_vline(aes(xintercept = breaks), breaks_df,
                 color = 'darkgray', size = .3) +
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
    site_data_sources = subset(data_sources, site == input$site)
    selectInput('data_source', 'Data Source:', site_data_sources$name)
  })
  output$measurements = renderUI({
    source_measurements = measurement_types %>%
      subset(site == input$site & data_source == input$data_source)
    selectInput('measurement', 'Measurement:', source_measurements$name)
  })
  output$plots = renderPlot({
    ## to make sure the time zone is handled correctly
    date_range = as.POSIXct(as.character(input$dateRange),
                            tz = 'EST')
    make_processing_plot(input$site, input$data_source, input$measurement,
                         date_range[1], date_range[2], input$plotTypes,
                         input$log, input$showFlagged)
  },
  height = 700, res = 100)
})
