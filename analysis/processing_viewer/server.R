library(atmoschem.process)
library(shiny)
library(magrittr)
library(DBI)
library(RSQLite)
library(ggplot2)
library(cowplot)

# relative path to the sqlite files from the app directory
interm_dir = file.path('..', 'intermediate')
config = read_csv_dir('../config')

# get only true values
is_true = function(x) !is.na(x) & x

## retrieve data from table based on timerange and measure
get_raw = function(s, ds, m, t1, t2) {
  dbpath = file.path(interm_dir, paste0('raw_', s, '_', ds, '.sqlite'))
  param_col = paste0('value.', m)
  param_fcol = paste0('flagged.', m)
  db = dbConnect(SQLite(), dbpath)
  on.exit(dbDisconnect(db))
  q = paste0('select time, ?, ? from measurements where time >= ? and time <= ? order by time asc')
  sql = sqlInterpolate(db, q, dbQuoteIdentifier(db, param_col),
                       dbQuoteIdentifier(db, param_fcol),
                       format(t1, tz = 'EST'), format(t2, tz = 'EST'))
  res = try(dbGetQuery(db, sql))
  if (inherits(res, 'try-error')) return(data.frame())
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
  on.exit(dbDisconnect(db))
  q = paste0('select time, ?, ? from measurements where time >= ? and time <= ? order by time asc')
  sql = sqlInterpolate(db, q, dbQuoteIdentifier(db, param_col),
                       dbQuoteIdentifier(db, param_fcol),
                       format(t1, tz = 'EST'), format(t2, tz = 'EST'))
  res = try(dbGetQuery(db, sql))
  if (inherits(res, 'try-error')) return(data.frame())
  res[, 1] = as.POSIXct(res[, 1], tz = 'EST')
  # booleans are stored as numbers in SQLite so they need to be converted
  res[, 3] = !is.na(res[, 3]) & as.logical(res[, 3])
  res
}

.get_cals = function(s, ds, m, type, t1, t2) {
  dbpath = file.path(interm_dir, paste0('processedcals_', s, '.sqlite'))
  db = dbConnect(SQLite(), dbpath)
  on.exit(dbDisconnect(db))
  # if end_time is missing, crudely estimate it by adding an hour to the
  # start_time
  q = "
select *
  from calibrations
 where data_source = ?
   and measurement_name = ?
   and type = ?
 order by time asc"
  sql = sqlInterpolate(db, q, ds, m, type)
  zeros = dbGetQuery(db, sql)
  zeros$time = as.POSIXct(zeros$time, tz = 'EST')
  zeros$flagged = as.logical(zeros$flagged)
  zeros$filtered = F
  zeros$label = type

  # Get the estimated (median filtered) zeros from the raw values. These
  # calculations are the same as in `drift_correct`
  m_conf = subset(config$channels, site == s & data_source == ds & name == m)
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
  fzeros$value = estimate_cals(zeros0$time, good_zeros,
                               m_conf$zero_smooth_window, zeros$time, z_breaks)
  fzeros$filtered = if (nrow(fzeros)) T else logical()
  zeros = rbind(zeros, fzeros)

  cols = c('time', 'value', 'flagged', 'filtered', 'label')
  zeros[, cols]
}

get_cals = function(s, ds, m, t1, t2) {
  zeros = .get_cals(s, ds, m, 'zero', t1, t2)
  spans = .get_cals(s, ds, m, 'span', t1, t2)
  rbind(zeros, spans)
}

get_cal_breaks = function(s, ds, m, type, t1, t2) {
  dbpath = file.path(interm_dir, paste0('cals_', s, '.sqlite'))
  db = dbConnect(SQLite(), dbpath)
  on.exit(dbDisconnect(db))
  q = "
select end_time as time
  from calibrations
 where data_source = ?
   and measurement_name = ?
   and type = ?
   and corrected
   and end_time > ?
   and end_time < ?
 order by end_time asc"
  sql = sqlInterpolate(db, q, ds, m, type,
                       format(t1, '%Y-%m-%d %H:%M:%S', tz = 'EST'),
                       format(t2, '%Y-%m-%d %H:%M:%S', tz = 'EST'))
  res = dbGetQuery(db, sql)
  as.POSIXct(res[, 1], tz = 'EST')
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
  on.exit(dbDisconnect(db))
  q = paste0('select time, ?, ? from measurements where time >= ? and time <= ? order by time asc')
  sql = sqlInterpolate(db, q, dbQuoteIdentifier(db, param_col),
                       dbQuoteIdentifier(db, param_fcol),
                       format(t1, tz = 'EST'), format(t2, tz = 'EST'))
  res = try(dbGetQuery(db, sql))
  if (inherits(res, 'try-error')) return(data.frame())
  res[, 1] = as.POSIXct(res[, 1], tz = 'EST')
  res
}

make_processing_plot = function(s, ds, m, t1, t2, plot_types, logt = F,
                                show_flagged = T) {
  if (is.null(ds) || m == '') return(NULL)
  ## get measurement info
  m_info = subset(config$channels, site == s & data_source == ds & name == m)
  ylabel = paste0(m, ' (', m_info$units, ')')
  if (logt) ylabel = paste('Log', ylabel)
  has_raw = 'raw' %in% plot_types && !is_true(m_info$derived)
  has_processed = is_true(m_info$apply_processing) &
    'processed' %in% plot_types
  has_cal = is_true(m_info$has_calibration) &
    any(c('zero','span') %in% plot_types)
  has_ce = is_true(m_info$apply_ce) &
    'ce' %in% plot_types
  has_hourly = is_true(m_info$apply_processing) &
    'hourly' %in% plot_types

  # temporarily, until I rewrite the CE functions
  has_ce = F
  if (has_ce) ces = get_ces(m, t1, t2)
  if (has_ce && nrow(ces) > 0) {
    ces$label = 'Conversion Efficiency'
    ces = ces[, c('time', 'value', 'label', 'flagged', 'filtered')]
    df_list$ces = ces
  }

  # organize subplots
  n = 1
  plist = list()
  rel_heights = numeric()
  if (has_raw || has_processed) {
    df_list = list()
    if (has_raw && nrow(raw <- get_raw(s, ds, m, t1, t2))) {
      raw$label = 'raw'
      df_list$raw = raw
    }
    if (has_processed && nrow(processed <- get_processed(s, ds, m, t1, t2))) {
      processed$label = 'processed'
      df_list$processed = processed
    }
    if (length(df_list)) {
      meas_orig = do.call(rbind, df_list)
      names(meas_orig)[2:3] = c('value', 'flagged')
      if (logt) meas_orig$value = log(meas_orig$value)
      if (!show_flagged) meas_orig$value[meas_orig$flagged] = NA
      plist[[n]] = ggplot(meas_orig, aes(x = time, y = value, color = flagged,
                                         group = 1)) +
        geom_line(size = .2) +
        scale_color_manual(values = c('black', 'red')) +
        xlim(t1, t2) +
        facet_wrap(~ label, ncol = 1, scales = 'free_y',
                   strip.position = 'right') +
        xlab('Time (EST)') + ylab(ylabel)
      rel_heights = length(df_list)
      n = n + 1
    }
  }

  if (has_cal && nrow(cals <- get_cals(s, ds, m, t1, t2))) {
    cals$segment = 0
    breaks = as.POSIXct(numeric(), tz = 'EST', origin = '1970-01-01')
    labels = character()
    for (type in c('zero', 'span')) {
      if (type %in% plot_types) {
        zero_breaks = get_cal_breaks(s, ds, m, type, t1, t2)
        cals$segment[cals$label == type] =
          findInterval(cals$time[cals$label == type], zero_breaks)
        breaks = c(breaks, zero_breaks)
        labels = c(labels, rep(type, length(zero_breaks)))
      } else {
        cals = subset(cals, label != type)
      }
    }
    breaks_df = data.frame(breaks = breaks, label = labels)
    if (!show_flagged) cals$value[cals$flagged] = NA
    plist[[n]] = ggplot(cals, aes(x = time, y = value, color = flagged,
                                  group = interaction(filtered, segment))) +
      geom_point(aes(shape = filtered)) +
      geom_line(aes(linetype = filtered), color = 'black') +
      geom_vline(aes(xintercept = breaks), breaks_df, color = 'darkgray',
                 size = .3) +
      scale_color_manual(values = c('black', 'red')) +
      scale_shape_manual(values = c(19, NA)) +
      scale_linetype_manual(values = c('blank', 'solid')) +
      xlim(t1, t2) +
      facet_wrap(~ label, ncol = 1, scales = 'free_y',
                 strip.position = 'right') +
      xlab('Time (EST)') + ylab('value')
    rel_heights = c(rel_heights, length(unique(cals$label)))
    n = n + 1
  }

  if (has_hourly && nrow(hourly <- get_hourly(s, ds, m, t1, t2))) {
    hourly$label = 'hourly'
    names(hourly)[2:3] = c('value', 'flag')
    if (logt) hourly$value = log(hourly$value)
    if (!show_flagged) hourly$value[startsWith(hourly$flag, 'M')] = NA
    plist[[n]] = ggplot(hourly, aes(x = time, y = value, color = flag, group = 1)) +
      geom_line() +
      xlim(t1, t2) +
      facet_wrap(~ label, ncol = 1, scales = 'free_y', strip.position = 'right') +
      xlab('Time (EST)') + ylab(ylabel)
    rel_heights = c(rel_heights, 1)
  }

  # remove bottom axis, except for bottom plot
  plist[seq_len(length(plist) - 1)] =
    lapply(head(plist, -1), function(x) {
      x + theme(axis.text.x = element_blank(), axis.ticks.x = element_blank(),
                axis.title.x=element_blank())
    })
  # add space for the bottom axis labels
  rel_heights = .94 * rel_heights / sum(rel_heights)
  rel_heights[length(rel_heights)] = rel_heights[length(rel_heights)] + .06
  plot_grid(plotlist = plist, align = "v", ncol = 1, rel_heights = rel_heights)
}

shinyServer(function(input, output) {
  output$data_sources = renderUI({
    site_data_sources = subset(config$dataloggers, site == input$site)
    selectInput('data_source', 'Data Source:', site_data_sources$name)
  })
  output$measurements = renderUI({
    source_measurements = config$channels %>%
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
