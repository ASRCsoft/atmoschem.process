## process data

#' @importFrom lubridate %within%
#' @importFrom Rdpack reprompt

get_mtype_params = function(obj, m_id) {
  obj %>% tbl('measurement_types') %>%
    filter(id == m_id) %>%
    mutate(lower_range = lower(valid_range),
           upper_range = upper(valid_range),
           lower_inc = lower_inc(valid_range),
           upper_inc = upper_inc(valid_range)) %>%
    collect() %>%
    as.list()
}

get_flagged_periods = function(obj, m_id) {
  fp = obj %>% tbl('flagged_periods') %>%
    filter(measurement_type_id == m_id) %>%
    mutate(start_time = timezone('EST', lower(times)),
           end_time = timezone('EST', upper(times))) %>%
    select(start_time, end_time) %>%
    arrange(start_time) %>%
    collect()
  if (nrow(fp) == 0) return(vector())
  fp %>%
    mutate(interval = lubridate::interval(start_time, end_time)) %>%
    pull(interval)
}

in_interval = function(x, l, u, l_inc, u_inc) {
  res = TRUE
  if (!is.na(l)) {
    if (l_inc) {
      res = x >= l
    } else {
      res = x > l
    }
  }
  if (!is.na(u)) {
    if (u_inc) {
      res = res & x <= u
    } else {
      res = res & x < u
    }
  }
  res
}

## Rolling Median Absolute Deviation
roll_mad = function(x, k) {
  n = length(x)
  k2 = floor(k / 2)
  ## surprisingly this seems to be the most practical way to calculate
  ## running medians with NA values in R
  m = caTools::runquantile(x, k, .5)
  ## for each segment of sequential equal medians, get rolling median
  ## of |x - median|
  seg_inds = c(1, which(m[2:n] != m[1:(n - 1)]) + 1, n + 1)
  seg_m = m[seg_inds]
  mads = lapply(2:length(seg_inds), function(y) {
    ## index bounds, padding the range with k/2 values on each side
    ## (if available)
    lpad = max(1, seg_inds[y - 1] - k2)
    upad = min(n, seg_inds[y] - 1 + k2)
    ## the bounds, removing the padding
    lbound = seg_inds[y - 1] - lpad + 1
    ubound = seg_inds[y] - lpad
    caTools::runquantile(abs(x[lpad:upad] - seg_m[y - 1]), k, .5)[lbound:ubound]
  })
  ## multiply by 1.4826 to estimate standard deviation
  unlist(mads) * 1.4826
}

## this is the outlier detection used by the Hampel filter, with the
## exception that no values are flagged if the MAD is zero
hampel_outlier = function(x, k, threshold = 3.5) {
  medians = caTools::runquantile(x, k, probs = .5)
  mads = roll_mad(x, k)
  (mads != 0) & (abs(x - medians) / mads > threshold)
}

is_flagged = function(obj, m_id, times, x, flagged = FALSE) {
  if (length(times) == 0) {
    return(logical(0))
  }
  ## treat missing flagged values as false
  flagged = is_true(flagged)
  ## get the mtype attributes
  mtype = get_mtype_params(obj, m_id)

  flagged_periods = as.list(get_flagged_periods(obj, m_id))
  in_flagged_period = times %within% flagged_periods

  ## check for outliers
  if (!is.na(mtype$remove_outliers) && mtype$remove_outliers) {
    is_outlier = x %>%
      ## don't use previously flagged data (often indicating
      ## calibrations) during outlier detection
      replace(flagged | in_flagged_period, NA) %>%
      { if (is_true(mtype$spike_log_transform)) log(.) else . } %>%
      hampel_outlier(mtype$spike_window) %>%
      replace(., is.na(.), FALSE)
  } else {
    is_outlier = FALSE
  }

  ## check for invalid numbers
  if (!is.na(mtype$valid_range)) {
    is_valid = in_interval(x, mtype$lower_range, mtype$upper_range,
                           mtype$lower_inc, mtype$upper_inc)
    is_valid[is.na(is_valid)] = FALSE
  } else {
    is_valid = TRUE
  }

  ## check for abrupt jumps
  if (!is.na(mtype$max_jump)) {
    is_jump = c(FALSE, abs(diff(x)) > mtype$max_jump)
    is_jump[is.na(is_jump)] = FALSE
  } else {
    is_jump = FALSE
  }
  flagged | in_flagged_period | is_outlier | !is_valid | is_jump
}

get_measurements = function(obj, m_id, start_time, end_time) {
  if (is(start_time, 'POSIXt')) attributes(start_time)$tzone = 'EST'
  if (is(end_time, 'POSIXt')) attributes(end_time)$tzone = 'EST'
  obs = tbl(obj, 'processed_observations')
  tbl(obj, 'measurements') %>%
    filter(measurement_type_id == m_id) %>%
    left_join(obs, c('observation_id' = 'id')) %>%
    filter(time >= start_time, time <= end_time) %>%
    arrange(time) %>%
    mutate(time = timezone('EST', time)) %>%
    collect()
}

process = function(obj, msmts, m_id) {
  m_params = get_mtype_params(obj, m_id)
  ## if (nrow(msmts) == 0) {
  ##   warning('No measurements found.')
  ##   return(data.frame())
  ## }
  if (is_true(m_params$has_calibration)) {
    msmts$value = apply_cal(obj, m_id, msmts$time, msmts$value)
  }
  if (is_true(m_params$apply_ce)) {
    msmts$value = msmts$value / estimate_ces(obj, m_id, msmts$time)
  }
  msmts$flagged = is_flagged(obj, m_id, msmts$time, msmts$value,
                             msmts$flagged)
  msmts = msmts[, c('measurement_type_id', 'time', 'value', 'flagged')]
  attributes(msmts$time)$tzone = 'EST'
  msmts
}

#' @export
update_processing = function(obj, site, data_source, start_time,
                             end_time) {
  ignore_fields({
    site_id = get_site_id(obj$con, site)
    ds_id = get_data_source_id(obj$con, site, data_source,
                               add_new = FALSE)
    ## get measurement type ID's
    mtypes = obj %>%
      tbl('measurement_types') %>%
      collect()
    ds_mtypes = mtypes %>%
      filter(data_source_id == ds_id,
             !is.na(apply_processing) & apply_processing,
             is.na(derived) | !derived)
    
      message('Updating processing inputs ...')
      q_in = 'select update_processing_inputs(?site, ?ds, ?start, ?end)'
      sql_in = DBI::sqlInterpolate(obj$con, q_in, site = site_id,
                                   ds = data_source,
                                   start = start_time, end = end_time)
      DBI::dbExecute(obj$con, sql_in)
      
    ## delete old measurements
    if (is(start_time, 'POSIXt')) attributes(start_time)$tzone = 'EST'
    if (is(end_time, 'POSIXt')) attributes(end_time)$tzone = 'EST'
    q1 = '  delete
    from processed_measurements
   where measurement_type_id=any(get_data_source_ids(?site, ?ds))
     and (?start is null or time>=?start)
     and (?end is null or time<=?end)'
    sql1 = DBI::sqlInterpolate(obj$con, q1, site = site_id, ds = data_source,
                               start = start_time, end = end_time)
    DBI::dbExecute(obj$con, sql1)

    ## add new measurements
    for (n in 1:nrow(ds_mtypes)) {
      mname = ds_mtypes$name[n]
      message('Processing ', mname, '...')
      msmts = get_measurements(obj, ds_mtypes$id[n], start_time,
                               end_time)
      if (nrow(msmts) > 0) {
        tryCatch({
          pr_msmts = process(obj, msmts, ds_mtypes$id[n])
          DBI::dbWriteTable(obj$con, 'processed_measurements',
                            pr_msmts, row.names = FALSE,
                            append = TRUE)
        },
        error = function(e) {
          warning(mname, ' processing failed: ', e)
        })
      } else {
        warning('No measurements found.')
      }
    }

    ## add derived measurements
    if (site %in% names(derived_vals) &&
        data_source %in% names(derived_vals[[site]]) &&
        length(derived_vals[[site]][[data_source]]) > 0) {
      derive_list = derived_vals[[site]][[data_source]]
      for (n in 1:length(derive_list)) {
        tryCatch({
          f_n = derive_list[[n]]
          msmts = f_n(obj, start_time, end_time)
          if (nrow(msmts) > 0) {
            ## some functions return multiple derived measurements
            un_id = unique(msmts$measurement_type_id)
            for (id in un_id) {
              name = mtypes$name[match(id, mtypes$id)]
              message('Processing ', name, '...')
              ## delete old measurements
              q2 = '  delete
    from processed_measurements
   where measurement_type_id=?id
     and (?start is null or time>=?start)
     and (?end is null or time<=?end)'
              sql2 = DBI::sqlInterpolate(obj$con, q2, id = id,
                                         start = start_time, end = end_time)
              DBI::dbExecute(obj$con, sql2)
              id_msmts = subset(msmts, measurement_type_id == id)
              pr_msmts = process(obj, id_msmts, id)
              DBI::dbWriteTable(obj$con, 'processed_measurements',
                                pr_msmts, row.names = FALSE,
                                append = TRUE)
            }
          }
        },
        error = function(e) {
          warning('derived value (', n, ') processing failed: ', e)
        })
      }
    }

    message('Updating processing outputs ...')
    q_out = 'select update_processing_outputs(?site, ?ds, ?start, ?end)'
    sql_out = DBI::sqlInterpolate(obj$con, q_out, site = site_id,
                                  ds = data_source, start = start_time,
                                  end = end_time)
    DBI::dbExecute(obj$con, sql_out)
    
    TRUE
  })
}
