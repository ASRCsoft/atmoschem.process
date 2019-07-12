## process data

#' @importFrom lubridate %within%

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
    mutate(start_time = lower(times), end_time = upper(times)) %>%
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

is_flagged = function(obj, m_id, times, x) {
  if (length(times) == 0) {
    return(logical(0))
  }
  ## get the mtype attributes
  mtype = get_mtype_params(obj, m_id)

  flagged_periods = as.list(get_flagged_periods(obj, m_id))
  in_flagged_period = times %within% flagged_periods

  if (!is.na(mtype$remove_outliers) && mtype$remove_outliers) {
    is_outlier = seismicRoll::roll_hampel(x, 241) > 3.5
    is_outlier[is.na(is_outlier)] = FALSE
  } else {
    is_outlier = FALSE
  }

  if (!is.na(mtype$valid_range)) {
    is_valid = in_interval(x, mtype$lower_range, mtype$upper_range,
                           mtype$lower_inc, mtype$upper_inc)
    is_valid[is.na(is_valid)] = FALSE
  } else {
    is_valid = TRUE
  }
  in_flagged_period | is_outlier | !is_valid
}

get_measurements = function(obj, m_id, start_time, end_time) {
  obs = tbl(obj, 'processed_observations')
  msmts = tbl(obj, 'measurements') %>%
    filter(measurement_type_id == m_id) %>%
    left_join(obs, c('observation_id' = 'id')) %>%
    filter(time >= start_time, time <= end_time) %>%
    collect()
}

process = function(obj, msmts, m_id) {
  m_params = get_mtype_params(obj, m_id)
  ## if (nrow(msmts) == 0) {
  ##   warning('No measurements found.')
  ##   return(data.frame())
  ## }
  if (is_true(m_params$has_calibration)) {
    msmts = msmts %>%
      mutate(value = apply_cal(obj, m_id, time, value))
  }
  if (is_true(m_params$apply_ce)) {
    msmts = msmts %>%
      mutate(value = value / get_ces(obj, m_id, time))
  }
  msmts %>%
    mutate(flagged = is_true(flagged) |
             is_flagged(obj, m_id, time, value)) %>%
    select(measurement_type_id, time, value, flagged)
}

#' @export
update_processing = function(obj, site, data_source, start_time,
                             end_time) {
  site_id = get_site_id(obj$con, site)
  ds_id = get_data_source_id(obj$con, site, data_source,
                             add_new = FALSE)
  ## get measurement type ID's
  mtypes = obj %>%
    tbl('measurement_types') %>%
    collect()
  ds_mtypes = mtypes %>%
    filter(data_source_id == ds_id & !is.na(apply_processing) & apply_processing)
  
  DBI::dbWithTransaction(obj$con, {
    message('Updating processing inputs ...')
    DBI::dbExecute(obj$con, 'select update_processing_inputs()')
    
    ## delete old measurements
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
      message('Processing ', ds_mtypes$name[n], '...')
      msmts = get_measurements(obj, ds_mtypes$id[n], start_time,
                               end_time)
      if (nrow(msmts) > 0) {
        pr_msmts = process(obj, msmts, ds_mtypes$id[n])
        DBI::dbWriteTable(obj$con, 'processed_measurements',
                          pr_msmts, row.names = FALSE, append = TRUE)
      } else {
        warning('No measurements found.')
      }
    }

    ## add derived measurements
    if (site %in% names(derived_vals) &&
        length(derived_vals[[site]]) > 0) {
      derive_list = derived_vals[[site]]
      for (n in 1:length(derive_list)) {
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
      }
    }

    message('Updating processing outputs ...')
    DBI::dbExecute(obj$con, 'select update_processing_outputs()')
  })
  TRUE
}
