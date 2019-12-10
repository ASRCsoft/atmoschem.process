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

## this is the outlier detection used by the Hampel filter, with the
## exception that no values are flagged if the MAD is zero
is_hampel_outlier = function(x, k, threshold = 3.5) {
  ## I'm not sure how I feel about endrule = 'constant' here. The
  ## default endrule breaks with too many NA values, though, so it's
  ## unusable.
  medians = runmed(x, k, endrule = 'constant')
  mads = caTools::runmad(x, k, center = medians)
  abs(x - medians) / mads > threshold & (mads != 0)
}

is_flagged = function(obj, m_id, times, x) {
  if (length(times) == 0) {
    return(logical(0))
  }
  ## get the mtype attributes
  mtype = get_mtype_params(obj, m_id)

  flagged_periods = as.list(get_flagged_periods(obj, m_id))
  in_flagged_period = times %within% flagged_periods

  ## check for outliers
  if (!is.na(mtype$remove_outliers) && mtype$remove_outliers) {
    is_outlier = is_hampel_outlier(x, 241)
    is_outlier[is.na(is_outlier)] = FALSE
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
  in_flagged_period | is_outlier | !is_valid | is_jump
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
  msmts$flagged = is_true(msmts$flagged) |
    is_flagged(obj, m_id, msmts$time, msmts$value)
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
    
    DBI::dbWithTransaction(obj$con, {
      message('Updating processing inputs ...')
      DBI::dbExecute(obj$con, 'select update_processing_inputs()')
      
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
          DBI::dbSendQuery(obj$con, 'SAVEPOINT processing_savepoint')
          tryCatch({
            pr_msmts = process(obj, msmts, ds_mtypes$id[n])
            DBI::dbWriteTable(obj$con, 'processed_measurements',
                              pr_msmts, row.names = FALSE, append = TRUE)
          },
          error = function(e) {
            DBI::dbSendQuery(obj$con, 'ROLLBACK TO SAVEPOINT processing_savepoint')
            warning(mname, ' processing failed: ', e)
          })
          DBI::dbSendQuery(obj$con, 'RELEASE SAVEPOINT processing_savepoint')
        } else {
          warning('No measurements found.')
        }
      }

      ## add derived measurements
      if (site %in% names(derived_vals) &&
          length(derived_vals[[site]]) > 0) {
        derive_list = derived_vals[[site]]
        for (n in 1:length(derive_list)) {
          DBI::dbSendQuery(obj$con, 'SAVEPOINT processing_savepoint')
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
            DBI::dbSendQuery(obj$con, 'ROLLBACK TO SAVEPOINT processing_savepoint')
            warning('derived value (', n, ') processing failed: ', e)
          })
          DBI::dbSendQuery(obj$con, 'RELEASE SAVEPOINT processing_savepoint')
        }
      }

      message('Updating processing outputs ...')
      DBI::dbExecute(obj$con, 'select update_processing_outputs()')
    })
    TRUE
  })
}
