## useful functions

## ignore "unrecognized PostgreSQL field type" warnings from
## RPostgreSQL
muffle_warning_pattern = function(w, pattern) {
  if (grepl(pattern, conditionMessage(w))) invokeRestart("muffleWarning")
}
ignore_fields = function(expr) {
  withCallingHandlers(expr, warning = function(w)
    muffle_warning_pattern(w, 'unrecognized PostgreSQL field type'))
}

is_true = function(x) !is.na(x) & x

is_psp_42C_cal = function(f)
  startsWith(basename(f), 'Pinnacle_42C')
is_psp_API300EU_cal = function(f)
  startsWith(basename(f), 'Pinnacle_API300EU_CO_Weekly')
is_psp_ASRC_TEI42i_Y_NOy_cal = function(f)
  startsWith(basename(f), 'Pinnacle_ASRC_TEI42i_Y_NOy_146i_Weekly') |
    startsWith(basename(f), 'Pinnacle_ASRC_TEI42i_Y_NOy_T700_Weekly')
is_psp_DEC_TEI42i_NOy_cal = function(f)
  startsWith(basename(f), 'Pinnacle DEC TEI42i NOy Weekly') |
    startsWith(basename(f), 'Pinnacle_DEC_TEI42i_NOy_Weekly')
is_psp_TEI43i_SO2_cal = function(f)
  startsWith(basename(f), 'Pinnacle_TEI43i_SO2_Weekly') |
    startsWith(basename(f), 'Pinnacle_TEI43i_SO2_146i_Weekly')
is_psp_TEI49i_O3_49i_cal = function(f)
  startsWith(basename(f), 'Pinnacle_TEI49i_O3_49i_Weekly') |
    startsWith(basename(f), 'Pinnacle_TEI49i_O3_Weekly')

star_if_null = function(x) {
  if (is.null(x)) {
    '*'
  } else {
    x
  }
}

## get a vector of file glob that will find the specified files
make_file_globs = function(root_folder, sites = NULL,
                           data_sources = NULL, years = NULL) {
  site_str = star_if_null(sites)
  ds_str = star_if_null(data_sources)
  year_str = star_if_null(years)
  glob_df_m = expand.grid(site_str, ds_str, year_str)
  m_glob_str = file.path(root_folder, glob_df_m[, 1], 'measurements',
                         glob_df_m[, 2], glob_df_m[, 3], '*')
  glob_df_c = expand.grid(site_str, year_str)
  c_glob_str = file.path(root_folder, glob_df_c[, 1], 'calibrations',
                         '*', glob_df_c[, 2], '*')
  c(m_glob_str, c_glob_str)
}

get_site_from_path = function(base, f) {
  re_str = paste0('^', base, '/([^/]+)/.*$')
  sites = gsub(re_str, '\\1', f)
}

get_type_from_path = function(base, f) {
  re_str = paste0('^', base, '/[^/]+/([^/]+)/.*$')
  sites = gsub(re_str, '\\1', f)
}

get_data_source_from_path = function(base, f) {
  re_str = paste0('^', base, '/[^/]+/[^/]+/([^/]+)/.*$')
  sites = gsub(re_str, '\\1', f)
}

## match DB rows to a dataframe and return the corresponding ID's
get_id_from_pg = function(pg, df, tbl_name) {
  q1 = paste('select * from', tbl_name)
  pg_df = DBI::dbGetQuery(pg, q1)
  if (ncol(df) == 1) {
    ## if there's only one column to match this becomes easy and fast
    colname = names(df)
    if (!colname %in% names(pg_df)) {
      rep(NA, nrow(df))
    } else {
      pg_df$id[match(df[, colname], pg_df[, colname])]
    }
  } else {
    ## otherwise do some fancy merging to match multiple columns
    df$order = 1:nrow(df)
    df2 = merge(df, pg_df, all.x = TRUE)
    if (!'id' %in% names(df2)) {
      rep(NA, nrow(df))
    } else {
      ## df2 is sorted, have to unsort it
      df2$id[order(df2$order)]
    }
  }
}

get_site_id = function(pg, x) {
  df = data.frame(short_name = x)
  get_id_from_pg(pg, df, 'sites')
}

add_new_data_sources = function(pg, site, data_source) {
  df_in = data.frame(site = site,
                     name = data_source)
  uniq_df = na.omit(unique(df_in))
  ds_ids = get_data_source_id(pg, uniq_df$site, uniq_df$name,
                              add_new = FALSE)
  if (sum(is.na(ds_ids)) > 0) {
    ## insert new measurement types
    site_id = get_site_id(pg, uniq_df$site[is.na(ds_ids)])
    new_dss = data.frame(site_id = site_id,
                         name = uniq_df$name[is.na(ds_ids)])
    DBI::dbWriteTable(pg, 'data_sources', new_dss,
                      row.names = FALSE, append = TRUE)
  }
}

get_data_source_id = function(pg, site, data_source,
                              add_new = TRUE) {
  if (add_new) {
    ## make sure we aren't asking for ID's that don't exist yet
    add_new_data_sources(pg, site, data_source)
  }
  site_ids = get_site_id(pg, site)
  df_in = data.frame(site_id = site_ids,
                     name = data_source)
  get_id_from_pg(pg, df_in, 'data_sources')
}

add_new_file = function(pg, site, data_source, f,
                        calibration) {
  data_source_id = get_data_source_id(pg, site, data_source)
  file_df = data.frame(data_source_id = data_source_id,
                       name = basename(f),
                       calibration = calibration)
  DBI::dbWriteTable(pg, 'files', file_df,
                    row.names = FALSE, append = TRUE)
}

get_file_id = function(pg, site, data_source, f,
                       calibration) {
  ds_id = get_data_source_id(pg, site, data_source)
  df_in = data.frame(data_source_id = ds_id,
                     name = basename(f),
                     calibration = calibration)
  get_id_from_pg(pg, df_in, 'files')
}

add_new_obs = function(pg, file_id, record, time) {
  if (is.na(file_id)) stop('file_id cannot be NA')
  obs = data.frame(line = record,
                   time = time)
  uniq_obs = unique(obs)
  obs_ids = get_obs_id(pg, file_id, uniq_obs$line,
                       uniq_obs$time, add_new = FALSE)
  missing_obs = uniq_obs[is.na(obs_ids), ]
  if (nrow(missing_obs) > 0) {
    ## insert new measurement types
    new_obs = data.frame(file_id = file_id,
                         line = missing_obs$line,
                         time = missing_obs$time)
    DBI::dbWriteTable(pg, 'observations', new_obs,
                      row.names = FALSE, append = TRUE)
  }
}

get_obs_id = function(pg, file_id, record, time,
                      add_new = TRUE) {
  ## safe to assume there's only one file here
  if (is.na(file_id)) stop('file_id cannot be NA')
  if (add_new) {
    ## make sure we aren't asking for ID's that don't exist yet
    add_new_obs(pg, file_id, record, time)
  }
  obs_df = data.frame(line = record)
  tbl_sql = paste0('observations where file_id=', file_id)
  get_id_from_pg(pg, obs_df, tbl_sql)
}

add_new_measurement_types = function(pg, site, data_source,
                                     names) {
  mtype_df = data.frame(site = site, data_source = data_source,
                        name = names)
  uniq_mtypes = na.omit(unique(mtype_df))
  m_ids = get_measurement_type_id(pg, uniq_mtypes$site,
                                  uniq_mtypes$data_source,
                                  uniq_mtypes$name,
                                  add_new = FALSE)
  if (sum(is.na(m_ids)) > 0) {
    ## insert new measurement types
    uniq_mtypes$data_source_id = get_data_source_id(pg, uniq_mtypes$site,
                                                    uniq_mtypes$data_source)
    new_mtypes = uniq_mtypes[is.na(m_ids), c('data_source_id', 'name')]
    DBI::dbWriteTable(pg, 'measurement_types', new_mtypes,
                      row.names = FALSE, append = TRUE)
  }
}

get_measurement_type_id = function(pg, site,
                                   data_source,
                                   name,
                                   add_new = TRUE) {
  if (length(name) == 0) return(integer(0))
  if (add_new) {
    ## make sure we aren't asking for ID's that don't exist yet
    add_new_measurement_types(pg, site, data_source,
                              name)
  }
  data_source_id = get_data_source_id(pg, site, data_source,
                                      add_new)
  df = data.frame(data_source_id = data_source_id,
                  name = name)
  get_id_from_pg(pg, df, 'measurement_types')
}

#' @export
view_processing = function(obj) {
  shiny::shinyOptions(obj = obj)
  shiny::shinyOptions(pg = obj$con)
  shiny::runApp(system.file('processing_viewer', package = 'nysatmoschem'))
}
