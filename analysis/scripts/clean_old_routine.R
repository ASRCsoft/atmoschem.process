## reorganize and clean the original processed routine data files

library(magrittr)

out_dir = 'analysis/intermediate'
site = commandArgs(trailingOnly = TRUE)[1]

## convert a list to a named vector
map_to_dict = function(l) {
  sdf = stack(l)
  setNames(as.character(sdf$ind), sdf$values)
}

params_map = list(
    NO = 'NO_TEI',
    NO2 = 'NO2_TEI',
    NOy = 'NOy_TEI',
    `PM2.5` = 'TMC',
    Precip = 'PRECIP',
    SO2 = 'SO2(MDL0.2)',
    T = 'Temp',
    `Ultrafine PM` = 'Ultrafine',
    WD = 'RWD',
    WS = 'RWS',
    WS_Max = 'WS_MAX'
)
units_map = list(
    `AQS flag` = c('AQS', 'AQS_flag'),
    degrees = c('Az.', 'Azimuth', 'deg', 'Deg'),
    flag = c('NARSTO', 'NARSTO_flag'),
    `g/m3` = c('gm-3', 'g/m^3', 'gm^-3'),
    `in` = c('In', 'INCHES', 'Inches'),
    mbar = 'mb',
    `mm since 00Z` = c('MM Since 00Z', 'MM Since 00LT'),
    `M/m` = 'Mm-1',
    `particles/cm3` = 'particles/cm4',
    ppbv = c('ppb', 'ppbv '),
    `W/m2` = c('watts/(meter squared)', 'w/m2', 'w/m^2', 'wm-2'),
    `ug/m3` = c('ug/m3', 'ugm-3', 'ug/m³', 'μg/m3'),
    `%` = c('pct', 'pc', 'percent'),
    `degrees C` = c('deg C', 'oC', 'oc', '°C')
)
drop_list = c(
    'CH4 (ppbv)',
    'CNC',
    'H2O2 (ppbv)',
    'LWC (volts)',
    'LWC (g/m3)',
    'MWS (m/s)',
    'NMHC (ug/m3)',
    'NMHC (ppbv)',
    'PSA (cm2/m3)',
    'SO2(MDL2) (ppbv)',
    'SR_2 (W/m2)',
    'Ultrafine PM (AQS flag)',
    'WD_ELM (dir_qualifier)',
    'WS_ELM (m/s)',
    'WD_raw (degrees)',
    'WS_raw (m/s)'
)

params_dict = map_to_dict(params_map)
units_dict = map_to_dict(units_map)

standardize_col_names = function(params, units) {
  units = units %>%
    replace(., . %in% c('', 'NA'), NA) %>%
    ifelse(. %in% names(units_dict), units_dict[.], .)
  params = ifelse(params %in% names(params_dict), params_dict[params],
                  params)
  ## rename some precip columns
  params[which(units == 'mm since 00Z') + 0:1] = 'Precip since 00Z'
  units[units == 'mm since 00Z'] = 'mm'
  ## format column names
  paste0(params, ifelse(is.na(units), '', paste0(' (', units, ')')))
}

patch_wfms = function(f, df) {
  ## rename wind columns for consistency
  if (any(grepl('WD_V ', names(df)))) {
    ## WD_V is preferred over older plain WD (though both are pretty
    ## sucky before 2011)
    if (basename(f) == 'WFMS2009.csv') {
      ## combine earlier WD with WD_V
      df$`WD_V (degrees)`[df$time < '2009-09-01'] =
        df$`WD (degrees)`[df$time < '2009-09-01']
      df$`WD_V (flag)`[df$time < '2009-09-01'] =
        df$`WD (flag)`[df$time < '2009-09-01']
    }
    df[, grepl('WD ', names(df))] = NULL
    names(df) = names(df) %>%
      sub('WD_V ', 'WD ', .)
  }
  if (basename(f) < 'WFMS2011_v4.csv') {
    ## all wind speeds/directions are raw means before 2011
    names(df) = names(df) %>%
      sub('WS ', 'WS_raw ', .) %>%
      sub('WD ', 'WD_raw ', .)
  }
  # Flag very suspicious (probably wrong) CO dip with V4
  suspicious_co = df$`Time (EST)` >= as.POSIXct('1999-03-01', tz = 'EST') &
    df$`Time (EST)` < as.POSIXct('2001-05-02 05:00', tz = 'EST')
  if (any(suspicious_co)) {
    make_v4 = suspicious_co & df$`CO (flag)` != 'M1'
    df$`CO (flag)`[make_v4] = 'V4'
  }
  # Wind speed max values are not calculated correctly
  df$`WS_Max (m/s)` = NA
  df$`WS_Max (flag)` = 'M1'
  # In July 2014, something knocked the wind direction sensor off by 140
  # degrees. Brian, not yet knowing the true offset or timing of the error,
  # corrected the winds by 75 degrees, gradually phasing in the full 75 degree
  # correction over the month of July. I dug around in the data and found that
  # the error occurred abruptly at 9pm July 18th. Rich corrected the issue in
  # January 2015 and found that the true offset was 140 degrees. After the
  # sensor was corrected, Brian forgot to remove the original 75 degree
  # correction, so it mistakenly continued through to 2018 when he left and the
  # old dataset ends. Here I'm undoing Brian's adjustments and applying a more
  # accurate adjustment.
  phasein = df$`Time (EST)` >= as.POSIXct('2014-07-01', tz = 'EST') &
    df$`Time (EST)` < as.POSIXct('2014-08-01', tz = 'EST')
  if (any(phasein)) {
    phasein_hours = which(phasein)
    h_since_jul1 = difftime(df$`Time (EST)`[phasein_hours],
                            as.POSIXct('2014-07-01', tz = 'EST'),
                            units = 'hours')
    # He phased the correction in by minute. This is the best I can do to
    # correct it without looking at the minute data. If some minutes are flagged
    # or missing in an hour then this will be very slightly inaccurate.
    correction = 75 * (29.5 + as.numeric(h_since_jul1) * 60) / (60 * 24 * 31)
    df$`WD (degrees)`[phasein_hours] =
      round((df$`WD (degrees)`[phasein_hours] - correction) %% 360, 1)
    # Flag the exact hour that the event occurred
    jul18 = which(df$`Time (EST)` == as.POSIXct('2014-07-18 21:00', tz = 'EST'))
    df$`WD (degrees)`[jul18] = NA
    df$`WD (flag)`[jul18] = 'M1'
    df$`WS (m/s)`[jul18] = NA
    df$`WS (flag)`[jul18] = 'M1'
  }
  corrected75 = df$`Time (EST)` >= as.POSIXct('2014-08-01', tz = 'EST')
  if (any(corrected75)) {
    df$`WD (degrees)`[corrected75] =
      round((df$`WD (degrees)`[corrected75] - 75) %% 360, 1)
  }
  off140 = df$`Time (EST)` >= as.POSIXct('2014-07-18 22:00', tz = 'EST') &
    df$`Time (EST)` < as.POSIXct('2015-01-14 12:00', tz = 'EST')
  if (any(off140)) {
    df$`WD (degrees)`[off140] = round((df$`WD (degrees)`[off140] + 140) %% 360, 1)
  }
  df
}

patch_wfml = function(f, df) {
  ## fix incorrect NMHC units
  names(df) = sub('NMHC (ppmv)', 'NMHC (ppmC)', names(df))
  ## rename wind columns for consistency
  if (any(grepl('WD_V ', names(df)))) {
    ## WD_V is preferred over older plain WD (though both are pretty
    ## sucky before 2011)
    if (basename(f) == 'WFML2009.csv') {
      ## combine earlier WD with WD_V
      df$`WD_V (degrees)`[df$time < '2009-09-01'] =
        df$`WD (degrees)`[df$time < '2009-09-01']
      df$`WD_V (flag)`[df$time < '2009-09-01'] =
        df$`WD (flag)`[df$time < '2009-09-01']
    }
    df[, grepl('WD ', names(df))] = NULL
    names(df) = names(df) %>%
      sub('WD_V ', 'WD ', .)
  }
  if (basename(f) < 'WFML2012_v03.csv') {
    ## all wind speeds are raw means before 2012
    names(df) = names(df) %>%
      sub('WS ', 'WS_raw ', .) %>%
      sub('WD ', 'WD_raw ', .)
  }
  df
}

patch_psp = function(f, df) {
  ## fix some wind info
  if (basename(f) == 'PSP1995_v02.csv') {
    ## original (1995) WD/WS is raw
    names(df) = names(df) %>%
      sub('WS ', 'WS_raw ', .) %>%
      sub('WD ', 'WD_raw ', .)
  }
  ## RW[SD] is vector averaged and MWS is raw mean
  names(df) = names(df) %>%
    sub('RWS ', 'WS ', .) %>%
    sub('RWD ', 'WD ', .) %>%
    sub('MWS ', 'WS_raw ', .)
  # wind speeds/directions from 2009-04-14 09:00 until 2016-07-26 11:00:00 are
  # raw averages instead of vector averages
  raw = df$`Time (EST)` >= as.POSIXct('2009-04-14 09:00', tz = 'EST') &
    df$`Time (EST)` < as.POSIXct('2016-07-26 11:00:00', tz = 'EST')
  if (any(raw)) {
    df$`WD (degrees)`[raw] = NA
    df$`WD (flag)`[raw] = 'M1'
    df$`WS (m/s)`[raw] = NA
    df$`WS (flag)`[raw] = 'M1'
  }
  df
}

patch_qc = function(f, df) {
  ## fix methane with incorrect units
  if (df$`Time (EST)`[1] == '2008-01-01 00:00') {
    df$`CH4 (ppbC)` = 1000 * df$`CH4 (ug/m3)`
  }
  if ('CH4 (ug/m3)' %in% names(df)) {
    df[, 'CH4 (ug/m3)'] = NULL
  }
  df
}

make_site_specific_fixes = function(site, f, df) {
  if (site == 'WFMS') {
    patch_wfms(f, df)
  } else if (site == 'WFML') {
    patch_wfml(f, df)
  } else if (site == 'PSP') {
    patch_psp(f, df)
  } else if (site == 'QC') {
    patch_qc(f, df)
  }
}

## read processed files in the old awkward format
read_processed = function(f, index_cols = 5) {
  message('parsing ', f)
  site = basename(dirname(f))
  ## organize the header data
  headers = read.csv(f, skip = 3, nrows = 1, check.names = F,
                     stringsAsFactors = F)
  params = names(headers)
  units = as.character(headers)
  units[1:index_cols] = NA
  ## get parameters for QC columns
  for (n in 1:2) {
    ## repeat in case a column has two QC columns
    qc_params = which(params == 'QC')
    ## get the parameter name from the previous column
    params[qc_params] = params[qc_params - 1]
  }
  columns = standardize_col_names(params, units)
  ## get the data
  na_strings = c('-999', '#DIV/0!', '', 'NA', '#N/A')
  df = read.csv(f, header = F, na.strings = na_strings,
                skip = 5, fileEncoding = 'UTF-8', col.names = columns,
                check.names = F)
  ## remove last column if it's empty
  if (any(params[4:length(params)] == '')) {
    if (tail(params, 1) == '') {
      df = df[, 1:(ncol(df) - 1)]
      params = params[1:ncol(df)]
      units = units[1:ncol(df)]
      warning('Unlabeled column removed')
      if (any(params[1:length(params)] == '')) {
        warning('Unlabeled column, not removed')
      }
    } else {
      warning('Unlabeled column, not removed')
    }
  }
  ## remove empty rows
  non_empty = !is.na(df[, 1]) | !is.na(df[, 2]) | !is.na(df[, 3])
  df = df[non_empty, ]
  ## get a nice datetime
  first_date = as.character(df[1, 3])
  if (nchar(tail(strsplit(first_date, '/')[[1]], 1)) == 2) {
    date_format = '%m/%d/%y %H:%M'
  } else {
    date_format = '%m/%d/%Y %H:%M'
  }
  df$time = as.POSIXct(paste(df[, 3], df[, 4]),
                       format = date_format, tz = 'EST')
  if (any(is.na(df$time))) warning('unrecognized time values')
  ## remove some columns
  if (any(names(df) %in% drop_list)) {
    drop_params = params[names(df) %in% drop_list]
    df = df[, !params %in% drop_params]
    units = units[!params %in% drop_params]
    params = params[!params %in% drop_params]
  }
  ncols = ncol(df)
  ## make sure non-flag columns are numeric
  for (n in (index_cols + 1):(ncols - 1)) {
    if (!grepl('.*flag', units[n], ignore.case = T)) {
      if (is(df[, n], 'factor') || is(df[, n], 'character')) {
        warning('non-numeric values in ', names(df)[n])
        tmp_char = as.character(df[, n])
        old_nas = is.na(df[, n])
        df[, n] = as.numeric(tmp_char)
        new_nas = is.na(df[, n])
        print(tmp_char[head(which(new_nas & !old_nas))])
      }
    }
  }
  ## organize the index columns
  df = df[, c(ncols, (index_cols + 1):(ncols - 1))]
  ## combine column with identical params and units
  dup_cols = names(df)[grep('.*\\.1$', names(df))]
  for (dup_name in dup_cols) {
    orig_name = sub('\\.1$', '', dup_name)
    df[is.na(df[, orig_name]), orig_name] =
      df[is.na(df[, orig_name]), dup_name]
    df[, dup_name] = NULL
  }
  names(df)[1] = 'Time (EST)'
  df = make_site_specific_fixes(site, f, df)
  if (any(grepl('^WD_V', names(df))))
    warning('WD_V was not correctly replaced in', f)
  df
}

add_missing_cols = function(dfin, cols) {
  missing = cols[!cols %in% names(dfin)]
  dfin[, missing] = NA
  dfin
}

merge_dfs = function(dflist) {
  all_cols = dflist %>%
    lapply(names) %>%
    unlist %>%
    unique
  ## all_cols = unique(unlist(lapply(dflist, names)))
  dflist %>%
    lapply(function(x) add_missing_cols(x, all_cols)) %>%
    do.call(rbind, .)
  ## do.call(rbind, lapply(dflist, function(x) add_missing_cols(x, all_cols)))
}

order_columns = function(x) {
  ## reorder columns alphabetically (mostly)
  params = gsub(' \\(.*$', '', x)
  units = gsub('^.* \\((.*)\\)$|^[^()]*$', '\\1', x)
  order(params != 'Time', params, units %in% c('flag', 'AQS_flag'),
        units == 'AQS_flag')
}

write_site_file = function(site, ...) {
  if (site == 'WFMB') {
    # used to be WFML (lodge)
    site_path = file.path('analysis/raw/routine_chemistry_v0.1', 'WFML')
  } else {
    site_path = file.path('analysis/raw/routine_chemistry_v0.1', site)
  }
  files = site_path %>%
    list.files(full.names = T) %>%
    subset(., !grepl('instruments', .)) %>%
    sort
  n_files = length(files)
  dflist = lapply(files, read_processed, ...)
  finaldf = merge_dfs(dflist)
  finaldf = finaldf[, order_columns(names(finaldf))]
  out_path = file.path(out_dir, paste0('old_', site, '.csv'))
  write.csv(finaldf, file = out_path, row.names = F)
}


dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)
## missing WFMS 1990 4th quarter?

if (site == 'PSP') {
  write_site_file('PSP', index_cols = 4)
} else {
  write_site_file(site)
}
