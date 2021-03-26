# Create the routine chemistry dataset package.

# run this script from the project root directory with
# Rscript analysis/routine_package.R <out directory>

# produces file analysis/out/routine_chemistry_v$(PKGVERS).zip

library(atmoschem.process)
library(magrittr)
library(DBI)
library(RSQLite)
options(warn = 1) # print warnings immediately

out_dir = commandArgs(trailingOnly = TRUE)[1]
dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)
old_processed_dir = file.path('analysis', 'intermediate')
config = read_csv_dir('analysis/config')

# combine site/data source hourly files into a single site hourly file, using
# column names from report_columns
get_site_df = function(site) {
  site_sources = config$dataloggers$name[config$dataloggers$site == site]
  meas_list = list()
  for (s in site_sources) {
    dbpath = file.path('analysis', 'intermediate',
                       paste0('hourly_', site, '_', s, '.sqlite'))
    db = dbConnect(SQLite(), dbpath)
    meas = dbReadTable(db, 'measurements', check.names = F)
    dbDisconnect(db)
    meas$time = as.POSIXct(meas$time, tz = 'EST')
    # get only the parameters included in the dataset
    s_cols = config$report_columns[config$report_columns$site == site, ] %>%
      subset(data_source == s)
    params = c('time', s_cols$measurement)
    meas = meas[, sub('^value\\.|^flag\\.', '', names(meas)) %in% params]
    # add units to vals column names
    mtypes = config$channels[config$channels$site == site &
                             config$channels$data_source == s, ]
    val_cols = grep('^value\\.', names(meas))
    units = names(meas)[val_cols] %>%
      sub('^value\\.', '', .) %>%
      match(mtypes$name) %>%
      mtypes$units[.]
    names(meas)[val_cols] = paste0(names(meas)[val_cols], ' (', units, ')')
    # fix column name formatting
    names(meas) = names(meas) %>%
      sub('^value\\.', '', .) %>%
      sub('^flag\\.(.*)', '\\1 (flag)', .)
    old_params = sub(' \\(flag\\)$', '', names(meas)[-1])
    new_params = s_cols$column[match(old_params, s_cols$measurement)]
    names(meas)[-1] =
      paste0(new_params, sub('.*( \\(flag\\))$', '\\1', names(meas)[-1]))
    names(meas)[1] = 'Time (EST)'
    meas_list[[s]] = meas
  }
  Reduce(function(x, y) merge(x, y, all = T), meas_list)
}

for (site in config$sites$abbreviation) {
  message('Organizing ', site, ' data')
  csv_file = paste0('old_', site, '.csv')
  old_processed_file = file.path(old_processed_dir, csv_file)
  out_file = file.path(out_dir, paste0('hourly_', site, '.csv'))

  oldp = read.csv(old_processed_file, na.strings = c('NA', '-999'),
                  check.names = FALSE)
  oldp$`Time (EST)` = as.POSIXct(oldp$`Time (EST)`, tz = 'EST')

  if (site != 'QC') {
    newp = get_site_df(site)
    # make sure flag column names are all formatted consistently
    names(newp) = gsub('\\(NARSTO\\)', '\\(flag\\)', names(newp))
    names(newp) = gsub('\\(AQS\\)', '\\(AQS flag\\)', names(newp))
    names(oldp) = gsub('\\(AQS_flag\\)', '\\(AQS flag\\)', names(oldp))

    # fix a silly inconsistency in the lodge data
    if (site == 'WFML') {
      names(newp) = gsub('Precip \\(mm since 00Z\\)', 'Precip since 00Z \\(mm\\)', names(newp))
      names(newp) = gsub('Precip \\(flag\\)', 'Precip since 00Z \\(flag\\)', names(newp))
    }

    # make sure datasets have the same columns
    all_cols = union(names(newp), names(oldp))
    oldp[setdiff(names(newp), names(oldp))] = NA
    newp[setdiff(names(oldp), names(newp))] = NA

    # put the columns in the correct ordering
    cols = data.frame(name = all_cols)
    cols$time = cols$name == 'Time (EST)'
    cols$param = sub(' \\([^(]*\\)$', '', cols$name)
    cols$flag = grepl('flag\\)$', cols$name)
    cols$aqs_flag = grepl('AQS flag\\)$', cols$name)
    cols$order = with(cols, order(!time, param, flag, aqs_flag))
    cols = cols[cols$order, ]

    oldp = oldp[, cols$name]
    newp = newp[, cols$name]

    pout = rbind(oldp, newp)
  } else {
    # put the columns in the correct ordering
    cols = data.frame(name = names(oldp))
    cols$time = cols$name == 'Time (EST)'
    cols$param = sub(' \\([^(]*\\)$', '', cols$name)
    cols$flag = grepl('flag\\)$', cols$name)
    cols$aqs_flag = grepl('AQS flag\\)$', cols$name)
    cols$order = with(cols, order(!time, param, flag, aqs_flag))
    cols = cols[cols$order, ]

    oldp = oldp[, cols$name]
    
    pout = oldp
  }

  # add missing hours
  hours = seq.POSIXt(min(pout$`Time (EST)`), max(pout$`Time (EST)`), by = 'hour')
  pout = merge(data.frame('Time (EST)' = hours, check.names = F), pout,
               all.x = T)

  # replace NA flags with M1
  flag_cols = subset(cols, flag & !aqs_flag)$name
  for (f in flag_cols) pout[is.na(pout[, f]), f] = 'M1'

  pout$`Time (EST)` = format(pout$`Time (EST)`, format = '%Y-%m-%d %H:%M')
  write.csv(pout, file = out_file, na = '', row.names = FALSE)
}

# Supplementary data

## site info
write.csv(config$sites, file = file.path(out_dir, 'sites.csv'), na = '',
          row.names = FALSE)

## instrument info
# match column measurements to measurement instruments
measurement_insts = merge(config$channel_instruments, config$instruments,
                          by.x = c('site', 'instrument'),
                          by.y = c('site', 'name'))
column_insts = atmoschem.process:::merge_timerange(config$report_columns,
                                                   measurement_insts, 'times')
instr_cols = c('site', 'column', 'times', 'brand', 'model', 'serial_number')
instruments = column_insts[, instr_cols]
instr_path = file.path(out_dir, 'instruments.csv')
write.csv(instruments, file = instr_path, na = '', row.names = FALSE)
# readme
file.copy(file.path('analysis', 'docs', 'routine.md'),
          file.path(out_dir, 'README.txt'), overwrite = T)

# zip
# paths in the zipped file are determined by the working directory
setwd(dirname(out_dir))
routine_dir = basename(out_dir)
zip(paste0(routine_dir, '.zip'), routine_dir)
