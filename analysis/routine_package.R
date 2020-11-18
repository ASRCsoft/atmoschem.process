# This file creates the routine chemistry bulk data files. It takes the old
# processed data and combines it with the new processed data.

library(atmoschem.process)
options(warn = 1) # print warnings immediately

old_processed_dir = 'analysis/cleaned/old_routine'
new_processed_dir = 'analysis/cleaned/processed_data'
out_dir = commandArgs(trailingOnly = TRUE)[1]
dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)

for (site in sites$abbreviation) {
  message('Organizing ', site, ' data')
  csv_file = paste0(site, '.csv')
  old_processed_file = file.path(old_processed_dir, csv_file)
  new_processed_file = file.path(new_processed_dir, csv_file)
  out_file = file.path(out_dir, csv_file)

  oldp = read.csv(old_processed_file, na.strings = c('NA', '-999'),
                  check.names = FALSE)

  if (file.exists(new_processed_file)) {
    newp = read.csv(new_processed_file, na.strings = c('NA', '-999'),
                    check.names = FALSE)
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
  pout$`Time (EST)` = as.POSIXct(pout$`Time (EST)`, tz = 'EST')
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
write.csv(sites, file = file.path(out_dir, 'sites.csv'), na = '',
          row.names = FALSE)

## instrument info
# match column measurements to measurement instruments
measurement_insts = merge(measurement_sources, instruments,
                          by.x = c('site', 'instrument'),
                          by.y = c('site', 'name'))
column_insts = atmoschem.process:::merge_timerange(report_columns,
                                                   measurement_insts, 'times')
instr_cols = c('site', 'column', 'times', 'brand', 'model', 'serial_number')
instruments = column_insts[, instr_cols]
instr_path = file.path(out_dir, 'instruments.csv')
write.csv(instruments, file = instr_path, na = '', row.names = FALSE)
