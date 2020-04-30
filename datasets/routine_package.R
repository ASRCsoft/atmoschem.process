# This file creates the routine chemistry bulk data files. It takes the old
# processed data and combines it with the new processed data.

options(warn = 1) # print warnings immediately

old_processed_dir = 'datasets/cleaned/routine_chemistry'
new_processed_dir = 'datasets/cleaned/processed_data'
out_dir = 'datasets/out/routine_chemistry'
dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)

sites = c('WFMS', 'WFML', 'PSP', 'QC')
for (site in sites) {
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

  # replace NA flags with M1
  flag_cols = subset(cols, flag & !aqs_flag)$name
  for (f in flag_cols) pout[is.na(pout[, f]), f] = 'M1'

  write.csv(pout, file = out_file, row.names = FALSE)
}

# should add the readme and supplementary data
# ...
