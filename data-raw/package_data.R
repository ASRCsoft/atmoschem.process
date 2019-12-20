## move supporting datasets (in data-raw/package_data) to /data

library(usethis)

csv_files = list.files('data-raw/package_data', pattern = '\\.csv$',
                       full.names = TRUE)

## usethis::use_data requires passing the data as a list of symbols,
## so we have to assign the data frames and pass a list of the data
## frame names as symbols
df_names = tools::file_path_sans_ext(basename(csv_files))
for (n in 1:length(csv_files)) {
  df_n = read.csv(csv_files[n], na.strings = c('NA', ''))
  assign(df_names[n], df_n)
}
dflist = lapply(df_names, as.symbol)
dflist$overwrite = TRUE
do.call(use_data, dflist)
