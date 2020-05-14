# move dataset in data-raw/package_data to /data

library(usethis)

csv_file = commandArgs(trailingOnly = TRUE)[1]

# use_data requires the data name as a symbol, so we have to assign the data
# frame and pass the data frame name as symbol
data_name = tools::file_path_sans_ext(basename(csv_file))
csv_data = read.csv(csv_file, na.strings = c('NA', ''), stringsAsFactors = FALSE)
assign(data_name, csv_data)
do.call(use_data, list(as.symbol(data_name), overwrite = TRUE))
