# move datasets in data-raw to data

library(usethis)

narsto_flags = read.csv('data-raw/narsto_flags.csv', stringsAsFactors = F)
aqs_flags = read.csv('data-raw/aqs_flags.csv', stringsAsFactors = F)
use_data(narsto_flags, aqs_flags, overwrite = T)
