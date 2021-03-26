# move datasets in data-raw to data

library(usethis)

narsto_flags = read.csv('data-raw/narsto_flags.csv', stringsAsFactors = F)
aqs_flags = read.csv('data-raw/aqs_flags.csv', stringsAsFactors = F)

# add narsto flags to aqs data
aqs_narsto = read.csv('data-raw/aqs_narsto.csv', stringsAsFactors = F)
aqs_flags = merge(aqs_flags, aqs_narsto, by.x = 'code', by.y = 'aqs')

use_data(narsto_flags, aqs_flags, overwrite = T)
