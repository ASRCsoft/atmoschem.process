# collect and organize Queens College data

library(magrittr)
library(atmoschem.process)

years = 2001:2021
site = '36-081-0124'
email = ''
key = ''
aqs_params = read.csv('analysis/config/aqs_queens.csv')
aqs_params$aqs_code = as.character(aqs_params$aqs_code)

aqs_dict = aqs_flags %>%
  subset(!is.na(narsto)) %>%
  with(setNames(narsto, code))

# get NARSTO flag from AQS flag and data value
narsto_from_aqs = function(val, aqs) {
  if (!all(na.omit(unique(aqs)) %in% names(aqs_dict))) {
    distinct_aqs = na.omit(unique(aqs))
    missing = distinct_aqs[!distinct_aqs %in% names(aqs_dict)]
    missing_str = paste(missing, collapse = ', ')
    stop('Untranslated flags: ', missing_str)
  }
  # stopifnot(all(na.omit(aqs) %in% names(aqs_dict)))
  flag = aqs_dict[aqs]
  # if there's no data (flag or value), NARSTO says M1
  flag[is.na(val) & is.na(flag)] = 'M1'
  # if AQS has no flag, meaning no problems, NARSTO says V0
  flag[!is.na(val) & is.na(flag)] = 'V0'
  # use M2 for data that doesn't need to be missing according to NARSTO but has
  # been removed anyway
  flag[is.na(val) & !startsWith(flag, 'M')] = 'M2'
  flag
}


s1 = aqs_api_samples(aqs_params$aqs_code, site, years, email, key)


make_wide = function(samples) {
  wide = samples %>%
    transform(time = as.POSIXct(paste(date_gmt, time_gmt), tz = 'UTC')) %>%
    subset(select = c('time', 'parameter_code', 'sample_measurement', 'qualifier')) %>%
    reshape(v.names = c('sample_measurement', 'qualifier'),
            timevar = 'parameter_code', idvar = 'time', direction = 'wide')
  # wide = wide[order(wide$date_local), ]
  names(wide) = names(wide) %>%
    # sub('time', 'sdatel', .) %>%
    sub('sample_measurement\\.', '', .) %>%
    sub('qualifier\\.(.*)', '\\1 (flag)', .)
  attr(wide$sdatel, 'tzone') = 'EST'
  wide
}

w1 = make_wide(s1)

# just need to: fix column names, convert flags, choose methods
w2 = w1
param_codes = sub(' .*', '', names(w2)[-1])
keep = !param_codes %in% c('43101', '43102')
w2 = w2[, c(TRUE, keep)]
param_codes = param_codes[keep]
param_names = aqs_params$column[match(param_codes, aqs_params$aqs_code)]
names(w2)[-1] = paste0(param_names, sub('[0-9]+', '', names(w2)[-1]))

