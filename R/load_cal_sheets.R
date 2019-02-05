#!/usr/bin/env Rscript
## get manual calibrations from pdf cal sheets

## run this script from the command line like so:
## Rscript load_cal_sheets.R dbname ~/data/campbell/WFMS/WFMFS_SUMMIT_Table1_2018_06_29_2300.dat

library(dplyr)
library(tidyr)
library(dbx)

dbname = commandArgs(trailingOnly = T)[1]

## get the sites and corresponding IDs
pg = dbxConnect(adapter = 'postgres', dbname = dbname)
sites = dbxSelect(pg, 'select * from stations')
dbxDisconnect(pg)

## Get the fields from a calibration pdf form.
read_pdf_form = function(f) {
  command_str = paste('pdftk', f, 'dump_data_fields')
  pdftk_strs = system(command_str, intern = T)
  data.frame(text = pdftk_strs, stringsAsFactors = F) %>%
    mutate(field = cumsum(text == '---')) %>%
    subset(text != '---') %>%
    separate(text, into = c('descriptor', 'value'), sep = ': ',
             extra = 'merge') %>%
    ## don't want the enumerated field options, which mess up the
    ## spread function
    subset(descriptor != 'FieldStateOption') %>%
    spread(descriptor, value)
}

write_cal = function(station, cal_type, cal_time,
                     measured_value, corrected) {
  station_id = sites$id[match(station, sites$short_name)]
  pg = dbxConnect(adapter = 'postgres', dbname = dbname)
  dbxUpsert(pg, table, campbell, where_cols = 'instrument_time',
            skip_existing = T)
  dbxDisconnect(pg)
}

no1 = read_pdf_form('/home/wmay/tests/cal_sheets/42C_180103.pdf')
## [don't need] cal start time: time_log_1
## zero time: time_log_3
## NO zero: measured_zero_noy_a_3
## NOx zero: measured_zero_noy_b_3
## set 42C zero: set_42ctls_to_zero_3
## span time: time_log_5
## NO span: measured_span_noy_a_5
## NOx span: measured_span_noy_b_5
## set NO span: set_span_noy_a_5
## set NOx span: set_span_noy_b_5
## zero check time: time_log_7
## NO zero check: 42ctls_zero_noy_a_7
## NOx zero check: 42ctls_zero_noy_b_7
## [don't need] cal end time: time_log_8

noy_file = '/home/wmay/2018_Summit_Cal_Sheets/42Cs_180103.pdf'
noy = read_pdf_form(noy_file)
## 42Cs sheets contain 2 calibrations!

## [don't need] cal start time: time_log_1
## zero time: time_log_3
## NOy A zero: measured_zero_noy_a_3
## NOy B zero: measured_zero_noy_b_3
## set 42Cs zero: set_42ctls_to_zero_3
## span time: time_log_5
## NOy A span: measured_span_noy_a_5
## NOy B span: measured_span_noy_b_5
## set NOy A span: set_span_noy_a_5
## set NOy B span: set_span_noy_b_5
## zero check time: time_log_7
## NO zero check: 42ctls_zero_noy_a_7
## NOx zero check: 42ctls_zero_noy_b_7
## [don't need] cal end time: time_log_8

so2_file = '/home/wmay/2018_Summit_Cal_Sheets/43C_180103.pdf'
so2 = read_pdf_form(so2_file)
## cal start time: time_log_1
## So2 zero: measured_zero_so2_3
## set 42Cs zero: set_43c_to_zero_3
## So2 span: measured_span_so2_5
## set So2 span: set_span_so2_5
## cal end time: time_log_8

co_file = '/home/wmay/2018_Summit_Cal_Sheets/300EU_180103.pdf'
co = read_pdf_form(co_file)
## cal start time: time_log_1
## Co zero: measured zero 3
## set 42Cs zero: set 300EU to zero 3
## Co span: measured span 5
## set Co span: set 300EU span 5
## cal end time: time_log_8
