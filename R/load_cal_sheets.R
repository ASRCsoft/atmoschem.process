#!/usr/bin/env Rscript
## get manual calibrations from pdf cal sheets

## run this script from the command line like so:
## Rscript load_cal_sheets.R dbname ~/data/calibrations/WFMS/42Cs_180103.pdf

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
  df = data.frame(text = pdftk_strs, stringsAsFactors = F) %>%
    mutate(field = cumsum(text == '---')) %>%
    subset(text != '---') %>%
    separate(text, into = c('descriptor', 'value'), sep = ': ',
             extra = 'merge') %>%
    ## don't want the enumerated field options, which mess up the
    ## spread function
    subset(descriptor != 'FieldStateOption') %>%
    spread(descriptor, value)
  ## organize as a list
  pdfls = as.list(df$FieldValue)
  names(pdfls) = df$FieldName
  pdfls
}

write_cal = function(station, measure, cal_type, cal_time,
                     measured_value, corrected) {
  station_id = sites$id[match(station, sites$short_name)]
  df = data.frame(station_id = station_id,
                  instrument = measure,
                  type = cal_type,
                  cal_time = cal_time,
                  measured_value = measured_value,
                  corrected = corrected)
  idx_cols = c('station_id', 'instrument', 'cal_time')
  pg = dbxConnect(adapter = 'postgres', dbname = dbname)
  dbxUpsert(pg, 'manual_calibrations', df, where_cols = idx_cols)
  dbxDisconnect(pg)
}

## return True if the box is checked, otherwise False
box_checked = function(box) !is.na(box) & box == 'On'

write_42C = function(f) {
  pdf = read_pdf_form(f)
  ## get the station
  path_folders = strsplit(f, '/')[[1]]
  station = path_folders[length(path_folders) - 1]
  ## [don't need] cal start time: time_log_1
  ## has zero: zero_cal_mode_2?
  ## zero time: time_log_3
  ## NO zero: measured_zero_noy_a_3
  ## NOx zero: measured_zero_noy_b_3
  ## set 42C zero: set_42ctls_to_zero_3
  ## has span: span_cal_mode_4?
  ## span time: time_log_5
  ## NO span: measured_span_noy_a_5
  ## NOx span: measured_span_noy_b_5
  ## set NO span: set_span_noy_a_5
  ## set NOx span: set_span_noy_b_5
  ## has zero check: zero_check_7?
  ## zero check time: time_log_7
  ## NO zero check: 42ctls_zero_noy_a_7
  ## NOx zero check: 42ctls_zero_noy_b_7
  ## [don't need] cal end time: time_log_8
  if (box_checked(pdf$zero_cal_mode_2) && !is.na(pdf$time_log_3)) {
    cal_time = strptime(paste(pdf$date, pdf$time_log_3),
                        '%d-%b-%y %H:%M')
    corrected = box_checked(pdf$set_42ctls_to_zero_3)
    write_cal(station, 'NO', 'zero', cal_time,
              pdf$measured_zero_noy_a_3,
              corrected)
    write_cal(station, 'NOx', 'zero', cal_time,
              pdf$measured_zero_noy_a_3,
              corrected)
  }
  if (box_checked(pdf$span_cal_mode_4) && !is.na(pdf$time_log_5)) {
    cal_time = strptime(paste(pdf$date, pdf$time_log_5),
                        '%d-%b-%y %H:%M')
    write_cal(station, 'NO', 'span', cal_time,
              pdf$measured_span_noy_a_5,
              box_checked(pdf$set_span_noy_a_5))
    write_cal(station, 'NOx', 'span', cal_time,
              pdf$measured_span_noy_b_5,
              box_checked(pdf$set_span_noy_b_5))
  }
  if (box_checked(pdf$zero_check_7) && !is.na(pdf$time_log_7)) {
    cal_time = strptime(paste(pdf$date, pdf$time_log_7),
                        '%d-%b-%y %H:%M')
    write_cal(station, 'NO', 'zero', cal_time,
              pdf$`42ctls_zero_noy_a_7`, FALSE)
    write_cal(station, 'NOx', 'zero', cal_time,
              pdf$`42ctls_zero_noy_b_7`, FALSE)
  }
}

files = commandArgs(trailingOnly = T)[-1]
for (f in files) {
  message(paste('Importing', f))
  file_type = gsub('^.*/|_[^/]*$', '', f)
  if (file_type == '42C') {
    write_42C(f)
  }
}


## noy_file = '/home/wmay/2018_Summit_Cal_Sheets/42Cs_180103.pdf'
## noy = read_pdf_form(noy_file)
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

## so2_file = '/home/wmay/2018_Summit_Cal_Sheets/43C_180103.pdf'
## so2 = read_pdf_form(so2_file)
## cal start time: time_log_1
## So2 zero: measured_zero_so2_3
## set 42Cs zero: set_43c_to_zero_3
## So2 span: measured_span_so2_5
## set So2 span: set_span_so2_5
## cal end time: time_log_8

## co_file = '/home/wmay/2018_Summit_Cal_Sheets/300EU_180103.pdf'
## co = read_pdf_form(co_file)
## cal start time: time_log_1
## Co zero: measured zero 3
## set 42Cs zero: set 300EU to zero 3
## Co span: measured span 5
## set Co span: set 300EU span 5
## cal end time: time_log_8
