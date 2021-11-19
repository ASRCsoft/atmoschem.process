# load raw data into SQLite

# run like so:
# Rscript analysis/load_raw.R <site> <data_source>

# produces file analysis/intermediate/raw_<site>_<data_source>.sqlite

library(atmoschem.process)
library(magrittr)
library(DBI)
library(RSQLite)

site = commandArgs(trailingOnly = T)[1]
data_source = commandArgs(trailingOnly = T)[2]
raw_folder = paste0('raw_data_v', Sys.getenv('raw_version'))

# transform a raw data file according to its site and data source
transform_raw = function(f) {
  ds = data_source
  if (ds == 'campbell') return(transform_campbell(f, site))
  if (site == 'WFMS' && ds == 'envidas') return(transform_envidas_daily(f))
  if (site == 'WFMS' && ds == 'DEC_envidas') return(transform_wfm_envidas(f))
  if (site == 'WFMB' && ds == 'envidas') return(transform_wfm_envidas(f))
  if (site == 'PSP' && ds == 'envidas') {
    # check to see if the file is in the simpler daily format
    is_daily_format = grepl('^[0-9]{8}_envidas.csv$', basename(f))
    if (is_daily_format) {
      return(transform_envidas_daily(f))
    } else {
      return(transform_psp_envidas(f))
    }
  }
  # these remaining functions are from older code that requires the results to
  # be reshaped
  res = if (ds == 'ultrafine') {
    transform_ultrafine(f)
  } else if (ds == 'mesonet') {
    transform_mesonet(f)
  } else if (site == 'WFMS' && ds == 'aethelometer') {
    transform_wfms_aethelometer(f)
  }
  if (!nrow(res)) return(data.frame())
  res %>%
    transform(time = as.POSIXct(instrument_time, tz = 'EST')) %>%
    reshape(timevar = 'measurement_name', idvar = 'time', direction = 'wide',
            drop = c('record', 'instrument_time'))
}

# make corrections to raw data
make_corrections = function(f, dat) {
  if (site == 'WFMS') {
    if (data_source == 'campbell') {
      # replace mislabeled NO2 column
      names(dat) = sub('NO2', 'NOx', names(dat))
      # correct miscalculated wind speeds
      wind_miscalculated = dat$time > '2016-12-14' &
        dat$time < '2019-02-14 15:57'
      if (any(wind_miscalculated)) {
        for (windvar in c('value.WS3Cup_Avg', 'value.WS3Cup_Max')) {
          dat[wind_miscalculated, windvar] =
            (.5 / .527) * (dat[wind_miscalculated, windvar] - 1) - .5
        }
      }
      # ignore some incorrect flags
      dat[dat$time > '2018-12-01' & dat$time < '2019-01-01',
          paste0('flagged', c('T', 'RH'), '_Avg')] = F
      dat[dat$time >= '2019-11-07 04:15' & dat$time < '2019-11-15',
          'flagged.CO_Avg'] = F
      dat[dat$time >= '2020-02-25 12:01' & dat$time < '2020-03-05 10:37',
          paste0('flagged', c('NO', 'NOx'), '_Avg')] = F
      dat[dat$time >= '2020-07-15 14:03' & dat$time < '2020-07-22 12:00',
          'flagged.CO_Avg'] = F
      # T/RH flags have not been working correctly and were probably never
      # flagged
      dat$flagged.T_Avg = F
      dat$flagged.RH_Avg = F
      # replace malfunctioning wind direction values with NA
      dat[dat$value.WindDir_SD1_WVT == 0,
          c('value.WindDir_D1_WVT', 'value.WindDir_SD1_WVT')] = NA
      # a few column names got messed up 2020-06-30 when changes were made to
      # wind columns
      if ('value.WS3CupB_SWS3CupB_S_WVT' %in% names(dat)) {
        names(dat) = sub('WS3CupB_SWS3CupB_S_WVT', 'WS3CupB_S', names(dat))
        names(dat) = sub('WindDirB_D1_WVT', 'WS3CupB_S_WVT', names(dat))
        names(dat) = sub('WindDirB_SD1_WVT', 'WindDirB_D1_WVT', names(dat))
        names(dat) = sub('WS3CupB_WVc', 'WindDirB_SD1_WVT', names(dat))
      }
    } else if (data_source == 'DEC_envidas') {
      # Unflagged WFMS ozone calibrations, 2020-06-27 to 2020-09-01, 2-4am every
      # 3 days
      if ('value.O3' %in% names(dat)) {
        botched_period = dat$time > as.POSIXct('2020-06-27', tz = 'EST') &
          dat$time < as.POSIXct('2020-09-02', tz = 'EST')
        day3 = as.integer(as.Date(dat$time) - as.Date('2020-06-27')) %% 3 == 0
        am2to4 = substr(dat$time, 12, 13) %in% c('02', '03')
        dat$flagged.O3[botched_period & day3 & am2to4] = T
      }
    } else if (data_source == 'ultrafine') {
      # fix some times
      date_str = basename(tools::file_path_sans_ext(f))
      if (date_str >= '21022402' && date_str <= '21031601') {
        # Year says 2020, should be 2021. Due to leap year, some days are also
        # off by one. Can solve using Julian days
        dat$time = as.POSIXct(format(dat$time, '2021 %j %T'), tz = 'EST',
                              format = '%Y %j %T')
      }
    }
  } else if (site == 'WFMB') {
    if (data_source == 'campbell') {
      # replace mislabeled NO2 column
      names(dat) = sub('NO2', 'NOX', names(dat))
      # add missing data, 2021-02-16 to 2021-02-24
      if (basename(f) == 'WFMS_LODGE_Table1_2021_02_15_0001.dat') {
        # Missing data is in file info/2021_gap.dat. That file has the same
        # structure as a normal campbell file, but without the header and with
        # tabs instead of commas.
        gap_file = file.path(dirname(dirname(f)), 'info', '2021_gap.dat')
        gap_lines = gsub('\t', ',', readLines(gap_file, warn = F))
        f_header = readLines(f, n = 4)
        gap_conn = textConnection(c(f_header, gap_lines))
        gap_dat = transform_campbell(gap_conn, 'WFMB') %>%
          subset(time > as.POSIXct('2021-02-16 00:00', tz = 'EST') &
                 time < as.POSIXct('2021-02-24 00:00', tz = 'EST'))
        dat = rbind(dat, gap_dat)
      }
    } else if (data_source == 'envidas') {
      # flag left on after instrument installed, 2020-01-03 14:02 to 2020-01-07
      # 09:04
      if ('flagged.PM25C' %in% names(dat)) {
        dat$flagged.PM25C[dat$time >= as.POSIXct('2020-01-03 14:02', tz = 'EST') &
                          dat$time < as.POSIXct('2020-01-07 09:04', tz = 'EST')] = F
      }
    }
  } else if (site == 'PSP') {
    if (data_source == 'envidas') {
      # fix some black carbon data, which is incorrectly recorded as zero when it's
      # missing
      if (all(c('value.BC1', 'value.BC6') %in% names(dat))) {
        na_bc = which(dat$value.BC1 == 0 & dat$value.BC6 == 0)
        if (length(na_bc)) dat[na_bc, c('value.BC1', 'value.BC6')] = NA
      }
      # fix some incorrectly set flags
      dat$flagged.NOy[dat$time >= '2020-01-24 12:00' &
                      dat$time < '2020-01-27 11:00'] = F
      dat$flagged.CO[dat$time >= '2020-08-17 14:10' &
                     dat$time < '2020-08-18 10:00'] = F
      dat$flagged.NOy[dat$time >= '2020-08-19 14:00' &
                      dat$time < '2020-08-20 09:00'] = F
      dat$flagged.VWS[dat$time >= '2020-11-04 09:23' &
                      dat$time <= '2020-11-16 09:44'] = F
      dat$flagged.VWD[dat$time >= '2020-11-04 09:23' &
                      dat$time <= '2020-11-16 09:44'] = F
    }
  }
  dat
}

# add columns to the db table if needed
add_db_columns = function(db, cols) {
  cur_fields = dbListFields(db, 'measurements')
  if (!all(cols %in% cur_fields)) {
    new_fields = setdiff(cols, cur_fields)
    for (fname in new_fields) {
      sql_text = 'alter table measurements add column ?'
      sql = sqlInterpolate(db, sql_text, dbQuoteIdentifier(db, fname))
      dbExecute(db, sql)
    }
  }
}

# read the file and add it to the sqlite database
load_file = function(f, db) {
  dat = tryCatch(transform_raw(f), error = function(e) {
    stop(f, ' loading failed: ', e)
  })
  if (!nrow(dat)) return()
  dat = make_corrections(f, dat)
  add_db_columns(db, names(dat))
  dat$time = format(dat$time, '%Y-%m-%d %H:%M:%S', tz = 'EST')
  dbWriteTable(db, 'measurements', dat, append = T)
}

# set up the sqlite database
interm_dir = file.path('analysis', 'intermediate')
dir.create(interm_dir, F, T)
dbpath = paste0('raw_', site, '_', data_source, '.sqlite') %>%
  file.path(interm_dir, .)
if (file.exists(dbpath)) invisible(file.remove(dbpath))
db = dbConnect(SQLite(), dbpath)
invisible(dbExecute(db, 'create table measurements(time text)'))

# load data into sqlite
files = file.path('analysis', 'raw', raw_folder, site, 'measurements',
                  data_source, '*', '*') %>%
  Sys.glob
# exclude files in the 'info' folder
files = files[basename(dirname(files)) != 'info']
message('Loading ', length(files), ' files into ', basename(dbpath), '...')
for (f in files) load_file(f, db)

# speed up future queries, especially queries that filter by time of day
invisible(dbExecute(db, 'create index time_index on measurements(time, substr(time, 12, 5))'))
dbDisconnect(db)
