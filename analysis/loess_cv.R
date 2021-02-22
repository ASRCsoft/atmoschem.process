# Find optimal span values for loess smoothing (QC-RLSC) using cross validation

# run this script from the project root directory with
# Rscript analysis/loess_cv.R <site>

# produces file analysis/intermediate/loess_<site>.csv

library(atmoschem.process)
library(DBI)
library(RSQLite)

set.seed(1234)
start_time = as.POSIXct('2018-10-01', tz = 'EST')
end_time = as.POSIXct(Sys.getenv('processing_end'), tz = 'EST')

# estimate flow values with piecewise_approx
get_flows = function(mname, xout) {
  cal_flows = gilibrator[gilibrator$site == site &
                         gilibrator$measurement_name == mname, ]
  # flow times are type Date instead of POSIXct, so they have to be converted
  cal_flows$time = as.POSIXct(cal_flows$time, tz='EST')
  flow_breaks = cal_flows$time[!is.na(cal_flows$changed) & cal_flows$changed]
  atmoschem.process:::piecewise_approx(cal_flows$time, cal_flows$measured_value,
                                       xout, flow_breaks, rule = 2)
}

# add an entry to a dimension of a list (for example from the output of `by`)
expand_list_dim = function(l, i, label) {
  old_attr = attributes(l)
  new_dim = dim(l)
  new_dim[i] = new_dim[i] + 1
  new_dimnames = dimnames(l)
  new_dimnames[[i]] = c(new_dimnames[[i]], label)
  new_l = array(vector('list', prod(new_dim)), dim = new_dim,
                dimnames = new_dimnames)
  old_ind = as.matrix(do.call(expand.grid, old_attr$dimnames))
  new_l[old_ind] = l[old_ind]
  attributes(new_l) = c(old_attr[!names(old_attr) %in% c('dim', 'dimnames')],
                        attributes(new_l))
  new_l
}

# read cals from sqlite file
dbpath = file.path('analysis', 'intermediate', paste0('cals_', site, '.sqlite'))
db = dbConnect(SQLite(), dbpath)
sql_text = 'select * from calibrations where end_time >= ? and end_time <= ? order by end_time asc'
sql = sqlInterpolate(db, sql_text, format(start_time, '%Y-%m-%d %H:%M:%S', tz = 'EST'),
                     format(end_time, '%Y-%m-%d %H:%M:%S', tz = 'EST'))
cals = dbGetQuery(db, sql, check.names = F)
dbDisconnect(db)
cals$time = as.POSIXct(cals$end_time, tz = 'EST')
cals$ntime = as.numeric(cals$time)
cals$corrected = as.logical(cals$corrected)
cals$flagged = as.logical(cals$flagged)
cals$manual = as.logical(cals$manual)
cals = subset(cals, !is.na(time) & !is.na(measured_value))

# fix up CE values somehow -- The fix in this case is to relabel NO2 as NOx,
# since that's the channel that measures the NO2.
no2_ce = cals$measurement_name == 'NO2' & cals$type == 'CE'
cals$measurement_name[no2_ce] = 'NOx'

# get 'provided' values from flow measurements if available (only applies to
# whiteface)
cal_channels = unique(cals[, c('data_source', 'measurement_name')])
cal_channels = merge(cal_channels,
                     measurement_types[measurement_types$site == site, ],
                     by.x = c('data_source', 'measurement_name'),
                     by.y = c('data_source', 'name'))
cal_channels = subset(cal_channels,
                      !is.na(gilibrator_span) | !is.na(gilibrator_ce))
for (i in seq_len(nrow(cal_channels))) {
  dsource = cal_channels$data_source[i]
  mname = cal_channels$measurement_name[i]
  for (type in c('span', 'CE')) {
    type_flow = cal_channels[[paste0('gilibrator_', tolower(type))]][i]
    if (!is.na(type_flow)) {
      mname_cal = cals$data_source == dsource & cals$measurement_name == mname &
        cals$type == type
      cals[mname_cal, 'provided_value'] =
        get_flows(type_flow, cals$time[mname_cal])
    }
  }
}

# fit the loess curves
out = by(cals, cals[, c('data_source', 'measurement_name')], function(x) {
  message('Fitting ', x$data_source[1], ' / ', x$measurement_name[1],
          ' loess curves...')
  qcrlsc_fit(measured_value ~ ntime, subset(x, !flagged), smin = .1)
})

# take care of some tricky CE calculations
if (site == 'WFMS') {
  # WFMS NOx channel NO2 CEs should be applied to derived NO2, not NOx
  out = expand_list_dim(out, 2, 'NO2')
  out[['campbell', 'NO2']] = out[['campbell', 'NOx']]
  out[['campbell', 'NO2']][c('zero', 'span')] = NULL
  out[['campbell', 'NOx']]$CE = NULL
} else if (site == 'PSP') {
  # PSP NO2 conversion efficiencies require subtracting NO (because the CE air
  # includes both NO2 and NO I think?)
  out = expand_list_dim(out, 2, 'NO2')
  qcrlsc_correct(out[['envidas', 'NO']])
  qcrlsc_correct(out[['envidas', 'NOx']])
  # ...
  out[['campbell', 'NO2']] = qcrlsc_fit(measured_value ~ ntime, x)
  # ---
  all_cals2 = all_cals
  all_cals2$time = all_cals2$end_time
  all_cals2$date = as.Date(all_cals2$time)
  ces_list = list()
  for (mname in c('NO', 'NOx')) {
    m_cals = subset(all_cals2, measurement_name == mname)
    m_ces = subset(m_cals, type == 'CE')
    m_conf = subset(measurement_types, site == 'PSP' & name == mname)
    m_ces[, mname] =
      drift_correct(m_ces$time, m_ces$measured_value,
                    m_cals[m_cals$type == 'zero', ],
                    m_cals[m_cals$type == 'span', ], config = m_conf)
    ces_list[[mname]] = m_ces
  }
  # the NO and NOx start and end times aren't exactly the same, so match by date
  # instead
  same_cols = c('date', 'type', 'provided_value', 'data_source', 'manual',
                'corrected')
  both_ces = merge(ces_list[['NO']], ces_list[['NOx']], by = same_cols)
  no2_ces = both_ces %>%
    transform(measurement_name = 'NO2',
              measured_value = NOx - NO,
              flagged = flagged.x | flagged.y,
              start_time = start_time.x, end_time = end_time.x) %>%
    subset(select = c(same_cols[-1], 'measurement_name', 'start_time',
                      'end_time', 'measured_value', 'flagged'))
  all_cals = rbind(all_cals, no2_ces[, names(all_cals)])
}

# write results
interm_dir = file.path('analysis', 'intermediate')
dir.create(interm_dir, F, T)
rdspath = file.path(interm_dir, paste0('loess_', site, '.rds'))
saveRDS(out, rdspath)
