#' Get the EPA monitoring day from a date
#'
#' Returns the corresponding day in the EPA's 12-day sampling schedule for the
#' given date.
#'
#' The EPA collects some samples every 3, 6, or 12 days. It provides a recurring
#' 12-day schedule for these samples. This function represents the schedule with
#' the integers 1 to 12. A sample taken every 3 days should be collected on days
#' 3, 6, 9, and 12. A sample on the 6-day schedule should be collected on days 6
#' and 12, and a sample on the 12-day schedule should be collected on day 12.
#'
#' @param d A date.
#' @return An integer from 1 to 12 representing the day in the EPA's 12-day
#'   monitoring schedule.
#' @examples
#' epa_schedule(as.Date('2020-01-01'))
#'
#' @seealso \url{https://www.epa.gov/amtic/sampling-schedule-calendar},
#'   \url{https://www3.epa.gov/ttn/amtic/calendar.html}
#' @export
epa_schedule = function(d) as.integer(d - as.Date('2020-01-05')) %% 12 + 1

# combine state, county, and site numbers into an AQS site ID string (makes
# subsetting by site easier)
format_aqs_site = function(state, county, site) {
  paste(formatC(state, width = 2, format = "d", flag = "0"),
        formatC(county, width = 3, format = "d", flag = "0"),
        formatC(site, width = 4, format = "d", flag = "0"), sep = '-')
}

# Download a set of AQS bulk download (zipped csv) files. See the available
# files at https://aqs.epa.gov/aqsweb/airdata/download_files.html.
download_aqs = function(name, years, destdir, overwrite = FALSE) {
  aqsweb = 'https://aqs.epa.gov/aqsweb/'
  zips = paste0('https://aqs.epa.gov/aqsweb/airdata/', name, '_', years, '.zip')
  for (z in zips) {
    dest_path = file.path(destdir, basename(z))
    if (overwrite || !file.exists(dest_path)) download.file(z, dest_path)
  }
}

# `read.table` is slow when applied to unz connections (the normal way of
# reading a zipped csv). For unclear reasons, this version using `readChar` is
# faster.
read_zipped_csv = function(z, f, ...) {
  zlist = zip::zip_list(z)
  size = zlist$uncompressed_size[zlist$filename == f]
  con = unz(z, f)
  on.exit(close(con))
  read.csv(text = readChar(con, nchars = size, useBytes = TRUE), ...)
}

# Some of the zipped datasets are very large. In order to read them into memory,
# they need to be subsetted one year at a time while loading. (The dataset name
# is the file name without including the year.)
subset_aqs_dataset = function(name, sites, datadir, years = NULL) {
  zips = list.files(datadir, paste0(name, '.*\\.zip'), full.names = TRUE)
  if (!is.null(years)) {
    zip_years = as.integer(gsub('.*_|\\.zip', '', zips))
    zips = zips[zip_years %in% years]
  }
  dat_list = lapply(zips, function(x) {
    csv = basename(sub('\\.zip$', '.csv', x))
    subset(read_zipped_csv(x, csv, check.names = FALSE),
           format_aqs_site(`State Code`, `County Code`, `Site Num`) %in% sites)
  })
  do.call(rbind, dat_list)
}

# Make a param x year matrix of AQS data availability. Availability is
# determined using the yearly summary data, as suggested at
# https://aqs.epa.gov/aqsweb/documents/data_api.html#tips.
aqs_availability_matrix = function(site, years, datadir) {
  download_aqs('annual_conc_by_monitor', years, datadir, overwrite = FALSE)
  annual = subset_aqs_dataset('annual_conc_by_monitor', site, datadir, years)
  long = aggregate(`Observation Count` ~ `Parameter Code` + Year, data = annual,
                   FUN = sum)
  wide = reshape(long, v.names = 'Observation Count',
                 timevar = 'Parameter Code', idvar = 'Year', direction = 'wide')
  mat = as.matrix(wide[, -1])
  colnames(mat) = sub('Observation Count\\.', '', colnames(mat))
  row.names(mat) = wide$Year
  !is.na(mat) & mat > 0
}

# get all available samples for the given parameters, site, and years
#' @export
aqs_bulk_samples = function(params, datasets, site, years, datadir) {
  # add 'hourly' to the front because these are sample datasets
  datasets = paste0('hourly_', datasets)
  avail_mat = aqs_availability_matrix(site, years, datadir)
  years_avail = as.integer(row.names(avail_mat)[rowSums(avail_mat) > 0])
  if (!all(years %in% years_avail)) {
    years_str = paste(setdiff(years, years_avail), collapse = ', ')
    warning('Some years are missing data: ', years_str)
    years = years_avail
  }
  params_avail = colnames(avail_mat)[colSums(avail_mat) > 0]
  if (!all(params %in% params_avail)) {
    params_str = paste(setdiff(params, params_avail), collapse = ', ')
    warning('Some params are missing data: ', params_str)
    params = intersect(params, params_avail)
  }
  # for each dataset, extract the contained parameters
  res = list()
  for (ds in unique(datasets)) {
    message('Getting ', ds, ' samples')
    ds_params = params[datasets == ds]
    ds_avail = avail_mat[, ds_params, drop = FALSE]
    ds_years = row.names(ds_avail)[rowSums(ds_avail) > 0]
    download_aqs(ds, ds_years, datadir, overwrite = FALSE)
    res[[ds]] = subset_aqs_dataset(ds, site, datadir, ds_years)
  }
  do.call(rbind, unname(res))
}
