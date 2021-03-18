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

# Efficiently subset large AQS zipped csv's by site, without using large amounts
# of RAM
subset_zipped_csv = function(z, f, sites, ...) {
  # Strategy: read chunks from zipped file, apply strsplit with `fixed` which is
  # very fast, and also startsWith instead of grep (also really fast)
  zfile = unz(z, f, 'rb')
  on.exit(close(zfile))
  site_txt = paste0('"', gsub('-', '","', sites), '"')
  contains_site = function(x) {
    if (length(x) == 1) {
      any(startsWith(x, site_txt))
    } else {
      rowSums(sapply(site_txt, function(y) startsWith(x, y))) > 0
    }
  }
  mb100 = 20 * 2 ^ 20 # 20MB. No particular reason
  # `readChar`, unlike `readLines`, does not require file seeking, which the unz
  # connection can't do
  chunk = readChar(zfile, mb100, useBytes = TRUE)
  last_line = ''
  res = list()
  i = 1
  while(length(chunk)) {
    lines = strsplit(chunk, '\n', fixed = TRUE)[[1]]
    # get the header
    if (i == 1) {
      res[[1]] = lines[1]
      i = 2
    }
    # piece together the new first line, adding the ending line fragment from
    # the previous chunk
    lines[1] = paste0(last_line, lines[1])
    last_line = tail(lines, 1)
    lines = head(lines, -1)
    res[[i]] = lines[contains_site(lines)]
    i = i + 1
    chunk = readChar(zfile, mb100, useBytes = TRUE)
  }
  if (contains_site(last_line)) res[[i]] = last_line
  read.csv(text = unlist(res), check.names = FALSE)
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
    subset_zipped_csv(x, csv, sites)
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

# `limit_rate` controls the time between the beginning of one request and the
# beginning of the next, but AQS wants us to put 5 seconds between the *end* of
# a request and the beginning of the next. This is kind of a silly workaround to
# make that happen.
aqs_5sec1 = ratelimitr::limit_rate(function() {}, rate(1, period = 5))
aqs_5sec2 = ratelimitr::limit_rate(function() {}, rate(1, period = 5))
aqs_5sec_toggle = TRUE
aqs_wait_5sec = function() if (aqs_5sec_toggle) aqs_5sec1() else aqs_5sec2()
aqs_restart_5sec = function() {
  # flip the toggle to switch timers
  aqs_5sec_toggle <<- !aqs_5sec_toggle
  aqs_wait_5sec()
}
.aqs_request = function(x) {
  aqs_wait_5sec()
  res = jsonlite::fromJSON(URLencode(x))
  aqs_restart_5sec()
  res
}

# send a request to the AQS data mart, following the API terms:
# https://aqs.epa.gov/aqsweb/documents/data_api.html#terms
# and caching results to avoid needlessly repeating requests
aqs_request =
  ratelimitr::limit_rate(.aqs_request, ratelimitr::rate(n = 10, period = 60))
