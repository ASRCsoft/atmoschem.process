# Changelog

## [Unreleased]
### Changed
- Move raw data into PostgreSQL.
- Automate data processing.
- Correct for clock errors using linear interpolation rather than by
  discarding measurements with overlapping times.
- Ignore ultrafine low pulse height flag.
- Replace standard deviation-based outlier detection with median
  absolute deviation-based outlier detection.
- Use 3-minute moving averages for all calibration estimates.

### Fixed
- Fix bug ignoring ultrafine 'Nozzle Pressure' flag (400) instead of
  'Service Reminder' flag (4000).
- Account for zero offsets when estimating calibration spans.
- Correct miscalculated WFMS wind speeds from 2016 to 2019.
