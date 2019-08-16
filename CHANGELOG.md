# Changelog

## [Unreleased]
### Added
- Add 2019 1st and 2nd quarter data annotations.

## [0.1.2]
### Added
- Add WFML 2018 4th quarter data annotations.
- Add manual flags for calibrations.

### Changed
- Apply smoothing to zero and span calibration values.

### Fixed
- Convert NYS Mesonet data times to EST.

## [0.1.1] - 2019-06-10
### Added
- Add WFMS 2018 4th quarter data annotations.

### Changed
- Create separate minute files for each data source.
- Flag negative WFMS 370nm black carbon and wood smoke values as below
  the method detection limit.
- Treat zero WFMS 370nm and 880nm black carbon as missing data.

## [0.1.0] - 2019-05-31
### Added
- Move raw data into PostgreSQL.
- Automate data processing.
- Add PSP 2018 4th quarter data annotations.

### Changed
- Correct for clock errors using linear interpolation rather than by
  discarding measurements with overlapping times.
- Ignore ultrafine low pulse height flag.
- Replace standard deviation-based outlier detection with median
  absolute deviation-based outlier detection.
- Use 3-minute moving averages for all calibration estimates.
- Treat zero WindDir\_SD1\_WVT as an indicator of wind direction
  instrument malfunction.
- Apply M1 flag to hours with >50% missing data.
- Simplify data report format.

### Fixed
- Fix bug ignoring ultrafine 'Nozzle Pressure' flag (400) instead of
  'Service Reminder' flag (4000).
- Account for zero offsets when estimating calibration spans.
- Correct miscalculated WFMS wind speeds from 2016 to 2019.
- Fix PSP NO2 conversion efficiency calculations.
- Fix PSP solar radiation zero offset.
