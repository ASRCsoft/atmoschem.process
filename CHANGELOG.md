# Changelog

## [Unreleased]
### Added
- Add 2020 2nd quarter WFMS, WFML, and PSP data flags.

## [0.4.0] - 2020-07-01
### Added
- Add 2020 1st quarter WFMS, WFML, and PSP data flags.
- Add site information file.
- Add Addison Mesonet winds to PSP data.

### Fixed
- Remove incorrect WFMS wind speed maximums.

## [0.3.0] - 2020-05-11
### Added
- Add 2019 4th quarter WFMS, WFML, and PSP data flags.
- Add code for cleaning original processed routine data.
- Add Makefile and scripts for reproducing the routine chemistry dataset
  package.

### Changed
- Prioritize V4 over V1 flags when both apply.
- Replace NA flag values with M1.
- Flag suspicious 1999-2001 WFMS CO drop as V4.
- Create unified dataset package instead of yearly data files.
- Reprocess all data starting from 2018-10-01.
- Set R package version number to match dataset version.

### Fixed
- Flag data and ignore calibrations during WFMS 300EU auto-reference (AREF)
  periods.
- Correct a variety of WFMS and PSP wind direction issues in both the current
  and previously processed data.
- Replace zeros with NA during PSP black carbon tape changes.

## [0.1.3] - 2020-01-23
### Added
- Add 2019 1st, 2nd, and 3rd quarter data annotations.
- Generate instrument info file with datasets.
- Interpolate calibrator instrument flow values.
- Add PSP aethelometer method detection limits.

### Changed
- Simplify and standardize high time resolution dataset files.
- Improve PSP conversion efficiency calculations.
- Improve outlier detection in the presence of missing or flagged
  data.
- Flag more data at the end of auto-calibrations.
- Adjust WFMS perm tube calibration values based on GPT and IPN tests.
- Use unicode sub- and superscripts in generated dataset column names.
- Apply spike filter to WFMS ultrafine data.

## [0.1.2] - 2019-08-16
### Added
- Add WFML 2018 4th quarter data annotations.
- Add manual flags for calibrations.
- Break calibration smoothing at instrument adjustment times.

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
