# Changelog

## [Unreleased]
### Changed
- Move raw data into PostgreSQL.
- Automate data processing.
- Correct for clock errors using linear interpolation rather than by
  discarding measurements with overlapping times.

### Fixed
- Fix bug ignoring ultrafine 'Nozzle Pressure' flag (400) instead of
  'Service Reminder' flag (4000).
