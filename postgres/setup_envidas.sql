/* Organizing various data contained in envidas files */

create table envidas_common (
  station_id int references stations,
  file text,
  row int,
  instrument_time timestamp,
  temperature numeric,
  rh numeric,
  no numeric,
  no2 numeric,
  o3 numeric,
  co numeric,
  so2 numeric,
  primary key(station_id, file, row)
);

create table envidas_wfms (
  temp_rh_flag numeric,
  wind_flag numeric,
  ptemp numeric,
  rain numeric,
  co_flag numeric,
  so2_flag numeric,
  nox_flag numeric,
  noy numeric,
  noy_flag numeric,
  primary key(station_id, file, row)
) INHERITS (envidas_common);

-- create table envidas_wfml (
--   ptemp numeric,
--   ptemp_flag numeric,
--   methane numeric,
--   methane_flag numeric,
--   nmhc numeric,
--   nmhc_flag numeric,
--   pm25 numeric,
--   pm25_flag numeric,
--   ngn3 numeric,
--   ngn3_flag numeric
-- ) INHERITS (envidas_common);

-- create table envidas_psp (
--   rain numeric,
--   rain_flag text,
--   noy numeric,
--   noy_flag text,
--   methane numeric,
--   methane_flag text,
--   nmhc numeric,
--   nmhc_flag text,
--   pm25 numeric,
--   pm25_flag text,
--   pm25c numeric,
--   pm25c_flag text,
--   ngn3 numeric,
--   ngn3_flag text,
--   thc numeric,
--   thc_flag text,
-- ) INHERITS (envidas_common);
