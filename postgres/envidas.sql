/* Organizing various data contained in envidas files */

create extension hstore;
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

create table envidas (
  station_id int references stations not null,
  source sourcerow not null,
  instrument_time timestamp not null,
  temperature numeric,
  rh numeric,
  bp numeric,
  no numeric,
  co numeric,
  so2 numeric,
  data_dict hstore
);
SELECT create_hypertable('envidas', 'instrument_time');
/* create index useful for subsetting based on station and recorded
   instrument time (used when plotting the raw data and when running
   processing queries) */
CREATE INDEX envidas_station_raw_time ON envidas(station_id, instrument_time);
