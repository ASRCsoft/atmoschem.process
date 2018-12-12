/* Organizing various data contained in envidas files */

create extension hstore;

create table envidas (
  station_id int references stations,
  file text,
  row int,
  instrument_time timestamp,
  temperature numeric,
  rh numeric,
  bp numeric,
  no numeric,
  co numeric,
  so2 numeric,
  data_dict hstore,
  primary key(station_id, file, row)
);
/* create index useful for subsetting based on recorded instrument
   time (often used for displaying the raw data) */
CREATE INDEX raw_envidas_time ON envidas(instrument_time);
