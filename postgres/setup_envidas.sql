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
