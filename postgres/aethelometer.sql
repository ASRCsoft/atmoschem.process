create table wfms_aethelometer (
  station_id int references stations,
  source sourcerow,
  instrument_time timestamp,
  channel int,
  concentration numeric,
  sz numeric,
  sb numeric,
  rz numeric,
  rb numeric,
  fraction numeric,
  attenuation numeric,
  primary key(station_id, source, channel)
);
