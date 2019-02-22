create table wfms_aethelometer (
  site_id int references sites,
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
  primary key(site_id, source, channel)
);
