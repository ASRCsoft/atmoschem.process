/* Organizing various data contained in envidas files */

create table envidas (
  site_id int references sites not null,
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
/* create index useful for subsetting based on site and recorded
   instrument time (used when plotting the raw data and when running
   processing queries) */
CREATE INDEX envidas_site_raw_time ON envidas(site_id, instrument_time);
