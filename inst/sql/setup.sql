/* Creating tables used by multiple instruments */

create extension btree_gist;

/* site table */
create table sites (
  id serial primary key,
  short_name text unique,
  long_name text unique
);
insert into sites(short_name, long_name)
values ('WFMS', 'Whiteface Mountain Summit'),
       ('WFML', 'Whiteface Mountain Lodge'),
       ('PSP', 'Pinnacle State Park'),
       ('QC', 'Queens College');

create table data_sources (
  id serial primary key,
  site_id int references sites,
  name text,
  unique(site_id, name)
);

create table files (
  id serial primary key,
  data_source_id int references data_sources,
  name text not null,
  calibration boolean not null,
  unique(data_source_id, name, calibration)
);

create table measurement_types (
  id serial primary key,
  data_source_id int references data_sources,
  name text not null,
  apply_processing boolean,
  valid_range numrange,
  mdl numeric,
  span numeric,
  has_calibration boolean,
  remove_outliers boolean,
  max_jump numeric,
  apply_ce boolean,
  max_ce numeric,
  unique(data_source_id, name)
);

create table measurements (
  measurement_type_id int references measurement_types,
  instrument_time timestamp,
  record int,
  value numeric,
  flagged boolean,
  primary key(measurement_type_id, instrument_time)
);
