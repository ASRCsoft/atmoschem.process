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
  name text not null,
  unique(site_id, name)
);

create table files (
  id serial primary key,
  data_source_id int references data_sources,
  name text not null,
  calibration boolean not null,
  unique(data_source_id, name, calibration)
);

create table observations (
  id serial primary key,
  file_id int references files,
  line int not null,
  time timestamp not null,
  unique(file_id, line)
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
  observation_id int references observations,
  measurement_type_id int references measurement_types,
  value numeric,
  flagged boolean,
  primary key(observation_id, measurement_type_id)
);

-- other code will often want to see the measurements along with the
-- time
create or replace view measurements2 as
  select time as instrument_time,
	 m1.*
    from measurements m1
	   join observations o1
	       on m1.observation_id=o1.id;
