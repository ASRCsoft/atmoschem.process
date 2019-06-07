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
CREATE INDEX observations_time_idx ON observations(time);

create table clock_audits (
  data_source_id int references data_sources,
  data_source_time timestamp,
  audit_time timestamp,
  corrected boolean,
  primary key(data_source_id, audit_time)
);

create table measurement_types (
  id serial primary key,
  data_source_id int references data_sources,
  name text not null,
  units text,
  report_decimals int,
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
  select time,
	 m1.*
    from measurements m1
	   join observations o1
	       on m1.observation_id=o1.id;

/* A few helpful ID-finding functions */
drop function if exists get_data_source_ids cascade;
CREATE OR REPLACE FUNCTION get_data_source_ids(int, text)
  RETURNS int[] as $$
  select array_agg(mt.id)
    from measurement_types mt
	   join data_sources ds
	   on mt.data_source_id=ds.id
   where site_id=$1
     and ds.name=$2;
$$ language sql STABLE PARALLEL SAFE;

drop function if exists get_measurement_id cascade;
CREATE OR REPLACE FUNCTION get_measurement_id(int, text, text)
  RETURNS int as $$
  select mt.id
    from measurement_types mt
	   join data_sources ds
	       on mt.data_source_id=ds.id
   where site_id=$1
     and ds.name=$2
     and mt.name=$3;
$$ language sql STABLE PARALLEL SAFE;
