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

create table measurement_types (
  id serial primary key,
  data_source_id int references data_sources,
  name text not null,
  units text,
  derived boolean,
  report_decimals int,
  apply_processing boolean,
  valid_range numrange,
  mdl numeric,
  span numeric,
  has_calibration boolean,
  zero_smooth_window int,
  span_smooth_window int,
  remove_outliers boolean,
  spike_window int,
  spike_log_transform boolean,
  max_jump numeric,
  apply_ce boolean,
  max_ce numeric,
  ce_smooth_window int,
  gilibrator_span text,
  gilibrator_ce text,
  unique(data_source_id, name)
);

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
