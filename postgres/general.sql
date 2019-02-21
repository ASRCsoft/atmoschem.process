/* Creating tables used by multiple instruments */

create extension hstore;
create extension btree_gist;
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

/* station table */
create table stations (
  id serial primary key,
  short_name text unique,
  long_name text unique
);
insert into stations(short_name, long_name)
values ('WFMS', 'Whiteface Mountain Summit'),
       ('WFML', 'Whiteface Mountain Lodge'),
       ('PSP', 'Pinnacle State Park'),
       ('QC', 'Queens College');


create table measurements (
  station_id int,
  measurement text,
  valid_range numrange,
  mdl numeric,
  span numeric,
  has_calibration boolean,
  remove_outliers boolean,
  primary key(station_id, measurement)
);
