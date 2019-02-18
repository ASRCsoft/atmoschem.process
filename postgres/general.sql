/* Creating tables used by multiple instruments */

create extension hstore;
create extension btree_gist;
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

/* station table */
create table stations (
  id serial primary key,
  short_name text,
  long_name text
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
  primary key(station_id, measurement)
);
