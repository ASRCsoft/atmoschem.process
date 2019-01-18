/* Creating tables used by multiple instruments */

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

/* Method detection limits */
create table mdls (
  station_id int references stations,
  measurement text,
  mdl numeric,
  primary key(station_id, measurement)
);
-- not quite, but something like this:
-- COPY mdls
--   FROM '/home/wmay/data/metadata/mdl.csv' DELIMITER ',' CSV HEADER;
