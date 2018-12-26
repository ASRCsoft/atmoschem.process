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

create table autocals (
  station_id int references stations,
  instrument text,
  type text,
  dates daterange,
  times timerange,
  primary key(station_id, instrument, dates, times)
);

create table clock_audits (
  instrument text,
  instrument_time timestamp,
  audit_time timestamp,
  corrected boolean,
  primary key(instrument, audit_time)
);
