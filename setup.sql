/* Set up the tables and extensions for the atmoschem project database */

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

/* These are the error codes described in the TSI3783 manual, p. B-5
*/
create table tsi3783_codes (
  id serial primary key,
  error text
);
insert into tsi3783_codes(error)
values ('Conditioner Temperature'),
       ('Growth Tube Temperature'),
       ('Optics Temperature'),
       ('Vacuum Level'),
       (null), -- they skipped one!
       ('Laser Status'),
       ('Water Level'),
       ('Concentration Over-range'),
       ('Pulse Height Fault'),
       ('Absolute Pressure'),
       ('Nozzle Pressure'),
       ('Water Separator Temperature'),
       ('Warmup'),
       ('Reserved (0x2000)'),
       ('Service Reminder'),
       ('Reserved (0x8000)');

create table cpc (
  station_id int references stations,
  measure_time timestamp,
  concentration numeric,
  count int,
  livetime numeric,
  pressure int,
  analog_voltage numeric,
  pulse_height int,
  pulse_std int,
  flags int[],
  primary key(station_id, measure_time)
);
