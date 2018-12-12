/* Set up the tables and extensions for ultrafine data */

/* These are the error codes described in the TSI3783 manual, p. B-5
*/
create table ultrafine_codes (
  id serial primary key,
  error text
);
insert into ultrafine_codes(error)
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

create table ultrafine (
  station_id int references stations,
  source sourcerow,
  instrument_time timestamp,
  concentration numeric,
  count int,
  livetime numeric,
  pressure int,
  analog_voltage numeric,
  pulse_height int,
  pulse_std int,
  flags int[],
  primary key(station_id, source)
);
/* create index useful for subsetting based on recorded instrument
   time (often used for displaying the raw data) */
CREATE INDEX raw_ultrafine_time ON ultrafine(instrument_time);

/* A table of time corrections needed for when an instrument clock was
   set to the wrong time-- not the same as clock drift, which is
   addressed in the clock audit table */
create table ultrafine_time_corrections (
  station_id int references stations,
  start_file text,
  start_row int,
  end_file text,
  end_row int,
  time_offset interval,
  explanation text,
  FOREIGN KEY (station_id, start_file, start_row) REFERENCES ultrafine (station_id, file, row),
  FOREIGN KEY (station_id, end_file, end_row) REFERENCES ultrafine (station_id, file, row)
);
