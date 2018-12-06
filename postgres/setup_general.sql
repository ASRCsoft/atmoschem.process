/* Creating tables used by multiple instruments */

create table autocals (
  station_id int references stations,
  instrument text,
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
