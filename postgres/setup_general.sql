/* Creating tables used by multiple instruments */

create table autocals (
  station_id int references stations,
  instrument text,
  dates daterange,
  start_time time,
  end_time time,
  primary key(station_id, instrument, dates, start_time, end_time)
);

create table clock_audits (
  instrument text,
  instrument_time timestamp,
  audit_time timestamp,
  corrected boolean,
  primary key(instrument, audit_time)
);
