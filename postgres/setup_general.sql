/* Creating tables used by multiple instruments */

create table clock_audits (
  instrument text,
  instrument_time timestamp,
  audit_time timestamp,
  corrected boolean,
  primary key(instrument, audit_time)
);
