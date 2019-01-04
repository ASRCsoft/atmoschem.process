/* Tables and functions for correcting instrument clock errors. */

create table clock_audits (
  instrument text,
  instrument_time timestamp,
  audit_time timestamp,
  corrected boolean,
  primary key(instrument, audit_time)
);

/* A table of time corrections needed for when an instrument clock was
   set to the wrong time-- not the same as clock drift, which is
   addressed in the clock audit table */
create table time_corrections (
  station_id int references stations,
  chemical text,
  start_row int,
  end_row int,
  time_offset interval,
  explanation text
);

/* Match ultrafine clock audits with the corresponding file/row from
   the raw data */
CREATE MATERIALIZED VIEW ultrafine_clock_audits as
  select *,
	 (select min(source)
	    from ultrafine
	   where station_id=3
	     and date_trunc('minute', ultrafine.instrument_time)=clock_audits.instrument_time) as ultrafine_sourcerow
    from clock_audits
   where instrument='EPC';


/* Correct ultrafine instrument time using linear interpolation */
CREATE OR REPLACE FUNCTION correct_ultrafine_time(sr sourcerow, t timestamp)
  RETURNS timestamp AS $$
  DECLARE
  matching_audit_time timestamp;
  x0 timestamp;
  x1 timestamp;
  y0 interval;
  y1 interval;
BEGIN
  -- 1) Find the closest clock audits.
  select audit_time
    from ultrafine_clock_audits
   where ultrafine_clock_audits.ultrafine_sourcerow=sr
    into matching_audit_time;
  if found then
    return matching_audit_time;
  end if;
  -- if the clock error was corrected, then the new instrument_time is
  -- the same as the audit_time and the new clock error is zero. If it
  -- wasn't corrected then the instrument_time and clock errors don't
  -- change.
  select case when corrected then audit_time
         else instrument_time end,
         case when corrected then '0'
         else audit_time - instrument_time end
    from ultrafine_clock_audits
   where ultrafine_clock_audits.ultrafine_sourcerow<sr
   order by ultrafine_sourcerow desc
   limit 1
    into x0, y0;
  select instrument_time,
    	 audit_time - instrument_time
    from ultrafine_clock_audits
   where ultrafine_clock_audits.ultrafine_sourcerow>sr
   order by ultrafine_sourcerow asc
   limit 1
    into x1, y1;
  return t + interpolate(x0, x1, y0, y1, t);
END;
$$ LANGUAGE plpgsql;
