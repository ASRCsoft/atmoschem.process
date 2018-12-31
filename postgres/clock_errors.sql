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
create table ultrafine_time_corrections (
  station_id int references stations,
  start_file text,
  start_row int,
  end_file text,
  end_row int,
  time_offset interval,
  explanation text,
  FOREIGN KEY (station_id, start_row) REFERENCES ultrafine (station_id, source),
  FOREIGN KEY (station_id, end_row) REFERENCES ultrafine (station_id, source)
);

/* Match ultrafine clock audits with the corresponding file/row from
   the raw data */
CREATE MATERIALIZED VIEW ultrafine_clock_audits as
  select *,
	 (select source
	    from ultrafine
	   where station_id=3
	     and date_trunc('minute', ultrafine.instrument_time)=clock_audits.instrument_time
	   order by source asc
	   limit 1) as ultrafine_sourcerow
    from clock_audits
   where instrument='EPC';


/* Correct ultrafine instrument time using linear interpolation */
CREATE OR REPLACE FUNCTION correct_ultrafine_time(file text, r int, t timestamp)
  RETURNS timestamp AS $$
  DECLARE
  matching_audit_time timestamp;
  has_lower_bound boolean;
  has_upper_bound boolean;
  x0 timestamp;
  x1 timestamp;
  y0 interval;
  y1 interval;
BEGIN
  -- 1) Find the closest clock audits.
  select audit_time
    from ultrafine_clock_audits
   where ultrafine_clock_audits.ultrafine_file=file
     and ultrafine_clock_audits.ultrafine_row=r
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
   where ultrafine_clock_audits.ultrafine_file<file
      or (ultrafine_clock_audits.ultrafine_file=file
    	  and ultrafine_clock_audits.ultrafine_row<r)
   order by ultrafine_file desc, ultrafine_row desc
   limit 1
    into x0, y0;
  if found then
    select true
      into has_lower_bound;
  end if;
  select instrument_time,
    	 audit_time - instrument_time
    from ultrafine_clock_audits
   where ultrafine_clock_audits.ultrafine_file>file
      or (ultrafine_clock_audits.ultrafine_file=file
    	  and ultrafine_clock_audits.ultrafine_row>r)
   order by ultrafine_file asc, ultrafine_row asc
   limit 1
    into x1, y1;
  if found then
    select true
    into has_upper_bound;
  end if;
  -- 2) Return the appropriate estimate of the clock error.
  if has_lower_bound and has_upper_bound then
    -- interpolate
    return t + (y0 + extract('epoch' from (t - x0)) * (y1 - y0) /
		extract('epoch' from (x1 - x0)));
  elsif has_lower_bound then
    -- go with the most recent audit
    return t + y0;
  elsif has_upper_bound then
    -- go with the next audit
    return t + y1;
  else
    -- return the original time
    return t;
  end if;
END;
$$ LANGUAGE plpgsql;
