/* Tables and functions for correcting instrument clock errors. */

create table clock_audits (
  instrument text,
  instrument_time timestamp,
  audit_time timestamp,
  corrected boolean,
  primary key(instrument, audit_time)
);
-- to read in clock audits from csv file:
-- COPY clock_audits
--  FROM '/home/wmay/data/metadata/clock_audits.csv' DELIMITER ',' CSV HEADER;

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

/* Match clock audits with the corresponding file/row from the raw
   data */
CREATE MATERIALIZED VIEW matched_clock_audits as
  select *,
	 case when instrument='EPC'
	 then (select min(source)
		 from ultrafine
		where station_id=3
		  and date_trunc('minute', ultrafine.instrument_time)=clock_audits.instrument_time)
	 else (select min(source)
		 from envidas
		where station_id=3
		  and date_trunc('minute', envidas.instrument_time)=clock_audits.instrument_time) end as data_source
    from clock_audits
   where instrument in ('EPC', 'DRDAS PC');


/* Correct ultrafine instrument time using linear interpolation */
CREATE OR REPLACE FUNCTION correct_instrument_time(station_id int, instrument text, sr sourcerow, t timestamp)
  RETURNS timestamp AS $$
  #variable_conflict use_variable
  DECLARE
  matching_audit_time timestamp;
  x0 timestamp;
  x1 timestamp;
  y0 interval;
  y1 interval;
  BEGIN
    -- only applies to PSP for now
    if station_id!=3 then
      return t;
    end if;
    -- 1) Find the closest clock audits.
    select audit_time
      from matched_clock_audits
     where matched_clock_audits.instrument=instrument
       and data_source=sr
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
      from matched_clock_audits
     where matched_clock_audits.instrument=instrument
       and data_source<sr
     order by data_source desc
     limit 1
      into x0, y0;
    select instrument_time,
	   audit_time - instrument_time
      from matched_clock_audits
     where matched_clock_audits.instrument=instrument
       and data_source>sr
     order by data_source asc
     limit 1
      into x1, y1;
    return t + interpolate(x0, x1, y0, y1, t);
  END;
$$ LANGUAGE plpgsql STABLE PARALLEL SAFE;
