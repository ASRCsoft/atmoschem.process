/* Process ultrafine data files */

/* Take the hex flags value, convert it to binary, and return an array
of the set flags */
CREATE OR REPLACE FUNCTION parse_flags(hex_str varchar(4))
  RETURNS int[] AS $flags$
  DECLARE
  flag_bin bit(16) := ('x' || LPAD(hex_str, 4, '0'))::bit(16);
  flags int[] := ARRAY[]::int[];
BEGIN
  FOR i IN 1..16 LOOP
    IF get_bit(flag_bin, 16 - i) THEN
      SELECT flags || i into flags;
    END IF;
  END LOOP;
  RETURN flags;
END;
$flags$ LANGUAGE plpgsql;

/* Load a ultrafine data file into the ultrafine table */
CREATE OR REPLACE FUNCTION load_ultrafine(station text, file text) RETURNS void AS $$
  declare
  station_id int;
  bash_str text;
  copy_str text;
  -- remove file path and extension, so we're looking at just the file
  -- name
  file_name text := REGEXP_REPLACE(REGEXP_REPLACE(file, '^.*/', ''),
				   '\.[^.]*$', '');
BEGIN
  SELECT id into station_id from stations where short_name=station;
  /* this temporary table will hold a copy of the data file */
  create temporary table ultrafine_file (
    row int,
    date date,
    time time,
    concentration numeric,
    count int,
    livetime numeric,
    blank text,
    pressure int,
    analog_voltage numeric,
    pulse_height int,
    pulse_std int,
    flags varchar(4)
  ) on commit drop;
  -- use the `tail` terminal command to ignore the first 5 lines of
  -- the data file so it can be copied as a csv, and then use awk to
  -- add row numbers
  select format('tail -n +6 "%s" | awk -vOFS="," ''NR == 1 {print "row", $0; next}{print (NR-1), $0}''',
      		file)
    into bash_str;
  select format('COPY ultrafine_file FROM PROGRAM ''%s'' delimiter '','' csv header',
		replace(bash_str, '''', ''''''))
    into copy_str;
  EXECUTE copy_str;
  INSERT INTO ultrafine
  SELECT station_id,
	 file_name,
	 row,
  	 date + time,
	 concentration,
	 count,
	 livetime,
	 pressure,
	 analog_voltage,
	 pulse_height,
	 pulse_std,
	 case when flags='0' then null else parse_flags(flags) end
  FROM ultrafine_file;
END;
$$ LANGUAGE plpgsql;

/* Get the appropriate NARSTO flag based on the number of ultrafine
   measurements in an hour and average concentration */
CREATE OR REPLACE FUNCTION ultrafine_narsto_flag(concentration numeric, n int) RETURNS text AS $$
  SELECT case when n=0 then 'M1'
	 when n<45 then 'V4'
	 when concentration<1 then 'V1'
	 else 'V0' end;
$$ LANGUAGE sql;

/* Match ultrafine clock audits with the corresponding file/row from
   the raw data */
CREATE MATERIALIZED VIEW ultrafine_clock_audits as
  select *,
	 (select file
	    from ultrafine
	   where station_id=3
	     and date_trunc('minute', ultrafine.instrument_time)=clock_audits.instrument_time
	   order by file, row asc
	   limit 1) as ultrafine_file,
	 (select row
	    from ultrafine
	   where station_id=3
	     and date_trunc('minute', ultrafine.instrument_time)=clock_audits.instrument_time
	   order by file, row asc
	   limit 1) as ultrafine_row
    from clock_audits
   where instrument='EPC';


/* Correct ultrafine instrument time using linear interpolation */
CREATE OR REPLACE FUNCTION correct_ultrafine_time(file text, r int, t timestamp)
  RETURNS timestamp AS $corrected_time$
  DECLARE
  matches_audit_time boolean;
  has_lower_bound boolean;
  has_upper_bound boolean;
  x0 timestamp;
  x1 timestamp;
  y0 interval;
  y1 interval;
  interpolated_error interval;
BEGIN
  select exists(select *
		  from ultrafine_clock_audits
		 where ultrafine_clock_audits.ultrafine_file=file
		   and ultrafine_clock_audits.ultrafine_row=r)
    into matches_audit_time;
  if matches_audit_time then
    return (select audit_time
	      from ultrafine_clock_audits
	     where ultrafine_clock_audits.ultrafine_file=file
	       and ultrafine_clock_audits.ultrafine_row=r);
  end if;
  select exists(select *
		  from ultrafine_clock_audits
		 where ultrafine_clock_audits.ultrafine_file<file
		    or (ultrafine_clock_audits.ultrafine_file=file
			and ultrafine_clock_audits.ultrafine_row<r))
    into has_lower_bound;
  select exists(select *
		  from ultrafine_clock_audits
		 where ultrafine_clock_audits.ultrafine_file>file
		    or (ultrafine_clock_audits.ultrafine_file=file
			and ultrafine_clock_audits.ultrafine_row>r))
    into has_upper_bound;
  if has_lower_bound then
    -- if the clock error was corrected, then the new instrument_time
    -- == audit_time, and if it wasn't corrected then the new
    -- instrument_time == original instrument_time
    select case when corrected then audit_time
           else instrument_time end
      from ultrafine_clock_audits
     where ultrafine_clock_audits.ultrafine_file<file
	or (ultrafine_clock_audits.ultrafine_file=file
	    and ultrafine_clock_audits.ultrafine_row<r)
     order by ultrafine_file desc, ultrafine_row desc
     limit 1
      into x0;
    select case when corrected then '0'
	   else audit_time - instrument_time end
      from ultrafine_clock_audits
     where ultrafine_clock_audits.ultrafine_file<file
	or (ultrafine_clock_audits.ultrafine_file=file
	    and ultrafine_clock_audits.ultrafine_row<r)
     order by ultrafine_file desc, ultrafine_row desc
     limit 1
      into y0;
  end if;
  if has_upper_bound then
    select instrument_time
      from ultrafine_clock_audits
     where ultrafine_clock_audits.ultrafine_file>file
	or (ultrafine_clock_audits.ultrafine_file=file
	    and ultrafine_clock_audits.ultrafine_row>r)
     order by ultrafine_file asc, ultrafine_row asc
     limit 1
      into x1;
    select audit_time - instrument_time
      from ultrafine_clock_audits
     where ultrafine_clock_audits.ultrafine_file>file
	or (ultrafine_clock_audits.ultrafine_file=file
	    and ultrafine_clock_audits.ultrafine_row>r)
     order by ultrafine_file asc, ultrafine_row asc
     limit 1
      into y1;
  end if;
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
$corrected_time$ LANGUAGE plpgsql;

/* View the processed ultrafine data */
CREATE MATERIALIZED VIEW processed_ultrafine as
  select *,
	 ultrafine_narsto_flag(concentration, count::int) as narsto_flag
    from (select station_id,
		 date_trunc('hour', corrected_time) as time,
		 avg(concentration) as concentration,
		 avg(pulse_height) as pulse_height,
		 count(*)
	    from (select station_id,
			 case when station_id=3 then correct_ultrafine_time(file, row, instrument_time) 
			 else instrument_time end as corrected_time,
			 concentration,
			 pulse_height
		    from ultrafine where flags is null or flags='{15}') u1
	   group by station_id, time) u2;
