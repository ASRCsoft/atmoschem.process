/* Set up the tables and functions for ultrafine data */

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
  file_n int := REGEXP_REPLACE(file_name, '.*\((.*)\)|^[0-9]{6}',
			       '\1');
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
  select format('COPY ultrafine_file FROM PROGRAM %s delimiter '','' csv header',
		quote_literal(bash_str))
    into copy_str;
  EXECUTE copy_str;
  INSERT INTO ultrafine
  SELECT station_id,
	 (substring(file_name, 1, 6), file_n, row)::sourcerow,
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
