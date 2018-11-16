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
declare station_id int;
BEGIN
  SELECT id into station_id from stations where short_name=station;
  /* this temporary table will hold a copy of the data file */
  create temporary table ultrafine_file (
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
  -- the data file so it can be read as a csv
  EXECUTE format('COPY ultrafine_file FROM PROGRAM ''tail -n +6 "%s"'' delimiter '','' csv header;',
    		 file);
  INSERT INTO ultrafine
  SELECT station_id,
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
