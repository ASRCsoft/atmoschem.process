/* Process cpc data files */

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

/* Load a cpc data file into the cpc table */
CREATE OR REPLACE FUNCTION load_cpc(file text) RETURNS void AS $$
BEGIN
  /* this temporary table will hold a copy of the data file */
  create temporary table cpc_file (
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
  EXECUTE format('COPY cpc_file FROM PROGRAM ''tail -n +6 "%s"'' delimiter '','' csv header;',
    		 file);
  INSERT INTO cpc
  SELECT 1,
  	 date + time,
	 concentration,
	 count,
	 livetime,
	 pressure,
	 analog_voltage,
	 pulse_height,
	 pulse_std,
	 case when flags='0' then null else parse_flags(flags) end
  FROM cpc_file;
END;
$$ LANGUAGE plpgsql;
