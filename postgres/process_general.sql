/* Load autocal schedules into the autocals table */
CREATE OR REPLACE FUNCTION load_autocals(file text)
  RETURNS void AS $$
  declare
  copy_str text;
BEGIN
  /* this temporary table will hold a copy of the data file */
  create temporary table autocals_file (
    station text,
    instrument text,
    start_date date,
    end_date date,
    start_time time,
    end_time time
  ) on commit drop;
  select format('COPY autocals_file FROM ''%s'' delimiter '','' csv header',
		file)
    into copy_str;
  EXECUTE copy_str;
  INSERT INTO autocals
  SELECT (select id
	    from stations
	   where short_name=station),
	 instrument,
	 daterange(start_date, end_date, '[]'),
	 start_time,
	 end_time
    FROM autocals_file;
END;
$$ LANGUAGE plpgsql;
