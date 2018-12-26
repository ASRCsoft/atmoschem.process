/* Creating tables used by multiple instruments */

/* station table */
create table stations (
  id serial primary key,
  short_name text,
  long_name text
);
insert into stations(short_name, long_name)
values ('WFMS', 'Whiteface Mountain Summit'),
       ('WFML', 'Whiteface Mountain Lodge'),
       ('PSP', 'Pinnacle State Park'),
       ('QC', 'Queens College');

create table autocals (
  station_id int references stations,
  instrument text,
  type text,
  dates daterange,
  times timerange,
  primary key(station_id, instrument, dates, times)
);

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
    type text,
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
	 type,
	 daterange(start_date, end_date, '[]'),
	 timerange(start_time, end_time, '[]')
    FROM autocals_file;
END;
$$ LANGUAGE plpgsql;
