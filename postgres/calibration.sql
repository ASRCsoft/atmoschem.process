/* Instrument calibration */

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

create or replace function estimate_min_val(val text, times timerange, cal_date date, site int) returns numeric as $$
  declare
  exec_str text = $exec$
    with moving_averages as (
      select AVG(%I) OVER(ORDER BY instrument_time
			  ROWS BETWEEN 2 PRECEDING AND 2 following) as moving_average
	from envidas
       where instrument_time between $1 and $2
	 and envidas.station_id=$3
    )
    select min(moving_average) from moving_averages;
  $exec$;
  start_time timestamp = cal_date + lower(times);
  end_time timestamp = cal_date + upper(times);
  zero_estimate numeric;
begin
  execute format(exec_str, val)
    into zero_estimate
    using start_time, end_time, site;
  RETURN zero_estimate;
end;
$$ language plpgsql;

create or replace function estimate_max_val(val text, times timerange, cal_date date, site int) returns numeric as $$
  declare
  exec_str text = $exec$
    with moving_averages as (
      select instrument_time,
	     AVG(%I) OVER(partition by station_id
			  ORDER BY instrument_time
			  ROWS BETWEEN 2 PRECEDING AND 2 following) as moving_average
	from envidas
       where instrument_time between ($1 - interval '2 minutes') and ($2 + interval '2 minutes')
	 and station_id=$3
    )
    select max(moving_average)
      from moving_averages
     where instrument_time between $1 and $2;
  $exec$;
  start_time timestamp = cal_date + lower(times);
  end_time timestamp = cal_date + upper(times);
  zero_estimate numeric;
begin
  execute format(exec_str, val)
    into zero_estimate
    using start_time, end_time, site;
  RETURN zero_estimate;
end;
$$ language plpgsql;

/* Get calibration estimates corresponding to the autocal periods */
CREATE MATERIALIZED VIEW calibration_values AS
  select station_id,
	 tsrange(cal_day + lower(cal_times),
		 cal_day + upper(cal_times),
		 '[]') as cal_times,
	 chemical,
	 type,
	 case
	 when type='zero' then estimate_min_val(lower(chemical), cal_times, cal_day, station_id)
	 -- ignore first 15 minutes of span calibration data due to spikes I think?
	 when type='span' then estimate_max_val(lower(chemical), timerange(lower(cal_times) + interval '15 minutes', upper(cal_times), '[]'), cal_day, station_id) end as value
    from (select station_id,
		 instrument as chemical,
		 type,
		 generate_series(lower(dates),
				 coalesce(upper(dates), CURRENT_DATE),
				 interval '1 day')::date as cal_day,
		 times as cal_times
	    from autocals) a1;
-- to make the interpolate_cal function faster
CREATE INDEX calibration_values_upper_time_idx ON calibration_values(upper(cal_times));
CREATE INDEX calibration_values_cal_times_gist_idx ON calibration_values using gist(station_id, chemical, cal_times);

/* Estimate calibration values using linear interpolation */
CREATE OR REPLACE FUNCTION interpolate_cal(station_id int, chemical text, type text, t timestamp)
  RETURNS numeric AS $$
  select interpolate(t0, t1, y0, y1, $4)
    from (select upper(cal_times) as t0,
		 value as y0
	    from calibration_values
	   where upper(cal_times)<=$4
	     and chemical=$2
	     and type=$3
	     and value is not null
	   order by upper(cal_times) desc
	   limit 1) calib0
	 full outer join
	 (select upper(cal_times) as t1,
		 value as y1
	    from calibration_values
	   where upper(cal_times)>$4
	     and chemical=$2
	     and type=$3
	     and value is not null
	   order by upper(cal_times) asc
	   limit 1) calib1
         on true;
$$ LANGUAGE sql STABLE PARALLEL SAFE;


CREATE OR REPLACE FUNCTION correct_no(station_id int, val numeric, t timestamp)
  RETURNS numeric AS $$
  DECLARE
  zero numeric = interpolate_cal(station_id, 'NO', 'zero', t);
  span numeric = interpolate_cal(station_id, 'NO', 'span', t);
  BEGIN
    return (val - zero) * 3.79 / (span - zero);
  END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION apply_calib(station_id int, measurement text, val numeric, t timestamp)
  RETURNS numeric AS $$
  DECLARE
  zero numeric = interpolate_cal(station_id, measurement, 'zero', t);
  span numeric = interpolate_cal(station_id, measurement, 'span', t);
  BEGIN
    return (val - zero) * 3.79 / (span - zero);
  END;
$$ LANGUAGE plpgsql STABLE PARALLEL SAFE;



-- just for testing
select avg(no)
  from (select station_id,
	       source,
	       corrected_time as time,
	       apply_calib(station_id, 'NO', no, corrected_time) as no,
	       data_dict->'no_flag' as no_flag
	  from (select *,
		       correct_instrument_time(station_id, 'DRDAS PC', source, instrument_time) as corrected_time
		  from envidas limit 100) e1) e2;

select station_id,
       source,
       corrected_time as time,
       apply_calib(station_id, 'NO', no, corrected_time) as no,
       interpolate_cal(station_id, 'NO', 'zero', corrected_time) as no_zero,
       data_dict->'no_flag' as no_flag
  from (select *,
	       correct_instrument_time(station_id, 'DRDAS PC', source, instrument_time) as corrected_time
	  from envidas limit 1) e1;
