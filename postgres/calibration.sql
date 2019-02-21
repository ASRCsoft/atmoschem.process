/* Instrument calibration */

create table autocals (
  station_id int references stations,
  instrument text,
  type text,
  dates daterange,
  times timerange,
  value numeric default 0,
  primary key(station_id, instrument, dates, times)
);

create table manual_calibrations (
  station_id int references stations,
  instrument text,
  type text,
  cal_time timestamp,
  measured_value numeric,
  corrected boolean not null,
  primary key(station_id, instrument, cal_time)
);

create or replace function estimate_cal(station int, val text, cal_type text, cal_times tsrange) returns numeric as $$
select case when cal_type='zero' then min(moving_average)
       else max(moving_average) end
  from (select instrument_time,
	       AVG(value) OVER(partition by station_id
			       ORDER BY instrument_time
			       ROWS BETWEEN 1 PRECEDING AND 1 following) as moving_average
	  from campbell
	 where station_id=station
	   and instrument_time between (lower(cal_times) - interval '2 minutes') and (upper(cal_times) + interval '2 minutes')) moving_averages
  -- ignore first 15 minutes of span calibration data due to spikes I
  -- think?
  where instrument_time <@ case when cal_type='zero' then cal_times
			   else tsrange(lower(cal_times) + interval '15 minutes', upper(cal_times)) end;
$$ language sql STABLE PARALLEL SAFE;

create or replace view calibration_periods as
  select station_id,
	 chemical,
	 type,
	 tsrange(cal_day + lower(cal_times),
		 cal_day + upper(cal_times),
		 '[]') as cal_times
    from (select station_id,
		 instrument as chemical,
		 type,
		 generate_series(lower(dates),
				 coalesce(upper(dates), CURRENT_DATE),
				 interval '1 day')::date as cal_day,
		 times as cal_times
	    from autocals) a1;

/* Store calibration estimates derived from the raw instrument values
 */
CREATE MATERIALIZED VIEW calibration_values AS
  select *,
	 estimate_cal(station_id, chemical, type, cal_times) as value
    from calibration_periods
   where type is not null;
-- to make the interpolate_cal function faster
CREATE INDEX calibration_values_upper_time_idx ON calibration_values(upper(cal_times));
CREATE INDEX calibration_values_gist_idx ON calibration_values using gist(station_id, chemical, cal_times);

/* Estimate calibration values using linear interpolation */
CREATE OR REPLACE FUNCTION interpolate_cal(station_id int, chemical text, type text, t timestamp)
  RETURNS numeric AS $$
  select interpolate(t0, t1, y0, y1, $4)
    from (select upper(cal_times) as t0,
		 value as y0
	    from calibration_values
	   where upper(cal_times)<=$4
	     and station_id=$1
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
	     and station_id=$1
	     and chemical=$2
	     and type=$3
	     and value is not null
	   order by upper(cal_times) asc
	   limit 1) calib1
         on true;
$$ LANGUAGE sql STABLE PARALLEL SAFE;


CREATE OR REPLACE FUNCTION apply_calib(station_id int, measurement text, val numeric, t timestamp)
  RETURNS numeric AS $$
  DECLARE
  zero numeric = interpolate_cal(station_id, measurement, 'zero', t);
  span numeric = interpolate_cal(station_id, measurement, 'span', t);
  BEGIN
    return case when span is null then val - zero
      else (val - zero) / (span - zero) * (select m.span
					     from measurements m
					    where m.station_id=$1
					      and m.measurement=$2) end;
  END;
$$ LANGUAGE plpgsql STABLE PARALLEL SAFE;
-- for whatever reason this is faster as a plpgsql function

-- CREATE OR REPLACE FUNCTION apply_calib(station_id int, measurement text, val numeric, t timestamp)
--   RETURNS numeric AS $$
--   select case when span is null then val - zero
-- 	 else (val - zero) / (span - zero) *
-- 	   (select m.span
-- 	      from measurements m
-- 	     where m.station_id=$1
-- 	       and m.measurement=$2) end
--   from (select interpolate_cal(station_id, measurement, 'zero', t) as zero,
-- 	       interpolate_cal(station_id, measurement, 'span', t) as span) as cals;
-- $$ LANGUAGE sql STABLE PARALLEL SAFE;



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
