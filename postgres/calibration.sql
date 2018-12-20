/* Instrument calibration */

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

/* Estimate calibration values using linear interpolation */
CREATE OR REPLACE FUNCTION interpolate_cal(station_id int, _chemical text, type text, t timestamp)
  RETURNS numeric AS $$
  #variable_conflict use_variable
  DECLARE
  t0 timestamp;
  t1 timestamp;
  y0 numeric;
  y1 numeric;
  BEGIN
    select upper(cal_times),
	   value
      from calibration_values
     where upper(cal_times)<=t
       and calibration_values.chemical=_chemical
       and calibration_values.type=type
     order by upper(cal_times) desc
     limit 1
      into t0, y0;

    select upper(cal_times),
	   value
      from calibration_values
     where upper(cal_times)>t
       and calibration_values.chemical=_chemical
       and calibration_values.type=type
     order by upper(cal_times) asc
     limit 1
      into t1, y1;

    return interpolate(t0, t1, y0, y1, t);
  END;
$$ LANGUAGE plpgsql;
