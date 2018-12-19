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
	 cal_day,
	 cal_times,
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

/* After getting these calibration values, need to use linear interpolation to get zero/top for every measurement time, then correct each measurement */
