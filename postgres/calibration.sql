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
      select AVG(%I) OVER(ORDER BY instrument_time
			  ROWS BETWEEN 2 PRECEDING AND 2 following) as moving_average
	from envidas
       where instrument_time between $1 and $2
	 and envidas.station_id=$3
    )
    select max(moving_average) from moving_averages;
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

/* After getting these calibration values, need to use linear interpolation to get zero/top for every measurement time, then correct each measurement */

/* Calibration table:
- day
- timerange
- chemical
- type
- value
*/

/* Match ultrafine clock audits with the corresponding file/row from
   the raw data */
CREATE MATERIALIZED VIEW calibration_values as
  select cal_day,
  cal_times,
  chemical,
  type,
  estimate_min_val(val, times, cal_day, site) as value
	 -- (select file
	 --    from ultrafine
	 --   where station_id=3
	 --     and date_trunc('minute', ultrafine.instrument_time)=clock_audits.instrument_time
	 --   order by file, row asc
	 --   limit 1) as ultrafine_file,
	 -- (select row
	 --    from ultrafine
	 --   where station_id=3
	 --     and date_trunc('minute', ultrafine.instrument_time)=clock_audits.instrument_time
	 --   order by file, row asc
	 --   limit 1) as ultrafine_row
    from clock_audits
  where instrument='EPC';
