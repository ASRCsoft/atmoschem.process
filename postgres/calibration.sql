/* Instrument calibration */

create or replace function estimate_zero_val(val text, times timerange, cal_date date, site int) returns numeric as $$
  declare
  exec_str text = $exec$
    with moving_averages as (
      select instrument_time,
	     AVG(%I) OVER(ORDER BY instrument_time
			  ROWS BETWEEN 2 PRECEDING AND 2 following) as moving_average
	from envidas
	       join autocals
		   on instrument_time::date=$2
	       and instrument_time::time <@ $1
	       and envidas.station_id=$3
    )
    select min(moving_average) from moving_averages;
  $exec$;
  zero_estimate numeric;
begin
  execute format(exec_str, val)
    into zero_estimate
    using times, cal_date, site;
  RETURN zero_estimate;
end;
$$ language plpgsql;
