/* Instrument calibration */

create extension btree_gist;

create table autocals (
  station_id int references stations,
  instrument text,
  type text,
  dates daterange,
  times timerange,
  value numeric default 0,
  primary key(station_id, instrument, dates, times)
);

create or replace function estimate_cal(tbl text, val text, caltype text, times tsrange) returns numeric as $$
  declare
  exec_str text = $exec$
    with moving_averages as (
      select instrument_time,
	     AVG(%I) OVER(ORDER BY instrument_time
			  ROWS BETWEEN 2 PRECEDING AND 2 following) as moving_average
	from %I
       where instrument_time between ($1 - interval '2 minutes') and ($2 + interval '2 minutes')
    )
    select %I(moving_average)
    from moving_averages
    where instrument_time between $1 and $2;
  $exec$;
  zero_estimate numeric;
  begin
    execute format(exec_str, val, tbl,
		   case when caltype='zero' then 'min'
		   when caltype='span' then 'max' end)
      into zero_estimate
      using lower(times), upper(times);
    RETURN zero_estimate;
  end;
$$ language plpgsql STABLE PARALLEL SAFE;

create or replace function estimate_cal(tbl text, station int, val text, caltype text, times tsrange) returns numeric as $$
  declare
  exec_str text = $exec$
    with moving_averages as (
      select instrument_time,
	     AVG(%I) OVER(partition by station_id
			  ORDER BY instrument_time
			  ROWS BETWEEN 2 PRECEDING AND 2 following) as moving_average
	from %I
       where instrument_time between ($1 - interval '2 minutes') and ($2 + interval '2 minutes')
	 and station_id=$3
    )
    select %I(moving_average)
    from moving_averages
    where instrument_time between $1 and $2;
  $exec$;
  zero_estimate numeric;
  begin
    execute format(exec_str, val, tbl,
		   case when caltype='zero' then 'min'
		   when caltype='span' then 'max' end)
      into zero_estimate
      using lower(times), upper(times), station;
    RETURN zero_estimate;
  end;
$$ language plpgsql STABLE PARALLEL SAFE;

/* Store calibration estimates derived from the raw instrument values
*/
CREATE MATERIALIZED VIEW calibration_values AS
  select *,
	 case
	 when station_id=1 then estimate_cal('campbell_wfms',
					     lower(chemical) || '_avg',
					     type, cal_times)
	 when station_id=2 then estimate_cal('campbell_wfml',
					     lower(chemical) || '_avg',
					     type, cal_times)
	 else estimate_cal('envidas', station_id, chemical, type, cal_times) end as value
    from (select station_id,
		 chemical,
		 type,
		 tsrange(case
			 -- ignore first 15 minutes of span
			 -- calibration data due to spikes I think?
			 when type='span' then cal_day + lower(cal_times) + interval '15 minutes'
			 else cal_day + lower(cal_times) end,
			 cal_day + upper(cal_times),
			 '[]') as cal_times
	    from (select station_id,
			 instrument as chemical,
			 type,
			 generate_series(lower(dates),
					 coalesce(upper(dates), CURRENT_DATE),
					 interval '1 day')::date as cal_day,
			 times as cal_times
		    from autocals) a1) a2;
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
