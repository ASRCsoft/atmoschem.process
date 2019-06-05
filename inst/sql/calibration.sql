/* Instrument calibration */

create table autocals (
  measurement_type_id int references measurement_types,
  type text,
  dates daterange,
  times timerange,
  value numeric default 0,
  primary key(measurement_type_id, dates, times),
  CONSTRAINT no_overlapping_autocals EXCLUDE USING GIST (
    measurement_type_id WITH =,
    dates WITH &&,
    times WITH &&
  )
);

create table manual_calibrations (
  measurement_type_id int references measurement_types,
  type text,
  times tsrange,
  provided_value numeric,
  measured_value numeric,
  corrected boolean not null,
  primary key(measurement_type_id, type, times),
  CONSTRAINT no_overlapping_manual_cals EXCLUDE USING GIST (
    measurement_type_id WITH =,
    type WITH =,
    times WITH &&
  )
);

create or replace function estimate_cal(measurement_type int, cal_type text, cal_times tsrange) returns numeric as $$
  begin
    return case when cal_type='zero' then min(moving_average)
	   else max(moving_average) end
      from (select instrument_time,
		   AVG(value) OVER(partition by measurement_type_id
				   ORDER BY instrument_time
				   ROWS BETWEEN 1 PRECEDING AND 1 following) as moving_average
	      from measurements2
	     where measurement_type_id=$1
	       and instrument_time between (lower(cal_times) - interval '2 minutes') and (upper(cal_times) + interval '2 minutes')) moving_averages
      -- ignore first 15 minutes of span calibration data due to spikes I
      -- think?
     where instrument_time <@ case when cal_type!='span' then cal_times
	   else tsrange(lower(cal_times) + interval '15 minutes', upper(cal_times)) end;
  end;
$$ language plpgsql STABLE PARALLEL SAFE;

create or replace view calibration_periods as
  select measurement_type_id,
	 type,
	 tsrange(cal_day + lower(cal_times),
		 cal_day + upper(cal_times),
		 '[]') as cal_times
    from (select measurement_type_id,
		 type,
		 generate_series(lower(dates),
				 coalesce(upper(dates), CURRENT_DATE),
				 interval '1 day')::date as cal_day,
		 times as cal_times
	    from autocals) a1;

CREATE materialized VIEW calibration_zeros AS
  select measurement_type_id,
	 type,
	 upper(cal_times) as cal_time,
	 value
    from (select *,
		 estimate_cal(measurement_type_id, type, cal_times) as value
	    from calibration_periods
	   where type = 'zero') c1
   where value is not null
  union
  select measurement_type_id,
	 type,
	 upper(times) as cal_time,
	 measured_value as value
    from manual_calibrations m1
	   join measurement_types m2
	       on m1.measurement_type_id=m2.id
  -- A few of these manual zero calibration values are bad and need to
  -- be excluded. This seems like the least awkward way to do that for
  -- now.
   where not (measurement_type_id=get_measurement_id(3, 'envidas', 'NO-DEC')
	      and upper(times)::date between '2018-10-23' and '2018-11-08')
     and type = 'zero';
CREATE INDEX calibration_zeros_time_idx ON calibration_zeros(measurement_type_id, type, cal_time);

CREATE OR REPLACE FUNCTION interpolate_cal_zero(measurement_type_id int, t timestamp)
  RETURNS numeric AS $$
  select interpolate(t0, t1, y0, y1, $2)
    from (select cal_time as t0,
		 value as y0
	    from calibration_zeros
	   where cal_time<=$2
	     and measurement_type_id=$1
	     and value is not null
	   order by cal_time desc
	   limit 1) calib0
	 full outer join
	 (select cal_time as t1,
		 value as y1
	    from calibration_zeros
	   where cal_time>$2
	     and measurement_type_id=$1
	     and value is not null
	   order by cal_time asc
	   limit 1) calib1
         on true;
$$ LANGUAGE sql STABLE PARALLEL SAFE;

CREATE or replace VIEW calibration_spans AS
  select c1.measurement_type_id,
	 c1.type,
	 upper(c1.cal_times) as cal_time,
	 m.span as provided_value,
	 c1.value as value
    from (select *,
		 estimate_cal(measurement_type_id, type, cal_times) as value
	    from calibration_periods
	   where type = 'span') c1
	   join
	   measurement_types m
	   on c1.measurement_type_id=m.id
   where value is not null
  union
  select measurement_type_id,
	 type,
	 upper(times) as cal_time,
	 provided_value,
	 measured_value as value
    from manual_calibrations m1
	   join measurement_types m2
	       on m1.measurement_type_id=m2.id
     and type = 'span';

CREATE OR REPLACE FUNCTION calc_span(measurement_type_id int, provided_val numeric, val numeric, ts timestamp)
  RETURNS numeric AS $$
  select (val - interpolate_cal_zero(measurement_type_id, ts)) / provided_val;
$$ LANGUAGE sql STABLE PARALLEL SAFE;

/* Store calibration estimates derived from the raw instrument values
 */
CREATE MATERIALIZED VIEW calibration_values AS
  select measurement_type_id,
	 type,
	 cal_time,
	 value
    from calibration_zeros
   union
  select measurement_type_id,
	 type,
	 cal_time,
	 calc_span(measurement_type_id, provided_value,
		   value, cal_time) as value
    from calibration_spans;
-- to make the interpolate_cal function faster
CREATE INDEX calibration_values_upper_time_idx ON calibration_values(measurement_type_id, type, cal_time);

/* Estimate calibration values using linear interpolation */
CREATE OR REPLACE FUNCTION interpolate_cal(measurement_type_id int, type text, t timestamp)
  RETURNS numeric AS $$
  select interpolate(t0, t1, y0, y1, $3)
    from (select cal_time as t0,
		 value as y0
	    from calibration_values
	   where cal_time<=$3
	     and measurement_type_id=$1
	     and type=$2
	     and value is not null
	   order by cal_time desc
	   limit 1) calib0
	 full outer join
	 (select cal_time as t1,
		 value as y1
	    from calibration_values
	   where cal_time>$3
	     and measurement_type_id=$1
	     and type=$2
	     and value is not null
	   order by cal_time asc
	   limit 1) calib1
         on true;
$$ LANGUAGE sql STABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION apply_calib(measurement_type_id int, val numeric, t timestamp)
  RETURNS numeric AS $$
  DECLARE
  zero numeric = interpolate_cal(measurement_type_id, 'zero', t);
  span numeric = interpolate_cal(measurement_type_id, 'span', t);
  BEGIN
    return case when span is null then val - zero
      else (val - zero) / span end;
  END;
$$ LANGUAGE plpgsql STABLE PARALLEL SAFE;
-- for whatever reason this is faster as a plpgsql function

-- CREATE OR REPLACE FUNCTION apply_calib(site_id int, measurement text, val numeric, t timestamp)
--   RETURNS numeric AS $$
--   select case when span is null then val - zero
-- 	 else (val - zero) / (span - zero) *
-- 	   (select m.span
-- 	      from measurements m
-- 	     where m.site_id=$1
-- 	       and m.measurement=$2) end
--   from (select interpolate_cal(site_id, measurement, 'zero', t) as zero,
-- 	       interpolate_cal(site_id, measurement, 'span', t) as span) as cals;
-- $$ LANGUAGE sql STABLE PARALLEL SAFE;

/* Guess the calibration time period using the calibration value and a
time range of possible times */
CREATE OR REPLACE FUNCTION guess_cal_time(mtype int, type text, cal_times tsrange, val numeric,
					  threshold numeric, maxdif numeric)
  RETURNS tsrange AS $$
  -- 1) select the mtype1 measurements within the time bounds
  with meas1 as (
    select *
      from measurements2
     where measurement_type_id=mtype
       and instrument_time <@ cal_times
  ),		       
  -- 2) use derivative to label groups of measurements
  meas_d1 as (
    select *,
	   value - lag(value) over w as d1
      from meas1
	     window w as (order by instrument_time)
  ),
  meas_disc as (
    select *,
	   abs(d1)>threshold as discontinuous
      from meas_d1
  ),
  meas_new_group as (
    select *,
	   discontinuous and not lag(discontinuous) over w as new_group
      from meas_disc
	     window w as (order by instrument_time)
  ),
  meas_groups as (
    select *
      from (select *,
		   sum(new_group::int) over w as group_id
	      from meas_new_group
		     window w as (order by instrument_time)) m1
     where not discontinuous
  ),
  -- 3) submit each group to `estimate_cal`
  group_times as (
    select measurement_type_id,
	   group_id,
	   tsrange(min(instrument_time), max(instrument_time), '[)') as times
      from meas_groups
     group by measurement_type_id, group_id
  ),
  group_cals as (
    select *,
	   estimate_cal(measurement_type_id, type, times) as measured_value
      from (select *
	      from group_times
	     where (upper(times) - lower(times))>='5 min') g1
  )
  -- 4) get the group with the closest value to val
  select case when dif>maxdif then null
	 else times end
    from (select *,
		 abs(measured_value - val) as dif
	    from group_cals) g1
   order by dif asc
   limit 1;
$$ LANGUAGE sql STABLE PARALLEL SAFE;

/* Guess the NOx/NO2 conversion efficiency calibration time period for
   using the calibration value and a time range of possible times */
CREATE OR REPLACE FUNCTION guess_no_ce_time(mtype int, cal_times tsrange, val numeric)
  RETURNS tsrange AS $$
  select guess_cal_time(mtype, 'CE', cal_times, val + 10, 1, 15);
$$ LANGUAGE sql STABLE PARALLEL SAFE;

create or replace view conversion_efficiency_inputs as
  select measurement_type_id,
	 type,
	 upper(cal_times) as cal_time,
	 max_ce as provided_value,
	 estimate_cal(measurement_type_id, type, cal_times) as measured_value
    from calibration_periods c1
	   join measurement_types m1
	       on c1.measurement_type_id=m1.id
   where type='CE'
   union
  select measurement_type_id,
	 type,
	 upper(times) as cal_time,
	 provided_value,
	 measured_value
    from manual_calibrations
   where type='CE'
   union
  select get_measurement_id(3, 'envidas', 'NO'),
	 type,
	 upper(times) as cal_time,
	 provided_value,
	 estimate_cal(get_measurement_id(3, 'envidas', 'NO'), 'CE',
		      guess_no_ce_time(measurement_type_id, times,
				       measured_value)) as measured_value
    from manual_calibrations
   where measurement_type_id=get_measurement_id(3, 'envidas', 'NOx')
     and type='CE';

create or replace view _conversion_efficiencies AS
  select measurement_type_id,
	 cal_time,
	 apply_calib(measurement_type_id, measured_value,
		     cal_time) / provided_value as efficiency
    from conversion_efficiency_inputs
   where measured_value is not null;

create or replace view derived_conversion_efficiencies AS
  select get_measurement_id(1, 'derived', 'NO2'),
	 cal_time,
	 efficiency
    from _conversion_efficiencies
   where measurement_type_id=get_measurement_id(1, 'campbell', 'NOx')
   union
  select get_measurement_id(3, 'derived', 'NO2'),
	 c2.cal_time,
	 (apply_calib(c3.measurement_type_id, c3.measured_value, c3.cal_time) -
	  apply_calib(c2.measurement_type_id, c2.measured_value, c2.cal_time)) /
	   c3.provided_value as efficiency
    from conversion_efficiency_inputs c2
	   join conversion_efficiency_inputs c3
	       on c2.cal_time=c3.cal_time
   where c2.measurement_type_id=get_measurement_id(3, 'envidas', 'NO')
     and c3.measurement_type_id=get_measurement_id(3, 'envidas', 'NOx');

CREATE MATERIALIZED VIEW conversion_efficiencies AS
  select *,
	 (median(efficiency) over w)::numeric as filtered_efficiency
    from (select * from _conversion_efficiencies
	   union select * from derived_conversion_efficiencies) ce1
  window w as (partition by measurement_type_id
	       order by cal_time
	       rows between 15 preceding and 15 following);
-- to make the interpolate_ce function faster
CREATE INDEX conversion_efficiencies_time_idx ON conversion_efficiencies(cal_time);

/* Estimate conversion efficiencies using linear interpolation */
CREATE OR REPLACE FUNCTION interpolate_ce(measurement_type_id int, t timestamp)
  RETURNS numeric AS $$
  select interpolate(t0, t1, y0, y1, $2)
    from (select cal_time as t0,
		 filtered_efficiency as y0
	    from conversion_efficiencies
	   where cal_time<=$2
	     and measurement_type_id=$1
	     and filtered_efficiency is not null
	   order by cal_time desc
	   limit 1) ce0
         full outer join
         (select cal_time as t1,
		 filtered_efficiency as y1
	    from conversion_efficiencies
	   where cal_time>$2
	     and measurement_type_id=$1
	     and filtered_efficiency is not null
	   order by cal_time asc
	   limit 1) ce1
         on true;
$$ LANGUAGE sql STABLE PARALLEL SAFE;
