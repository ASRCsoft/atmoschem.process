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
  file_id int references files on delete cascade,
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

create table calibration_flags (
  measurement_type_id int references measurement_types,
  type text,
  times tsrange,
  explanation text,
  primary key(measurement_type_id, type, times)
);

create or replace function estimate_cal(measurement_type int, cal_type text, cal_times tsrange) returns numeric as $$
  begin
    return case when cal_type='zero' then min(moving_average)
	   else max(moving_average) end
      from (select time,
		   AVG(value) OVER(partition by measurement_type_id
				   ORDER BY time
				   ROWS BETWEEN 1 PRECEDING AND 1 following) as moving_average
	      from measurements2
	     where measurement_type_id=$1
	       and time between (lower(cal_times) - interval '2 minutes') and (upper(cal_times) + interval '2 minutes')) moving_averages
      -- ignore first 15 minutes of span calibration data due to spikes I
      -- think?
     where time <@ case when cal_type!='span' then cal_times
	   else tsrange(lower(cal_times) + interval '15 minutes', upper(cal_times)) end;
  end;
$$ language plpgsql STABLE PARALLEL SAFE;

-- get manual calibration periods, grouped regardless of cal type
CREATE or replace VIEW manual_calibration_sessions AS
  select measurement_type_id,
	 range_union(times) as times
    from (select *,
		 sum(new_session::int) over w as session_number
	    from (select *,
			 isempty(times * lag(times) over w) as new_session
		    from manual_calibrations
			   window w as (partition by measurement_type_id
					order by lower(times))) w1
		   window w as (partition by measurement_type_id
				order by lower(times))) w2
   group by measurement_type_id, session_number;

create or replace view scheduled_autocals as
  select measurement_type_id,
	 type,
	 tsrange(cal_day + lower(cal_times),
		 cal_day + upper(cal_times),
		 '[]') as times
    from (select measurement_type_id,
		 type,
		 generate_series(lower(dates),
				 coalesce(upper(dates), CURRENT_DATE),
				 interval '1 day')::date as cal_day,
		 times as cal_times
	    from autocals) a1;
	 
create or replace view calibration_periods as
  select *
    from (select measurement_type_id,
		 type,
		 range_intersect(times) as times
	    from (select sa.measurement_type_id,
			 type,
			 sa.times as scheduled_times,
			 case when mc.times is null then sa.times
			 else sa.times - mc.times end as times
		    from scheduled_autocals sa
			   left join manual_calibration_sessions mc
			       on sa.measurement_type_id = mc.measurement_type_id
			       and sa.times && mc.times) sa1
	   group by measurement_type_id, type, scheduled_times) sa2
   where not isempty(times);

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
       and time <@ cal_times
  ),		       
  -- 2) use derivative to label groups of measurements
  meas_d1 as (
    select *,
	   value - lag(value) over w as d1
      from meas1
	     window w as (order by time)
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
	     window w as (order by time)
  ),
  meas_groups as (
    select *
      from (select *,
		   sum(new_group::int) over w as group_id
	      from meas_new_group
		     window w as (order by time)) m1
     where not discontinuous
  ),
  -- 3) submit each group to `estimate_cal`
  group_times as (
    select measurement_type_id,
	   group_id,
	   tsrange(min(time), max(time), '[)') as times
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
		 abs(100 * measured_value/ val - 100) as dif
	    from group_cals) g1
   order by dif asc
   limit 1;
$$ LANGUAGE sql STABLE PARALLEL SAFE;

/* Guess the NOx/NO2 conversion efficiency calibration time period for
   using the calibration value and a time range of possible times */
-- CREATE OR REPLACE FUNCTION guess_no_ce_time(mtype int, cal_times tsrange, val numeric)
--   RETURNS tsrange AS $$
--   select guess_cal_time(mtype, 'CE', cal_times2, val + 10, 1, 15);
-- $$ LANGUAGE sql STABLE PARALLEL SAFE;

-- For testing:
-- select guess_cal_time(mtype, 'CE', cal_times2, ceval, 1, 20)
--   from (select cal_times - stimes as cal_times2
-- 	  from (select guess_cal_time(mtype, 'span', cal_times, sval, 1, 5) as stimes
-- 		  from (select measured_value as sval
-- 			  from manual_calibrations
-- 			 where times=cal_times
-- 			   and type='span') s1) t1) t2;

-- with mtype as (select 58 as id),
--   cecals as (select times,
-- 		    measured_value as ceval
-- 	       from manual_calibrations
-- 	      where measurement_type_id=(select id
-- 					   from mtype)
-- 		and type='CE'),
--   scals as (select times,
-- 		   measured_value as sval
-- 	      from manual_calibrations
-- 	     where measurement_type_id=(select id
-- 					  from mtype)
-- 	       and type='span')
-- select guess_cal_time((select id
-- 			 from mtype), 'CE', cal_times2, ceval, 1, 20)
--   from (select tsrange(upper(stimes), upper(times)) as cal_times2,
-- 	       ceval
-- 	  from (select cecals.times,
-- 		       ceval,
-- 		       guess_cal_time((select id
-- 					 from mtype), 'span', cecals.times, sval, 1, 5) as stimes
-- 		  from scals
-- 			 join cecals
-- 			     on scals.times=cecals.times) t1) t2;

-- with mtype as (select 58 as id),
--   cecals as (select times,
-- 		    measured_value as ceval
-- 	       from manual_calibrations
-- 	      where measurement_type_id=(select id
-- 					   from mtype)
-- 		and type='CE'),
--   scals as (select times,
-- 		   measured_value as sval
-- 	      from manual_calibrations
-- 	     where measurement_type_id=(select id
-- 					  from mtype)
-- 	       and type='span')
-- select cecals.times,
--        ceval,
--        guess_cal_time((select id
-- 			 from mtype), 'CE', cecals.times, sval, 1, 5) as stimes
--   from scals
-- 	 join cecals
-- 	     on scals.times=cecals.times;

-- with mtype as (select 58 as id),
--   cecals as (select times,
-- 		    measured_value as ceval
-- 	       from manual_calibrations
-- 	      where measurement_type_id=(select id
-- 					   from mtype)
-- 		and type='CE'),
--   scals as (select times,
-- 		   measured_value as sval
-- 	      from manual_calibrations
-- 	     where measurement_type_id=(select id
-- 					  from mtype)
-- 	       and type='span')
-- select cecals.times
--   from scals
-- 	 join cecals
-- 	     on scals.times=cecals.times;

CREATE OR REPLACE FUNCTION guess_no_ce_time(mtype int, cal_times tsrange, ceval numeric)
  RETURNS tsrange AS $$
  -- find the span time and exclude it, then search the remaining time for the CE
  select case when cal_times2 is null then null
	 else guess_cal_time(mtype, 'CE', cal_times2, ceval, 1, 20) end
    from (select tsrange(upper(stimes), upper(cal_times)) as cal_times2
	    -- due to the way I have this coded I have to specify
	    -- type='CE' here instead of span, even though they are
	    -- spans
	    from (select guess_cal_time(mtype, 'CE', cal_times, sval, 1, 5) as stimes
		    from (select measured_value as sval
			    from manual_calibrations
			   where measurement_type_id=mtype
			     and times=cal_times
			     and type='span') s1) t1) t2;
$$ LANGUAGE sql STABLE PARALLEL SAFE;

create materialized view calibration_results as
  select cr1.*,
	 cf.times is not null as flagged
    from (select measurement_type_id,
		 type,
		 upper(times) as time,
		 m.span as provided_value,
		 estimate_cal(measurement_type_id, type, times) as measured_value
	    from calibration_periods cp1
		   join measurement_types m
		       on cp1.measurement_type_id=m.id
	   where ((type='zero' and upper(times) - lower(times) > interval '5 min')
		  or upper(times) - lower(times) > interval '10 min')
	   union
	  select measurement_type_id,
		 type,
		 upper(times) as time,
		 provided_value,
		 measured_value
	    from manual_calibrations
		 -- the WFML manual cals in general seem questionable
	   where not measurement_type_id in (select id
					       from measurement_types
					      where data_source_id in (select id
									 from data_sources
									where site_id=2))
	   union
	         -- find NO conversion efficiency results, which
	         -- weren't recorded in the PSP cal sheets
	  select get_measurement_id(3, 'envidas', 'NO'),
		 type,
		 upper(times) as time,
		 provided_value,
		 estimate_cal(get_measurement_id(3, 'envidas', 'NO'), 'CE',
			      guess_no_ce_time(measurement_type_id, times,
					       measured_value)) as measured_value
	    from manual_calibrations
	   where measurement_type_id=get_measurement_id(3, 'envidas', 'NOx')
	     and type='CE') cr1
	   left join calibration_flags cf
	       on cr1.measurement_type_id=cf.measurement_type_id
	       and cr1.type = cf.type
	       and cr1.time <@ cf.times
   where cr1.type in ('zero', 'span', 'CE');
CREATE INDEX calibration_results_idx ON calibration_results(measurement_type_id, type, flagged, time);
