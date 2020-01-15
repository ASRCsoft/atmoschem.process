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

create table gilibrator (
  measurement_type_id int references measurement_types,
  time timestamp,
  certified_value numeric,
  measured_value numeric,
  changed boolean,
  primary key(measurement_type_id, time)
);

create table calibration_flags (
  measurement_type_id int references measurement_types,
  type text,
  times tsrange,
  explanation text,
  primary key(measurement_type_id, type, times)
);

-- get the highest (or lowest) value of the 5-minute moving average in
-- a given calibration period
create or replace function estimate_cal(measurement_type int, cal_type text, cal_times tsrange) returns numeric as $$
  begin
    return case when cal_type='zero' then min(moving_average)
	   else max(moving_average) end
      from (select time,
		   AVG(value) OVER(partition by measurement_type_id
				   ORDER BY time
				   ROWS BETWEEN 2 PRECEDING AND 2 following) as moving_average
	      from measurements2
	     where measurement_type_id=$1
	       and time between (lower(cal_times) - interval '3 minutes') and (upper(cal_times) + interval '3 minutes')) moving_averages
      -- ignore first 15 minutes of span calibration data due to spikes I
      -- think?
     where time <@ case when cal_type!='span' then cal_times
	   else tsrange(least(lower(cal_times) + interval '15 minutes', upper(cal_times)),
			upper(cal_times)) end;
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

/* Guess the conversion efficiency calibration time period using the
   recorded NOx value from the cal sheet (noxval), the range of
   calibration times, and the NO/NOx divergence */
CREATE OR REPLACE FUNCTION guess_42C_ce_time(noid int, noxid int, cal_times tsrange, noxval numeric)
  RETURNS tsrange AS $$
  -- 1) get NOx minus NO (approximate NO2) from the measurements table
  with no2 as (
    select no.time as time,
	   nox.value as nox_val,
	   nox.value - no.value as no2_val
      from (select *
	      from measurements2
	     where measurement_type_id=noid) no
	     join
	     (select *
		from measurements2
	       where measurement_type_id=noxid) nox
		 on no.time=nox.time
     where no.time <@ cal_times
  ),
  -- 2) use derivative NOx and NO2 values to separate measurements
  -- into groups
  groups as (
    select tsrange(min(time), max(time), '[)') as times
      from (select *,
		   sum(new_group::int) over w as group_id
	      from (select *,
			   discontinuous and not lag(discontinuous) over w as new_group
		      from (select *,
				   abs(dnox)>1 or abs(dno2)>1 as discontinuous
			      from (select *,
					   nox_val - lag(nox_val) over w as dnox,
					   no2_val - lag(no2_val) over w as dno2
				      from no2
					     window w as (order by time)) d1) d2
			     window w as (order by time)) d3
		     window w as (order by time)) m1
     where not discontinuous
     group by group_id
    having max(time) - min(time) >= '10 min'
  ),
  -- 3) select the group with the largest NO2 values that matches CE
  -- criteria
  ce_cal as (
    select *,
	   nox - no as no2
      from (select *,
	           -- get the lowest value of NO, highest NOx
		   estimate_cal(noid, 'zero', times) as no,
		   estimate_cal(noxid, 'CE', times) as nox
	      from groups) g1
     -- filter out zero calibrations
     where no > .3
     -- make sure measured NOx value is within +/- 30% of the recorded
     -- NOx value
       and abs(100 * nox / noxval - 100) < 30
     order by nox - no desc
     limit 1
  )
  select times
    from ce_cal;
$$ LANGUAGE sql STABLE PARALLEL SAFE;

-- estimate the calibration results for the autocalibration periods
create or replace view _calibration_results as
  select measurement_type_id,
	 type,
	 upper(times) as time,
	 case when type='zero' then 0
	 when type='span' then m.span
	 when type='CE' then m.max_ce
	 else null end as provided_value,
	 estimate_cal(measurement_type_id, type, times) as measured_value
    from calibration_periods cp1
	   join measurement_types m
	       on cp1.measurement_type_id=m.id
   where (type='zero' and upper(times) - lower(times) > interval '5 min')
      or (type='span' and upper(times) - lower(times) > interval '15 min')
      or (type!='span' and upper(times) - lower(times) > interval '10 min');

-- find NO conversion efficiency results, which weren't recorded in
-- the PSP cal sheets
create view psp_no_ces as
  select get_measurement_id(3, 'envidas', 'NO'),
	 type,
	 upper(times) as time,
	 provided_value,
         -- get minimum (not maximum!) NO values for CE calibrations
	 estimate_cal(get_measurement_id(3, 'envidas', 'NO'), 'zero',
		      guess_42C_ce_time(get_measurement_id(3, 'envidas', 'NO'),
					get_measurement_id(3, 'envidas', 'NOx'),
					times,
					measured_value)) as measured_value
    from manual_calibrations
   where measurement_type_id=get_measurement_id(3, 'envidas', 'NOx')
     and type='CE';

-- gather the combined calibration results for the given data source
-- and time range
CREATE OR REPLACE FUNCTION get_calibration_results(site int, data_source text,
						   starttime timestamp,
						   endtime timestamp)
  RETURNS TABLE (
    measurement_type_id int,
    type text,
    "time" timestamp,
    provided_value numeric,
    measured_value numeric,
    flagged bool
  ) 
AS $$
  select cr1.*,
	 cf.times is not null as flagged
    from (select *
	    from _calibration_results
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
	  select *
	    from psp_no_ces) cr1
	   left join calibration_flags cf
	       on cr1.measurement_type_id=cf.measurement_type_id
	       and cr1.type = cf.type
	       and cr1.time <@ cf.times
  where cr1.measurement_type_id in (select id
				      from measurement_types
				     where data_source_id=(select id
							     from data_sources
							    where site_id=$1
							      and name=$2))
    and cr1.time>=$3 and cr1.time<$4
    and cr1.type in ('zero', 'span', 'CE');
$$ LANGUAGE sql;

create table calibration_results (
  measurement_type_id int not null,
  type text not null,
  time timestamp not null,
  provided_value numeric,
  measured_value numeric,
  flagged bool,
  unique(measurement_type_id, type, time)
);

CREATE OR REPLACE FUNCTION update_calibration_results(site int, data_source text,
						      starttime timestamp,
						      endtime timestamp)
  RETURNS void as $$
  delete 
    from calibration_results
   where measurement_type_id in (select id
				   from measurement_types
				  where data_source_id=(select id
							  from data_sources
							 where site_id=$1
							   and name=$2))
     and time>=$3 and time<$4;
  insert into calibration_results
  select *
    from get_calibration_results($1, $2, $3, $4);
$$ language sql;
