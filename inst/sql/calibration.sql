/* Instrument calibration */

create table autocals (
  measurement_type_id int references measurement_types,
  type text,
  dates tsrange,
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
  site_id int references sites not null,
  measurement_name text not null,
  time timestamp,
  certified_value numeric,
  measured_value numeric,
  changed boolean,
  primary key(site_id, measurement_name, time)
);

create table calibration_flags (
  measurement_type_id int references measurement_types,
  type text,
  times tsrange,
  explanation text,
  primary key(measurement_type_id, type, times)
);

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
	 times * dates as times
    from (select measurement_type_id,
		 type,
		 tsrange(cal_day + lower(cal_times),
			 cal_day + upper(cal_times),
			 '[]') as times,
		 dates
	    from (select measurement_type_id,
			 type,
			 generate_series(lower(dates),
					 coalesce(upper(dates), CURRENT_DATE),
					 interval '1 day')::date as cal_day,
			 times as cal_times,
			 dates
		    from autocals) a1) a2
   where times && dates;
	 
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

-- calibration results calculated in load_calibration.R
create table calibrations_wfms (
  measurement_type_id int not null,
  type text not null,
  time timestamp not null,
  provided_value numeric,
  measured_value numeric,
  flagged bool,
  unique(measurement_type_id, type, time)
);
create table calibrations_wfml (
  measurement_type_id int not null,
  type text not null,
  time timestamp not null,
  provided_value numeric,
  measured_value numeric,
  flagged bool,
  unique(measurement_type_id, type, time)
);
create table calibrations_psp (
  measurement_type_id int not null,
  type text not null,
  time timestamp not null,
  provided_value numeric,
  measured_value numeric,
  flagged bool,
  unique(measurement_type_id, type, time)
);
create view calibration_results as
  select * from calibrations_wfms
   union
  select * from calibrations_wfml
   union
  select * from calibrations_psp;
