/* Add derived measurements to the processed measurements. */

-- join two measurement types to make it easy to derive values from
-- multiple measurements
drop function if exists combine_measures cascade;
CREATE OR REPLACE FUNCTION combine_measures(site_id int, data_source text,
                                            measurement_name1 text,
					    measurement_name2 text)
  RETURNS TABLE (
    "time" timestamp,
    value1 numeric,
    value2 numeric,
    flagged1 boolean,
    flagged2 boolean
  ) as $$
  select m1.time,
	 m1.value,
	 m2.value,
	 coalesce(m1.flagged, false),
	 coalesce(m2.flagged, false)
    from (select *
	    from processed_measurements
	   where measurement_type_id=get_measurement_id(site_id, data_source, measurement_name1)) m1
	   join
	   (select *
	      from processed_measurements
	     where measurement_type_id=get_measurement_id(site_id, data_source, measurement_name2)) m2
	       on m1.time=m2.time;
$$ language sql;

/* WFMS */
drop view if exists wfms_hourly_winds cascade;
create or replace view wfms_hourly_winds as
  with windspeeds as (select date_trunc('hour', time) as time,
			     avg(value1) as u,
			     avg(value2) as v,
			     count(*) as n_values
			from combine_measures(1, 'derived', 'WS_u', 'WS_v')
		       where not flagged1
			 and not flagged2
		       group by date_trunc('hour', time))
  select get_measurement_id(1, 'derived', 'WS_hourly') as measurement_type_id,
	 time,
	 sqrt(u^2 + v^2)::numeric as value,
	 get_hourly_flag(get_measurement_id(1, 'derived', 'WS_hourly'),
			 0, n_values::int) as flag
    from windspeeds
   union
  select get_measurement_id(1, 'derived', 'WD_hourly') as measurement_type_id,
	 time,
	 (270 - (180 / pi()) * atan2(v, u))::numeric % 360 as value,
	 get_hourly_flag(get_measurement_id(1, 'derived', 'WD_hourly'),
			 0, n_values::int) as flag
    from windspeeds;

/* WFML */
drop view if exists wfml_hourly_winds cascade;
create or replace view wfml_hourly_winds as
  with windspeeds as (select date_trunc('hour', time) as time,
			     avg(value1) as u,
			     avg(value2) as v,
			     -- multiply the count by 5 since mesonet
			     -- measurements are only recorded every 5
			     -- minutes:
			     count(*) * 5 as n_values
			from combine_measures(2, 'derived', 'WS_u', 'WS_v')
		       where not flagged1
			 and not flagged2
		       group by date_trunc('hour', time))
  select get_measurement_id(2, 'derived', 'WS_hourly') as measurement_type_id,
	 time,
	 sqrt(u^2 + v^2)::numeric as value,
	 get_hourly_flag(get_measurement_id(2, 'derived', 'WS_hourly'),
			 0, n_values::int) as flag
    from windspeeds
   union
  select get_measurement_id(2, 'derived', 'WD_hourly') as measurement_type_id,
	 time,
	 (270 - (180 / pi()) * atan2(v, u))::numeric % 360 as value,
	 get_hourly_flag(get_measurement_id(2, 'derived', 'WD_hourly'),
			 0, n_values::int) as flag
    from windspeeds;

/* PSP */
drop view if exists psp_hourly_winds cascade;
create or replace view psp_hourly_winds as
  with windspeeds as (select date_trunc('hour', time) as time,
			     avg(value1) as u,
			     avg(value2) as v,
			     count(*) as n_values
			from combine_measures(3, 'derived', 'WS_u', 'WS_v')
		       where not flagged1
			 and not flagged2
		       group by date_trunc('hour', time))
  select get_measurement_id(3, 'derived', 'WS_hourly') as measurement_type_id,
	 time,
	 sqrt(u^2 + v^2)::numeric as value,
	 get_hourly_flag(get_measurement_id(3, 'derived', 'WS_hourly'),
			 0, n_values::int) as flag
    from windspeeds
   union
  select get_measurement_id(3, 'derived', 'WD_hourly') as measurement_type_id,
	 time,
	 (270 - (180 / pi()) * atan2(v, u))::numeric % 360 as value,
	 get_hourly_flag(get_measurement_id(3, 'derived', 'WD_hourly'),
			 0, n_values::int) as flag
    from windspeeds;

/* Combine all derived measurements. */
create or replace view hourly_derived_measurements as
  select * from wfms_hourly_winds
   union select * from wfml_hourly_winds
   union select * from psp_hourly_winds;
