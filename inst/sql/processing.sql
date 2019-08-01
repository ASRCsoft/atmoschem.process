/* Storing/organizing/calculating processing results. */

/* Correct the instrument clock times, where needed. */
drop materialized view if exists processed_observations cascade;
CREATE materialized VIEW processed_observations as
  select id,
	 correct_time(data_source_id, source_row,
		      time) as time
    from observation_sourcerows
   union
  select obs.id,
	 time
    from observations obs
	   join files
	       on obs.file_id=files.id
	   join data_sources ds
	       on files.data_source_id=ds.id
   where not (ds.site_id=3 and ds.name='envidas');
create index processed_observations_idx on processed_observations(id);

/* Store processed results */
drop table if exists processed_measurements cascade;
create table processed_measurements (
  measurement_type_id int references measurement_types,
  time timestamp,
  value numeric,
  flagged boolean not null,
  primary key(measurement_type_id, time)
);

/* This is another placeholder for derived values. */
drop view if exists hourly_derived_measurements cascade;
create or replace view hourly_derived_measurements as
  select *
    from (select 1 as measurement_type_id,
		 '2099-01-01'::timestamp as time,
		 1.5 as value,
		 'M1' as flag) t1
   limit 0;

/* Aggregate the processed data by hour using a function from
   flags.sql. */
drop materialized view if exists hourly_measurements cascade;
CREATE materialized VIEW hourly_measurements as
  select measurement_type_id,
	 time,
	 value,
	 get_hourly_flag(measurement_type_id, value::numeric, n_values::int) as flag
    from (select measurement_type_id,
		 date_trunc('hour', time) as time,
		 case when name ilike '%\_Max%' then max(value) FILTER (WHERE not flagged)
		 when name='Precip' then sum(value) FILTER (WHERE not flagged)
		 when pm.measurement_type_id=get_measurement_id(2, 'mesonet', 'precip_since_00Z [mm]')
		 -- just get the value at the top of the hour for now
		   then max(value) FILTER (WHERE extract(minute from time)=0)
		 else avg(value) FILTER (WHERE not flagged) end as value,
		 case when pm.measurement_type_id=any(get_data_source_ids(1, 'aethelometer'))
			or pm.measurement_type_id=get_measurement_id(1, 'derived', 'Wood smoke')
		 -- the aethelometer only records values every 15
		 -- minutes, have to adjust the count to compensate
		   then (count(value) FILTER (WHERE not flagged)) * 15
		   when pm.measurement_type_id=any(get_data_source_ids(2, 'mesonet'))
		   then (count(value) FILTER (WHERE not flagged)) * 5
		 else count(value) FILTER (WHERE not flagged) end as n_values
	    from processed_measurements pm
		   join measurement_types mt
		       on pm.measurement_type_id=mt.id
	   group by measurement_type_id, name, date_trunc('hour', time)) c1
   union select * from hourly_derived_measurements;
create index hourly_measurements_idx on hourly_measurements(measurement_type_id, time);

/* Update the processed data. */
drop function if exists update_processing_inputs cascade;
CREATE OR REPLACE FUNCTION update_processing_inputs()
  RETURNS void as $$
  refresh materialized view matched_clock_audits;
  refresh materialized view processed_observations;
  refresh materialized view calibration_results;
  refresh materialized view freezing_clusters;
  refresh materialized VIEW flagged_periods;
$$ language sql;

drop function if exists update_processing_outputs cascade;
CREATE OR REPLACE FUNCTION update_processing_outputs()
  RETURNS void as $$
  refresh materialized view hourly_measurements;
$$ language sql;
