/* Storing/organizing/calculating processing results. */

/* Store processed results */
drop table if exists processed_measurements cascade;
create table processed_measurements (
  measurement_type_id int references measurement_types,
  time timestamp,
  value numeric,
  flagged boolean not null
);
create index processed_measurements_idx on processed_measurements(measurement_type_id, time);

create table hourly_measurements (
  measurement_type_id int references measurement_types,
  time timestamp,
  value numeric,
  flag text,
  primary key(measurement_type_id, time)
);

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

/* This is another placeholder for derived values. */
drop view if exists hourly_derived_measurements cascade;
create or replace view hourly_derived_measurements as
  select *
    from (select 1 as measurement_type_id,
		 '2099-01-01'::timestamp as time,
		 1.5 as value,
		 'M1' as flag) t1
   limit 0;

/* Aggregate the processed data by hour and apply NARSTO flags. */
CREATE OR REPLACE FUNCTION get_hourly_measurements(site int, data_source text,
						   starttime timestamp,
						   endtime timestamp)
  RETURNS TABLE (
    measurement_type_id int,
    "time" timestamp,
    value numeric,
    flag text
  )
AS $$
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
		 -- the aethelometer only records values every 15
		 -- minutes, have to adjust the count to compensate
		   then (count(value) FILTER (WHERE not flagged)) * 15
		   when pm.measurement_type_id=any(get_data_source_ids(2, 'mesonet'))
		   then (count(value) FILTER (WHERE not flagged)) * 5
		 else count(value) FILTER (WHERE not flagged) end as n_values
	    from processed_measurements pm
		   join measurement_types mt
		       on pm.measurement_type_id=mt.id
	   where measurement_type_id in (select id
					   from measurement_types
					  where data_source_id=(select id
								  from data_sources
								 where site_id=$1
								   and name=$2))
	     and time>=$3 and time<$4
	   group by measurement_type_id, name, date_trunc('hour', time)) c1
   union
  select *
    from hourly_derived_measurements
   where measurement_type_id in (select id
				   from measurement_types
				  where data_source_id=(select id
							  from data_sources
							 where site_id=$1
							   and name=$2))
     and time>=$3 and time<$4;
$$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION update_hourly_measurements(site int, data_source text,
						      starttime timestamp,
						      endtime timestamp)
  RETURNS void as $$
  delete 
  from hourly_measurements
   where measurement_type_id in (select id
				   from measurement_types
				  where data_source_id=(select id
							  from data_sources
							 where site_id=$1
							   and name=$2))
  and time>=$3 and time<$4;
  insert into hourly_measurements
  select *
    from get_hourly_measurements($1, $2, $3, $4);
$$ language sql;

/* Update the processed data. */
drop function if exists update_processing_inputs cascade;
CREATE OR REPLACE FUNCTION update_processing_inputs(site int, data_source text,
						    starttime timestamp,
						    endtime timestamp)
  RETURNS void as $$
  refresh materialized view matched_clock_audits;
  refresh materialized view processed_observations;
  select update_calibration_results($1, $2, $3, $4);
  select update_freezing_clusters($1, $2, $3, $4);
  select update_power_outages($3, $4);
  refresh materialized VIEW flagged_periods;
$$ language sql;

drop function if exists update_processing_outputs cascade;
CREATE OR REPLACE FUNCTION update_processing_outputs(site int, data_source text,
						     starttime timestamp,
						     endtime timestamp)
  RETURNS void as $$
  select update_hourly_measurements($1, $2, $3, $4);
$$ language sql;
