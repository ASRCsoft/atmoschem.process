/* A few notes on how the processing is organized:

I found that the processing is bizarrely, shockingly slow if I attempt
to do it all in one query, but much faster if I split the processing
into multiple queries. To keep that speed in a single query I used
common table expressions (CTEs), which serve as optimization fences so
that postgres optimizes each query section separately.

I also split the processing by data source so that a new data file
does not require reprocessing all data. The `update_processing`
function will update the processing for a single data source. */

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

/* Apply calibration adjustments if needed, using functions from
   calibration.sql. */
drop view if exists calibrated_measurements cascade;
create or replace view calibrated_measurements as
  select measurement_type_id,
	 time as instrument_time,
	 value,
	 case when apply_ce then apply_calib(measurement_type_id, value, time) /
		interpolate_ce(measurement_type_id, time)
	 when has_calibration then apply_calib(measurement_type_id, value, time)
	 else value end as calibrated_value,
	 flagged,
	 valid_range,
	 mdl,
	 remove_outliers,
	 max_jump
    from measurements m
	   left join measurement_types mt
	       on m.measurement_type_id=mt.id
	   join processed_observations obs
	       on m.observation_id=obs.id
   where apply_processing;

drop function if exists process_measurements cascade;
CREATE OR REPLACE FUNCTION process_measurements(measurement_type_ids int[],
						start_time timestamp,
						end_time timestamp)
RETURNS TABLE (
    measurement_type_id int,
    measurement_time timestamp,
    value numeric,
    flagged boolean
  ) as $$
  with calibrated_measurements_subset as (
    select *
      from calibrated_measurements
     where measurement_type_id = any(measurement_type_ids)
       and (start_time is null or instrument_time>=(start_time - interval '1 day'))
       and (end_time is null or instrument_time<=(end_time + interval '1 day'))
  ), measurement_medians as (
    /* Calculate running medians and running Median Absolute
       Deviations (MAD) using functions from filtering.sql. These
       numbers are used to check for outliers. */
    select *,
	   case when remove_outliers then runmed(calibrated_value) over w
	   else null end as running_median,
	   case when remove_outliers then runmad(calibrated_value) over w
	   else null end as running_mad,
	   case when max_jump is not null then abs(value - lag(value) over w) > max_jump
	   else false end as is_jump
      from calibrated_measurements_subset
	   -- remove unneeded extra measurements
     where (start_time is null or instrument_time>=start_time)
       and (end_time is null or instrument_time<=end_time)
	     WINDOW w AS (partition by measurement_type_id
			  ORDER BY instrument_time
			  rows between 120 preceding and 120 following)
  )
  /* Check for flag conditions using functions from flags.sql. The end
     result is the fully processed data. */
  select measurement_type_id,
	 instrument_time as time,
	 calibrated_value as value,
	 is_flagged(measurement_type_id, null, instrument_time,
		    calibrated_value, flagged, running_median,
		    running_mad) or is_jump as flagged
    from measurement_medians;
$$ language sql;

/* Store processed results, before adding derived values */
drop table if exists _processed_measurements cascade;
create table _processed_measurements (
  measurement_type_id int references measurement_types,
  measurement_time timestamp,
  value numeric,
  flagged boolean,
  primary key(measurement_type_id, measurement_time)
);

/* This is a placeholder that will get replaced in
   derived_measurements.sql. */
drop view if exists derived_measurements cascade;
create or replace view derived_measurements as
  select *
    from _processed_measurements
   limit 0;

/* Combine all processed data. */
drop materialized view if exists processed_measurements cascade;
CREATE materialized VIEW processed_measurements as
  select * from _processed_measurements
   union select * from derived_measurements;
create index processed_measurements_idx on processed_measurements(measurement_type_id, measurement_time);

/* Aggregate the processed data by hour using a function from
   flags.sql. */
drop materialized view if exists hourly_measurements cascade;
CREATE materialized VIEW hourly_measurements as
  select measurement_type_id,
	 measurement_time,
	 value,
	 get_hourly_flag(measurement_type_id, value::numeric, n_values::int) as flag
    from (select measurement_type_id,
		 date_trunc('hour', measurement_time) as measurement_time,
		 case when (select name like '%\_Max'
			      from measurement_types
			     where id=measurement_type_id)
		   then max(value) FILTER (WHERE not flagged)
		 else avg(value) FILTER (WHERE not flagged) end as value,
		 count(value) FILTER (WHERE not flagged) as n_values
	    from processed_measurements
	   group by measurement_type_id, date_trunc('hour', measurement_time)) c1;
create index hourly_measurements_idx on hourly_measurements(measurement_type_id, measurement_time);

/* Update the processed data. */
drop function if exists update_processing_inputs cascade;
CREATE OR REPLACE FUNCTION update_processing_inputs()
  RETURNS void as $$
  refresh materialized view matched_clock_audits;
  refresh materialized view processed_observations;
  refresh materialized view calibration_values;
  refresh materialized view conversion_efficiencies;
  refresh materialized view freezing_clusters;
$$ language sql;

drop function if exists update_processing cascade;
CREATE OR REPLACE FUNCTION update_processing(int, text, timestamp, timestamp)
  RETURNS void as $$
  delete
    from _processed_measurements
   where measurement_type_id=any(get_data_source_ids($1, $2))
     and ($3 is null or measurement_time>=$3)
     and ($4 is null or measurement_time<=$4);
  insert into _processed_measurements
  select *
    from process_measurements(get_data_source_ids($1, $2), $3, $4);
$$ language sql;

drop function if exists update_processing_outputs cascade;
CREATE OR REPLACE FUNCTION update_processing_outputs()
  RETURNS void as $$
  refresh materialized view processed_measurements;
  refresh materialized view hourly_measurements;
$$ language sql;

drop function if exists update_all cascade;
CREATE OR REPLACE FUNCTION update_all(timestamp, timestamp)
  RETURNS void as $$
  select update_processing_inputs();
  select update_processing(site_id, name, $1, $2)
    from data_sources;
  select update_processing_outputs();
$$ language sql;
