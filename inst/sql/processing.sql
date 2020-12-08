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
  select update_power_outages($1, $3, $4);
  refresh materialized VIEW flagged_periods;
$$ language sql;
