/* A few notes on how the processing is organized:

I found that the processing is bizarrely, shockingly slow if I attempt
to do it all in one query, but much faster if I split the processing
into multiple queries. To keep that speed in a single query I used
common table expressions (CTEs), which serve as optimization fences so
that postgres optimizes each query section separately.

I also split the processing by data source so that a new data file
does not require reprocessing all data. The `update_processing`
function will update the processing for a single data source. */

/* Apply calibration adjustments if needed, using functions from
   calibration.sql. */
create or replace view calibrated_measurements as
  select c.measurement_type_id,
	 instrument_time,
	 record,
	 value,
	 case when apply_ce then apply_calib(c.measurement_type_id, value, instrument_time) /
		interpolate_ce(c.measurement_type_id, instrument_time)
	 when has_calibration then apply_calib(c.measurement_type_id, value, instrument_time)
	 else value end as calibrated_value,
	 flagged,
	 valid_range,
	 mdl,
	 remove_outliers,
	 max_jump
    from measurements c
	   left join measurement_types m
	       on c.measurement_type_id=m.id
   where apply_processing;

CREATE OR REPLACE FUNCTION process_measurements(measurement_type_ids int[])
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
create table _processed_measurements (
  measurement_type_id int references measurement_types,
  measurement_time timestamp,
  value numeric,
  flagged boolean,
  primary key(measurement_type_id, measurement_time)
);

CREATE OR REPLACE FUNCTION get_data_source_ids(int, text)
  RETURNS int[] as $$
  select array_agg(id)
    from measurement_types
   where site_id=$1
     and data_source=$2;
$$ language sql STABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION update_processing(int, text)
  RETURNS void as $$
  delete
    from _processed_measurements
   where measurement_type_id=any(get_data_source_ids($1, $2));
  insert into _processed_measurements
  select *
    from process_measurements(get_data_source_ids($1, $2));
$$ language sql;

/* Add derived measurements to the processed measurements. */
CREATE OR REPLACE FUNCTION get_measurement_id(int, text)
  RETURNS int as $$
  select id
    from measurement_types
   where site_id=$1
     and name=$2;
$$ language sql STABLE PARALLEL SAFE;

-- join two measurement types to make it easy to derive values from
-- multiple measurements
CREATE OR REPLACE FUNCTION combine_measures(measurement_type_id1 int, measurement_type_id2 int)
  RETURNS TABLE (
    measurement_time timestamp,
    value1 numeric,
    value2 numeric,
    flagged1 boolean,
    flagged2 boolean
  ) as $$
  select m1.measurement_time,
	 m1.value,
	 m2.value,
	 coalesce(m1.flagged, false),
	 coalesce(m2.flagged, false)
    from (select *
	    from _processed_measurements
	   where measurement_type_id=$1) m1
	   join
	   (select *
	      from _processed_measurements
	     where measurement_type_id=$2) m2
	       on m1.measurement_time=m2.measurement_time;
$$ language sql;

create or replace view wfms_no2 as
  select measurement_type_id,
	 measurement_time,
	 value,
	 flagged or
	   is_outlier(value, runmed(value) over w,
		      runmad(value) over w) as flagged
    from (select get_measurement_id(1, 'NO2') as measurement_type_id,
		 measurement_time,
		 (value2 - value1) /
		   interpolate_ce(get_measurement_id(1, 'NOx'),
				  measurement_time) as value,
		 flagged1 or flagged2 as flagged
	    from combine_measures(get_measurement_id(1, 'NO'),
				  get_measurement_id(1, 'NOx'))) cm1
	   WINDOW w AS (partition by measurement_type_id
			ORDER BY measurement_time
			rows between 120 preceding and 120 following);

create or replace view wfms_slp as
  select get_measurement_id(1, 'SLP'),
	 measurement_time,
	 value1 *
	   (1 - .0065 * 1483.5 /
	   (value2 + .0065 * 1483.5 + 273.15))^(-5.257) as value,
	 flagged1 or flagged2 as flagged
    from combine_measures(get_measurement_id(1, 'BP'),
			  get_measurement_id(1, 'PTemp_C'));

create or replace view wfms_ws as
  select get_measurement_id(1, 'WS'),
	 measurement_time,
	 greatest(case when not flagged1 then value1
		  else null end,
		  case when not flagged2 then value2
		  else null end) as value,
	 flagged1 and flagged2 as flagged
    from combine_measures(get_measurement_id(1, 'WS3Cup'),
			  get_measurement_id(1, 'WS3CupB'));

-- u and v vector wind speeds
create or replace view wfms_ws_components as
  with wswd as (select ws1.measurement_time as measurement_time,
		       ws1.value as ws,
		       pi() * (270 - wd1.value) / 180 as theta,
		       ws1.flagged or wd1.flagged as flagged
		  from wfms_ws ws1
  			 join (select *
				 from _processed_measurements
				where measurement_type_id=get_measurement_id(1, 'WindDir_D1_WVT')) wd1
			     on ws1.measurement_time=wd1.measurement_time)
  select get_measurement_id(1, 'WS_u'),
	 measurement_time,
	 ws * sin(theta) as value,
	 flagged
    from wswd
   union
  select get_measurement_id(1, 'WS_v'),
	 measurement_time,
	 ws * cos(theta) as value,
	 flagged
    from wswd;

create or replace view wfms_ws_max as
  select get_measurement_id(1, 'WS_Max'),
	 measurement_time,
	 greatest(case when not flagged1 then value1
		  else null end,
		  case when not flagged2 then value2
		  else null end) as value,
	 flagged1 and flagged2 as flagged
    from combine_measures(get_measurement_id(1, 'WS3Cup_Max'),
			  get_measurement_id(1, 'WS3CupB_Max'));

/* Combine all processed data. */
CREATE materialized VIEW processed_measurements as
  select * from _processed_measurements
   union
  select * from wfms_no2
   union
  select * from wfms_slp
   union
  select * from wfms_ws
   union
  select * from wfms_ws_max
   union
  select * from wfms_ws_components;
create index processed_measurements_idx on processed_measurements(measurement_type_id, measurement_time);

/* Aggregate the processed data by hour using a function from
   flags.sql. */
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
CREATE OR REPLACE FUNCTION update_all()
  RETURNS void as $$
  refresh materialized view calibration_values;
  refresh materialized view conversion_efficiencies;
  refresh materialized view freezing_clusters;
  select update_processing(site_id, data_source)
    from (select distinct site_id,
			  data_source
	    from measurement_types) m1;
  refresh materialized view processed_measurements;
  refresh materialized view hourly_measurements;
$$ language sql;
