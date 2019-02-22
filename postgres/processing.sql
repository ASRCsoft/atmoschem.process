/* A few notes on how the processing is organized:

I found that the processing is bizarrely, shockingly slow if I attempt
to do it all in one query. Therefore I split the following into
materialized steps so that processing can be done one step at a time.

I also split the processing by data source so that a new data file
does not require reprocessing all data. Therefore there are separate
materialized views for each source.

I've tried to keep the code as concise as possible within these
constraints. */

/* 1) Calibration.

These views apply calibration adjustments if needed. They rely on
functions from calibration.sql. */
CREATE or replace VIEW calibrated_measurements as
  select c.site_id,
	 instrument_time,
	 record,
	 c.measurement,
	 value,
	 case when has_calibration then apply_calib(c.site_id, c.measurement, value, instrument_time)
	 else value end as calibrated_value,
	 flagged,
	 valid_range,
	 mdl,
	 remove_outliers
    from measurements c
	   left join measurement_types m
	       on c.site_id=m.site_id
	       and c.measurement=m.measurement;

CREATE materialized VIEW calibrated_campbell_wfms as
  select *
    from calibrated_measurements
   where site_id=1;
create index calibrated_campbell_wfms_idx on calibrated_campbell_wfms(site_id, measurement, instrument_time);

CREATE materialized VIEW calibrated_campbell_wfml as
  select *
    from calibrated_measurements
   where site_id=2;
create index calibrated_campbell_wfml_idx on calibrated_campbell_wfml(site_id, measurement, instrument_time);

/* 2) Running medians.

These views calculate running medians and running Median Absolute
Deviations (MAD). They rely on functions from filtering.sql. These
numbers are used to check for outliers. */
CREATE materialized VIEW campbell_medians_wfms AS
  select *,
	 case when remove_outliers then runmed(calibrated_value) over w
	 else null end as running_median,
	 case when remove_outliers then runmad(calibrated_value) over w
	 else null end as running_mad
    from calibrated_campbell_wfms
  WINDOW w AS (partition by site_id, measurement
	       ORDER BY instrument_time
	       rows between 120 preceding and 120 following);
create index campbell_medians_wfms_idx on campbell_medians_wfms(site_id, measurement, instrument_time);

CREATE materialized VIEW campbell_medians_wfml AS
  select *,
	 case when remove_outliers then runmed(calibrated_value) over w
	 else null end as running_median,
	 case when remove_outliers then runmad(calibrated_value) over w
	 else null end as running_mad
    from calibrated_campbell_wfml
  WINDOW w AS (partition by site_id, measurement
	       ORDER BY instrument_time
	       rows between 120 preceding and 120 following);
create index campbell_medians_wfml_idx on campbell_medians_wfml(site_id, measurement, instrument_time);

/* 3) QC checks.

These views check for flag conditions. They rely on functions from
flags.sql. The end result is the fully processed data. */
CREATE materialized VIEW processed_campbell_wfms as
  select site_id,
	 instrument_time as time,
	 measurement,
	 calibrated_value as value,
	 is_flagged(measurement, site_id, null, instrument_time,
		    calibrated_value, flagged, running_median,
		    running_mad) as flagged
    from campbell_medians_wfms;
create index processed_campbell_wfms_idx on processed_campbell_wfms(site_id, measurement, time);

CREATE materialized VIEW processed_campbell_wfml as
  select site_id,
	 instrument_time as time,
	 measurement,
	 calibrated_value as value,
	 is_flagged(measurement, site_id, null, instrument_time,
		    calibrated_value, flagged, running_median,
		    running_mad) as flagged
    from campbell_medians_wfml;
create index processed_campbell_wfml_idx on processed_campbell_wfml(site_id, measurement, time);

/* 4) Hourly aggragates.

This view aggregates the processed data by hour. It relies on a
function from flags.sql. */
CREATE materialized VIEW hourly_campbell_wfms as
  select site_id,
	 time,
	 measurement,
	 value,
	 get_hourly_flag(site_id, measurement, value, n_values::int) as flag
    from (select site_id,
		 time_bucket('1 hour', time) as time,
		 measurement,
		 avg(value) FILTER (WHERE not flagged) as value,
		 count(value) FILTER (WHERE not flagged) as n_values
	    from processed_campbell_wfms
	   group by site_id, measurement, time_bucket('1 hour', time)) c1;
create index hourly_campbell_wfms_idx on hourly_campbell_wfms(site_id, measurement, time);

CREATE materialized VIEW hourly_campbell_wfml as
  select site_id,
	 time,
	 measurement,
	 value,
	 get_hourly_flag(site_id, measurement, value, n_values::int) as flag
    from (select site_id,
		 time_bucket('1 hour', time) as time,
		 measurement,
		 avg(value) FILTER (WHERE not flagged) as value,
		 count(value) FILTER (WHERE not flagged) as n_values
	    from processed_campbell_wfml
	   group by site_id, measurement, time_bucket('1 hour', time)) c1;
create index hourly_campbell_wfml_idx on hourly_campbell_wfml(site_id, measurement, time);

/* To update the processed data, simply need to refresh the relevant materialized views. For example, to update WFMS campbell results:
refresh materialized view calibration_values;
refresh materialized view calibrated_campbell_wfms;
refresh materialized view campbell_medians_wfms;
refresh materialized view processed_campbell_wfms;
refresh materialized view hourly_campbell_wfms; */
