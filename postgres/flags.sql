/* Determing flag values. */

CREATE EXTENSION btree_gist;

create table manual_flags (
  measurement text,
  station_id int references stations not null,
  source sourcerange not null,
  explanation text,
  CONSTRAINT overlapping_sources EXCLUDE USING GIST (
    measurement WITH =,
    station_id WITH =,
    source WITH &&
  )
);

create or replace function has_manual_flag(measurement text, station_id int, source sourcerow) RETURNS bool AS $$
  select exists(select *
		  from manual_flags
		 where measurement=$1
		   and station_id=$2
		   and $3 <@ source);
$$ LANGUAGE sql stable parallel safe;

create or replace function has_instrument_flag(measurement text, flag text) RETURNS bool AS $$
  select case when flag is null or flag in ('', 'OK') then false
	 when measurement='ultrafine' then array_length(parse_flags(flag), 1)>0
	 else true end;
$$ LANGUAGE sql immutable parallel safe;

create or replace function has_instrument_flag(measurement text, flag numeric) RETURNS bool AS $$
  select flag!=1;
$$ LANGUAGE sql immutable parallel safe;

create or replace function has_calibration_flag(measurement text, station_id int, measurement_time timestamp) RETURNS bool AS $$
  -- check to see if a measurement occurred in a calibration period,
  -- or immediately after one
  -- (need to add a check for manual calibrations here)
  select exists(select *
		  from autocals
		 where instrument=$1
		   and station_id=$2
		   and $3::date <@ dates
		   and $3::time <@ timerange(lower(times),
					     upper(times) + interval '5 minutes'));
$$ LANGUAGE sql stable parallel safe;

create or replace function is_valid_value(measurement text, station_id int, value numeric) RETURNS bool AS $$
  select coalesce(value <@ (select range
			      from valid_ranges
			     where measurement=$1
			       and site=$2), true);
$$ LANGUAGE sql stable parallel safe;

create or replace function is_outlier(value numeric, median double precision, mad double precision) RETURNS bool AS $$
  -- Decide if a value is an outlier using the method from the Hampel
  -- filter. The term (1.4826 * MAD) estimates the standard
  -- deviation. See
  -- https://en.wikipedia.org/wiki/Median_absolute_deviation#Relation_to_standard_deviation
  select (value - median) / (1.4826 * nullif(mad, 0)) > 3.5;
$$ LANGUAGE sql immutable RETURNS NULL ON NULL INPUT parallel safe;

create or replace function is_below_mdl(station_id int, measurement text, value numeric) RETURNS bool AS $$
  select coalesce(value < (select mdl
			     from mdls
			    where station_id=$1
			      and measurement=$2), false);
$$ LANGUAGE sql stable parallel safe;

/* Determine if a measurement is flagged. */
create or replace function is_flagged(measurement text, station_id int, source sourcerow, measurement_time timestamp, value numeric, flag text, median double precision, mad double precision) RETURNS bool AS $$
  -- steps:
  -- 1) apply manual flags
  select case when has_manual_flag(measurement, station_id, source) then true
	   -- 2) instrument flags
	 when has_instrument_flag(measurement, flag) then true
	   -- 3) calibration flags
	 when has_calibration_flag(measurement, station_id, measurement_time) then true
	   -- 4) extreme value flags
	 when not is_valid_value(measurement, station_id, value) then true
	   -- 5) outlier flags (based on the Hampel filter)
	 when is_outlier(value, median, mad) then true
	   -- 6) no flag
	 else false end;
$$ LANGUAGE sql stable parallel safe;

create or replace function is_flagged(measurement text, station_id int, source sourcerow, measurement_time timestamp, value numeric, flag numeric, median double precision, mad double precision) RETURNS bool AS $$
  -- steps:
  -- 1) apply manual flags
  select case when has_manual_flag(measurement, station_id, source) then true
	   -- 2) instrument flags
	 when has_instrument_flag(measurement, flag) then true
	   -- 3) calibration flags
	 when has_calibration_flag(measurement, station_id, measurement_time) then true
	   -- 4) extreme value flags
	 when not is_valid_value(measurement, station_id, value) then true
	   -- 5) outlier flags (based on the Hampel filter)
	 when is_outlier(value, median, mad) then true
	   -- 6) no flag
	 else false end;
$$ LANGUAGE sql stable parallel safe;

/* Get the NARSTO averaged data flag based on the number of
measurements and average value. */
CREATE OR REPLACE FUNCTION get_hourly_flag(station_id int, measurement text, value numeric, n int) RETURNS text AS $$
  SELECT case when n=0 then 'M1'
	 when is_below_mdl(station_id, measurement, value) then 'V1'
	 when n<45 then 'V4'
	 else 'V0' end;
$$ LANGUAGE sql immutable parallel safe;
