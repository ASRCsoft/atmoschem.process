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
$$ LANGUAGE sql;

create or replace function has_instrument_flag(measurement text, flag text) RETURNS bool AS $$
  select case when flag is null or flag='' then false
	 when measurement='ultrafine' then array_length(parse_flags(flag), 1)>0
	 else flag!='OK' end;
$$ LANGUAGE sql;

create or replace function has_calibration_flag(measurement text, station_id int, measurement_time timestamp) RETURNS bool AS $$
  -- need to add a check for manual calibrations here
  select exists(select *
		  from calibration_values
		 where chemical=$1
		   and station_id=$2
		   and $3 <@ cal_times);
$$ LANGUAGE sql;

create or replace function is_valid_value(measurement text, station_id int, value numeric) RETURNS bool AS $$
  select value <@ coalesce((select range
			      from valid_ranges
			     where measurement=$1
			       and site=$2),
			   '(,)'::numrange);
$$ LANGUAGE sql;

create or replace function is_outlier(value numeric, median numeric, mad numeric) RETURNS bool AS $$
  -- decide if a value is an outlier using the method from the Hampel
  -- filter
  select (value - median) / nullif(mad, 0) > 3;
$$ LANGUAGE sql;

/* Determine if a measurement is flagged. */
create or replace function is_flagged(measurement text, station_id int, source sourcerow, measurement_time timestamp, value numeric, flag text, median numeric, mad numeric) RETURNS bool AS $$
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
$$ LANGUAGE sql;
