/* Tables and functions for applying various filters to instrument
   data. */

create table valid_ranges (
  site int,
  measurement text,
  range numrange not null,
  primary key(site, measurement)
);

COPY valid_ranges
  FROM '/home/wmay/data/metadata/valid_ranges.csv' DELIMITER ',' CSV HEADER;

create or replace function remove_extreme_values(int, text, numeric) RETURNS numeric AS $$
  SELECT case when $3 <@ (select range
			    from valid_ranges
			   where valid_ranges.site=$1
			     and valid_ranges.measurement=$2)
	 then $3
	 else null end;
$$ LANGUAGE SQL;

create or replace function arr_median(arr numeric[]) RETURNS double precision AS $$
  select percentile_cont(.5) WITHIN GROUP (ORDER BY a)
    from unnest(arr) a;
$$ LANGUAGE SQL;

create or replace function array_remove(arr numeric[], n numeric) RETURNS numeric[] AS $$
  select remove_at(arr, array_position(arr, n));
$$ LANGUAGE SQL;

CREATE AGGREGATE median(numeric) (
  sfunc = array_append,
  stype = numeric[],
  finalfunc = arr_median,
  initcond = '{}',
  msfunc = array_append,
  minvfunc = array_remove,
  mstype = numeric[],
  MFINALFUNC = arr_median,
  minitcond = '{}'
);

/* Get the median absolute deviation from an array of numbers. This
function is more than 10 times faster when written in plpgsql but I
don't know why. */
create or replace function arr_mad(arr numeric[]) RETURNS double precision AS $$
  declare
  arr_med double precision = arr_median(arr);
  begin
    return percentile_cont(.5) WITHIN GROUP (ORDER BY abs_dev)
      from (select abs(a - arr_med) as abs_dev
	      from unnest(arr) a) b;
  end;
$$ LANGUAGE plpgsql;

CREATE AGGREGATE mad(numeric) (
  sfunc = array_append,
  stype = numeric[],
  finalfunc = arr_mad,
  initcond = '{}',
  msfunc = array_append,
  minvfunc = array_remove,
  mstype = numeric[],
  MFINALFUNC = arr_mad,
  minitcond = '{}'
);
