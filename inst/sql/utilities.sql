/* Get a time difference in seconds */
CREATE OR REPLACE FUNCTION _time_diff_seconds(t1 time, t2 time) RETURNS double precision AS $$
  SELECT EXTRACT(EPOCH FROM (t1 - t2));
$$ LANGUAGE sql IMMUTABLE;

CREATE TYPE timerange AS RANGE (
  subtype = time,
  subtype_diff = _time_diff_seconds
);

/* create a sourcerow composite data type and supporting functions to
document the origin of a measurement in a data file */
CREATE TYPE sourcerow AS (
  file_time timestamp,
  file_number int,
  file_row int);
CREATE TYPE sourcerange AS RANGE (
  subtype = sourcerow
);
CREATE FUNCTION least_sourcerow(sourcerow, sourcerow) RETURNS sourcerow
  LANGUAGE sql IMMUTABLE CALLED ON NULL INPUT
AS 'SELECT LEAST($1, $2)';
CREATE AGGREGATE min(sourcerow) (
  SFUNC = least_sourcerow,
  STYPE = sourcerow
);

/* estimate the y-value between x0 and x1 using linear interpolation
 */
CREATE OR REPLACE FUNCTION interpolate(x0 numeric, x1 numeric, y0 numeric, y1 numeric, x numeric)
  RETURNS numeric AS $$
  DECLARE
  has_lower_bound boolean;
  has_upper_bound boolean;
  BEGIN
    if x0 is not null and y0 is not null then
      select true
      into has_lower_bound;
    end if;
    if x1 is not null and y1 is not null then
      select true
      into has_upper_bound;
    end if;
    -- Return the appropriate estimate
    if has_lower_bound and has_upper_bound then
      -- interpolate
      return y0 + (x - x0) * (y1 - y0) / (x1 - x0);
    elsif has_lower_bound then
      -- go with the lowest value
      return y0;
    elsif has_upper_bound then
      -- go with the highest value
      return y1;
    else
      -- no interpolation to return
      return null;
    end if;
  END;
$$ LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE;

/* overload to allow timestamps as x values */
CREATE OR REPLACE FUNCTION interpolate(t0 timestamp, t1 timestamp, y0 numeric, y1 numeric, t timestamp)
  RETURNS numeric AS $$
  BEGIN
    return interpolate(extract(epoch from t0)::numeric,
		       extract(epoch from t1)::numeric,
		       y0, y1,
		       extract(epoch from t)::numeric);
  END;
$$ LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE;

/* also allow intervals as y values */
CREATE OR REPLACE FUNCTION interpolate(t0 timestamp, t1 timestamp, y0 interval, y1 interval, t timestamp)
  RETURNS interval AS $$
  BEGIN
    return (interpolate(t0, t1,
			EXTRACT(epoch FROM y0)::numeric,
			EXTRACT(epoch FROM y1)::numeric,
			t) || ' seconds')::interval;
  END;
$$ LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE;

/* also allow timestamps as y values */
CREATE OR REPLACE FUNCTION interpolate(t0 timestamp, t1 timestamp, y0 timestamp, y1 timestamp, t timestamp)
  RETURNS timestamp AS $$
  BEGIN
    -- subtract x-value times while doing calculations to keep the
    -- numbers from getting too large
    return interpolate(t0, t1, y0 - t0, y1 - t1, t) + t;
  END;
$$ LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE;

/* Delete a value from an array at the given index */
CREATE OR REPLACE FUNCTION remove_at(arr numeric[], n int)
  RETURNS numeric[] AS $$
  select arr[:(n - 1)] || arr[(n + 1):];
$$ LANGUAGE sql;


/* Inefficient but flexible median aggregate */
CREATE OR REPLACE FUNCTION median_finalfn(numeric[]) RETURNS double precision AS $$
  select percentile_cont(.5) within group (order by v)
    from unnest($1) v;
$$ LANGUAGE sql IMMUTABLE;

CREATE AGGREGATE median(numeric) (
  stype = numeric[],
  sfunc = array_append,
  finalfunc = median_finalfn,
  initcond = '{}',
  PARALLEL = SAFE
);

/* Efficient but currently inflexible median filter */
CREATE OR REPLACE FUNCTION runmed_transfn(internal, double precision)
RETURNS internal
AS 'median', 'median_transfn'
LANGUAGE c IMMUTABLE parallel safe;

CREATE OR REPLACE FUNCTION runmed_invtransfn(internal, double precision)
  RETURNS internal
AS 'median', 'median_invtransfn'
LANGUAGE c IMMUTABLE parallel safe;

CREATE OR REPLACE FUNCTION runmed_finalfn(internal)
RETURNS double precision
AS 'median', 'median_finalfn'
LANGUAGE c IMMUTABLE parallel safe;

CREATE AGGREGATE runmed(double precision) (
  stype = internal,
  sfunc = runmed_transfn,
  finalfunc = runmed_finalfn,
  mstype = internal,
  msfunc = runmed_transfn,
  minvfunc = runmed_invtransfn,
  mfinalfunc = runmed_finalfn,
  PARALLEL = SAFE
);

/* Efficient running MAD */
CREATE OR REPLACE FUNCTION runmad_transfn(internal, double precision)
RETURNS internal
AS 'median', 'mad_transfn'
LANGUAGE c IMMUTABLE parallel safe;

CREATE OR REPLACE FUNCTION runmad_invtransfn(internal, double precision)
  RETURNS internal
AS 'median', 'mad_invtransfn'
LANGUAGE c IMMUTABLE parallel safe;

CREATE OR REPLACE FUNCTION runmad_finalfn(internal)
RETURNS double precision
AS 'median', 'mad_finalfn'
LANGUAGE c IMMUTABLE parallel safe;

CREATE AGGREGATE runmad(double precision) (
  stype = internal,
  sfunc = runmad_transfn,
  finalfunc = runmad_finalfn,
  mstype = internal,
  msfunc = runmad_transfn,
  minvfunc = runmad_invtransfn,
  mfinalfunc = runmad_finalfn,
  PARALLEL = SAFE
);
