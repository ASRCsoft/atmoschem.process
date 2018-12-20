/* Get a time difference in seconds */
CREATE OR REPLACE FUNCTION _time_diff_seconds(t1 time, t2 time) RETURNS double precision AS $$
  SELECT EXTRACT(EPOCH FROM (t1 - t2));
$$ LANGUAGE sql IMMUTABLE;

CREATE TYPE timerange AS RANGE (
  subtype = time,
  subtype_diff = _time_diff_seconds
);

/* create a sourcerow composite data type, and corresponding
   sourcerange range type, for selecting intervals of source data */
CREATE TYPE sourcerow AS (date date, n int, row int);
CREATE TYPE sourcerange AS RANGE (
  subtype = sourcerow
);

/* estimate the y-value between x0 and x1 using linear interpolation
*/
CREATE OR REPLACE FUNCTION interpolate(t0 timestamp, t1 timestamp, y0 numeric, y1 numeric, t timestamp)
  RETURNS numeric AS $$
  DECLARE
  has_lower_bound boolean;
  has_upper_bound boolean;
  BEGIN
    if t0 is not null and y0 is not null then
      select true
      into has_lower_bound;
    end if;
    if t1 is not null and y1 is not null then
      select true
      into has_upper_bound;
    end if;
    -- Return the appropriate estimate
    if has_lower_bound and has_upper_bound then
      -- interpolate
      return y0 + extract(epoch from (t - t0)) * (y1 - y0) / extract(epoch from (t1 - t0));
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
$$ LANGUAGE plpgsql;
