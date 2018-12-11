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
