/* Get a time difference in seconds */
CREATE OR REPLACE FUNCTION _time_diff_seconds(t1 time, t2 time) RETURNS double precision AS $$
  SELECT EXTRACT(EPOCH FROM (t1 - t2));
$$ LANGUAGE sql IMMUTABLE;

CREATE TYPE timerange AS RANGE (
  subtype = time,
  subtype_diff = _time_diff_seconds
);

/* A few functions to help with time ranges */
CREATE AGGREGATE range_union(anyrange) (
  stype = anyrange,
  sfunc = range_union,
  PARALLEL = SAFE
);

CREATE AGGREGATE range_intersect(anyrange) (
  stype = anyrange,
  sfunc = range_intersect,
  PARALLEL = SAFE
);
