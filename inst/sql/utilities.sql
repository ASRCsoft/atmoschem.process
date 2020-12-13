/* Get a time difference in seconds */
CREATE OR REPLACE FUNCTION _time_diff_seconds(t1 time, t2 time) RETURNS double precision AS $$
  SELECT EXTRACT(EPOCH FROM (t1 - t2));
$$ LANGUAGE sql IMMUTABLE;

CREATE TYPE timerange AS RANGE (
  subtype = time,
  subtype_diff = _time_diff_seconds
);

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
