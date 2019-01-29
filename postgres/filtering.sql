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

/* Efficient median filter */
CREATE OR REPLACE FUNCTION runmed_transfn(internal, double precision)
RETURNS internal
AS '/home/wmay/nysatmoschem/src/median', 'median_transfn'
LANGUAGE c IMMUTABLE parallel safe;

CREATE OR REPLACE FUNCTION runmed_invtransfn(internal, double precision)
  RETURNS internal
AS '/home/wmay/nysatmoschem/src/median', 'median_invtransfn'
LANGUAGE c IMMUTABLE parallel safe;

CREATE OR REPLACE FUNCTION runmed_finalfn(internal)
RETURNS double precision
AS '/home/wmay/nysatmoschem/src/median', 'median_finalfn'
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

CREATE TYPE runmad_state AS (
  values double precision[],
  medians double precision[]
);

/* Median Absolute Deviation */
CREATE OR REPLACE FUNCTION runmad_transfn(runmad_state, double precision, double precision)
  RETURNS runmad_state AS $$
  select (array_append($1.values, $2),
	  array_append($1.medians, $3))::runmad_state;
$$ LANGUAGE sql IMMUTABLE parallel safe;

CREATE OR REPLACE FUNCTION runmad_invtransfn(runmad_state, double precision, double precision)
RETURNS runmad_state AS $$
  select ($1.values[2:], $1.medians[2:])::runmad_state;
$$ LANGUAGE sql IMMUTABLE parallel safe;

CREATE OR REPLACE FUNCTION runmad_finalfn(runmad_state)
  RETURNS double precision AS $$
  select percentile_cont(.5) WITHIN GROUP (order by abs_dev) from (select abs(unnest($1.values) - med) as abs_dev from (select $1.medians[ceil(array_length($1.medians, 1)::numeric / 2)] as med) m1) m2;
$$ LANGUAGE sql IMMUTABLE parallel safe;
  
CREATE AGGREGATE runmad(double precision, double precision) (
  stype = runmad_state,
  sfunc = runmad_transfn,
  finalfunc = runmad_finalfn,
  mstype = runmad_state,
  msfunc = runmad_transfn,
  minvfunc = runmad_invtransfn,
  mfinalfunc = runmad_finalfn,
  PARALLEL = SAFE
);
