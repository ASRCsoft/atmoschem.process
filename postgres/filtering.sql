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
  LANGUAGE c IMMUTABLE;

CREATE OR REPLACE FUNCTION runmed_invtransfn(internal, double precision)
  RETURNS internal
AS '/home/wmay/nysatmoschem/src/median', 'median_invtransfn'
LANGUAGE c IMMUTABLE;

CREATE OR REPLACE FUNCTION runmed_finalfn(internal)
RETURNS double precision
AS '/home/wmay/nysatmoschem/src/median', 'median_finalfn'
LANGUAGE c IMMUTABLE;

CREATE AGGREGATE runmed(double precision) (
  stype = internal,
  sfunc = runmed_transfn,
  finalfunc = runmed_finalfn,
  mstype = internal,
  msfunc = runmed_transfn,
  minvfunc = runmed_invtransfn,
  mfinalfunc = runmed_finalfn
);
