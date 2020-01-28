/* Determing flag values. */

create table manual_flags (
  measurement_type_id int references measurement_types,
  times tsrange,
  aqs_flag text not null,
  explanation text,
  primary key(measurement_type_id, times),
  CONSTRAINT no_duplicated_flags EXCLUDE USING GIST (
    measurement_type_id WITH =,
    explanation WITH =,
    times WITH &&
  )
);

create table freezing_clusters (
  measurement_type_id int not null,
  freeze_period tsrange
);
CREATE INDEX freezing_clusters_idx ON freezing_clusters using gist(measurement_type_id, freeze_period);

-- get the anemometer measurement type IDs
create or replace function anemometer_ids() RETURNS int[] AS $$
  select array_agg(id)
    from (select id
	    from measurement_types
	   where name in ('WS3Cup',
			  'WS3Cup_Max',
			  'WS3CupB',
			  'WS3CupB_Max')) m1;
$$ LANGUAGE sql stable parallel safe;

-- get clusters of frozen anemometer times, checking to make sure the
-- temperature is below freezing at the start of the frozen period
-- (currently only works for WFMS)
CREATE OR REPLACE FUNCTION get_freezing_clusters(site int, data_source text,
						 starttime timestamp,
						 endtime timestamp)
  RETURNS TABLE (
    measurement_type_id int,
    freeze_period tsrange
  ) 
AS $$
  with wind_looks_frozen as (
    select measurement_type_id,
	   time,
	   (bit_and(slow_wind::int) over w)::boolean as looks_frozen
      from (select measurement_type_id,
		   time,
		   value < .2 as slow_wind
	      from measurements2
	     where measurement_type_id=any(anemometer_ids())
	       and measurement_type_id in (select id
					     from measurement_types
					    where data_source_id=(select id
								    from data_sources
								   where site_id=$1
								     and name=$2))
	       and time>=$3 - interval '1 day' and time<$4 + interval '1 day') w1
	     window w as (partition by measurement_type_id
			  order by time
			  rows between current row and 30 following)
  ),
  contiguous_freezes as (
    select measurement_type_id,
	   tsrange(min(time) - interval '1 hour',
		   max(time) + interval '40 minutes') as freezing_times
      from (select *,
		   sum(freeze_starts::int) over w as freeze_group
	      from (select *,
			   looks_frozen and not lag(looks_frozen) over w as freeze_starts
		      from wind_looks_frozen
			     window w as (partition by measurement_type_id
					  order by time)) w1
		     window w as (partition by measurement_type_id
				  order by time)) w2
     where looks_frozen
     group by measurement_type_id, freeze_group
  ),
  contiguous_cold_freezes as (
    select f1.measurement_type_id,
	   freezing_times
      from contiguous_freezes f1
  	     join measurement_types mt1
		 on f1.measurement_type_id=mt1.id
	     join measurement_types mt2
		 on mt1.data_source_id=mt2.data_source_id
	     join measurements2 m1
		 on m1.measurement_type_id=mt2.id
		 and m1.time=(lower(freezing_times) + interval '1 hour')
     where mt2.name='T'
       and m1.value<5
  )
  select measurement_type_id,
	 tsrange(min(lower(freezing_times)),
		 max(upper(freezing_times))) as freeze_period
    from (select *,
		 sum(new_freeze::int) over w as freeze_cluster
	    from (select *,
			 coalesce(not freezing_times && lag(freezing_times) over w,
				  true) as new_freeze
		    from contiguous_cold_freezes
			   window w as (partition by measurement_type_id
					order by lower(freezing_times))) w1
		   window w as (partition by measurement_type_id
				order by lower(freezing_times))) w2
   group by measurement_type_id, freeze_cluster;
$$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION update_freezing_clusters(site int, data_source text,
						    starttime timestamp,
						    endtime timestamp)
  RETURNS void as $$
  delete 
    from freezing_clusters
   where measurement_type_id in (select id
				   from measurement_types
				  where data_source_id=(select id
							  from data_sources
							 where site_id=$1
							   and name=$2))
  and freeze_period && tsrange($3, $4);
  insert into freezing_clusters
  select *
    from get_freezing_clusters($1, $2, $3, $4);
$$ language sql;

-- gather all flagged periods
CREATE or replace VIEW _flagged_periods AS
  select measurement_type_id,
	 times
    from manual_calibrations
   union
  select measurement_type_id,
	 times
    from scheduled_autocals
   union
  select measurement_type_id,
	 times
    from manual_flags
   union
  select measurement_type_id,
	 freeze_period as times
    from freezing_clusters;

-- combine overlapping periods
CREATE materialized VIEW flagged_periods AS
  select measurement_type_id,
	 range_union(times) as times
    from (select *,
		 sum(new_period::int) over w as period_number
	    from (select *,
			 isempty(times * lag(times) over w) as new_period
		    from _flagged_periods
			   window w as (partition by measurement_type_id
					order by lower(times))) w1
		   window w as (partition by measurement_type_id
				order by lower(times))) w2
   group by measurement_type_id, period_number;
CREATE INDEX flagged_periods_idx ON flagged_periods using gist(measurement_type_id, times);

create or replace function is_below_mdl(measurement_type_id int, value numeric) RETURNS bool AS $$
  select coalesce(value < (select mdl
			     from measurement_types
			    where id=$1), false);
$$ LANGUAGE sql stable parallel safe;

/* Get the NARSTO averaged data flag based on the number of
measurements and average value. */
CREATE OR REPLACE FUNCTION get_hourly_flag(measurement_type_id int, value numeric, n int) RETURNS text AS $$
  SELECT case when n<30 then 'M1'
	 when is_below_mdl(measurement_type_id, value) then 'V1'
	 when n<45 then 'V4'
	 else 'V0' end;
$$ LANGUAGE sql immutable parallel safe;
