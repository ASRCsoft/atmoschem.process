/* Functions for correcting instrument clock errors. */

/* observations with source file name included */
create or replace view observations2 as
  select obs.id,
	 files.data_source_id,
	 files.name as fname,
	 line,
	 time
    from observations obs
	   join files
	       on obs.file_id=files.id
	   join data_sources ds
	       on files.data_source_id=ds.id
   where ds.site_id=3
     and ds.name='envidas';

/* Get info about the chronological order of files from file names */
create or replace function get_psp_envidas_file_time(f text)
  RETURNS timestamp AS $$
  select to_timestamp(substring(f from 10 for 4), 'YYMM')::timestamp;
$$ LANGUAGE sql STABLE PARALLEL SAFE;

create or replace function get_psp_envidas_file_index(f text)
  RETURNS int AS $$
  select coalesce(substring(substring(f from 14) from '[0-9]+')::int,
		  1);
$$ LANGUAGE sql STABLE PARALLEL SAFE;

/* observations with sourcerow included to allow chronological
   ordering of observations using file information */
create or replace view observation_sourcerows as
  select id,
	 data_source_id,
	 time,
	 (file_time, tiebreaker, line)::sourcerow as source_row
    from (select *,
		 get_psp_envidas_file_time(fname) as file_time,
		 get_psp_envidas_file_index(fname) as tiebreaker
	    from observations2) obs1;

/* Match clock audits with the corresponding file/row from the raw
   data */
CREATE MATERIALIZED VIEW matched_clock_audits as
  select data_source_id,
	 min(source_row) as source_row,
	 data_source_time,
	 audit_time,
	 corrected
    from (select ca.*,
		 source_row
	    from observation_sourcerows obs
		   join clock_audits ca
		       on date_trunc('minute', obs.time)=ca.data_source_time
		   and obs.data_source_id=ca.data_source_id) ca1
   group by data_source_id, data_source_time, audit_time, corrected;

/* Correct PSP file time using linear interpolation */
CREATE OR REPLACE FUNCTION correct_time(data_source_in int,
					source_row_in sourcerow,
					t timestamp)
  RETURNS timestamp AS $$
select interpolate(t0, t1, y0, y1, $3)
  from (select case when corrected then audit_time
	       else data_source_time end as t0,
	       audit_time as y0
	  from matched_clock_audits
	 where source_row<=$2
	   and data_source_id=$1
	 order by source_row desc
	 limit 1) ca0
  full outer join
	 (select data_source_time as t1,
		 audit_time as y1
	    from matched_clock_audits
	   where source_row>$2
	     and data_source_id=$1
	   order by source_row asc
	   limit 1) ca1
  on true;
$$ LANGUAGE sql STABLE PARALLEL SAFE;
