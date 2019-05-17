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
   where files.data_source_id=(select id
				 from data_sources
				where site_id=3
				  and name='envidas');

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
-- CREATE MATERIALIZED VIEW matched_clock_audits as
--   select *,
-- 	 case when instrument='EPC'
-- 	 then (select min(source)
-- 		 from ultrafine
-- 		where site_id=3
-- 		  and date_trunc('minute', ultrafine.instrument_time)=clock_audits.instrument_time)
-- 	 else (select min(source)
-- 		 from envidas
-- 		where site_id=3
-- 		  and date_trunc('minute', envidas.instrument_time)=clock_audits.instrument_time) end as data_source
--     from clock_audits
--    where instrument in ('EPC', 'DRDAS PC');


-- /* Correct ultrafine instrument time using linear interpolation */
-- CREATE OR REPLACE FUNCTION correct_instrument_time(site_id int, instrument text, sr sourcerow, t timestamp)
--   RETURNS timestamp AS $$
--   #variable_conflict use_variable
--   DECLARE
--   matching_audit_time timestamp;
--   x0 timestamp;
--   x1 timestamp;
--   y0 interval;
--   y1 interval;
--   BEGIN
--     -- only applies to PSP for now
--     if site_id!=3 then
--       return t;
--     end if;
--     -- 1) Find the closest clock audits.
--     select audit_time
--       from matched_clock_audits
--      where matched_clock_audits.instrument=instrument
--        and data_source=sr
--       into matching_audit_time;
--     if found then
--       return matching_audit_time;
--     end if;
--     -- if the clock error was corrected, then the new instrument_time is
--     -- the same as the audit_time and the new clock error is zero. If it
--     -- wasn't corrected then the instrument_time and clock errors don't
--     -- change.
--     select case when corrected then audit_time
--            else instrument_time end,
--            case when corrected then '0'
--            else audit_time - instrument_time end
--       from matched_clock_audits
--      where matched_clock_audits.instrument=instrument
--        and data_source<sr
--      order by data_source desc
--      limit 1
--       into x0, y0;
--     select instrument_time,
-- 	   audit_time - instrument_time
--       from matched_clock_audits
--      where matched_clock_audits.instrument=instrument
--        and data_source>sr
--      order by data_source asc
--      limit 1
--       into x1, y1;
--     return t + interpolate(x0, x1, y0, y1, t);
--   END;
-- $$ LANGUAGE plpgsql STABLE PARALLEL SAFE;
