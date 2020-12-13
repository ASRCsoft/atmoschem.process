/* Storing/organizing/calculating processing results. */

/* Update the processed data. */
drop function if exists update_processing_inputs cascade;
CREATE OR REPLACE FUNCTION update_processing_inputs(site int, data_source text,
						    starttime timestamp,
						    endtime timestamp)
  RETURNS void as $$
  refresh materialized VIEW flagged_periods;
$$ language sql;
