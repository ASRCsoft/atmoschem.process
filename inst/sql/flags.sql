/* Determing flag values. */

-- gather all flagged calibration periods
CREATE or replace VIEW _flagged_periods AS
  select measurement_type_id,
	 times
    from manual_calibrations
   union
  select measurement_type_id,
	 times
    from scheduled_autocals;

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
