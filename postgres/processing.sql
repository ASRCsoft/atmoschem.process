
/* Get calibrated envidas data */
CREATE materialized VIEW calibrated_envidas AS
  select station_id,
	 source,
	 corrected_time as time,
	 apply_calib(station_id, 'NO', no, corrected_time) as no,
	 data_dict->'no_flag' as no_flag
    from (select *,
		 correct_instrument_time(station_id, 'DRDAS PC', source, instrument_time) as corrected_time
	    from envidas) e1;
CREATE INDEX calibrated_envidas_station_time ON calibrated_envidas(station_id, time);


CREATE materialized VIEW filtered_envidas AS
  select station_id,
	 time,
	 no,
	 is_flagged('NO', station_id, source, time,
		    no, no_flag, median_no::numeric,
		    mad_no::numeric) as no_flagged
    from (select station_id,
		 source,
		 time,
		 no,
		 median(no) over w as median_no,
		 mad(no) over w as mad_no,
		 no_flag
	    from calibrated_envidas
	  WINDOW w AS (partition by station_id
		       ORDER BY time
		       rows between 20 preceding and 20 following)) e1;
