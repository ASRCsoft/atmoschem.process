
/* Get calibrated envidas data */
CREATE materialized VIEW calibrated_envidas AS
  select station_id,
	 corrected_time as time,
	 apply_calib(station_id, 'NO', no, corrected_time) as no
    from (select *,
		 correct_instrument_time(station_id, 'DRDAS PC', source, instrument_time) as corrected_time
	    from envidas) e1;


CREATE materialized VIEW filtered_envidas AS
  select station_id,
	 time,
	 case when (no - median_no) / mad_no > 3 then null
	 else no end as no
    from (select station_id,
		 time,
		 no,
		 median(no) over w as median_no,
		 mad(no) over w as mad_no
	    from calibrated_envidas
	  WINDOW w AS (partition by station_id
		       ORDER BY time
		       rows between 20 preceding and 20 following)) e1;
/* Definitely need to handle zero MAD values here as there are
sections of faulty data with the same numbered repeated over and
over. */
