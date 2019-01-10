
/* Get calibrated envidas data */
CREATE materialized VIEW calibrated_envidas AS
  select corrected_time as time,
	 apply_calib(station_id, 'NO', no, corrected_time) as no
    from (select *,
		 correct_instrument_time(station_id, 'DRDAS PC', source, instrument_time) as corrected_time
	    from envidas) e1;
