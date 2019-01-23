/* Campbell datalogger files */

-- I might do this later but not for now
/*
create table campbell_common (
  file text not null,
  timestamp timestamp not null,
  record int,
  no_avg numeric,
  no2_avg numeric,
  ozone_avg numeric,
  co_avg numeric,
  so2_avg numeric,
  t_avg numeric,
  rh_avg numeric,
  bp_avg numeric,
  ptemp_c_avg numeric,
  batt_volt_avg numeric,
  f_co_avg numeric,
  f_nox_avg numeric
);
*/

create table campbell_wfms (
  file text not null,
  instrument_time timestamp primary key,
  record int,
  no_avg numeric,
  no2_avg numeric,
  noy_avg numeric,
  ozone_avg numeric,
  co_avg numeric,
  so2_avg numeric,
  lamp_avg numeric,
  t_avg numeric,
  rh_avg numeric,
  ws3cup_avg numeric,
  ws_ms_avg numeric,
  winddir_avg numeric,
  ws_ms_s_wvt numeric,
  winddir_d1_wvt numeric,
  winddir_sd1_wvt numeric,
  ws3cup_max numeric,
  ws3cupb_avg numeric,
  ws3cupb_s_wvt numeric,
  ws3cupb_max numeric,
  bp_avg numeric,
  slrw_avg numeric,
  ws_ms_max numeric,
  rain_mm_tot numeric,
  ptemp_c_avg numeric,
  batt_volt_avg numeric,
  f_co_avg numeric,
  f_noy_avg numeric,
  f_so2_avg numeric,
  f_nox_avg numeric,
  f_trh_avg numeric,
  f_wind_avg numeric
);
SELECT create_hypertable('campbell_wfms', 'instrument_time');

create table campbell_wfml (
  file text not null,
  instrument_time timestamp primary key,
  record int,
  no_avg numeric,
  no2_avg numeric,
  nox_avg numeric,
  bp2_avg numeric,
  ozone_avg numeric,
  co_avg numeric,
  so2_avg numeric,
  methane_avg numeric,
  nmhc_avg numeric,
  t_avg numeric,
  rh_avg numeric,
  ws_avg numeric,
  wd_avg numeric,
  mean_wind_speed numeric,
  vector_mean_dir numeric,
  sd_wind_dir numeric,
  bp_avg numeric,
  pm25_avg numeric,
  ngn3_avg numeric,
  ptemp_c_avg numeric,
  batt_volt_avg numeric,
  f_nox_avg numeric,
  f_co_avg numeric,
  f_ngn_avg numeric
);
SELECT create_hypertable('campbell_wfml', 'instrument_time');


/* Process the WFMS campbell data */
CREATE materialized VIEW processed_campbell_wfms AS
  select time,
	 no,
	 -- ugh need to add source to campbell tables for manual flag
	 -- matching
	 is_flagged('NO', 1, null, time,
		    no, f_nox_avg::text, median_no::numeric,
		    mad_no::numeric) as no_flagged,
	 no2,
	 is_flagged('NO2', 1, null, time,
		    no, f_nox_avg::text, median_no::numeric,
		    mad_no::numeric) as no2_flagged
    from (select instrument_time as time,
	         -- collect rolling medians and MADs for flagging
		 no,
		 median(no) over w as median_no,
		 mad(no) over w as mad_no,
		 no2,
		 median(no2) over w as median_no2,
		 mad(no2) over w as mad_no2,
		 f_nox_avg
	    from (select *,
		         -- calibrate everything
			 apply_calib(1, 'NO', no_avg, instrument_time) as no,
			 -- these two need to be calibrated when
			 -- manual calibrations are added
			 no2_avg as no2,
			 -- noy_avg as noy,
			 ozone_avg as ozone,
			 apply_calib(1, 'CO', co_avg, instrument_time) as co,
			 apply_calib(1, 'SO2', so2_avg, instrument_time) as so2,
			 t_avg
		    from campbell_wfms
		   limit 50000) c1
	  WINDOW w AS (ORDER BY instrument_time
		       rows between 20 preceding and 20 following)) c2;
