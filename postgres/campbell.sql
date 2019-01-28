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


CREATE or replace VIEW campbell_wfms_medians AS
  select *,
  -- get medians as needed
	 runmed(no) over w as no_med,
	 runmed(no2) over w as no2_med,
	 runmed(noy) over w as noy_med,
	 runmed(ozone) over w as ozone_med,
	 runmed(co) over w as co_med,
	 runmed(so2) over w as so2_med,
	 runmed(temp) over w as temp_med,
	 runmed(rh) over w as rh_med,
	 runmed(bp) over w as bp_med
    from (select *,
	  -- calibrate everything
		 apply_calib(1, 'NO', no_avg, instrument_time) as no,
	  -- these two need to be calibrated when
	  -- manual calibrations are added
		 no2_avg as no2,
		 noy_avg as noy,
		 ozone_avg as ozone,
		 apply_calib(1, 'CO', co_avg, instrument_time) as co,
		 apply_calib(1, 'SO2', so2_avg, instrument_time) as so2,
		 t_avg as temp,
		 rh_avg as rh,
		 -- get the max recorded wind speed
		 ws3cup_avg as ws,
		 winddir_avg % 360 as wd,
		 winddir_d1_wvt % 360 as wd_v,
		 ws3cup_max as ws_max,
		 bp_avg as bp
	    from campbell_wfms) c1
	   WINDOW w AS (ORDER BY instrument_time
			rows between 7 preceding and 7 following);

CREATE or replace VIEW campbell_wfms_mad AS
  select *,
  -- calculate Median Absolute Deviations
	 runmed(no - c1.no_med) over w as no_mad,
	 runmed(no2 - c1.no2_med) over w as no2_mad,
	 runmed(noy - c1.noy_med) over w as noy_mad,
	 runmed(ozone - c1.ozone_med) over w as ozone_mad,
	 runmed(co - c1.co_med) over w as co_mad,
	 runmed(so2 - c1.so2_med) over w as so2_mad,
	 runmed(temp - c1.temp_med) over w as temp_mad,
	 runmed(rh - c1.rh_med) over w as rh_mad,
	 runmed(bp - c1.bp_med) over w as bp_mad
    from campbell_wfms_medians c1
	   WINDOW w AS (ORDER BY instrument_time
			rows between 7 preceding and 7 following);

/* Get the fully processed WFMS campbell data */
CREATE materialized VIEW processed_campbell_wfms as
  select instrument_time as time,
	 no,
  -- ugh need to add source to campbell tables for manual flag
  -- matching
	 is_flagged('NO', 1, null, instrument_time,
		    no, f_nox_avg::text, no_med,
		    no_mad) as no_flagged,
	 no2,
	 is_flagged('NO2', 1, null, instrument_time,
		    no2, f_nox_avg::text, no2_med,
		    no2_mad) as no2_flagged,
	 noy,
	 is_flagged('NOy', 1, null, instrument_time,
		    noy, f_noy_avg::text, noy_med,
		    noy_mad) as noy_flagged,
	 ozone,
	 is_flagged('Ozone', 1, null, instrument_time,
		    ozone, null, ozone_med,
		    ozone_mad) as ozone_flagged,
	 co,
	 is_flagged('CO', 1, null, instrument_time,
		    co, f_co_avg::text, co_med,
		    co_mad) as co_flagged,
	 so2,
	 is_flagged('SO2', 1, null, instrument_time,
		    so2, f_so2_avg::text, so2_med,
		    so2_mad) as so2_flagged,
	 temp,
	 is_flagged('Temp', 1, null, instrument_time,
		    temp, f_trh_avg::text, temp_med,
		    temp_mad) as temp_flagged,
	 rh,
	 is_flagged('RH', 1, null, instrument_time,
		    rh, f_trh_avg::text, rh_med,
		    rh_mad) as rh_flagged,
	 ws,
	 is_flagged('WS', 1, null, instrument_time,
		    ws, f_wind_avg::text,
		    null, null) as ws_flagged,
	 wd,
	 is_flagged('WD', 1, null, instrument_time,
		    wd, f_wind_avg::text,
		    null, null) as wd_flagged,
	 wd_v,
	 is_flagged('WD_V', 1, null, instrument_time,
		    wd_v, null, null, null) as wd_v_flagged,
	 ws_max,
	 is_flagged('WS_MAX', 1, null, instrument_time,
		    ws_max, f_wind_avg::text,
		    null, null) as ws_max_flagged,
	 bp,
	 is_flagged('BP', 1, null, instrument_time,
		    bp, null, bp_med,
		    bp_mad) as bp_flagged
    from campbell_wfms_mad;
