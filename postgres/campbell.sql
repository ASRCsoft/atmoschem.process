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
		 apply_calib(1, 'NO2', no2_avg, instrument_time) as no2,
		 apply_calib(1, 'NOy', noy_avg, instrument_time) as noy,
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
	 runmad(no, no_med) over w as no_mad,
	 runmad(no2, no2_med) over w as no2_mad,
	 runmad(noy, noy_med) over w as noy_mad,
	 runmad(ozone, ozone_med) over w as ozone_mad,
	 runmad(co, co_med) over w as co_mad,
	 runmad(so2, so2_med) over w as so2_mad,
	 runmad(temp, temp_med) over w as temp_mad,
	 runmad(rh, rh_med) over w as rh_mad,
	 runmad(bp, bp_med) over w as bp_mad
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
		    no, f_nox_avg, no_med,
		    no_mad) as no_flagged,
	 no2,
	 is_flagged('NO2', 1, null, instrument_time,
		    no2, f_nox_avg, no2_med,
		    no2_mad) as no2_flagged,
	 noy,
	 is_flagged('NOy', 1, null, instrument_time,
		    noy, f_noy_avg, noy_med,
		    noy_mad) as noy_flagged,
	 ozone,
	 is_flagged('Ozone', 1, null, instrument_time,
		    ozone, null, ozone_med,
		    ozone_mad) as ozone_flagged,
	 co,
	 is_flagged('CO', 1, null, instrument_time,
		    co, f_co_avg, co_med,
		    co_mad) as co_flagged,
	 so2,
	 is_flagged('SO2', 1, null, instrument_time,
		    so2, f_so2_avg, so2_med,
		    so2_mad) as so2_flagged,
	 temp,
	 is_flagged('Temp', 1, null, instrument_time,
		    temp, f_trh_avg, temp_med,
		    temp_mad) as temp_flagged,
	 rh,
	 is_flagged('RH', 1, null, instrument_time,
		    rh, f_trh_avg, rh_med,
		    rh_mad) as rh_flagged,
	 ws,
	 is_flagged('WS', 1, null, instrument_time,
		    ws, f_wind_avg,
		    null, null) as ws_flagged,
	 wd,
	 is_flagged('WD', 1, null, instrument_time,
		    wd, f_wind_avg,
		    null, null) as wd_flagged,
	 wd_v,
	 is_flagged('WD_V', 1, null, instrument_time,
		    wd_v, null, null, null) as wd_v_flagged,
	 ws_max,
	 is_flagged('WS_MAX', 1, null, instrument_time,
		    ws_max, f_wind_avg,
		    null, null) as ws_max_flagged,
	 bp,
	 is_flagged('BP', 1, null, instrument_time,
		    bp, null, bp_med,
		    bp_mad) as bp_flagged
    from campbell_wfms_mad;


CREATE materialized VIEW hourly_campbell_wfms as
  select time,
	 no,
	 get_hourly_flag(1, 'NO', no, n_no::int) as no_flag,
	 no2,
	 get_hourly_flag(1, 'NO2', no2, n_no2::int) as no2_flag,
	 noy,
	 get_hourly_flag(1, 'NOY', noy, n_noy::int) as noy_flag,
	 ozone,
	 get_hourly_flag(1, 'OZONE', ozone, n_ozone::int) as ozone_flag,
	 co,
	 get_hourly_flag(1, 'CO', co, n_co::int) as co_flag,
	 so2,
	 get_hourly_flag(1, 'SO2', so2, n_so2::int) as so2_flag,
	 temp,
	 get_hourly_flag(1, 'TEMP', temp, n_temp::int) as temp_flag,
	 rh,
	 get_hourly_flag(1, 'RH', rh, n_rh::int) as rh_flag,
	 ws,
	 get_hourly_flag(1, 'WS', ws, n_ws::int) as ws_flag,
	 wd,
	 get_hourly_flag(1, 'WD', wd, n_wd::int) as wd_flag,
	 wd_v,
	 get_hourly_flag(1, 'WD_V', wd_v, n_wd_v::int) as wd_v_flag,
	 ws_max,
	 get_hourly_flag(1, 'WS_MAX', ws_max, n_ws_max::int) as ws_max_flag,
	 bp,
	 get_hourly_flag(1, 'BP', bp, n_bp::int) as bp_flag
    from (select time_bucket('1 hour', time) as time,
		 avg(no) FILTER (WHERE not no_flagged) as no,
		 count(no) FILTER (WHERE not no_flagged) as n_no,
		 avg(no2) FILTER (WHERE not no2_flagged) as no2,
		 count(no2) FILTER (WHERE not no2_flagged) as n_no2,
		 avg(noy) FILTER (WHERE not noy_flagged) as noy,
		 count(noy) FILTER (WHERE not noy_flagged) as n_noy,
		 avg(ozone) FILTER (WHERE not ozone_flagged) as ozone,
		 count(ozone) FILTER (WHERE not ozone_flagged) as n_ozone,
		 avg(co) FILTER (WHERE not co_flagged) as co,
		 count(co) FILTER (WHERE not co_flagged) as n_co,
		 avg(so2) FILTER (WHERE not so2_flagged) as so2,
		 count(so2) FILTER (WHERE not so2_flagged) as n_so2,
		 avg(temp) FILTER (WHERE not temp_flagged) as temp,
		 count(temp) FILTER (WHERE not temp_flagged) as n_temp,
		 avg(rh) FILTER (WHERE not rh_flagged) as rh,
		 count(rh) FILTER (WHERE not rh_flagged) as n_rh,
		 avg(ws) FILTER (WHERE not ws_flagged) as ws,
		 count(ws) FILTER (WHERE not ws_flagged) as n_ws,
		 avg(wd) FILTER (WHERE not wd_flagged) as wd,
		 count(wd) FILTER (WHERE not wd_flagged) as n_wd,
		 avg(wd_v) FILTER (WHERE not wd_v_flagged) as wd_v,
		 count(wd_v) FILTER (WHERE not wd_v_flagged) as n_wd_v,
		 avg(ws_max) FILTER (WHERE not ws_max_flagged) as ws_max,
		 count(ws_max) FILTER (WHERE not ws_max_flagged) as n_ws_max,
		 avg(bp) FILTER (WHERE not bp_flagged) as bp,
		 count(bp) FILTER (WHERE not bp_flagged) as n_bp
	    from processed_campbell_wfms
	   group by time_bucket('1 hour', time)) c1;


/* might try this later for storing processed data */
/*
CREATE TABLE processed_campbell_wfms_test (
  like processed_campbell_wfms
);
*/

CREATE or replace VIEW campbell_wfml_medians AS
  select *,
  -- get medians as needed
	 runmed(no) over w as no_med,
	 runmed(nox) over w as nox_med,
	 runmed(ozone) over w as ozone_med,
	 runmed(co) over w as co_med,
	 runmed(so2) over w as so2_med,
	 runmed(temp) over w as temp_med,
	 runmed(rh) over w as rh_med,
	 runmed(bp) over w as bp_med
    from (select *,
	  -- calibrate everything
		 apply_calib(2, 'NO', no_avg, instrument_time) as no,
		 apply_calib(2, 'NOx', nox_avg, instrument_time) as nox,
		 ozone_avg as ozone,
		 apply_calib(2, 'CO', co_avg, instrument_time) as co,
		 apply_calib(2, 'SO2', so2_avg, instrument_time) as so2,
		 t_avg as temp,
		 rh_avg as rh,
		 -- ws3cup_avg as ws,
		 -- winddir_avg % 360 as wd,
		 -- winddir_d1_wvt % 360 as wd_v,
		 -- ws3cup_max as ws_max,
		 bp2_avg as bp
	    from campbell_wfml) c1
	   WINDOW w AS (ORDER BY instrument_time
			rows between 7 preceding and 7 following);

CREATE or replace VIEW campbell_wfml_mad AS
  select *,
  -- calculate Median Absolute Deviations
	 runmad(no, no_med) over w as no_mad,
	 runmad(nox, nox_med) over w as nox_mad,
	 runmad(ozone, ozone_med) over w as ozone_mad,
	 runmad(co, co_med) over w as co_mad,
	 runmad(so2, so2_med) over w as so2_mad,
	 runmad(temp, temp_med) over w as temp_mad,
	 runmad(rh, rh_med) over w as rh_mad,
	 runmad(bp, bp_med) over w as bp_mad
    from campbell_wfml_medians c1
	   WINDOW w AS (ORDER BY instrument_time
			rows between 7 preceding and 7 following);

/* Get the fully processed WFML campbell data */
CREATE materialized VIEW processed_campbell_wfml as
  select instrument_time as time,
	 no,
  -- ugh need to add source to campbell tables for manual flag
  -- matching
	 is_flagged('NO', 1, null, instrument_time,
		    no, f_nox_avg, no_med,
		    no_mad) as no_flagged,
	 nox,
	 is_flagged('NOx', 1, null, instrument_time,
		    nox, f_nox_avg, nox_med,
		    nox_mad) as nox_flagged,
	 ozone,
	 is_flagged('Ozone', 1, null, instrument_time,
		    ozone, null, ozone_med,
		    ozone_mad) as ozone_flagged,
	 co,
	 is_flagged('CO', 1, null, instrument_time,
		    co, f_co_avg, co_med,
		    co_mad) as co_flagged,
	 so2,
	 is_flagged('SO2', 1, null, instrument_time,
		    so2, null, so2_med,
		    so2_mad) as so2_flagged,
	 temp,
	 is_flagged('Temp', 1, null, instrument_time,
		    temp, null, temp_med,
		    temp_mad) as temp_flagged,
	 rh,
	 is_flagged('RH', 1, null, instrument_time,
		    rh, null, rh_med,
		    rh_mad) as rh_flagged,
	 -- ws,
	 -- is_flagged('WS', 1, null, instrument_time,
	 -- 	    ws, f_wind_avg,
	 -- 	    null, null) as ws_flagged,
	 -- wd,
	 -- is_flagged('WD', 1, null, instrument_time,
	 -- 	    wd, f_wind_avg,
	 -- 	    null, null) as wd_flagged,
	 -- wd_v,
	 -- is_flagged('WD_V', 1, null, instrument_time,
	 -- 	    wd_v, null, null, null) as wd_v_flagged,
	 -- ws_max,
	 -- is_flagged('WS_MAX', 1, null, instrument_time,
	 -- 	    ws_max, f_wind_avg,
	 -- 	    null, null) as ws_max_flagged,
	 bp,
	 is_flagged('BP', 1, null, instrument_time,
		    bp, null, bp_med,
		    bp_mad) as bp_flagged
    from campbell_wfml_mad;

CREATE materialized VIEW hourly_campbell_wfml as
  select time,
	 no,
	 get_hourly_flag(1, 'NO', no, n_no::int) as no_flag,
	 nox,
	 get_hourly_flag(1, 'NOx', nox, n_nox::int) as nox_flag,
	 ozone,
	 get_hourly_flag(1, 'OZONE', ozone, n_ozone::int) as ozone_flag,
	 co,
	 get_hourly_flag(1, 'CO', co, n_co::int) as co_flag,
	 so2,
	 get_hourly_flag(1, 'SO2', so2, n_so2::int) as so2_flag,
	 temp,
	 get_hourly_flag(1, 'TEMP', temp, n_temp::int) as temp_flag,
	 rh,
	 get_hourly_flag(1, 'RH', rh, n_rh::int) as rh_flag,
	 -- ws,
	 -- get_hourly_flag(1, 'WS', ws, n_ws::int) as ws_flag,
	 -- wd,
	 -- get_hourly_flag(1, 'WD', wd, n_wd::int) as wd_flag,
	 -- wd_v,
	 -- get_hourly_flag(1, 'WD_V', wd_v, n_wd_v::int) as wd_v_flag,
	 -- ws_max,
	 -- get_hourly_flag(1, 'WS_MAX', ws_max, n_ws_max::int) as ws_max_flag,
	 bp,
	 get_hourly_flag(1, 'BP', bp, n_bp::int) as bp_flag
    from (select time_bucket('1 hour', time) as time,
		 avg(no) FILTER (WHERE not no_flagged) as no,
		 count(no) FILTER (WHERE not no_flagged) as n_no,
		 avg(nox) FILTER (WHERE not nox_flagged) as nox,
		 count(nox) FILTER (WHERE not nox_flagged) as n_nox,
		 avg(ozone) FILTER (WHERE not ozone_flagged) as ozone,
		 count(ozone) FILTER (WHERE not ozone_flagged) as n_ozone,
		 avg(co) FILTER (WHERE not co_flagged) as co,
		 count(co) FILTER (WHERE not co_flagged) as n_co,
		 avg(so2) FILTER (WHERE not so2_flagged) as so2,
		 count(so2) FILTER (WHERE not so2_flagged) as n_so2,
		 avg(temp) FILTER (WHERE not temp_flagged) as temp,
		 count(temp) FILTER (WHERE not temp_flagged) as n_temp,
		 avg(rh) FILTER (WHERE not rh_flagged) as rh,
		 count(rh) FILTER (WHERE not rh_flagged) as n_rh,
		 -- avg(ws) FILTER (WHERE not ws_flagged) as ws,
		 -- count(ws) FILTER (WHERE not ws_flagged) as n_ws,
		 -- avg(wd) FILTER (WHERE not wd_flagged) as wd,
		 -- count(wd) FILTER (WHERE not wd_flagged) as n_wd,
		 -- avg(wd_v) FILTER (WHERE not wd_v_flagged) as wd_v,
		 -- count(wd_v) FILTER (WHERE not wd_v_flagged) as n_wd_v,
		 -- avg(ws_max) FILTER (WHERE not ws_max_flagged) as ws_max,
		 -- count(ws_max) FILTER (WHERE not ws_max_flagged) as n_ws_max,
		 avg(bp) FILTER (WHERE not bp_flagged) as bp,
		 count(bp) FILTER (WHERE not bp_flagged) as n_bp
	    from processed_campbell_wfml
	   group by time_bucket('1 hour', time)) c1;
