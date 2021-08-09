

## transform a raw data file according to its site and data source
transform_calibration = function(f, site, ds) {
  if (site == 'PSP') {
    transform_psp_calibrations(f)
  } else if (site == 'WFMS' && ds == 'Teledyne_300EU') {
    transform_wfms_300EU(f)
  } else if (site == 'WFMS' && ds == 'Thermo_42C') {
    transform_wfms_42C(f)
  } else if (site == 'WFMS' && ds == 'Thermo_42Cs') {
    transform_wfms_42Cs(f)
  } else if (site == 'WFMS' && ds == 'Thermo_43C') {
    transform_wfms_43C(f)
  } else if (site == 'WFML' && ds == 'Thermo_42i') {
    transform_wfml_42i(f)
  } else if (site == 'WFML' && ds == 'Thermo_48C') {
    transform_wfml_48C(f)
  }
}

transform_measurement = function(f, site, ds) {
  if (ds == 'campbell') {
    return(transform_campbell(f, site))
  } else if (site == 'PSP' & ds == 'envidas') {
    return(transform_psp_envidas(f))
  } else if (site == 'WFML' && ds == 'envidas') {
    return(transform_wfml_envidas(f))
  } else if (site == 'WFMS' && ds == 'DEC_envidas') {
    # this is really the same format as the WFML envidas
    return(transform_wfml_envidas(f, 'WFMS'))
  }
  res = if (ds == 'ultrafine') {
    transform_ultrafine(f)
  } else if (ds == 'mesonet') {
    transform_mesonet(f)
  } else if (site == 'WFMS' && ds == 'aethelometer') {
    transform_wfms_aethelometer(f)
  }
  if (!nrow(res)) return(data.frame())
  res %>%
    transform(time = as.POSIXct(instrument_time, tz = 'EST')) %>%
    reshape(timevar = 'measurement_name', idvar = 'time', direction = 'wide',
            drop = c('record', 'instrument_time'))
}
