## derive values from measurements

combine_measures = function(obj, site, data_source, m1, m2,
                            start_time, end_time) {
  site_id = nysatmoschem:::get_site_id(obj$con, site)
  q1 = 'select * from combine_measures(?site, ?ds, ?m1, ?m2) where time between ?start and ?end'
  sql1 = DBI::sqlInterpolate(obj$con, q1, site = site_id, ds = data_source,
                             m1 = m1, m2 = m2, start = start_time, end = end_time)
  obj %>%
    tbl(sql(sql1)) %>%
    collect()
}

sea_level_pressure = function(bp, ptemp, height, lapse_rate = .0065,
                              gM_over_RstarL = -5.257) {
  ptempK = ptemp + 273.15
  bp * (ptempK / (ptempK + lapse_rate * height))^gM_over_RstarL
}


## each function takes an etl object, start time and end time, and returns the
## unprocessed data

## WFMS functions
wfms_no2 = function(obj, start_time, end_time) {
  combine_measures(obj, 'WFMS', 'campbell', 'NO', 'NOx',
                   start_time, end_time) %>%
    mutate(measurement_type_id =
             get_measurement_type_id(obj$con, 'WFMS', 'derived', 'NO2'),
           value = value2 - value1,
           flagged = flagged1 | flagged2) %>%
    select(measurement_type_id, time, value, flagged)
}

wfms_slp = function(obj, start_time, end_time) {
  combine_measures(obj, 'WFMS', 'campbell', 'BP', 'PTemp_C',
                   start_time, end_time) %>%
    mutate(measurement_type_id =
             get_measurement_type_id(obj$con, 'WFMS', 'derived', 'SLP'),
           value = sea_level_pressure(value1, value2, 1483.5),
           flagged = flagged1 | flagged2) %>%
    select(measurement_type_id, time, value, flagged)
}

wfms_ws = function(obj, start_time, end_time) {
  combine_measures(obj, 'WFMS', 'campbell', 'WS3Cup', 'WS3CupB',
                   start_time, end_time) %>%
    mutate(measurement_type_id =
             get_measurement_type_id(obj$con, 'WFMS', 'derived', 'WS'),
           value = pmax(value1, value2, na.rm = TRUE),
           flagged = flagged1 & flagged2) %>%
    select(measurement_type_id, time, value, flagged)
}

wfms_ws_components = function(obj, start_time, end_time) {
  ws = combine_measures(obj, 'WFMS', 'campbell', 'WS', 'WindDir_D1_WVT',
                        start_time, end_time) %>%
    mutate(ws = value1,
           theta = pi * (270 - value2) / 180,
           flagged = flagged1 | flagged2) %>%
    mutate(u = ws * sin(theta),
           v = ws * cos(theta)) %>%
    select(time, u, v, flagged) %>%
    tidyr::spread(component, value, -time, -flagged) %>%
    mutate(measurement_type_id =
             ifelse(component == 'u',
                    get_measurement_type_id(obj$con, 'WFMS', 'derived', 'WS_u'),
                    get_measurement_type_id(obj$con, 'WFMS', 'derived', 'WS_v'))) %>%
    select(measurement_type_id, time, value, flagged)
}

wfms_ws_max = function(obj, start_time, end_time) {
  combine_measures(obj, 'WFMS', 'campbell', 'WS3Cup_Max', 'WS3CupB_Max',
                   start_time, end_time) %>%
    mutate(measurement_type_id =
             get_measurement_type_id(obj$con, 'WFMS', 'derived', 'WS_Max'),
           value = pmax(value1, value2, na.rm = TRUE),
           flagged = flagged1 & flagged2) %>%
    select(measurement_type_id, time, value, flagged)
}

wfms_wood_smoke = function(obj, start_time, end_time) {
  combine_measures(obj, 'WFMS', 'aethelometer',
                   'concentration_370', 'concentration_880',
                   start_time, end_time) %>%
    mutate(measurement_type_id =
             get_measurement_type_id(obj$con, 'WFMS', 'derived',
                                     'Wood smoke'),
           value = value1 - value2,
           flagged = flagged1 | flagged2) %>%
    select(measurement_type_id, time, value, flagged)
}

derived_wfms = list(wfms_no2, wfms_slp, wfms_ws, wfms_ws_components,
                    wfms_ws_max, wfms_wood_smoke)


## WFML functions
wfml_slp = function(obj, start_time, end_time) {
  combine_measures(obj, 'WFML', 'campbell', 'BP2', 'PTemp_C',
                   start_time, end_time) %>%
    mutate(measurement_type_id =
             get_measurement_type_id(obj$con, 'WFML', 'derived', 'SLP'),
           value = sea_level_pressure(value1, value2, 604),
           flagged = flagged1 | flagged2) %>%
    select(measurement_type_id, time, value, flagged)
}

derived_wfml = list(wfml_slp)


## PSP functions
psp_no2 = function(obj, start_time, end_time) {
  combine_measures(obj, 'PSP', 'envidas', 'NO', 'NOx',
                   start_time, end_time) %>%
    mutate(measurement_type_id =
             get_measurement_type_id(obj$con, 'PSP', 'derived', 'NO2'),
           value = value2 - value1,
           flagged = flagged1 | flagged2) %>%
    select(measurement_type_id, time, value, flagged)
}

psp_hno3 = function(obj, start_time, end_time) {
  combine_measures(obj, 'PSP', 'envidas', 'NOy', 'NOy-HNO3',
                   start_time, end_time) %>%
    mutate(measurement_type_id =
             get_measurement_type_id(obj$con, 'PSP', 'derived', 'NO2'),
           value = value1 - value2,
           flagged = flagged1 | flagged2) %>%
    select(measurement_type_id, time, value, flagged)
}

## fix this one!
psp_precip = function(obj, start_time, end_time) {
  obj %>%
    tbl('measurements') %>%
    filter(measurement_type_id =
             get_measurement_type_id(obj$con, 'PSP', 'envidas', 'Rain'))
    collect()
  combine_measures(obj, 'PSP', 'envidas', 'NOy', 'NOy-HNO3',
                   start_time, end_time) %>%
    mutate(measurement_type_id =
             get_measurement_type_id(obj$con, 'PSP', 'derived', 'NO2'),
           value = value1 - value2,
           flagged = flagged1 | flagged2) %>%
    select(measurement_type_id, time, value, flagged)
}

psp_teoma25_base = function(obj, start_time, end_time) {
  combine_measures(obj, 'PSP', 'envidas', 'TEOMA(2.5)MC', 'TEOMA(2.5)RefMC',
                   start_time, end_time) %>%
    mutate(measurement_type_id =
             get_measurement_type_id(obj$con, 'PSP', 'derived',
                                     'TEOMA(2.5)BaseMC'),
           value = value1 + value2,
           flagged = flagged1 | flagged2) %>%
    select(measurement_type_id, time, value, flagged)
}

psp_teombcrs_base = function(obj, start_time, end_time) {
  combine_measures(obj, 'PSP', 'envidas', 'TEOMB(crs)MC', 'TEOMB(crs)RefMC',
                   start_time, end_time) %>%
    mutate(measurement_type_id =
             get_measurement_type_id(obj$con, 'PSP', 'derived',
                                     'TEOMB(crs)BaseMC'),
           value = value1 + value2,
           flagged = flagged1 | flagged2) %>%
    select(measurement_type_id, time, value, flagged)
}

psp_dichot10_base = function(obj, start_time, end_time) {
  combine_measures(obj, 'PSP', 'envidas', 'Dichot(10)MC', 'Dichot(10)RefMC',
                   start_time, end_time) %>%
    mutate(measurement_type_id =
             get_measurement_type_id(obj$con, 'PSP', 'derived',
                                     'Dichot(10)BaseMC'),
           value = value1 + value2,
           flagged = flagged1 | flagged2) %>%
    select(measurement_type_id, time, value, flagged)
}

psp_wood_smoke = function(obj, start_time, end_time) {
  combine_measures(obj, 'PSP', 'envidas', 'BC1', 'BC6',
                   start_time, end_time) %>%
    mutate(measurement_type_id =
             get_measurement_type_id(obj$con, 'PSP', 'derived',
                                     'Wood smoke'),
           value = value1 - value2,
           flagged = flagged1 | flagged2) %>%
    select(measurement_type_id, time, value, flagged)
}

psp_ws_components = function(obj, start_time, end_time) {
  ws = combine_measures(obj, 'PSP', 'envidas', 'VWS', 'VWD',
                        start_time, end_time) %>%
    mutate(ws = value1,
           theta = pi * (270 - value2) / 180,
           flagged = flagged1 | flagged2) %>%
    mutate(u = ws * sin(theta),
           v = ws * cos(theta)) %>%
    select(time, u, v, flagged) %>%
    tidyr::spread(component, value, -time, -flagged) %>%
    mutate(measurement_type_id =
             ifelse(component == 'u',
                    get_measurement_type_id(obj$con, 'PSP', 'derived', 'WS_u'),
                    get_measurement_type_id(obj$con, 'PSP', 'derived', 'WS_v'))) %>%
    select(measurement_type_id, time, value, flagged)
}

psp_slp = function(obj, start_time, end_time) {
  combine_measures(obj, 'PSP', 'envidas', 'BP', 'AmbTemp',
                   start_time, end_time) %>%
    mutate(measurement_type_id =
             get_measurement_type_id(obj$con, 'PSP', 'derived', 'SLP'),
           value = sea_level_pressure(value1, value2, 504),
           flagged = flagged1 | flagged2) %>%
    select(measurement_type_id, time, value, flagged)
}

psp_sr2 = function(obj, start_time, end_time) {
    tbl(obj, 'measurements') %>%
    filter(measurement_type_id =
             get_measurement_type_id(obj$con, 'PSP', 'envidas', 'SR')) %>%
    mutate(measurement_type_id =
             get_measurement_type_id(obj$con, 'PSP', 'derived', 'SR2'),
           value = value + 17.7) %>%
    select(measurement_type_id, time, value, flagged)
}

derived_psp = list(psp_no2, psp_hno3, psp_precip, psp_teoma25_base,
                   psp_teombcrs_base, psp_dichot10_base,
                   psp_wood_smoke, psp_ws_components, psp_slp,
                   psp_sr2)


derived_vals = list('WFMS' = derived_wfms,
                    'WFML' = derived_wfml,
                    'PSP' = derived_psp)
