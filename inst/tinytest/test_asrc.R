library(atmoschem.process)

for (f in list.files('asrc_cals', full.names = T)) {
  model = sub('_.*', '', basename(f))
  site = switch(model, '42i' = 'WFML', '48C' = 'WFML', 'WFMS')
  brand = switch(model, '300EU' = 'Teledyne', 'Thermo')
  instrument = paste(brand, model, sep = '_')
  cal = atmoschem.process:::transform_calibration(f, site, instrument)
  info = paste('transform_calibration parses', basename(f))
  expect_true(nrow(cal) > 0, info = info)
}
