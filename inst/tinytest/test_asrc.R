library(atmoschem.process)

for (f in list.files('asrc_cals', full.names = T)) {
  model = sub('_.*', '', basename(f))
  site = switch(model, '42i' = 'WFML', '48C' = 'WFML', 'WFMS')
  brand = switch(model, '300EU' = 'Teledyne', 'Thermo')
  instrument = paste(brand, model, sep = '_')
  transform_fun = switch(model, '42C' = 'transform_wfms_42C',
                         '42Cs' = 'transform_wfms_42Cs',
                         '42i' = 'transform_wfml_42i',
                         '43C' = 'transform_wfms_43C',
                         '48C' = 'transform_wfml_48C',
                         '300EU' = 'transform_wfms_300EU')
  cal = get(transform_fun)(f)
  info = paste(transform_fun, 'parses', basename(f))
  expect_true(nrow(cal) > 0, info = info)
}
