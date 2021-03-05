library(atmoschem.process)

for (f in list.files('dec_cals', full.names = T)) {
  cal = atmoschem.process:::transform_calibration(f, 'PSP')
  info = paste('transform_calibration parses', basename(f))
  expect_true(nrow(cal) > 0, info = info)
}
