library(atmoschem.process)

for (f in list.files('dec_cals', full.names = T)) {
  model = gsub('Pinnacle_(ASRC_|DEC_)?([^_]+).*', '\\1\\2', basename(f))
  transform_fun = switch(model, '42C' = 'transform_psp_42C_calibrations',
                         'API300EU' = 'transform_psp_API300EU_calibrations',
                         'ASRC_TEI42i' = 'transform_psp_ASRC_TEI42i_Y_NOy_calibrations',
                         'DEC_TEI42i' = 'transform_psp_DEC_TEI42i_NOy_calibrations',
                         'TEI43i' = 'transform_psp_TEI43i_SO2_calibrations',
                         'TEI49i' = 'transform_psp_TEI49i_O3_49i_calibrations')
  cal = get(transform_fun)(f)
  info = paste(transform_fun, 'parses', basename(f))
  expect_true(nrow(cal) > 0, info = info)
}
