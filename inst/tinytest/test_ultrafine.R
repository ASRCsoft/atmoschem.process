library(atmoschem.process)

parse_ultrafine_flag = atmoschem.process:::parse_ultrafine_flag

expect_false(parse_ultrafine_flag('100'), 'Pulse Height Fault flag is ignored')
expect_false(parse_ultrafine_flag('4000'), 'Service Reminder flag is ignored')

for (i in setdiff(1:16, c(9, 15))) {
  expect_true(parse_ultrafine_flag(as.hexmode(2^(i - 1))),
              'other ultrafine flags not ignored')
}
