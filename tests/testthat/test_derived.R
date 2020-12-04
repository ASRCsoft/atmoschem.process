test_that('resultant average of a single value is the value', {
  expect_equal(res_wind_speed(5, 180), 5)
  expect_equal(res_wind_dir(1, 320), 320)
})

test_that('resultant of opposite winds is zero', {
  expect_equal(res_wind_speed(1, c(0, 180)), 0)
})

test_that('resultant direction around 360/0 is correct', {
  expect_equal(res_wind_dir(1, c(355, 5)), 0)
})

test_that('sea_level_pressure returns something reasonable', {
  # pressure should go up as height drops
  expect_gt(sea_level_pressure(800, 30, 1483.5), 800)
})
