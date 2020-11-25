library(atmoschem.process)

test_that('sea_level_pressure returns something reasonable', {
  # pressure should go up as height drops
  expect_gt(sea_level_pressure(800, 30, 1483.5), 800)
})
