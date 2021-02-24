library(atmoschem.process)

expect_equal(res_wind_speed(5, 180), 5,
             info = 'resultant average of a single value is the value')
expect_equal(res_wind_speed(1, c(0, 180)), 0,
             info = 'resultant of opposite winds is zero')

expect_equal(res_wind_dir(1, 320), 320,
             info = 'resultant average of a single value is the value')
expect_equal(res_wind_dir(1, c(355, 5)), 0,
             info = 'resultant direction around 360/0 is correct')

expect_true(sea_level_pressure(800, 30, 1483.5) > 800,
            info = 'pressure increases as height drops')
