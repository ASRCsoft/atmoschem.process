library(atmoschem.process)

expect_equal(epa_schedule(as.Date('2020-01-07')), 3,
             info = 'epa_schedule gets the right day')
