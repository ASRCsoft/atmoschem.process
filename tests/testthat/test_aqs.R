test_that('epa_schedule gets the right day', {
  expect_equal(epa_schedule(as.Date('2020-01-07')), 3)
})
