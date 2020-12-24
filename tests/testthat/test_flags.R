test_that('narsto_agg_flag preserves matrix attributes', {
  m = matrix(rep(.6, 4), 2, 2, dimnames = list(c('a','b'), c('c','d')))
  expect_equal(attributes(m), attributes(narsto_agg_flag(m)))
})
