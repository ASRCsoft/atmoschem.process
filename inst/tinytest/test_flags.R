library(atmoschem.process)

m = matrix(rep(.6, 4), 2, 2, dimnames = list(c('a','b'), c('c','d')))
expect_equal(attributes(m), attributes(narsto_agg_flag(m)),
             info = 'narsto_agg_flag preserves matrix attributes')

# this should pass if the caTools::runmad NA bug is fixed (see the caTools news
# for version 0.1.18.1)
set.seed(1234)
k = 11
k2 = 11 %/% 2
n = 500
x = rnorm(n)
x[sample(1:n, 50)] = NA
# compare caTools::runmad output to base R mad function
y1 = vector('numeric', length = length(n))
not_ends = (1 + k2):(n - k2)
for (j in not_ends) y1[j] = mad(x[(j - k2):(j + k2)], na.rm = T)
y2 = caTools::runmad(x, k, caTools::runquantile(x, k, .5))
expect_equal(y1[not_ends], y2[not_ends], info = 'caTools::runmad handles NAs')
