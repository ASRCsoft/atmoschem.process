rds_files = list.files(file.path('..', 'intermediate'), pattern = 'loess_.*\\.rds',
                       full.names = T)

test_that("loess reciprocal condition numbers aren't zero", {
  # A reciprocal condition number zero means that a local regression wasn't able
  # to be estimated properly, which can create absurd spikes in the loess fit
  skip_if(length(rds_files) == 0)
  for (rds in rds_files) {
    fits = readRDS(rds)
    site = gsub('.*/loess_|\\.rds', '', rds)
    for (i in seq_len(length(fits))) {
      fit = fits[[i]]
      if (is.null(fit)) next()
      ind = arrayInd(i, dim(fits))
      logger = rownames(fits)[ind[1]]
      param = colnames(fits)[ind[2]]
      types = names(fit)[names(fit) %in% c('zero', 'span', 'CE')]
      for (type in types) {
        info = paste(site, logger, param, type, sep = ' / ')
        expect_false(any(fit[[type]]$singular$rcond == 0), info)
      }
    }
  }
})
