## R package variables
# h/t to @jimhester and @yihui for this parse block:
# https://github.com/yihui/knitr/blob/dc5ead7bcfc0ebd2789fe99c527c7d91afb3de4a/Makefile#L1-L4
PKGNAME := $(shell sed -n "s/Package: *\([^ ]*\)/\1/p" DESCRIPTION)
PKGVERS := $(shell sed -n "s/Version: *\([^ ]*\)/\1/p" DESCRIPTION)
r_files := $(wildcard R/*.R)
sql_files := $(wildcard inst/sql/*.sql)
pkgdata_csv := $(wildcard data-raw/package_data/*.csv)
pkgdata_rda := $(patsubst data-raw/package_data/%.csv,data/%.rda,$(pkgdata_csv))
build_file := $(PKGNAME)_$(PKGVERS).tar.gz
## Data processing variables
sites := WFMS WFML PSP QC
raw_dir := analysis/raw
cleaned_dir := analysis/cleaned
interm_dir := analysis/intermediate
out_dir := analysis/out
download_url := http://atmoschem.asrc.cestm.albany.edu/~aqm/AQM_Products/downloads
routine_zip := $(raw_dir)/routine_chemistry_v0.1.zip
clean_old_routine_out := $(patsubst %,$(cleaned_dir)/old_routine/%.csv,$(sites))
raw_zip := $(raw_dir)/raw_data_v0.3.zip
# get <site>_<data_source> for each entry in data_sources.csv
data_sources := $(shell sed "1d;s/^\([^,]*\),\([^,]*\).*/\1_\2/" data-raw/package_data/data_sources.csv)
hourly_files := $(patsubst %,$(interm_dir)/hourly_%.sqlite,$(data_sources))
routine_out := routine_chemistry_v$(PKGVERS)

.PHONY: all
all: routine_dataset

## Atmoschem Dataset

# save intermediate sqlite files for the processing viewer
.SECONDARY:

.PHONY: routine_dataset
routine_dataset: $(clean_old_routine_out) $(hourly_files)
	mkdir -p $(out_dir)/$(routine_out) && \
	Rscript analysis/routine_package.R $(out_dir)/$(routine_out)
	cp analysis/README.txt $(out_dir)/$(routine_out)
	cd $(out_dir); zip -r $(routine_out).zip $(routine_out)

$(interm_dir)/hourly_%.sqlite: $(interm_dir)/processed_%.sqlite analysis/aggregate_hourly.R
	Rscript analysis/aggregate_hourly.R $(shell echo $* | sed "s/_/ /")

$(interm_dir)/processed_%.sqlite: $(interm_dir)/raw_%.sqlite processingdb \
                                  analysis/process_new_data.R
	Rscript analysis/process_new_data.R $(shell echo $* | sed "s/_/ /")

$(interm_dir)/raw_%.sqlite: processingdb analysis/load_raw.R
	Rscript analysis/load_raw.R $(shell echo $* | sed "s/_/ /")

.INTERMEDIATE: processingdb
processingdb: $(sql_files) $(raw_zip)
	unzip -nq $(raw_zip) -d $(raw_dir) && \
	Rscript analysis/initdb.R

.PHONY: clean_old_routine
clean_old_routine: $(clean_old_routine_out)

$(cleaned_dir)/old_routine/%.csv: $(routine_zip) analysis/clean_old_routine.R
	unzip -nq $(routine_zip) -d $(raw_dir) && \
	Rscript analysis/clean_old_routine.R $*

$(raw_dir)/%.zip:
	mkdir -p $(raw_dir) && \
	wget --user=aqm --ask-password -O $@ $(download_url)/$(shell echo $* | sed -E s/_v[0-9.]+$$//)/$*.zip

## R package

.PHONY: check
check: $(build_file)
	R CMD check --no-manual $(build_file)

.PHONY: install
install: docs
	Rscript \
	-e 'if (!requireNamespace("devtools")) install.packages("devtools")' \
	-e 'devtools::install(build = FALSE)'
# this alternative installation method is inconvenient due to the very slow
# build step
# install: install_deps $(build_file)
# 	R CMD INSTALL $(build_file)

.INTERMEDIATE: install_deps
install_deps: DESCRIPTION
	Rscript \
	-e 'if (!requireNamespace("remotes")) install.packages("remotes")' \
	-e 'remotes::install_deps(dependencies = TRUE)'

$(build_file): docs
	R CMD build .

.INTERMEDIATE: docs
docs: $(pkgdata_rda) $(r_files) README.md
	Rscript \
	-e 'if (!requireNamespace("roxygen2")) install.packages("roxygen2")' \
	-e 'roxygen2::roxygenise()'

data/%.rda: data-raw/package_data/%.csv data-raw/package_data.R
	Rscript data-raw/package_data.R $<

README.md: README.Rmd DESCRIPTION
	Rscript \
	-e 'if (!requireNamespace("rmarkdown")) install.packages("rmarkdown")' \
	-e 'rmarkdown::render("README.Rmd")'

.PHONY: clean
clean:
	@rm -rf $(build_file) $(PKGNAME).Rcheck
