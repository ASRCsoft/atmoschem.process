## R package variables
# h/t to @jimhester and @yihui for this parse block:
# https://github.com/yihui/knitr/blob/dc5ead7bcfc0ebd2789fe99c527c7d91afb3de4a/Makefile#L1-L4
PKGNAME := $(shell sed -n "s/Package: *\([^ ]*\)/\1/p" DESCRIPTION)
PKGVERS := $(shell sed -n "s/Version: *\([^ ]*\)/\1/p" DESCRIPTION)
r_files := $(wildcard R/*.R)
pkgdata_csv := $(wildcard data-raw/package_data/*.csv)
pkgdata_rda := $(patsubst data-raw/package_data/%.csv,data/%.rda,$(pkgdata_csv))
build_file := $(PKGNAME)_$(PKGVERS).tar.gz
## Data processing variables
rscript := Rscript --vanilla
export processing_end := 2021-01-01
export raw_version := 0.3
sites := WFMS WFML PSP QC
raw_dir := analysis/raw
interm_dir := analysis/intermediate
out_dir := analysis/out
download_url := http://atmoschem.asrc.cestm.albany.edu/~aqm/AQM_Products/downloads
routine_zip := $(raw_dir)/routine_chemistry_v0.1.zip
old_routine_out := $(patsubst %,$(interm_dir)/old_%.csv,$(sites))
raw_data := $(raw_dir)/raw_data_v$(raw_version)
raw_zip := $(raw_dir)/raw_data_v$(raw_version).zip
# get <site>_<data_source> for each entry in data_sources.csv
data_sources := $(shell sed "1d;s/^\([^,]*\),\([^,]*\).*/\1_\2/" data-raw/package_data/data_sources.csv)
hourly_files := $(patsubst %,$(interm_dir)/hourly_%.sqlite,$(data_sources))
routine_out := routine_chemistry_v$(PKGVERS)

.PHONY: all
all: routine

## Atmoschem Dataset

.PHONY: check_data
check_data:
	$(rscript) \
	-e 'if (!requireNamespace("tinytest")) install.packages("tinytest")' \
	-e 'tinytest::run_test_dir("analysis/tests")'

# save intermediate sqlite files for the processing viewer
.SECONDARY:

.PHONY: routine
routine: $(routine_out).zip

$(routine_out).zip: $(old_routine_out) $(hourly_files)
	$(rscript) analysis/routine_package.R $(out_dir)/$(routine_out)

$(interm_dir)/hourly_%.sqlite: $(interm_dir)/processed_%.sqlite analysis/aggregate_hourly.R
	$(rscript) analysis/aggregate_hourly.R $(shell echo $* | sed "s/_/ /")

# The way calibration files are created for each site (without data source)
# makes things a bit awkward. Processed site/data source files depend on the
# calibration site file
.SECONDEXPANSION:
$(interm_dir)/processed_%.sqlite: $(interm_dir)/raw_%.sqlite \
                                  $(interm_dir)/cals_$$(shell echo $$* | sed "s/_.*//").sqlite \
                                  analysis/process_new_data.R
	$(rscript) analysis/process_new_data.R $(shell echo $* | sed "s/_/ /")

# Calibration site file depends on all the raw site/calibration files
make_cal_deps = $(patsubst %,$(interm_dir)/raw_%.sqlite, $(filter $(1)%, $(data_sources)))
$(interm_dir)/cals_%.sqlite: $$(call make_cal_deps,$$*) \
                             raw_data analysis/load_calibration.R
	$(rscript) analysis/load_calibration.R $*

$(interm_dir)/raw_%.sqlite: raw_data analysis/load_raw.R
	$(rscript) analysis/load_raw.R $(shell echo $* | sed "s/_/ /")

$(interm_dir)/old_%.csv: $(routine_zip) analysis/clean_old_routine.R
	$(rscript) -e 'unzip("$(routine_zip)", overwrite = F, exdir = "$(raw_dir)")' && \
	$(rscript) analysis/clean_old_routine.R $*

.INTERMEDIATE: raw_data
raw_data: $(raw_zip)
	$(rscript) -e 'unzip("$(raw_zip)", overwrite = F, exdir = "$(raw_dir)")'

$(raw_dir)/%.zip:
	mkdir -p $(raw_dir) && \
	wget --user=aqm --ask-password -O $@ $(download_url)/$(shell echo $* | sed -E s/_v[0-9.]+$$//)/$*.zip

.PHONY: view
view:
	$(rscript) -e 'shiny::runApp("analysis/processing_viewer", launch.browser = T)'

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
	$(rscript) data-raw/package_data.R $<

README.md: README.Rmd DESCRIPTION
	Rscript \
	-e 'if (!requireNamespace("rmarkdown")) install.packages("rmarkdown")' \
	-e 'rmarkdown::render("README.Rmd")'

.PHONY: clean
clean:
	@rm -rf $(build_file) $(PKGNAME).Rcheck
