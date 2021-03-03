## R package variables
# h/t to @jimhester and @yihui for this parse block:
# https://github.com/yihui/knitr/blob/dc5ead7bcfc0ebd2789fe99c527c7d91afb3de4a/Makefile#L1-L4
PKGNAME := $(shell sed -n "s/Package: *\([^ ]*\)/\1/p" DESCRIPTION)
PKGVERS := $(shell sed -n "s/Version: *\([^ ]*\)/\1/p" DESCRIPTION)
r_files := $(wildcard R/*.R)
pkgdata_csv := $(wildcard data-raw/*.csv)
pkgdata_rda := $(patsubst data-raw/%.csv,data/%.rda,$(pkgdata_csv))
build_file := $(PKGNAME)_$(PKGVERS).tar.gz
# processing directories
raw_dir := analysis/raw
interm_dir := analysis/intermediate
out_dir := analysis/out
scripts_dir := analysis/scripts
# processing variables
rscript := Rscript --vanilla
export processing_end := 2021-01-01
export raw_version := 0.3
sites := WFMS WFML PSP QC
download_url := http://atmoschem.asrc.cestm.albany.edu/~aqm/AQM_Products/downloads
routine_zip := $(raw_dir)/routine_chemistry_v0.1.zip
old_routine_out := $(patsubst %,$(interm_dir)/old_%.csv,$(sites))
raw_data := $(raw_dir)/raw_data_v$(raw_version)
raw_zip := $(raw_dir)/raw_data_v$(raw_version).zip
# get <site>_<data_source> for each entry in data_sources.csv
data_sources := $(shell sed "1d;s/^\([^,]*\),\([^,]*\).*/\1_\2/" analysis/config/data_sources.csv)
hourly_files := $(patsubst %,$(interm_dir)/hourly_%.sqlite,$(data_sources))
routine_out := routine_chemistry_v$(PKGVERS)
# ensure R package availability
check_rpkg = if (!requireNamespace("$(1)")) install.packages("$(1)")

.PHONY: all
all: routine

## Atmoschem Dataset

.PHONY: check_data
check_data:
	$(rscript) \
	-e '$(call check_rpkg,tinytest)' \
	-e 'tinytest::run_test_dir("analysis/tests")'

# save intermediate sqlite files for the processing viewer
.SECONDARY:

.PHONY: routine
routine: $(routine_out).zip

$(routine_out).zip: $(old_routine_out) $(hourly_files)
	$(rscript) $(scripts_dir)/routine_package.R $(out_dir)/$(routine_out)

$(interm_dir)/hourly_%.sqlite: $(interm_dir)/processed_%.sqlite $(scripts_dir)/aggregate_hourly.R
	$(rscript) $(scripts_dir)/aggregate_hourly.R $(shell echo $* | sed "s/_/ /")

# The way calibration files are created for each site (without data source)
# makes things a bit awkward. Processed site/data source files depend on the
# calibration site file
.SECONDEXPANSION:
$(interm_dir)/processed_%.sqlite: $(interm_dir)/raw_%.sqlite \
                                  $(interm_dir)/cals_$$(shell echo $$* | sed "s/_.*//").sqlite \
                                  $(scripts_dir)/process_new_data.R
	$(rscript) $(scripts_dir)/process_new_data.R $(shell echo $* | sed "s/_/ /")

# Calibration site file depends on all the raw site/calibration files
make_cal_deps = $(patsubst %,$(interm_dir)/raw_%.sqlite, $(filter $(1)%, $(data_sources)))
$(interm_dir)/cals_%.sqlite: $$(call make_cal_deps,$$*) \
                             raw_data $(scripts_dir)/load_calibration.R
	$(rscript) $(scripts_dir)/load_calibration.R $*

$(interm_dir)/raw_%.sqlite: raw_data $(scripts_dir)/load_raw.R
	$(rscript) $(scripts_dir)/load_raw.R $(shell echo $* | sed "s/_/ /")

$(interm_dir)/old_%.csv: $(routine_zip) $(scripts_dir)/clean_old_routine.R
	$(rscript) -e 'unzip("$(routine_zip)", overwrite = F, exdir = "$(raw_dir)")' && \
	$(rscript) $(scripts_dir)/clean_old_routine.R $*

.INTERMEDIATE: raw_data
raw_data: $(raw_zip)
	$(rscript) -e 'unzip("$(raw_zip)", overwrite = F, exdir = "$(raw_dir)")'

$(raw_dir)/%.zip:
	mkdir -p $(raw_dir) && \
	wget --user=aqm --ask-password -O $@ $(download_url)/$(shell echo $* | sed -E s/_v[0-9.]+$$//)/$*.zip

.PHONY: view
view:
	$(rscript) \
	-e '$(call check_rpkg,shiny)' \
	-e 'shiny::runApp("analysis/processing_viewer", launch.browser = T)'

## R package

.PHONY: check
check: $(build_file)
	R CMD check --no-manual $(build_file)

.PHONY: website
website: docs
	Rscript \
	-e '$(call check_rpkg,pkgdown)' \
	-e 'pkgdown::build_site(preview = TRUE)'

.PHONY: install
install: docs
	Rscript \
	-e '$(call check_rpkg,devtools)' \
	-e 'devtools::install(build = FALSE)'
# this alternative installation method is inconvenient due to the very slow
# build step
# install: install_deps $(build_file)
# 	R CMD INSTALL $(build_file)

.INTERMEDIATE: install_deps
install_deps: DESCRIPTION
	Rscript \
	-e '$(call check_rpkg,remotes)' \
	-e 'remotes::install_deps(dependencies = TRUE)'

$(build_file): docs
	R CMD build .

.INTERMEDIATE: docs
docs: $(pkgdata_rda) $(r_files) README.md
	Rscript \
	-e '$(call check_rpkg,roxygen2)' \
	-e 'roxygen2::roxygenise()'

$(pkgdata_rda): data-raw/package_data.R data-raw/narsto_flags.csv \
                data-raw/aqs_flags.csv
	$(rscript) data-raw/package_data.R

README.md: README.Rmd DESCRIPTION
	Rscript \
	-e '$(call check_rpkg,rmarkdown)' \
	-e 'rmarkdown::render("README.Rmd")'

.PHONY: clean
clean:
	@rm -rf $(build_file) $(PKGNAME).Rcheck
