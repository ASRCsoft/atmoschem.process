# R package variables
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
docs_dir := analysis/docs
conf_dir := analysis/config
# processing variables
rscript := Rscript --vanilla
export processing_end := 2021-07-01
export raw_version := $(PKGVERS)
sites := WFMS WFMB PSP QC
download_url := http://atmoschem.asrc.cestm.albany.edu/~aqm/AQM_Products/downloads
routine_zip := $(raw_dir)/routine_chemistry_v0.1.zip
old_routine_out := $(patsubst %,$(interm_dir)/old_%.csv,$(sites))
raw_data := $(raw_dir)/raw_data_v$(raw_version)
raw_zip := $(raw_dir)/raw_data_v$(raw_version).zip
# get <site>_<data_source> for each entry in data_sources.csv
data_sources := $(shell sed "1d;s/^\([^,]*\),\([^,]*\).*/\1_\2/" analysis/config/dataloggers.csv)
hourly_files := $(patsubst %,$(interm_dir)/hourly_%.sqlite,$(data_sources))
export routine_out := $(out_dir)/routine_chemistry_v$(PKGVERS)
# ensure R package availability
check_rpkg = if (!requireNamespace("$(1)")) install.packages("$(1)")

## Options available for make:	
##
## help       : Display available make commands.
.PHONY : help
help : Makefile
	@sed -n 's/^##//p' $<

.PHONY: all
all: routine

##
## Data processing commands
##

## check_data : Run data tests.
.PHONY: check_data
check_data:
	$(rscript) \
	-e '$(call check_rpkg,tinytest)' \
	-e 'tinytest::run_test_dir("analysis/tests")'

## clean_data : Remove intermediate data processing files.
.PHONY: clean_data
clean_data:
	@rm -rf $(raw_dir) && rm -rf $(interm_dir) && rm -rf $(out_dir)

# save intermediate sqlite files for the processing viewer
.SECONDARY:

## routine    : Generate routine monitoring dataset.
.PHONY: routine
routine: $(routine_out).zip

$(routine_out).zip: $(old_routine_out) $(hourly_files) $(docs_dir)/routine.md \
                    $(scripts_dir)/routine_package.R
	$(rscript) $(scripts_dir)/routine_package.R $(routine_out)

$(docs_dir)/routine.md: $(docs_dir)/routine.Rmd $(docs_dir)/routine.bib
	Rscript \
	-e '$(call check_rpkg,rmarkdown)' \
	-e 'rmarkdown::render("analysis/docs/routine.Rmd")'

$(interm_dir)/hourly_QC_AQS.sqlite: $(scripts_dir)/queens.R
	$(rscript) $(scripts_dir)/queens.R

$(interm_dir)/hourly_%.sqlite: $(interm_dir)/processed_%.sqlite $(scripts_dir)/aggregate_hourly.R
	$(rscript) $(scripts_dir)/aggregate_hourly.R $(shell echo $* | sed "s/_/ /")

# The way calibration files are created for each site (without data source)
# makes things a bit awkward. Processed site/data source files depend on the
# calibration site file
.SECONDEXPANSION:
$(interm_dir)/processed_%.sqlite: $(interm_dir)/raw_%.sqlite \
                                  $(interm_dir)/processedcals_$$(shell echo $$* | sed "s/_.*//").sqlite \
                                  $(scripts_dir)/process_new_data.R \
                                  $(conf_dir)/manual_flags.csv
	$(rscript) $(scripts_dir)/process_new_data.R $(shell echo $* | sed "s/_/ /")
# the WFMB campbell processing also depends on the processed Mesonet data
$(interm_dir)/processed_WFMB_campbell.sqlite: $(interm_dir)/processed_WFMB_mesonet.sqlite

$(interm_dir)/processedcals_%.sqlite: $(scripts_dir)/process_calibrations.R \
                             $(interm_dir)/cals_%.sqlite $(conf_dir)/cal_flags.csv
	$(rscript) $(scripts_dir)/process_calibrations.R $*

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
	wget --user=$(asrc_user) --password=$(asrc_pass) -O $@ $(download_url)/$(shell echo $* | sed -E s/_v[0-9.]+$$//)/$*.zip

## view       : Open the data viewer web app.
.PHONY: view
view:
	$(rscript) \
	-e '$(call check_rpkg,shiny)' \
	-e 'shiny::runApp("analysis/processing_viewer", launch.browser = T)'

## save_raw   : Package the raw data.
.PHONY: save_raw
save_raw:
	cd $(raw_dir) && \
	$(rscript) \
	-e 'zip("../../$(out_dir)/raw_data_v$(raw_version).zip", "raw_data_v$(raw_version)")'

##
## R package commands
##

## check      : Run `R CMD check` on the package.
.PHONY: check
check: $(build_file)
	R CMD check --no-manual $(build_file)

## website    : Generate and open package documentation website.
.PHONY: website
website: docs
	Rscript \
	-e '$(call check_rpkg,pkgdown)' \
	-e 'pkgdown::build_site(preview = TRUE)'

## install    : Install the package.
.PHONY: install
install: docs
	Rscript \
	-e '$(call check_rpkg,devtools)' \
	-e 'devtools::install(build = FALSE, upgrade = FALSE)'
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

## docs       : Update the package documentation.
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

## clean      : Remove package build files.
.PHONY: clean
clean:
	@rm -rf $(build_file) $(PKGNAME).Rcheck
