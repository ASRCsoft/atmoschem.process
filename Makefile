# h/t to @jimhester and @yihui for this parse block:
# https://github.com/yihui/knitr/blob/dc5ead7bcfc0ebd2789fe99c527c7d91afb3de4a/Makefile#L1-L4
# Note the portability change as suggested in the manual:
# https://cran.r-project.org/doc/manuals/r-release/R-exts.html#Writing-portable-packages
PKGNAME = `sed -n "s/Package: *\([^ ]*\)/\1/p" DESCRIPTION`
PKGVERS := $(shell sed -n "s/Version: *\([^ ]*\)/\1/p" DESCRIPTION)

# these are the .rda and .Rd files automatically generated from the
# package data .csv files
pkgdata_csv = $(wildcard data-raw/package_data/*.csv)
pkgdata_rda = $(patsubst data-raw/package_data/%.csv,data/%.rda,$(pkgdata_csv))
pkgdata_man = $(patsubst data-raw/package_data/%.csv,man/%.Rd,$(pkgdata_csv))
pkgdata_out = $(pkgdata_rda) $(pkgdata_man)

all: check

# following https://stackoverflow.com/a/10609434/5548959
.INTERMEDIATE: update_pkgdata clean_old_routine0 new_processed_data0

update_pkgdata: data-raw/package_data.R R/data.R $(pkgdata_csv)
	Rscript data-raw/package_data.R && \
	touch $(pkgdata_out)

$(pkgdata_out): update_pkgdata ;

pkgdata: $(pkgdata_out)

build:
	R CMD build .

check: build
	R CMD check --no-manual $(PKGNAME)_$(PKGVERS).tar.gz

install_deps:
	Rscript \
	-e 'if (!requireNamespace("remotes") install.packages("remotes")' \
	-e 'remotes::install_deps(dependencies = TRUE)'

install: install_deps build
	R CMD INSTALL $(PKGNAME)_$(PKGVERS).tar.gz

clean:
	@rm -rf $(PKGNAME)_$(PKGVERS).tar.gz $(PKGNAME).Rcheck

# Data processing targets
sites = WFMS WFML PSP QC
raw_dir = datasets/raw
cleaned_dir = datasets/cleaned
out_dir = datasets/out
download_url = http://atmoschem.asrc.cestm.albany.edu/~aqm/AQM_Products/bulk_downloads
routine_zip = $(raw_dir)/routine_chemistry_v0.1.zip
clean_old_routine_out = $(patsubst %,$(cleaned_dir)/old_routine/%.csv,$(sites))
raw_zip = $(raw_dir)/raw_data_v0.1.zip
# the new processed data doesn't yet contain all the sites
sites2 = WFMS WFML PSP
sites3 = WFMS PSP
new_hourly_csvs = $(patsubst %,$(cleaned_dir)/processed_data/%.csv,$(sites2))
new_instrument_csvs = $(patsubst %,$(cleaned_dir)/processed_data/%_instruments.csv,$(sites3))
new_processed_files = $(new_hourly_csvs) $(new_instrument_csvs)
routine_out = routine_chemistry_v$(PKGVERS)

$(raw_dir)/%.zip:
	mkdir -p $(raw_dir) && \
	wget --user=aqm --ask-password -O $@ $(download_url)/$(shell echo $* | sed -E s/_v[0-9.]+$$//)/$*.zip

clean_old_routine0: $(routine_zip) datasets/clean_old_routine.R
	unzip -nq $(routine_zip) -d $(raw_dir) && \
	Rscript datasets/clean_old_routine.R

$(clean_old_routine_out): clean_old_routine0 ;

clean_old_routine: $(clean_old_routine_out)

new_processed_data0: $(raw_zip) datasets/process_new_data.R
	unzip -nq $(raw_zip) -d $(raw_dir) && \
	Rscript datasets/process_new_data.R

$(new_processed_files): new_processed_data0 ;

new_processed_data: $(new_processed_files)

routine_dataset: clean_old_routine new_processed_data
	mkdir -p $(out_dir)/$(routine_out) && \
	Rscript datasets/routine_package.R $(out_dir)/$(routine_out)
	cp datasets/README.txt $(cleaned_dir)/processed_data/*_instruments.csv $(out_dir)/$(routine_out)
	cd $(out_dir); zip -r $(routine_out).zip $(routine_out)
