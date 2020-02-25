# h/t to @jimhester and @yihui for this parse block:
# https://github.com/yihui/knitr/blob/dc5ead7bcfc0ebd2789fe99c527c7d91afb3de4a/Makefile#L1-L4
# Note the portability change as suggested in the manual:
# https://cran.r-project.org/doc/manuals/r-release/R-exts.html#Writing-portable-packages
PKGNAME = `sed -n "s/Package: *\([^ ]*\)/\1/p" DESCRIPTION`
PKGVERS = `sed -n "s/Version: *\([^ ]*\)/\1/p" DESCRIPTION`

# these are the .rda and .Rd files automatically generated from the
# package data .csv files
pkgdata_csv = $(wildcard data-raw/package_data/*.csv)
pkgdata_rda = $(patsubst data-raw/package_data/%.csv,data/%.rda,$(pkgdata_csv))
pkgdata_man = $(patsubst data-raw/package_data/%.csv,man/%.Rd,$(pkgdata_csv))
pkgdata_out = $(pkgdata_rda) $(pkgdata_man)

all: check

# following https://stackoverflow.com/a/10609434/5548959
.INTERMEDIATE: update_pkgdata clean_routine

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
raw_folder = datasets/raw
download_url = http://atmoschem.asrc.cestm.albany.edu/~aqm/AQM_Products/bulk_downloads
routine_url = $(download_url)/routine_chemistry/routine_chemistry_v0.1.zip
routine_zip = $(raw_folder)/routine_chemistry_v0.1.zip
cleaned_routine_csvs = $(patsubst %,datasets/cleaned/routine_chemistry/%.csv,$(sites))

$(routine_zip):
	mkdir -p ${raw_folder} && \
	wget --user=aqm --ask-password -O ${routine_zip} ${routine_url}

clean_routine: $(routine_zip) datasets/clean_processed_routine.R
	unzip -n ${routine_zip} -d ${raw_folder} && \
	Rscript datasets/clean_processed_routine.R

$(cleaned_routine_csvs): clean_routine ;

cleaned_routine_chemistry: $(cleaned_routine_csvs)
