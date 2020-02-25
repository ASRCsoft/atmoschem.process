# h/t to @jimhester and @yihui for this parse block:
# https://github.com/yihui/knitr/blob/dc5ead7bcfc0ebd2789fe99c527c7d91afb3de4a/Makefile#L1-L4
# Note the portability change as suggested in the manual:
# https://cran.r-project.org/doc/manuals/r-release/R-exts.html#Writing-portable-packages
PKGNAME = `sed -n "s/Package: *\([^ ]*\)/\1/p" DESCRIPTION`
PKGVERS = `sed -n "s/Version: *\([^ ]*\)/\1/p" DESCRIPTION`

# these are the .rda and .Rd files automatically generated from the
# package data .csv files
pkgdata_csv_files != find data-raw/package_data/*.csv
pkgdata_rda_files != echo ${pkgdata_csv_files} | sed 's/.*\//data\//; s/csv/rda/'
pkgdata_man_files != echo ${pkgdata_csv_files} | sed 's/.*\//man\//; s/csv/Rd/'
pkgdata_out_files = $(pkgdata_rda_files) $(pkgdata_man_files)

all: check

# following https://stackoverflow.com/a/10609434/5548959
.INTERMEDIATE: update_pkgdata routine_chemistry_unzip clean_routine_chemistry

update_pkgdata: data-raw/package_data.R R/data.R $(pkgdata_csv_files)
	Rscript data-raw/package_data.R
	# package_data.R doesn't always rewrite files. `touch` updates
	# the file modified times so make knows they're up to date.
	touch $(pkgdata_out_files)

$(pkgdata_out_files): update_pkgdata ;

pkgdata: $(pkgdata_out_files)

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
routine_chemistry_url = $(download_url)/routine_chemistry/routine_chemistry_v0.1.zip
routine_chemistry_zip = $(raw_folder)/routine_chemistry_v0.1.zip
cleaned_routine_chemistry_csvs != echo ${sites} | tr ' ' '\n' | sed 's/^/datasets\/cleaned\/routine_chemistry\//; s/$$/\.csv/'

$(routine_chemistry_zip):
	mkdir -p ${raw_folder}
	wget --user=aqm --ask-password -O ${routine_chemistry_zip} ${routine_chemistry_url}

routine_chemistry_unzip: $(routine_chemistry_zip)
	unzip -n ${routine_chemistry_zip} -d ${raw_folder}

clean_routine_chemistry: routine_chemistry_unzip datasets/clean_processed_routine.R
	Rscript datasets/clean_processed_routine.R

$(cleaned_routine_chemistry_csvs): clean_routine_chemistry ;

cleaned_routine_chemistry: $(cleaned_routine_chemistry_csvs)
