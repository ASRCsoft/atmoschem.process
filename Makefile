# h/t to @jimhester and @yihui for this parse block:
# https://github.com/yihui/knitr/blob/dc5ead7bcfc0ebd2789fe99c527c7d91afb3de4a/Makefile#L1-L4
# Note the portability change as suggested in the manual:
# https://cran.r-project.org/doc/manuals/r-release/R-exts.html#Writing-portable-packages
PKGNAME = `sed -n "s/Package: *\([^ ]*\)/\1/p" DESCRIPTION`
PKGVERS = `sed -n "s/Version: *\([^ ]*\)/\1/p" DESCRIPTION`

# these are the .rda and .Rd files automatically generated from the
# package data .csv files
pkgdata_csv_files != find data-raw/package_data/*.csv
pkgdata_rda_files != echo ${pkgdata_csv_files} | sed 's/.*\//data\//' | sed 's/csv/rda/'
pkgdata_man_files != echo ${pkgdata_csv_files} | sed 's/.*\//man\//' | sed 's/csv/Rd/'
pkgdata_out_files = $(pkgdata_rda_files) $(pkgdata_man_files)

all: check

$(pkgdata_out_files) &: data-raw/package_data.R R/data.R $(pkgdata_csv_files)
	Rscript data-raw/package_data.R
	# package_data.R doesn't always rewrite files. `touch` updates
	# the file modified times so make knows they're up to date.
	touch $(pkgdata_out_files)

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
