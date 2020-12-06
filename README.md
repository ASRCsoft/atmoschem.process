
<!-- README.md is generated from README.Rmd. Please edit that file -->

# ASRC Atmospheric Chemistry Data Processing

[![R build
status](https://github.com/ASRCsoft/atmoschem.process/workflows/R-CMD-check/badge.svg)](https://github.com/ASRCsoft/atmoschem.process/actions)
[![Codecov test
coverage](https://codecov.io/gh/ASRCsoft/atmoschem.process/branch/master/graph/badge.svg)](https://codecov.io/gh/ASRCsoft/atmoschem.process?branch=master)

The `atmoschem.process` R package processes atmospheric chemistry data
from ASRC sites in New York State. It provides tools to generate reports
and processed datasets from the ASRC’s atmospheric chemistry data, and
tools to visualize the data.

## Installation

To install the package, run (from within R)

``` r
install.packages('remotes')
remotes::install_github('ASRCsoft/atmoschem.process')
```

To generate the processed dataset, additional dependencies are requried:

``` r
remotes::install_github('ASRCsoft/atmoschem.process', dependencies = TRUE)
```

## Reproducing the routine chemistry dataset

### Requirements

  - Linux
  - R and R package dependencies
  - PostgreSQL, and a user with permission to create and delete
    databases

The processing is currently very computationally intensive and requires
about 8GB of RAM and 30GB of disk space.

### Creating the dataset

Download or clone the repository. The dataset package can be generated
by running (from a terminal)

``` sh
cd path/to/atmoschem.process
make routine_dataset
```

You will be asked for the atmoschem server’s data password (twice),
which can be obtained from [the atmoschem
website](http://atmoschem.asrc.cestm.albany.edu/).

### Viewing the data

The R package comes with a Shiny app for viewing the processing steps.
After data has been processed, it can be opened from within R:

``` r
library(atmoschem.process)
# Create a postgres database connection and atmoschem.process dataset object
dbcon = src_postgres(dbname = 'nysatmoschemdb', user = 'user')
nysac = etl('atmoschem.process', db = dbcon, dir = 'data')
# Open the Shiny app
view_processing(nysac)
```

## Citation

The package citation can be obtained by running
`citation('atmoschem.process')`:

``` 

To cite package 'atmoschem.process' in publications use:

  William May (2020). atmoschem.process: ASRC Atmospheric Chemistry Data Processing. R
  package version 0.5.0. https://github.com/ASRCsoft/atmoschem.process

A BibTeX entry for LaTeX users is

  @Manual{,
    title = {atmoschem.process: ASRC Atmospheric Chemistry Data Processing},
    author = {William May},
    year = {2020},
    note = {R package version 0.5.0},
    url = {https://github.com/ASRCsoft/atmoschem.process},
  }
```

## License

`atmoschem.process` is released under the open source MIT license.
