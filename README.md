# The ASRC NYS Atmospheric Chemistry Database

The ASRC NYS Atmospheric Chemistry Database is a PostgreSQL database containing atmospheric chemistry data from ASRC sites in New York State.

The `nysatmoschem` R package provides utilities to reproduce the database, along with functions to generate various reports and processed datasets, and tools to visualize the data.

## Usage

`nysatmoschem` builds on the [`etl` package](https://cran.r-project.org/web/packages/etl/index.html) and follows the `etl` syntax and design structure. To reproduce the database, run `etl_init()` and `etl_update()` with a `nysatmoschem` dataset object. Users require a username and password to download the data, which can be obtained from the [ASRC AQM Data Products page](http://pireds.asrc.cestm.albany.edu:3000/).

```R
library(nysatmoschem)

# create a postgres database connection and nysatmoschem dataset object
dbcon = src_postgres(dbname = 'nysacdb', user = 'user')
nysac = etl('nysatmoschem', db = dbcon, dir = 'data')

# set up the database and add data from the ASRC's atmoschem server
nysac %>%
  etl_init() %>%
  etl_update(user = 'user', password = 'pass')
```

## Installation

Install `nysatmoschem` from R using the `remotes` package:

```R
install.packages('remotes')
remotes::install_github('ASRCsoft/nysatmoschem')
```

The package requires the PostgreSQL server to be running locally, and the database user account must have the required postgres privileges.

## License

`nysatmoschem` is released under the open source MIT license.
