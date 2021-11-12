
<!-- README.md is generated from README.Rmd. Please edit that file -->

# ASRC Atmospheric Chemistry Data Processing

[![R build
status](https://github.com/ASRCsoft/atmoschem.process/workflows/R-CMD-check/badge.svg)](https://github.com/ASRCsoft/atmoschem.process/actions)
[![Codecov test
coverage](https://codecov.io/gh/ASRCsoft/atmoschem.process/branch/master/graph/badge.svg)](https://codecov.io/gh/ASRCsoft/atmoschem.process?branch=master)
[![DOI](https://zenodo.org/badge/157783204.svg)](https://zenodo.org/badge/latestdoi/157783204)

<img src="man/figures/whiteface-station.jpg" width="400px" align="right" style="padding:0px 10px 10px 10px;" />

The `atmoschem.process` R package processes atmospheric chemistry data
from ASRC sites in New York State. It provides tools to generate reports
and processed datasets from the ASRC’s atmospheric chemistry data, and
tools to visualize the data.

## Table of contents

  - [Overview](#overview)
  - [Installation](#installation)
  - [Reproducing the routine chemistry
    dataset](#reproducing-the-routine-chemistry-dataset)
  - [Citation](#citation)
  - [License](#license)
  - [References](#references)

## Overview

The atmospheric chemistry group at the ASRC, run by Dr. Jim Schwab,
collects data from instruments at 4 sites across the state of New York:
Whiteface Mountain (summit and base), Pinnacle State Park, and Queens
College. We maintain a variety of instruments measuring ozone and ozone
precursors, airborne particulate matter, sulfur dioxide, and
meteorology.

The data undergoes a variety of adjustments and quality assurance checks
before being released to the public, where it is used by government
agencies and atmospheric science researchers, among others. Users can
access the datasets online via our website at
<http://atmoschem.asrc.cestm.albany.edu/>.

This software, which generates the processed datasets, synthesizes
advice from atmospheric monitoring, statistical programming, and data
management. In terms of data processing, we tend to follow guidelines
from NARSTO (Christensen et al. [2000](#ref-christensen_narsto_2000)),
and we also work regularly with EPA standards. The code is organized as
an R package (Marwick, Boettiger, and Mullen
[2018](#ref-marwick_packaging_2018)), and we use GitHub for project
management (Bryan [2018](#ref-bryan_excuse_2018)). For data management,
we tend to follow Briney ([2015](#ref-briney_data_2015)), and we try to
make the data convenient to use (White et al.
[2013](#ref-white_nine_2013)).

The data collection at Whiteface Mountain is described in detail in a
series of publications (Schwab, Wolfe, et al.
[2016](#ref-schwab_atmospheric_2016); Brandt et al.
[2016](#ref-brandt_atmospheric_2016); Schwab, Casson, et al.
[2016](#ref-schwab_atmospheric_2016-1)), and another paper describes the
measurements at Pinnacle State Park (Schwab, Spicer, and Demerjian
[2009](#ref-schwab_ozone_2009)).

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

  - R and R package dependencies
  - GNU Make
  - 15GB of disk space and 8GB of RAM
  - An Air Quality System API key (get one
    [here](https://aqs.epa.gov/aqsweb/documents/data_api.html))
  - A user/password for the ASRC’s atmoschem server (sign up
    [here](http://atmoschem.asrc.cestm.albany.edu/))

### Creating the dataset

Download or clone the repository. The dataset package can be generated
by running, from a terminal,

``` sh
cd /path/to/atmoschem.process
make routine asrc_user=youruser asrc_pass=yourpassword aqs_email=youremail aqs_key=yourkey
```

(replacing the values with your information).

Run `make help` to see more make options.

### Viewing the data

The R package comes with a Shiny app for viewing the processing steps.
After data has been processed, it can be launched with `make`:

``` sh
make view
```

## Citation

Please cite this package using the citation available from Zenodo:
[![DOI](https://zenodo.org/badge/157783204.svg)](https://zenodo.org/badge/latestdoi/157783204)

## License

`atmoschem.process` is released under the open source MIT license.

## References

<div id="refs" class="references">

<div id="ref-brandt_atmospheric_2016">

Brandt, Richard E., James J. Schwab, Paul W. Casson, Utpal K.
Roychowdhury, Douglas Wolfe, Kenneth L. Demerjian, Kevin L. Civerolo,
Oliver V. Rattigan, and H. Dirk Felton. 2016. “Atmospheric Chemistry
Measurements at Whiteface Mountain, NY: Ozone and Reactive Trace Gases.”
*Aerosol and Air Quality Research* 16 (3): 873–84.
<https://doi.org/10.4209/aaqr.2015.05.0376>.

</div>

<div id="ref-briney_data_2015">

Briney, Kristin. 2015. *Data Management for Researchers: Organize,
Maintain and Share Your Data for Research Success*. Pelagic Publishing
Ltd.

</div>

<div id="ref-bryan_excuse_2018">

Bryan, Jennifer. 2018. “Excuse Me, Do You Have a Moment to Talk About
Version Control?” *The American Statistician* 72 (1): 20–27.
<https://doi.org/10.1080/00031305.2017.1399928>.

</div>

<div id="ref-christensen_narsto_2000">

Christensen, Sigurd W., Thomas A. Boden, Les A. Hook, and Meng-Dawn
Cheng. 2000. “NARSTO Data Management Handbook.” NARSTO Quality Systems
Science Center.
<https://web.archive.org/web/20030401082229/http://cdiac.esd.ornl.gov:80/programs/NARSTO/pdf/dmhb_current_version.PDF>.

</div>

<div id="ref-marwick_packaging_2018">

Marwick, Ben, Carl Boettiger, and Lincoln Mullen. 2018. “Packaging Data
Analytical Work Reproducibly Using R (and Friends).” *The American
Statistician* 72 (1): 80–88.
<https://doi.org/10.1080/00031305.2017.1375986>.

</div>

<div id="ref-schwab_atmospheric_2016-1">

Schwab, James J., Paul Casson, Richard Brandt, Liquat Husain, Vincent
Dutkewicz, Douglas Wolfe, Kenneth L. Demerjian, et al. 2016.
“Atmospheric Chemistry Measurements at Whiteface Mountain, NY: Cloud
Water Chemistry, Precipitation Chemistry, and Particulate Matter.”
*Aerosol and Air Quality Research* 16 (3): 841–54.
<https://doi.org/10.4209/aaqr.2015.05.0344>.

</div>

<div id="ref-schwab_ozone_2009">

Schwab, James J., John B. Spicer, and Kenneth L. Demerjian. 2009.
“Ozone, Trace Gas, and Particulate Matter Measurements at a Rural Site
in Southwestern New York State: 1995–2005.” *Journal of the Air & Waste
Management Association* 59 (3): 293–309.
<https://doi.org/10.3155/1047-3289.59.3.293>.

</div>

<div id="ref-schwab_atmospheric_2016">

Schwab, James J., Douglas Wolfe, Paul Casson, Richard Brandt, Kenneth L.
Demerjian, Liquat Husain, Vincent A. Dutkiewicz, Kevin L. Civerolo, and
Oliver V. Rattigan. 2016. “Atmospheric Science Research at Whiteface
Mountain, NY: Site Description and History.” *Aerosol and Air Quality
Research* 16 (3): 827–40. <https://doi.org/10.4209/aaqr.2015.05.0343>.

</div>

<div id="ref-white_nine_2013">

White, Ethan P., Elita Baldridge, Zachary T. Brym, Kenneth J. Locey,
Daniel J. McGlinn, and Sarah R. Supp. 2013. “Nine Simple Ways to Make It
Easier to (Re)use Your Data.” *Ideas in Ecology and Evolution* 6 (2).
<https://doi.org/10.4033/iee.2013.6b.6.f>.

</div>

</div>
