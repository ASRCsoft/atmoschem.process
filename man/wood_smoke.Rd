% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/derived_values.R
\name{wood_smoke}
\alias{wood_smoke}
\title{Wood smoke indicator based on light absorption}
\usage{
wood_smoke(bc370, bc880)
}
\arguments{
\item{bc370}{370nm estimate of black carbon (UVBC).}

\item{bc880}{880nm estimate of black carbon.}
}
\value{
Delta-C wood smoke indicator values.
}
\description{
Derive a semi-quantitative wood smoke indicator from aethalometer
measurements using the Delta-C method.
}
\details{
Wood smoke is estimated using the Delta-C method introduced by
\insertCite{allen_evaluation_2004;textual}{atmoschem.process}. Delta-C
(sometimes called UVPM) is the difference between aethalometer estimates at
370nm (sometimes called UVBC) and 880nm:

\deqn{Delta\textrm{-}C = BC_{370nm} - BC_{880nm}}{Delta-C = BC_370nm - BC_880nm}

While absorption in the range of 880nm is known to identify black carbon,
additional material absorbs light in the ultraviolet range measured at
370nm. Specifically, organic compounds associated with wood smoke become more
absorbent and are included in the 370nm measurement.

The resulting estimate is "semi-quantitative" in the sense that it doesn't
represent the true quantity of wood smoke particles-- rather it is roughly
proportional to the true value.

\insertNoCite{wang_characterization_2011}{atmoschem.process}
\insertNoCite{zhang_joint_2017}{atmoschem.process}
}
\examples{
wood_smoke(1000, 800)

}
\references{
\insertAllCited{}
}
