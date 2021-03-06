% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/derived_values.R
\name{res_wind_speed}
\alias{res_wind_speed}
\alias{res_wind_dir}
\title{Resultant wind speed and direction}
\usage{
res_wind_speed(speed, dir, na.rm = TRUE)

res_wind_dir(speed, dir, na.rm = TRUE)
}
\arguments{
\item{speed}{Wind speeds.}

\item{dir}{Wind directions in degrees clockwise from north.}

\item{na.rm}{A logical value indicating whether \code{NA} values should be
stripped before the computation proceeds.}
}
\value{
\code{res_wind_speed} returns the resultant wind
  speed. \code{res_wind_dir} returns the resultant wind direction.
}
\description{
Calculate the resultant wind speed and direction from a collection of wind
speeds and directions.
}
\details{
Resultant wind speeds are vector-averaged. We first find the average values
of the east-west and north-south components,

\deqn{V_e = -\frac{1}{N} \sum u_i \sin(\theta_i)}{Ve = -sum(u_i * sin(theta_i)) / N}
\deqn{V_n = -\frac{1}{N} \sum u_i \cos(\theta_i)}{Vn = -sum(u_i * cos(theta_i)) / N}

where \eqn{u} is the speed and \eqn{\theta}{theta} is the direction. The
resultant speed and direction can then be calculated with

\deqn{U_R = \sqrt{V_e^2 + V_n^2}}{U_R = sqrt(Ve^2 + Vn^2)}
\deqn{\theta_R = \textrm{arctan}(\frac{V_e}{V_n}) + 180}{theta_R = arctan(Ve/Vn) + 180}
}
\examples{
# opposite winds cancel out
res_wind_speed(1, c(0, 180))

# A scalar average would return 180 in this case. Resultant wind direction
# produces the correct value.
res_wind_dir(1, c(355, 5))

}
\references{
\insertRef{us_environmental_protection_agency_meteorological_2000}{atmoschem.process}
}
