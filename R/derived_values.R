# derive values from measurements

#' Resultant wind speed and direction
#'
#' Calculate the resultant wind speed and direction from a collection of wind
#' speeds and directions.
#'
#' Resultant wind speeds are vector-averaged. We first find the average values
#' of the east-west and north-south components,
#'
#' \deqn{V_e = -\frac{1}{N} \sum u_i \sin(\theta_i)}{Ve = -sum(u_i * sin(theta_i)) / N}
#' \deqn{V_n = -\frac{1}{N} \sum u_i \cos(\theta_i)}{Vn = -sum(u_i * cos(theta_i)) / N}
#'
#' where \eqn{u} is the speed and \eqn{\theta}{theta} is the direction. The
#' resultant speed and direction can then be calculated with
#'
#' \deqn{U_R = \sqrt{V_e^2 + V_n^2}}{U_R = sqrt(Ve^2 + Vn^2)}
#' \deqn{\theta_R = \textrm{arctan}(\frac{V_e}{V_n}) + 180}{theta_R = arctan(Ve/Vn) + 180}
#'
#' @param speed Wind speeds.
#' @param dir Wind directions in degrees clockwise from north.
#' @param na.rm A logical value indicating whether \code{NA} values should be
#'   stripped before the computation proceeds.
#' @return \code{res_wind_speed} returns the resultant wind
#'   speed. \code{res_wind_dir} returns the resultant wind direction.
#' @examples
#' # opposite winds cancel out
#' res_wind_speed(1, c(0, 180))
#'
#' # A scalar average would return 180 in this case. Resultant wind direction
#' # produces the correct value.
#' res_wind_dir(1, c(355, 5))
#'
#' @references
#'   \insertRef{us_environmental_protection_agency_meteorological_2000}{atmoschem.process}
#' @export
res_wind_speed = function(speed, dir, na.rm = TRUE) {
  n = max(length(speed), length(dir))
  theta = pi * dir / 180
  Ve = -mean(speed * sin(theta), na.rm = na.rm)
  Vn = -mean(speed * cos(theta), na.rm = na.rm)
  sqrt(Ve^2 + Vn^2)
}

#' @rdname res_wind_speed
#' @export
res_wind_dir = function(speed, dir, na.rm = TRUE) {
  n = max(length(speed), length(dir))
  theta = pi * dir / 180
  Ve = -mean(speed * sin(theta), na.rm = na.rm)
  Vn = -mean(speed * cos(theta), na.rm = na.rm)
  (180 * atan2(Ve, Vn) / pi + 180) %% 360
}

#' Sea level pressure
#'
#' Estimate sea level pressure from local pressure, temperature, and height
#' using the barometric formula.
#'
#' We estimate the sea level pressure using the barometric formula,
#'
#' \deqn{P = P_0 (\frac{T_0}{T})^\frac{Mg}{R^*L}}{P = P0 (T0/T)^(Mg / (Rstar L))}
#'
#' where \eqn{P} is pressure, \eqn{T} is temperature (in Kelvin), \eqn{H} is
#' height, and \eqn{L} is the temperature lapse rate. Variables with subscript 0
#' are the same at sea level. Substituting \eqn{T_0 = T - LH}{T0 = T - LH}, we
#' can solve for \eqn{P_0}{P0} in terms of pressure, temperature, and height:
#'
#' \deqn{P_0 = P (\frac{T}{T - LH})^\frac{Mg}{R^*L}}{P0 = P (T/(T - LH))^(Mg/(Rstar L))}
#'
#' The values of physical constants are taken from
#' \insertCite{coesa_us_1976;textual}{atmoschem.process}:
#'
#' \describe{
#'   \item{\eqn{L} (lapse rate)}{-.0065 K/m}
#'   \item{\eqn{M} (molar mass of air)}{0.0289644 kg/mol}
#'   \item{\eqn{g} (gravitational acceleration)}{9.80665 m/s^2}
#'   \item{\eqn{R^*}{Rstar} (universal gas constant)}{8.31432 J/(mol K)}
#' }
#'
#' @param press Pressure in millibars.
#' @param temp Temperature in Celsius.
#' @param height Height above sea level in meters.
#' @return Estimated sea level pressure.
#' @examples
#' # estimate sea level pressure at the Whiteface Mountain summit
#' sea_level_pressure(800, 30, 1483.5)
#'
#' @references \insertAllCited{}
#' @export
sea_level_pressure = function(press, temp, height) {
  tempK = temp + 273.15
  press * (tempK / (tempK + .0065 * height))^(-5.256)
}

#' Wood smoke indicator based on light absorption
#'
#' Derive a semi-quantitative wood smoke indicator from aethalometer
#' measurements using the Delta-C method.
#'
#' Wood smoke is estimated using the Delta-C method introduced by
#' \insertCite{allen_evaluation_2004;textual}{atmoschem.process}. Delta-C
#' (sometimes called UVPM) is the difference between aethalometer estimates at
#' 370nm (sometimes called UVBC) and 880nm:
#'
#' \deqn{Delta\textrm{-}C = BC_{370nm} - BC_{880nm}}{Delta-C = BC_370nm - BC_880nm}
#'
#' While absorption in the range of 880nm is known to identify black carbon,
#' additional material absorbs light in the ultraviolet range measured at
#' 370nm. Specifically, organic compounds associated with wood smoke become more
#' absorbent and are included in the 370nm measurement.
#'
#' The resulting estimate is "semi-quantitative" in the sense that it doesn't
#' represent the true quantity of wood smoke particles-- rather it is roughly
#' proportional to the true value.
#'
#' \insertNoCite{wang_characterization_2011}{atmoschem.process}
#' \insertNoCite{zhang_joint_2017}{atmoschem.process}
#'
#' @param bc370 370nm estimate of black carbon (UVBC).
#' @param bc880 880nm estimate of black carbon.
#' @return Delta-C wood smoke indicator values.
#' @examples
#' wood_smoke(1000, 800)
#'
#' @references \insertAllCited{}
#' @export
wood_smoke = function(bc370, bc880) bc370 - bc880
