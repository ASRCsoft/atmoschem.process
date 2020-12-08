## derive values from measurements

combine_measures = function(obj, site, data_source, m1, m2,
                            start_time, end_time) {
  site_id = atmoschem.process:::get_site_id(obj$con, site)
  if (is(start_time, 'POSIXt')) attributes(start_time)$tzone = 'EST'
  if (is(end_time, 'POSIXt')) attributes(end_time)$tzone = 'EST'
  q1 = 'select * from combine_measures(?site, ?ds, ?m1, ?m2) where time between ?start and ?end'
  sql1 = DBI::sqlInterpolate(obj$con, q1, site = site_id, ds = data_source,
                             m1 = m1, m2 = m2, start = start_time, end = end_time)
  obj %>%
    tbl(sql(sql1)) %>%
    mutate(time = timezone('EST', time)) %>%
    collect()
}

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

## each function takes an etl object, start time and end time, and returns the
## unprocessed data

## WFMS functions
wfms_no2 = function(obj, start_time, end_time) {
  combine_measures(obj, 'WFMS', 'campbell', 'NO', 'NOx',
                   start_time, end_time) %>%
    mutate(measurement_type_id =
             get_measurement_type_id(obj$con, 'WFMS', 'campbell', 'NO2'),
           value = value2 - value1,
           flagged = flagged1 | flagged2) %>%
    select(measurement_type_id, time, value, flagged)
}

wfms_slp = function(obj, start_time, end_time) {
  combine_measures(obj, 'WFMS', 'campbell', 'BP', 'PTemp_C',
                   start_time, end_time) %>%
    mutate(measurement_type_id =
             get_measurement_type_id(obj$con, 'WFMS', 'campbell', 'SLP'),
           value = sea_level_pressure(value1, value2, 1483.5),
           flagged = flagged1 | flagged2) %>%
    select(measurement_type_id, time, value, flagged)
}

wfms_ws = function(obj, start_time, end_time) {
  combine_measures(obj, 'WFMS', 'campbell', 'WS3Cup', 'WS3CupB',
                   start_time, end_time) %>%
    mutate(measurement_type_id =
             get_measurement_type_id(obj$con, 'WFMS', 'campbell', 'WS'),
           value = pmax(value1, value2, na.rm = TRUE),
           flagged = flagged1 & flagged2) %>%
    select(measurement_type_id, time, value, flagged)
}

wfms_ws_components = function(obj, start_time, end_time) {
  ws = combine_measures(obj, 'WFMS', 'campbell', 'WS', 'WindDir_D1_WVT',
                        start_time, end_time) %>%
    mutate(ws = value1,
           theta = pi * (270 - value2) / 180,
           flagged = flagged1 | flagged2) %>%
    mutate(u = ws * cos(theta),
           v = ws * sin(theta)) %>%
    select(time, u, v, flagged) %>%
    tidyr::gather(component, value, -time, -flagged) %>%
    mutate(measurement_type_id =
             ifelse(component == 'u',
                    get_measurement_type_id(obj$con, 'WFMS', 'campbell', 'WS_u'),
                    get_measurement_type_id(obj$con, 'WFMS', 'campbell', 'WS_v'))) %>%
    select(measurement_type_id, time, value, flagged)
}

wfms_ws_max = function(obj, start_time, end_time) {
  combine_measures(obj, 'WFMS', 'campbell', 'WS3Cup_Max', 'WS3CupB_Max',
                   start_time, end_time) %>%
    mutate(measurement_type_id =
             get_measurement_type_id(obj$con, 'WFMS', 'campbell', 'WS_Max'),
           value = pmax(value1, value2, na.rm = TRUE),
           flagged = flagged1 & flagged2) %>%
    select(measurement_type_id, time, value, flagged)
}

wfms_wood_smoke = function(obj, start_time, end_time) {
  combine_measures(obj, 'WFMS', 'aethelometer',
                   'concentration_370', 'concentration_880',
                   start_time, end_time) %>%
    mutate(measurement_type_id =
             get_measurement_type_id(obj$con, 'WFMS', 'aethelometer',
                                     'Wood smoke'),
           value = wood_smoke(value1, value2),
           flagged = flagged1 | flagged2) %>%
    select(measurement_type_id, time, value, flagged)
}

derived_wfms = list(
    campbell = list(wfms_no2, wfms_slp, wfms_ws, wfms_ws_components,
                    wfms_ws_max),
    aethelometer = list(wfms_wood_smoke)
)


## WFML functions
wfml_no2 = function(obj, start_time, end_time) {
  combine_measures(obj, 'WFML', 'campbell', 'NO', 'NOX',
                   start_time, end_time) %>%
    mutate(measurement_type_id =
             get_measurement_type_id(obj$con, 'WFML', 'campbell', 'NO2'),
           value = value2 - value1,
           flagged = flagged1 | flagged2) %>%
    select(measurement_type_id, time, value, flagged)
}

wfml_ozone = function(obj, start_time, end_time) {
  ## convert ozone to ppbv
  o3_id = get_measurement_type_id(obj$con, 'WFML', 'envidas',
                                  'API-T400-OZONE')
  get_measurements(obj, o3_id, start_time, end_time) %>%
    mutate(measurement_type_id =
             get_measurement_type_id(obj$con, 'WFML', 'envidas', 'Ozone_ppbv'),
           value = value * 1000) %>%
    select(measurement_type_id, time, value, flagged)
}

wfml_slp = function(obj, start_time, end_time) {
  combine_measures(obj, 'WFML', 'campbell', 'BP2', 'PTemp_C',
                   start_time, end_time) %>%
    mutate(measurement_type_id =
             get_measurement_type_id(obj$con, 'WFML', 'campbell', 'SLP'),
           value = sea_level_pressure(value1, value2, 604),
           flagged = flagged1 | flagged2) %>%
    select(measurement_type_id, time, value, flagged)
}

wfml_ws_components = function(obj, start_time, end_time) {
  ws = combine_measures(obj, 'WFML', 'mesonet', 'wind_speed [m/s]',
                        'wind_direction [degrees]', start_time,
                        end_time)
  if (nrow(ws) == 0) return(ws)
  ws %>%
    mutate(ws = value1,
           theta = pi * (270 - value2) / 180,
           flagged = flagged1 | flagged2) %>%
    mutate(u = ws * cos(theta),
           v = ws * sin(theta)) %>%
    select(time, u, v, flagged) %>%
    tidyr::gather(component, value, -time, -flagged) %>%
    mutate(measurement_type_id =
             ifelse(component == 'u',
                    get_measurement_type_id(obj$con, 'WFML', 'mesonet', 'WS_u'),
                    get_measurement_type_id(obj$con, 'WFML', 'mesonet', 'WS_v'))) %>%
    select(measurement_type_id, time, value, flagged)
}

derived_wfml = list(
    campbell = list(wfml_no2, wfml_slp),
    envidas = list(wfml_ozone),
    mesonet = list(wfml_ws_components)
)


## PSP functions
psp_no2 = function(obj, start_time, end_time) {
  combine_measures(obj, 'PSP', 'envidas', 'NO', 'NOx',
                   start_time, end_time) %>%
    mutate(measurement_type_id =
             get_measurement_type_id(obj$con, 'PSP', 'envidas', 'NO2'),
           value = value2 - value1,
           flagged = flagged1 | flagged2) %>%
    select(measurement_type_id, time, value, flagged)
}

psp_hno3 = function(obj, start_time, end_time) {
  combine_measures(obj, 'PSP', 'envidas', 'NOy', 'NOy-HNO3',
                   start_time, end_time) %>%
    mutate(measurement_type_id =
             get_measurement_type_id(obj$con, 'PSP', 'envidas', 'HNO3'),
           value = value1 - value2,
           flagged = flagged1 | flagged2) %>%
    select(measurement_type_id, time, value, flagged)
}

## fix this one!
psp_precip = function(obj, start_time, end_time) {
  rain_id = get_measurement_type_id(obj$con, 'PSP', 'envidas', 'Rain')
  ## carefully deal with value resets from 0.5 back to 0, which
  ## usually happens in only one minute, but sometimes takes two
  ## minutes with an average measurement (of the previous value and
  ## zero) in between
  get_measurements(obj, rain_id, start_time, end_time) %>%
    arrange(time) %>%
    mutate(measurement_type_id =
             get_measurement_type_id(obj$con, 'PSP', 'envidas', 'Precip'),
           d1 = c(NA, diff(value)),
           resetting = d1 <= -.02,
           value = case_when(
               resetting & !lag(resetting) & !lead(resetting) ~ d1 + .5,
               resetting & lead(resetting) ~ .5 - lag(value),
               resetting & lag(resetting) ~ value,
               TRUE ~ d1
           ) * 25.4,
           flagged = is_true(flagged | lag(flagged))) %>%
    select(measurement_type_id, time, value, flagged)
}

psp_teoma25_base = function(obj, start_time, end_time) {
  combine_measures(obj, 'PSP', 'envidas', 'TEOMA(2.5)MC', 'TEOMA(2.5)RefMC',
                   start_time, end_time) %>%
    mutate(measurement_type_id =
             get_measurement_type_id(obj$con, 'PSP', 'envidas',
                                     'TEOMA(2.5)BaseMC'),
           value = value1 + value2,
           flagged = flagged1 | flagged2) %>%
    select(measurement_type_id, time, value, flagged)
}

psp_teombcrs_base = function(obj, start_time, end_time) {
  combine_measures(obj, 'PSP', 'envidas', 'TEOMB(crs)MC', 'TEOMB(crs)RefMC',
                   start_time, end_time) %>%
    mutate(measurement_type_id =
             get_measurement_type_id(obj$con, 'PSP', 'envidas',
                                     'TEOMB(crs)BaseMC'),
           value = value1 + value2,
           flagged = flagged1 | flagged2) %>%
    select(measurement_type_id, time, value, flagged)
}

psp_dichot10_base = function(obj, start_time, end_time) {
  combine_measures(obj, 'PSP', 'envidas', 'Dichot(10)MC', 'Dichot(10)RefMC',
                   start_time, end_time) %>%
    mutate(measurement_type_id =
             get_measurement_type_id(obj$con, 'PSP', 'envidas',
                                     'Dichot(10)BaseMC'),
           value = value1 + value2,
           flagged = flagged1 | flagged2) %>%
    select(measurement_type_id, time, value, flagged)
}

psp_wood_smoke = function(obj, start_time, end_time) {
  combine_measures(obj, 'PSP', 'envidas', 'BC1', 'BC6',
                   start_time, end_time) %>%
    mutate(measurement_type_id =
             get_measurement_type_id(obj$con, 'PSP', 'envidas',
                                     'Wood smoke'),
           value = wood_smoke(value1, value2),
           flagged = flagged1 | flagged2) %>%
    select(measurement_type_id, time, value, flagged)
}

psp_ws_components = function(obj, start_time, end_time) {
  ws = combine_measures(obj, 'PSP', 'envidas', 'VWS', 'VWD',
                        start_time, end_time) %>%
    mutate(ws = value1,
           theta = pi * (270 - value2) / 180,
           flagged = flagged1 | flagged2) %>%
    mutate(u = ws * cos(theta),
           v = ws * sin(theta)) %>%
    select(time, u, v, flagged) %>%
    tidyr::gather(component, value, -time, -flagged) %>%
    mutate(measurement_type_id =
             ifelse(component == 'u',
                    get_measurement_type_id(obj$con, 'PSP', 'envidas', 'WS_u'),
                    get_measurement_type_id(obj$con, 'PSP', 'envidas', 'WS_v'))) %>%
    select(measurement_type_id, time, value, flagged)
}

psp_meso_ws_components = function(obj, start_time, end_time) {
  ws = combine_measures(obj, 'PSP', 'mesonet', 'wind_speed [m/s]',
                        'wind_direction [degrees]', start_time,
                        end_time)
  if (nrow(ws) == 0) return(ws)
  ws %>%
    mutate(ws = value1,
           theta = pi * (270 - value2) / 180,
           flagged = flagged1 | flagged2) %>%
    mutate(u = ws * cos(theta),
           v = ws * sin(theta)) %>%
    select(time, u, v, flagged) %>%
    tidyr::gather(component, value, -time, -flagged) %>%
    mutate(measurement_type_id =
             ifelse(component == 'u',
                    get_measurement_type_id(obj$con, 'PSP', 'mesonet', 'WS_u'),
                    get_measurement_type_id(obj$con, 'PSP', 'mesonet', 'WS_v'))) %>%
    select(measurement_type_id, time, value, flagged)
}

psp_slp = function(obj, start_time, end_time) {
  combine_measures(obj, 'PSP', 'envidas', 'BP', 'AmbTemp',
                   start_time, end_time) %>%
    mutate(measurement_type_id =
             get_measurement_type_id(obj$con, 'PSP', 'envidas', 'SLP'),
           value = sea_level_pressure(value1, value2, 504),
           flagged = flagged1 | flagged2) %>%
    select(measurement_type_id, time, value, flagged)
}

psp_sr2 = function(obj, start_time, end_time) {
  sr_id = get_measurement_type_id(obj$con, 'PSP', 'envidas', 'SR')
  get_measurements(obj, sr_id, start_time, end_time) %>%
    mutate(measurement_type_id =
             get_measurement_type_id(obj$con, 'PSP', 'envidas', 'SR2'),
           value = value + 17.7) %>%
    select(measurement_type_id, time, value, flagged)
}

derived_psp = list(
    envidas = list(psp_no2, psp_hno3, psp_precip, psp_teoma25_base,
                   psp_teombcrs_base, psp_dichot10_base, psp_wood_smoke,
                   psp_ws_components, psp_slp, psp_sr2),
    mesonet = list(psp_meso_ws_components)
)

derived_vals = list('WFMS' = derived_wfms,
                    'WFML' = derived_wfml,
                    'PSP' = derived_psp)
