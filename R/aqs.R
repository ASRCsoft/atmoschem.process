#' Get the EPA monitoring day from a date
#'
#' Returns the corresponding day in the EPA's 12-day sampling schedule for the
#' given date.
#'
#' The EPA collects some samples every 3, 6, or 12 days. It provides a recurring
#' 12-day schedule for these samples. This function represents the schedule with
#' the integers 1 to 12. A sample taken every 3 days should be collected on days
#' 3, 6, 9, and 12. A sample on the 6-day schedule should be collected on days 6
#' and 12, and a sample on the 12-day schedule should be collected on day 12.
#'
#' @param d A date.
#' @return An integer from 1 to 12 representing the day in the EPA's 12-day
#'   monitoring schedule.
#' @examples
#' epa_schedule(as.Date('2020-01-01'))
#'
#' @seealso \url{https://www.epa.gov/amtic/sampling-schedule-calendar},
#'   \url{https://www3.epa.gov/ttn/amtic/calendar.html}
#' @export
epa_schedule = function(d) as.integer(d - as.Date('2020-01-05')) %% 12 + 1
