library(atmoschem.process)

config = read_csv_dir('../config')

flag_periods = atmoschem.process:::as_interval(config$manual_flags$times)
time_start = lubridate::int_start(flag_periods)
time_end = lubridate::int_end(flag_periods)
expect_true(all(time_start < time_end), 'manual_flags time periods make sense')
