## getting envidas data into postgres

library(RPostgreSQL)

env_file = '/home/wmay/data/envidas/WFMS/envi-rpt-1808.csv'
lines = readLines(env_file)
csv_lines = lines[c(2, 5:length(lines))]
csv_text = paste(csv_lines, collapse = '\n')
env = read.csv(text = csv_text)
## cool it works!

## tidy column names
names(env) = gsub('_Avg$', '', names(env))
names(env) = gsub('^F_(.*)$', '\\1_flag', names(env))
names(env) = tolower(names(env))
## remove useless columns
env = env[, !grepl('^Spare', names(env))]
## format columns
env$TIMESTAMP = as.POSIXct(env$TIMESTAMP,
                           format = '%m/%d/%Y %H:%M',
                           tz = 'EST')
env$row = as.integer(row.names(env))
## rename a few columns
col_dict = c(TIMESTAMP = 'instrument_time',
             T = 'temperature', TRH_flag = 'temp_rh_flag',
             Rain_mm_Tot = 'rain', PTemp_C = 'ptemp',
             Ozone = 'o3')
names(env) = ifelse(names(env) %in% names(col_dict),
                    col_dict[names(env)], names(env))
## remove more columns
env = env[, c(1, 3:8, 10:11, 21:22, 24:29, 33)]
## add info
env$station_id = 1
env$file = 'envi-rpt-1808.csv'

## write to postgres
pg = dbConnect(PostgreSQL(), dbname = 'chemtest')
dbWriteTable(pg, 'envidas_wfms', env, append = T, row.names = F)
