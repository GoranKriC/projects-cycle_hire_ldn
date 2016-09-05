# This script process data from <http://cycling.data.tfl.gov.uk/>

lapply(c('data.table', 'jsonlite', 'RMySQL'), require, character.only = TRUE)
db_conn = dbConnect(MySQL(), group = 'homeserver', dbname = 'londonCycleHire')
setwd('/home/datamaps/data/londonCycleHire/')
trim <- function(x) gsub('^\\s+|\\s+$', '', x)
year_path <- '2016'
filenames <- list.files(year_path, pattern = '*.csv', full.names = TRUE)
for(fl in 1:length(filenames)){
    print(paste('Working on file', fl, 'out of', length(filenames) ) )
    dataset <- fread(filenames[fl])
    setnames(dataset, c('rental_id', 'duration', 'bike_id', 'end_date', 'end_station_id', 'end_station_name', 'start_date', 'start_station_id', 'start_station_name'))
    dataset$end_station_name <- NULL
    ### split start_date into numeric day, hour and minute
    dataset[, start_day := as.numeric(paste(substr(start_date, 7, 10), substr(start_date, 4, 5), substr(start_date, 1, 2), sep = ''))]
    dataset[, start_hour := as.numeric( substr(start_date, 12, 13) ) ]
    dataset[, start_min := as.numeric( substr(start_date, 15, 16) ) ]
    dataset$start_date <- NULL    
    ### split end_date into numeric day, hour and minute
    dataset[, end_day := as.numeric(paste(substr(end_date, 7, 10), substr(end_date, 4, 5), substr(end_date, 1, 2), sep = ''))]
    dataset[, end_hour := as.numeric( substr(end_date, 12, 13) ) ]
    dataset[, end_min := as.numeric( substr(end_date, 15, 16) ) ]
    dataset$end_date <- NULL
    dataset[end_station_id == 0, c('end_day', 'end_hour', 'end_min', 'duration') := list(start_day, start_hour, start_min, 0)]
    ### detect and save new stations and/or new info for old stations
    tmp <- unique(dataset[, .(station_id = start_station_id, start_station_name)])
    for(idx in 1:nrow(tmp)){
        if(length(grep(',', tmp$start_station_name[idx])) == 0) tmp$start_station_name[idx] <- paste(tmp$start_station_name[idx], ', void')
    }
    tmp[, place := trim( substr(start_station_name, 1, regexpr(',', start_station_name) - 1 ) ) ]
    tmp[, area  := trim( substr(start_station_name, regexpr(',', start_station_name) + 1, nchar(start_station_name) ) ) ]
    tmp$start_station_name <- NULL
    dbWriteTable(db_conn, 'stations', tmp, row.names = FALSE, append = TRUE, overwrite = FALSE)
    dataset$start_station_name <- NULL    
    dbWriteTable(db_conn, 'hires', dataset, row.names = FALSE, append = TRUE, overwrite = FALSE)
    print(paste('Saved', nrow(dataset), 'records for file', fl))
}

# UPDATE ALL CONNECTED INFORMATION
setwd('/home/datamaps//projects-london_cycle_hire/')
source('additional_queries.R')

# BYE
dbDisconnect(db_conn)
rm(list = ls())
gc()

