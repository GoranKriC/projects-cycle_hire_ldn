# This script process data from <http://cycling.data.tfl.gov.uk/>

# DATA STILL MISSING BECAUSE OF TFL INCOMPETENTS:
# - 6 is 7, real 6 (18/05-24/05) is missing
# - 20 and 21 are the same days, real 21 (31/08-06/09) is missing

lapply(c('data.table', 'jsonlite', 'RMySQL'), require, character.only = TRUE)
db_conn = dbConnect(MySQL(), group = 'homeserver', dbname = 'londonCycleHire')
setwd('/home/datamaps/data/londonCycleHire/')
trim <- function(x) gsub('^\\s+|\\s+$', '', x)

year_path <- '2016'
filenames <- list.files(year_path, pattern = '*.csv', full.names = TRUE)
records_processed <- 0
for(fl in 1:length(filenames)){
    print(paste('Working on file', fl, 'out of', length(filenames) ) )
    dataset <- fread(filenames[fl])
    setnames(dataset, c('rental_id', 'duration', 'bike_id', 'end_date', 'end_station_id', 'end_station_name', 'start_date', 'start_station_id', 'start_station_name'))
    dataset$end_station_name <- NULL
    ### because of some parasite idiots @ TFL: 
    endyear <- ifelse(substr(dataset$start_date[1], 9, 9) == ' ', 8, 10)
    ### split start_date into numeric day, hour and minute
    dataset[, start_day := as.numeric(paste(substr(start_date, 7, endyear), substr(start_date, 4, 5), substr(start_date, 1, 2), sep = ''))]
    dataset[, start_hour := as.numeric( substr(start_date, endyear + 2, endyear + 3) ) ]
    dataset[, start_min := as.numeric( substr(start_date, endyear + 5, endyear + 6) ) ]
    dataset$start_date <- NULL    
    ### split end_date into numeric day, hour and minute
    dataset[, end_day := as.numeric(paste(substr(end_date, 7, endyear), substr(end_date, 4, 5), substr(end_date, 1, 2), sep = ''))]
    dataset[, end_hour := as.numeric( substr(end_date, endyear + 2, endyear + 3) ) ]
    dataset[, end_min := as.numeric( substr(end_date, endyear + 5, endyear + 6) ) ]
    dataset$end_date <- NULL
    if(endyear == 8) dataset[, `:=`(start_day = start_day + 20000000, end_day = end_day + 20000000 ) ]
    dataset[end_station_id == 0, c('end_day', 'end_hour', 'end_min', 'duration') := list(start_day, start_hour, start_min, 0)]
    setcolorder(dataset, 
        c('start_station_name', 'rental_id', 'bike_id', 
          'start_station_id', 'start_day', 'start_hour', 'start_min', 
          'end_station_id', 'end_day', 'end_hour', 'end_min', 
          'duration'
    ))
    ### detect and save new stations and/or new info for old stations
    tmp <- unique(dataset[, .(station_id = start_station_id, start_station_name)])
    for(idx in 1:nrow(tmp)){
        if(length(grep(',', tmp$start_station_name[idx])) == 0) tmp$start_station_name[idx] <- paste(tmp$start_station_name[idx], ', void')
    }
    tmp[, place := trim( substr(start_station_name, 1, regexpr(',', start_station_name) - 1 ) ) ]
    tmp[, area  := trim( substr(start_station_name, regexpr(',', start_station_name) + 1, nchar(start_station_name) ) ) ]
    tmp$start_station_name <- NULL
    dbSendQuery(db_conn, "DROP TABLE IF EXISTS tmpLoad")
    dbWriteTable(db_conn, 'tmpLoad', tmp, row.names = FALSE)
    dbSendQuery(db_conn, "UPDATE stations st JOIN tmpLoad t ON t.station_id = st.station_id SET st.place = t.place, st.area = t.area")
    dataset$start_station_name <- NULL
    dbSendQuery(db_conn, "DROP TABLE IF EXISTS tmpLoad")
    dbWriteTable(db_conn, 'tmpLoad', dataset, row.names = FALSE)
    dbSendQuery(db_conn, "INSERT IGNORE INTO hires SELECT * FROM tmpLoad")
    print(paste('Processed', nrow(dataset), 'records for file', fl))
    dbSendQuery(db_conn, "DROP TABLE IF EXISTS tmpLoad")
    records_processed <- records_processed + nrow(dataset)
}
print(paste('Total records for the year: ', records_processed))

setwd('/home/datamaps//projects-london_cycle_hire/')

##################################################
# CLEAN AND UPDATE ALL CONNECTED INFORMATION
#
print('************************************************')
print('UPDATE <calendar> TABLE WITH NEW DATES')
dbSendQuery(db_conn, "CALL proc_fill_calendar();")

print('************************************************')
print('ADD/UPDATE OUTPUT AREA ID TO <stations>')
dbSendQuery(db_conn, "
    UPDATE stations st 
        JOIN geo_postcodes pc ON pc.postcode = st.postcode 
    SET st.OA_id = pc.OA_id
")

print('************************************************')
print('DELETING RIDES FROM/TO "VOID" STATIONS ')
dbSendQuery(db_conn, "
    DELETE h FROM hires h JOIN (
         SELECT station_id
         FROM stations
         WHERE area = 'void'
    ) t ON t.station_id = h.start_station_id
")
dbSendQuery(db_conn, "
    DELETE h FROM hires h JOIN (
         SELECT station_id
         FROM stations
         WHERE area = 'void'
    ) t ON t.station_id = h.end_station_id
")

print('************************************************')
print('DELETING RIDES WITH DURATION = 0')
dbSendQuery(db_conn, "DELETE FROM hires WHERE duration = 0")

print('************************************************')
print('UPDATE <distances> TABLE WITH NEW STATIONS')
source('calc_distances.R')

print('************************************************')
print('UPDATE CNT OF HIRES AND AVG DURATION IN <distances>') # AVG(CASE WHEN duration < 86400 THEN duration ELSE 86400 END)) to limit single hire duration to 24h
dbSendQuery(db_conn, "
    UPDATE distances dt JOIN (
        SELECT start_station_id, end_station_id, count(*) AS c, ROUND(AVG(duration)) as d
        FROM hires
        WHERE start_station_id != end_station_id
        GROUP BY start_station_id, end_station_id
    ) t ON t.start_station_id = dt.start_station_id AND t.end_station_id = dt.end_station_id 
    SET dt.hires = t.c, dt.duration = t.d
")

print('************************************************')
print('UPDATE first_hire, last_hire IN <stations>')
dbSendQuery(db_conn, "
    UPDATE stations st JOIN ( 
    	SELECT start_station_id, MIN(start_day) AS sd, MAX(start_day) AS ed 
    	FROM hires 
    	GROUP BY start_station_id
    ) t ON t.start_station_id = st.station_id
    SET st.first_hire = t.sd, st.last_hire = t.ed
")

print('************************************************')
print('UPDATE is_active IN <stations>')
dbSendQuery(db_conn, "UPDATE stations SET is_active = 1")
dbSendQuery(db_conn, "
    UPDATE stations 
    SET is_active = 0
    WHERE docks = 0 OR ISNULL(postcode) OR ISNULL(first_hire) OR first_hire = 0 OR last_hire < ( SELECT DATEd FROM calendar WHERE daysPast = 6 )
")

print('**************************************************************************')
print('UPDATE CNT OF HIRES AND AVG DURATION FOR "SELF HIRES" IN <stations>') 
dbSendQuery(db_conn, "
    UPDATE stations st JOIN (
        SELECT start_station_id, COUNT(*) AS c, ROUND(AVG(duration)) AS d
        FROM hires
        WHERE start_station_id = end_station_id
        GROUP BY start_station_id
    ) t ON t.start_station_id = st.station_id
    SET st.hires_self = t.c, st.duration_self = t.d
")

print('UPDATE CNT OF HIRES AND AVG DURATION FOR ALL STARTING HIRES IN <stations>') 
dbSendQuery(db_conn, "
    UPDATE stations st JOIN (
        SELECT start_station_id, COUNT(*) AS c, ROUND(AVG(duration)) AS d
        FROM hires
        GROUP BY start_station_id
    ) t ON t.start_station_id = st.station_id
    SET st.hires_started = t.c, st.duration_started = t.d
")

print('UPDATE CNT OF HIRES AND AVG DURATION FOR ALL ENDING HIRES IN <stations>') 
dbSendQuery(db_conn, "
    UPDATE stations st JOIN (
        SELECT end_station_id, COUNT(*) AS c, ROUND(AVG(duration)) AS d
        FROM hires
        GROUP BY end_station_id
    ) t ON t.end_station_id = st.station_id
    SET st.hires_ended = t.c, st.duration_ended = t.d
")

print('UPDATE CNT OF HIRES AND AVG DURATION FOR NOSELF STARTING HIRES IN <stations>') 
dbSendQuery(db_conn, "
    UPDATE stations st JOIN (
        SELECT start_station_id, COUNT(*) AS c, ROUND(AVG(duration)) AS d
        FROM hires
        WHERE start_station_id != end_station_id
        GROUP BY start_station_id
    ) t ON t.start_station_id = st.station_id
    SET st.hires_started_noself = t.c, st.duration_started_noself = t.d
")

print('UPDATE CNT OF HIRES AND AVG DURATION FOR NOSELF ENDING HIRES IN <stations>') 
dbSendQuery(db_conn, "
    UPDATE stations st JOIN (
        SELECT end_station_id, COUNT(*) AS c, ROUND(AVG(duration)) AS d
        FROM hires
        WHERE start_station_id != end_station_id
        GROUP BY end_station_id
    ) t ON t.end_station_id = st.station_id
    SET st.hires_ended_noself = t.c, st.duration_ended_noself = t.d
")


##################################################
# CREATE SUMMARY TABLES FOR SHINY APPS
##################################################

print('************************************************')
print('CREATE DAILY SUMMARIES FROM STARTING POINTS')
dbSendQuery(db_conn, "TRUNCATE TABLE smr_sStations")
dbSendQuery(db_conn, "
    INSERT INTO smr_sStations 
    	SELECT start_day AS datefield, start_station_id AS station_id, COUNT(*) AS hires, AVG(duration) AS duration
    	FROM hires 
    	GROUP BY datefield, station_id
")

print('************************************************')
print('CREATE DAILY SUMMARIES TO ENDING POINTS')
dbSendQuery(db_conn, "TRUNCATE TABLE smr_eStations")
dbSendQuery(db_conn, "
    INSERT INTO smr_eStations 
    	SELECT end_day AS datefield, end_station_id AS station_id, COUNT(*) AS hires, AVG(duration) AS duration
    	FROM hires 
    	GROUP BY datefield, station_id
")

print('************************************************')
print('CREATE DAILY SUMMARIES FROM STARTING POINTS TO ENDING POINTS')
dbSendQuery(db_conn, "TRUNCATE TABLE smr_seStations")
dbSendQuery(db_conn, "
    INSERT INTO smr_seStations 
    	SELECT end_day AS datefield, start_station_id AS sStation_id, end_station_id AS eStation_id, COUNT(*) AS hires, AVG(duration) AS duration
    	FROM hires 
    	GROUP BY datefield, sStation_id, estation_id
")



print('DONE!')

# BYE
dbDisconnect(db_conn)
rm(list = ls())
gc()

