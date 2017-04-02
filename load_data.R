# This script process data from <http://cycling.data.tfl.gov.uk/>

lapply(c('data.table', 'jsonlite', 'RMySQL'), require, character.only = TRUE)
db_conn = dbConnect(MySQL(), group = 'homeserver', dbname = 'londonCycleHire')
data.path <- '/home/datamaps/data/londonCycleHire/'
if(Sys.info()[['sysname']] == 'Windows') data.path <- 'D:/cloud/onedrive/UK/LondonCycleHire/'
setwd(data.path)

year_path <- '2017'
filenames <- list.files(year_path, pattern = '*.csv', full.names = TRUE)
records_processed <- 0
for(fl in 1:length(filenames)){
    print(paste('Working on file', fl, 'out of', length(filenames) ) )
    dataset <- fread(filenames[fl])
    setnames(dataset, c('rental_id', 'duration', 'bike_id', 'end_date', 'end_station_id', 'end_station_name', 'start_date', 'start_station_id', 'start_station_name'))
    dataset$end_station_name <- NULL
    ### some files have an error in the end year...
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
    tmp[, place := trimws( substr(start_station_name, 1, regexpr(',', start_station_name) - 1 ) ) ]
    tmp[, area  := trimws( substr(start_station_name, regexpr(',', start_station_name) + 1, nchar(start_station_name) ) ) ]
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

##################################################
# CLEAN AND UPDATE ALL CONNECTED INFORMATION
#
print('************************************************')
print('UPDATE <calendar> TABLE WITH NEW DATES')
dbSendQuery(db_conn, "
    CALL proc_fill_calendar();
")
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
dbSendQuery(db_conn, "
    DELETE FROM hires WHERE duration = 0
")
print('************************************************')
print('UPDATE <distances> TABLE WITH NEW STATIONS')
if(Sys.info()[['sysname']] == 'Windows'){
    setwd('D:/R/projects/projects-london_cycle_hire/')
} else {
    setwd('/home/datamaps/projects-london_cycle_hire/')
}
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
print('UPDATE DAILY SUMMARIES FROM STARTING POINTS')
dbSendQuery(db_conn, paste("DELETE FROM smr_sStations WHERE datefield > ", as.integer(year_path) * 10000))
dbSendQuery(db_conn, paste("
    INSERT IGNORE INTO smr_sStations 
    	SELECT start_day AS datefield, start_station_id AS station_id, COUNT(*) AS hires, AVG(duration) AS duration
    	FROM hires 
        WHERE start_day > ", as.integer(year_path) * 10000, "
    	GROUP BY datefield, station_id
"))
print('************************************************')
print('UPDATE DAILY SUMMARIES TO ENDING POINTS')
dbSendQuery(db_conn, paste("DELETE FROM smr_eStations WHERE datefield > ", as.integer(year_path) * 10000))
dbSendQuery(db_conn, paste("
    INSERT IGNORE INTO smr_eStations 
    	SELECT end_day AS datefield, end_station_id AS station_id, COUNT(*) AS hires, AVG(duration) AS duration
    	FROM hires 
        WHERE end_day > ", as.integer(year_path) * 10000, "
    	GROUP BY datefield, station_id
"))
print('************************************************')
print('UPDATE DAILY SUMMARIES FROM STARTING POINTS TO ENDING POINTS')
dbSendQuery(db_conn, paste("DELETE FROM smr_seStations WHERE datefield > ", as.integer(year_path) * 10000))
dbSendQuery(db_conn, paste("
    INSERT IGNORE INTO smr_seStations 
    	SELECT start_day AS datefield, start_station_id AS sStation_id, end_station_id AS eStation_id, COUNT(*) AS hires, AVG(duration) AS duration
    	FROM hires
        WHERE start_day > ", as.integer(year_path) * 10000, "
    	GROUP BY datefield, sStation_id, eStation_id
"))
print('************************************************')
print('UPDATE MONTHLY SUMMARIES FROM STARTING POINTS TO ENDING POINTS')
dbSendQuery(db_conn, paste("DELETE FROM smrM_seStations WHERE datefield > ", as.integer(year_path) * 100))
dbSendQuery(db_conn, paste("
    INSERT INTO smrM_seStations 
    	SELECT LEFT(start_day, 6) AS datefield, start_station_id AS sStation_id, end_station_id AS eStation_id, COUNT(*) AS hires, AVG(duration) AS duration
    	FROM hires 
        WHERE start_day > ", as.integer(year_path) * 10000, "
    	GROUP BY datefield, sStation_id, eStation_id
"))
print('************************************************')
print('UPDATE WEEKLY SUMMARIES FROM STARTING POINTS TO ENDING POINTS')
start_day <- dbGetQuery(db_conn, "SELECT MIN(DATEwd) FROM calendar WHERE DATEd >= 20170000")
dbSendQuery(db_conn, paste("DELETE FROM smrM_seStations WHERE datefield >=", start_day))
dbSendQuery(db_conn, paste("
    INSERT IGNORE INTO smrW_seStations
    	SELECT DATEwd AS datefield, start_station_id AS sStation_id, end_station_id AS eStation_id, COUNT(*) AS hires, AVG(duration) AS duration
    	FROM hires h JOIN calendar c ON h.start_day = c.DATEd
        WHERE start_day >=", start_day, "
    	GROUP BY DATEwd, sStation_id, eStation_id
"))


# BYE
print('DONE!')
dbDisconnect(db_conn)
rm(list = ls())
gc()

