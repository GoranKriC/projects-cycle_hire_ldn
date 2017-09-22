##################################################
# LONDON cycle hire - DATA PROCESSING
##################################################
# This script process data from <http://cycling.data.tfl.gov.uk/>

lapply(c('data.table', 'jsonlite', 'RMySQL'), require, character.only = TRUE)
dbc = dbConnect(MySQL(), group = 'dataOps', dbname = 'london_cycle_hire')

data.path <- '/home/datamaps/data/UK/cycle_hires/ldn/'
if(Sys.info()[['sysname']] == 'Windows') data.path <- 'D:/cloud/onedrive/data/UK/cycle_hires/ldn/'
setwd(data.path)

year_path <- '2017'
filenames <- list.files(year_path, pattern = '*.csv', full.names = TRUE)
fstart <- 28
records_processed <- 0
for(fl in fstart:length(filenames)){
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
    dbSendQuery(dbc, "DROP TABLE IF EXISTS tmpLoad")
    dbWriteTable(dbc, 'tmpLoad', tmp, row.names = FALSE)
    dbSendQuery(dbc, "UPDATE stations st JOIN tmpLoad t ON t.station_id = st.station_id SET st.place = t.place, st.area = t.area")
    dataset$start_station_name <- NULL
    dbSendQuery(dbc, "DROP TABLE IF EXISTS tmpLoad")
    dbWriteTable(dbc, 'tmpLoad', dataset, row.names = FALSE)
    dbSendQuery(dbc, "INSERT IGNORE INTO hires SELECT * FROM tmpLoad")
    print(paste('Processed', nrow(dataset), 'records for file', fl))
    dbSendQuery(dbc, "DROP TABLE IF EXISTS tmpLoad")
    records_processed <- records_processed + nrow(dataset)
}
print(paste('Total records processed: ', records_processed))

##################################################
# CLEAN AND UPDATE ALL CONNECTED INFORMATION
#
print('************************************************')
print('UPDATE <calendar> TABLE WITH NEW DATES')
dbSendQuery(dbc, "
    CALL proc_fill_calendar();
")
print('************************************************')
print('UPDATE STATIONS with geo info from postcodes and lookups')
strSQL = "
    UPDATE stations st 
    	JOIN geo_postcodes pc ON st.postcode = pc.postcode
    SET st.OA = pc.OA, st.PCS = pc.PCS, st.PCD = pc.PCD, st.PCA = pc.PCA 
"
dbSendQuery(dbc, strSQL)
strSQL = "
    UPDATE stations st 
    	JOIN geo_lookups lk ON st.OA = lk.OA
    SET st.LSOA = lk.LSOA, st.MSOA = lk.MSOA, st.LAD = lk.LAD, st.WARD = lk.WARD, st.PCON = lk.PCON
"
dbSendQuery(dbc, strSQL)
print('************************************************')
print('DELETING RIDES FROM/TO "VOID" STATIONS ')
dbSendQuery(dbc, "
    DELETE h FROM hires h JOIN (
         SELECT station_id
         FROM stations
         WHERE area = 'void'
    ) t ON t.station_id = h.start_station_id
")
dbSendQuery(dbc, "
    DELETE h FROM hires h JOIN (
         SELECT station_id
         FROM stations
         WHERE area = 'void'
    ) t ON t.station_id = h.end_station_id
")
print('************************************************')
print('DELETING RIDES SAME STATIONS WITH DURATION <= 60')
dbSendQuery(dbc, "
    DELETE FROM hires WHERE duration <= 60 AND start_station_id = end_station_id
")
print('************************************************')
print('UPDATE <distances> TABLE WITH NEW STATIONS')
if(Sys.info()[['sysname']] == 'Windows'){
    setwd('D:/R/projects/projects-london_cycle_hire/')
} else {
    setwd('/home/datamaps/projects/projects-london_cycle_hire/')
}
source('calc_distances.R')

print('************************************************')
print('UPDATE CNT OF HIRES AND AVG DURATION IN <distances>') # AVG(CASE WHEN duration < 86400 THEN duration ELSE 86400 END)) to limit single hire duration to 24h
dbSendQuery(dbc, "
    UPDATE distances dt JOIN (
        SELECT start_station_id, end_station_id, count(*) AS c, ROUND(AVG(duration)) as d
        FROM hires
        WHERE start_station_id != end_station_id
        GROUP BY start_station_id, end_station_id
    ) t ON t.start_station_id = dt.start_station_id AND t.end_station_id = dt.end_station_id 
    SET dt.hires = t.c, dt.duration = t.d
")
dbSendQuery(dbc, "
    UPDATE distances
    SET duration = NULL
    WHERE hires = 0
")
dbSendQuery(dbc, "
    DELETE from distances
    WHERE 
	 	start_station_id IN (SELECT station_id FROM stations WHERE area = 'void')
	 		OR
	 	end_station_id IN (SELECT station_id FROM stations WHERE area = 'void')
")

print('************************************************')
print('UPDATE first_hire, last_hire IN <stations>')
dbSendQuery(dbc, "
    UPDATE stations st JOIN ( 
    	SELECT start_station_id, MIN(start_day) AS sd, MAX(start_day) AS ed 
    	FROM hires 
    	GROUP BY start_station_id
    ) t ON t.start_station_id = st.station_id
    SET st.first_hire = t.sd, st.last_hire = t.ed
")
print('************************************************')
print('UPDATE is_active IN <stations>')
dbSendQuery(dbc, "UPDATE stations SET is_active = 1")
dbSendQuery(dbc, "
    UPDATE stations 
    SET is_active = 0
    WHERE docks = 0 OR ISNULL(postcode) OR ISNULL(first_hire) OR first_hire = 0 OR last_hire < ( SELECT d0 FROM calendar WHERE days_past = 6 )
")
print('**************************************************************************')
print('UPDATE CNT OF HIRES AND AVG DURATION FOR "SELF HIRES" IN <stations>') 
dbSendQuery(dbc, "
    UPDATE stations st JOIN (
        SELECT start_station_id, COUNT(*) AS c, ROUND(AVG(duration)) AS d
        FROM hires
        WHERE start_station_id = end_station_id
        GROUP BY start_station_id
    ) t ON t.start_station_id = st.station_id
    SET st.hires_self = t.c, st.duration_self = t.d
")
print('UPDATE CNT OF HIRES AND AVG DURATION FOR ALL STARTING HIRES IN <stations>') 
dbSendQuery(dbc, "
    UPDATE stations st JOIN (
        SELECT start_station_id, COUNT(*) AS c, ROUND(AVG(duration)) AS d
        FROM hires
        GROUP BY start_station_id
    ) t ON t.start_station_id = st.station_id
    SET st.hires_started = t.c, st.duration_started = t.d
")
print('UPDATE CNT OF HIRES AND AVG DURATION FOR ALL ENDING HIRES IN <stations>') 
dbSendQuery(dbc, "
    UPDATE stations st JOIN (
        SELECT end_station_id, COUNT(*) AS c, ROUND(AVG(duration)) AS d
        FROM hires
        GROUP BY end_station_id
    ) t ON t.end_station_id = st.station_id
    SET st.hires_ended = t.c, st.duration_ended = t.d
")
print('UPDATE CNT OF HIRES AND AVG DURATION FOR NOSELF STARTING HIRES IN <stations>') 
dbSendQuery(dbc, "
    UPDATE stations st JOIN (
        SELECT start_station_id, COUNT(*) AS c, ROUND(AVG(duration)) AS d
        FROM hires
        WHERE start_station_id != end_station_id
        GROUP BY start_station_id
    ) t ON t.start_station_id = st.station_id
    SET st.hires_started_noself = t.c, st.duration_started_noself = t.d
")
print('UPDATE CNT OF HIRES AND AVG DURATION FOR NOSELF ENDING HIRES IN <stations>') 
dbSendQuery(dbc, "
    UPDATE stations st JOIN (
        SELECT end_station_id, COUNT(*) AS c, ROUND(AVG(duration)) AS d
        FROM hires
        WHERE start_station_id != end_station_id
        GROUP BY end_station_id
    ) t ON t.end_station_id = st.station_id
    SET st.hires_ended_noself = t.c, st.duration_ended_noself = t.d
")

# SAVE stations and distances as csv files --------------------------------------------------------------------------------------
stations <- dbReadTable(dbc, 'stations')
write.csv(stations, 'stations.csv', row.names = FALSE)
distances <- dbReadTable(dbc, 'distances')
write.csv(distances, 'distances.csv', row.names = FALSE)


# CLEAN AND EXIT  ---------------------------------------------------------------------------------------------------------------
print('DONE!')
dbDisconnect(dbc)
rm(list = ls())
gc()

