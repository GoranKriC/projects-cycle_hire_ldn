# 
rm(list = ls())
gc()
libs <- c('data.table', 'RCurl', 'RMySQL', 'XML')
lapply(libs, require, character.only = TRUE)
rm(libs)
all_cons <- dbListConnections(MySQL())
for(con in all_cons) dbDisconnect(con)

distance.driving.google <- function(orig, dest){
    xml.url <- paste('http://maps.googleapis.com/maps/api/distancematrix/xml?origins=', orig, '&destinations=', dest, '&mode=bicycling&sensor=false', sep = '')
    xmlfile <- xmlParse(getURL(xml.url))
    results <- numeric(2)
    results[1] <- xmlValue(xmlChildren(xpathApply(xmlfile, "//distance")[[1]])$value)
    results[2] <- xmlValue(xmlChildren(xpathApply(xmlfile, "//duration")[[1]])$value)
    return(results)
}
db_conn = dbConnect(MySQL(), host='192.168.0.200', user='datamaps', password='mSt53dMP', dbname='londonCycleHire')

# FIRST TIME
stations <- suppressWarnings(data.table(dbGetQuery(db_conn, 
    "SELECT dt.start_station_id, sts.lat as lats, sts.`long` as longs, dt.end_station_id, ste.lat as late, ste.`long` as longe
     FROM distances dt
	     JOIN stations sts ON sts.station_id = dt.start_station_id
	     JOIN stations ste ON ste.station_id = dt.end_station_id
     WHERE dt.distance = 0	 
	")))
output <- data.frame(start_station_id = numeric(0), end_station_id = numeric(0), distance = numeric(0), time = numeric(0) )
for(idx in 1:nrow(stations)){
    print(paste('Search number', idx))
    print(paste('Looking for distance and time between stations', stations[idx, .(start_station_id)], 'and', stations[idx, .(end_station_id)]))
    orig <- paste(stations[idx, .(lats, longs)], collapse = ',')
    dest <- paste(stations[idx, .(late, longe)], collapse = ',')
    outg <- as.numeric(distance.driving.google(orig, dest))
    output <- rbind(output, c(stations[idx, .(start_station_id, end_station_id)], distance = outg[1], time = outg[2]))
    print(paste('Done! Distance is', outg[1], 'meters and time is', outg[2], 'seconds'))
}

dbSendQuery(db_conn, "TRUNCATE TABLE tmp")
dbWriteTable(db_conn, 'tmp', output, row.names = FALSE, append = TRUE)
dbDisconnect(db_conn)
rm(list = ls())
gc()


# WHEN ADDING NEW STATIONS


dbDisconnect(db_conn)
rm(list = ls())
gc()
