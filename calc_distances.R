lapply(c('data.table', 'RCurl', 'RMySQL', 'XML'), require, character.only = TRUE)

distance.driving.google <- function(orig, dest){
    xml.url <- paste('http://maps.googleapis.com/maps/api/distancematrix/xml?origins=', orig, '&destinations=', dest, '&mode=bicycling&sensor=false', sep = '')
    xmlfile <- xmlParse(getURL(xml.url))
    results <- numeric(2)
    results[1] <- xmlValue(xmlChildren(xpathApply(xmlfile, "//distance")[[1]])$value)
    results[2] <- xmlValue(xmlChildren(xpathApply(xmlfile, "//duration")[[1]])$value)
    return(results)
}
db_conn2 = dbConnect(MySQL(), group = 'homeserver', dbname = 'londonCycleHire')

# Fill <distances> with "new" stations (first create cross join of all valid stations, then insert only the new ones)
strSQL <- "
    INSERT IGNORE INTO distances
        SELECT sts.station_id AS start_station_id, ste.station_id AS end_station_id, 0, 0, 0, 0
        FROM stations sts CROSS JOIN stations ste
        WHERE sts.lat != 0 AND ste.lat != 0 AND sts.station_id != ste.station_id
        ORDER BY start_station_id, end_station_id
"
dbSendQuery(db_conn2, strSQL)

# Extract from <distances> only the stations with a null distance
strSQL <- "
    SELECT dt.start_station_id, sts.lat as lats, sts.`long` as longs, dt.end_station_id, ste.lat as late, ste.`long` as longe
    FROM distances dt
        JOIN stations sts ON sts.station_id = dt.start_station_id
        JOIN stations ste ON ste.station_id = dt.end_station_id
    WHERE dt.time = 0
    ORDER BY start_station_id, end_station_id
"
stations <- suppressWarnings(data.table(dbGetQuery(db_conn2, strSQL) ) )

# Update distance and riding time using google maps API (ggmap)
if(nrow(stations)){
    for(idx in 1:nrow(stations)){
        print(paste('Search number', idx))
        print(paste('Looking for distance and time between stations', stations[idx, .(start_station_id)], 'and', stations[idx, .(end_station_id)]))
        orig <- paste(stations[idx, .(lats, longs)], collapse = ',')
        dest <- paste(stations[idx, .(late, longe)], collapse = ',')
        outg <- as.numeric(distance.driving.google(orig, dest))
        strSQL <- paste(
            "UPDATE distances SET distance =", outg[1],
            ", time =", outg[2],
            "WHERE start_station_id =", stations[idx, start_station_id],
            "AND end_station_id =", stations[idx, end_station_id]
        )
        dbSendQuery(db_conn2, strSQL)
        print(paste('Done! Distance is', outg[1], 'meters and time is', outg[2], 'seconds'))
        Sys.sleep(runif(1, 0.05, 0.25))
    }
}

# BYE BYE ...
dbDisconnect(db_conn2)
