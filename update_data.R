#####################################################################
# LONDON Cycle Hire - Live Data processing, cronjob every 5 mins
#####################################################################

# load packages
lapply(c('data.table', 'jsonlite', 'RMySQL'), require, character.only = TRUE)

# set scheme
scheme <- 1

# Retrieve db name
dbc = dbConnect(MySQL(), group = 'dataOps', dbname = 'common')
db_name <- dbGetQuery(dbc, paste("SELECT db_name FROM cycle_hires WHERE scheme_id =", scheme))[[1]]
dbDisconnect(dbc)

# connect to database
dbc = dbConnect(MySQL(), group = 'dataOps', dbname = db_name)

# load data from TFL
stations <- data.table(fromJSON(txt = 'https://api.tfl.gov.uk/bikepoint'), key = 'id')

# Update CURRENT ----------------------------------------------------------------------------------------------------------------
# extract from the list in the field "additionalProperties" the following info: [col 7: NbBikes; col 8: NbEmptyDocks; col 9: NbDocks]
current <- as.data.table(cbind(
                V1 = substr(gsub('[^0-9]', '', Sys.time()), 3, 12),
                V2 = sub('BikePoints_', '', stations$id), 
                matrix(
                    unlist(lapply(
                        1:dim(stations)[1], 
                        function(x) unlist(t(stations$additionalProperties[[x]][, 5][7:9]))
                    )),
                    ncol = 3, 
                    byrow = TRUE
                )
))
# convert station_id for ordering
current[, V2 := as.numeric(V2)]
# change names to fields
setnames(current, c('updated_at', 'station_id', 'bikes', 'free_docks', 'tot_docks'))
# save dataset
dbWriteTable(dbc, 'current', current[order(station_id)], append = TRUE, row.names = FALSE)

# Update STATIONS (at midnight) --------------------------------------------------------------------------
if(format(Sys.time(), '%H') == '00' & format(Sys.time(), '%M') < 15){
    # extract id, commonName, lat, lon, plus [col 1: terminalName] from the field "additionalProperties" 
    stations <- as.data.table(cbind(
                    station_id = sub('BikePoints_', '', stations$id), 
                    terminal_id = sapply(1:dim(stations)[1], function(x) stations$additionalProperties[[x]][, 5][1]),
                    x_lon = stations$lon,
                    y_lat = stations$lat,
                    address = stations$commonName
    ))
    # save dataset
    dbWriteTable(dbc, 'stations', stations, overwrite = FALSE, append = TRUE, row.names = FALSE)
    # update field "docks" in "stations" with field "tot_docks" from "current" 
    dbSendQuery(dbc, "DROP TABLE IF EXISTS tmp")    
    dbWriteTable(dbc, 'tmp', current[, .(station_id, tot_docks)], append = TRUE, row.names = FALSE)
    dbSendQuery(dbc, "UPDATE stations s JOIN tmp t ON t.station_id = s.station_id SET s.docks = t.tot_docks")    
    dbSendQuery(dbc, "DROP TABLE tmp")    
}

# Update last24
dbSendQuery(dbc, "TRUNCATE TABLE last24")
strSQL <- "
    INSERT INTO last24
        SELECT * 
        FROM current 
        WHERE updated_at > (
        	SELECT * 
        	FROM (
        		SELECT DISTINCT updated_at 
        		FROM current
        		ORDER BY updated_at desc
        		LIMIT 300
        	) t
        	ORDER BY updated_at
        	LIMIT 1
        )
"
dbSendQuery(dbc, strSQL)

# Clean & Exit ------------------------------------------------------------------------------------------------------------------
dbDisconnect(dbc)
rm(list = ls())
gc()

