##################################################
# LONDON Cycle Hire - Live Data processing
##################################################
# This script process LIVE data from <https://api.tfl.gov.uk/bikepoint/>

lapply(c('data.table', 'jsonlite', 'RMySQL'), require, character.only = TRUE)
# Retrieve db name
dbc = dbConnect(MySQL(), group = 'dataOps', dbname = 'common')
db_name <- dbGetQuery(dbc, "SELECT db_name FROM common.cycle_hires WHERE scheme_id = 1")[[1]]
dbDisconnect(dbc)

# connect to database
dbc = dbConnect(MySQL(), group = 'dataOps', dbname = db_name)

# LOAD AND STRUCTURE DATA ------------------------------------------
stations <- data.table(fromJSON(txt = 'https://api.tfl.gov.uk/bikepoint'), key = 'id')
# extract from the list in the field "additionalProperties" the following info:
# - col 1: TerminalName 
# - col 7: NbBikes
# - col 8: NbEmptyDocks
# - col 9: NbDocks
tmp <- as.data.table(matrix(
                        unlist(lapply(
                            1:dim(stations)[1], 
                            function(x) unlist(t(stations$additionalProperties[[x]][, 5][c(1, 7, 8, 9)]))
                        )),
                        ncol = 4, 
                        byrow = TRUE
      ))
# keep only terminal
stations <- stations[, .(station_id = as.numeric(sub('BikePoints_', '', id)), name = commonName, lat, lon)]
stations[name == 'Imperial Wharf Station', name := paste(name, ', Chelsea', sep = '') ]
stations[nchar(name) - nchar(gsub(',', '', name)) == 0, name := paste(name, ', void', sep = '')]
stations[nchar(name) - nchar(gsub(',', '', name)) > 1, name := substr(name, 1, gregexpr(pattern = ',', name)[[1]][2] - 1) ]
stations[, name := gsub(' , ', ', ', name) ]
stations <- cbind(
              stations, 
              apply(
                  matrix(unlist(strsplit(stations$name,",")), ncol = 2, byrow = TRUE), 
                  2, 
                  function(x) gsub("^\\s+|\\s+$", "", x) 
              )
          )
stations$name <- NULL
stations <- cbind(stations, tmp)
rm(tmp)
stations[, OA := '0']
setnames(stations, c('station_id', 'y_lat', 'x_lon', 'place', 'area', 'terminal_id', 'bikes', 'free_docks', 'docks', 'OA'))
setcolorder(stations, c('station_id', 'terminal_id', 'x_lon', 'y_lat', 'OA', 'place', 'area', 'docks', 'free_docks', 'bikes'))
stations <- stations[order(station_id)]

# UPDATE CURRENT ----------------------------------------------------
time = substr(gsub('[^0-9]', '', Sys.time()), 3, 12)
current <- cbind(
                day = substr(time, 1, 6), 
                hour = substr(time, 7, 8), 
                min = substr(time, 9, 10), 
                stations[, .(station_id, free_docks, bikes)]
)
setkey(current, 'station_id')
strSQL <- "
    SELECT station_id, ANY_VALUE(free_docks) AS recent
    FROM (
        SELECT station_id, free_docks
        FROM current
        ORDER BY day DESC, hour DESC, min DESC
    ) t
    GROUP BY station_id
"
recent <- data.table(dbGetQuery(dbc, strSQL), key = 'station_id')
current <- current[recent][as.numeric(free_docks) != recent ]
current[, recent := NULL]
current <- current[as.numeric(free_docks) < 255 & as.numeric(bikes) < 255 ]
dbWriteTable(dbc, 'current', current, row.names = FALSE, append = TRUE)

# UPDATE STATIONS AND DOCKS (ONLY JUST AFTER MIDNIGHT) -------------
if(format(Sys.time(), '%H') == '00' & format(Sys.time(), '%M') < 15){
    stations[, `:=`(free_docks = NULL, bikes = NULL)]
    dbSendQuery(dbc, "DROP TABLE IF EXISTS ttmp")
    dbWriteTable(dbc, 'ttmp', stations, row.names = FALSE)
    dbSendQuery(dbc, "
        INSERT IGNORE INTO stations (station_id, terminal_id, x_lon, y_lat, place, area, docks)
            SELECT station_id, terminal_id, x_lon, y_lat, place, area, docks
            FROM ttmp
    ")
    dbSendQuery(dbc, "
        UPDATE stations st 
           JOIN ttmp t ON st.station_id = t.station_id
        SET st.docks = t.docks
    ")
    stations <- stations[docks > 0, .(station_id, docks)]
    setkey(stations, 'station_id')
    docks <- data.table(dbGetQuery(dbc, "SELECT station_id, docks AS oldDocks FROM docks"), key = 'station_id')
        stations <- docks[stations][oldDocks > 0][oldDocks != docks ]
    stations[, oldDocks := NULL]
    stations <- stations[, date_updated := as.numeric(format(Sys.Date(), '%Y%m%d')) ]
    setcolorder(stations, c('station_id', 'date_updated', 'docks'))
    dbWriteTable(dbc, 'docks', stations, row.names = FALSE, append = TRUE)
}

# BYE
dbDisconnect(dbc)
rm(list = ls())
gc()

