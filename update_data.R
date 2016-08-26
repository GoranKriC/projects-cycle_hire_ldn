# 
rm(list = ls())
gc()
libs <- c('data.table', 'jsonlite', 'RMySQL')
libs <- lapply(libs, require, character.only = TRUE)
rm(libs)
db_conn = dbConnect(MySQL(), group = 'homeserver', dbname = 'londonCycleHire')

# LOAD AND STRUCTURE DATA ------------------------------------------
stations <- data.table(fromJSON(txt = 'https://api.tfl.gov.uk/bikepoint'), key = 'id')
tmp <- as.data.table(matrix(
                        unlist(lapply(
                            1:dim(stations)[1], 
                            function(x) unlist(t(stations$additionalProperties[[x]][, 5][c(1, 7, 8, 9)]))
                        )),
                        ncol = 4, 
                        byrow = TRUE
      ))
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
stations[, OA_id := '0']
setnames(stations, c('station_id', 'lat', 'long', 'place', 'area', 'terminal_id', 'bikes', 'freeDocks', 'docks', 'OA_id'))
setcolorder(stations, c('station_id', 'terminal_id', 'lat', 'long', 'OA_id', 'place', 'area', 'docks', 'freeDocks', 'bikes'))
stations <- stations[order(station_id)]

# UPDATE CURRENT ----------------------------------------------------
time = substr(gsub('[^0-9]', '', Sys.time()), 3, 12)
current <- cbind(day = substr(time, 1, 6), hour = substr(time, 7, 8), min = substr(time, 9, 10), stations[, .(station_id, freeDocks, bikes)])
setkey(current, 'station_id')
recent <- data.table(dbGetQuery(db_conn, 
                "SELECT station_id, freeDocks AS recent FROM (SELECT station_id, freeDocks FROM current ORDER BY day DESC, hour DESC, min DESC) t GROUP BY station_id"
          ), key = 'station_id')
current <- current[recent][as.numeric(freeDocks) != recent ]
current[, recent := NULL]
current <- current[as.numeric(freeDocks) < 255 & as.numeric(bikes) < 255 ]
dbWriteTable(db_conn, 'current', current, row.names = FALSE, append = TRUE)

# UPDATE STATIONS AND DOCKS (ONLY JUST AFTER MIDNIGHT) -------------
if(format(Sys.time(), '%H') == '00' & format(Sys.time(), '%M') < 15){
    stations[, `:=`(freeDocks = NULL, bikes = NULL)]
    dbSendQuery(db_conn, "DROP TABLE IF EXISTS tmpS")
    dbWriteTable(db_conn, 'tmpS', stations, row.names = FALSE)
    dbSendQuery(db_conn, "INSERT IGNORE INTO stations SELECT station_id, terminal_id, lat, `long`, '', '', OA_id, place, area, docks, 0 FROM tmpS")
    stations <- stations[docks > 0, .(station_id, docks)]
    setkey(stations, 'station_id')
    docks <- data.table(dbGetQuery(db_conn, "SELECT station_id, docks AS oldDocks FROM docks"), key = 'station_id')
    stations <- docks[stations][oldDocks > 0][oldDocks != docks ]
    stations[, oldDocks := NULL]
    stations <- stations[, date_updated := as.numeric(format(Sys.Date(), '%Y%m%d')) ]
    setcolorder(stations, c('station_id', 'date_updated', 'docks'))
    dbWriteTable(db_conn, 'docks', stations, row.names = FALSE, append = TRUE)
}

# BYE
dbDisconnect(db_conn)
rm(list = ls())
gc()

