#########################################################################
# LONDON Cycle Hire - 17. Calculate routes between each pair of stations
#########################################################################

# load packages
pkg <- c('data.table', 'mapsapi', 'RMySQL', 'sp')
invisible(lapply(pkg, require, char = TRUE))

# Retrieve db name
dbc = dbConnect(MySQL(), group = 'dataOps', dbname = 'common')
db_name <- dbGetQuery(dbc, "SELECT db_name FROM common.cycle_hires WHERE scheme_id = 1")[[1]]
dbDisconnect(dbc)

# connect to database
dbc = dbConnect(MySQL(), group = 'dataOps', dbname = db_name)

# load some data about stations
strSQL <- "
    SELECT station_id, CONCAT(place, ', ', area) AS name, x_lon, y_lat 
    FROM stations 
    WHERE area <> 'void'
"
stations <- data.table(dbGetQuery(dbc, strSQL), key = 'station_id')
# load id couples for stations with route already stored
routes <- data.table(dbGetQuery(dbc, "SELECT DISTINCT start_station_id, end_station_id FROM routes") )

# Download routes using google maps API
if(nrow(stations)){
    for(idx_A in stations[, station_id]){
        for(idx_B in stations[, station_id]){
            # exit if A == B
            if(idx_A == idx_B) next
            # proceed only if A & B have not already been processed
            if(!nrow(routes[start_station_id == idx_A & end_station_id == idx_B])){
                # print message 
                message(
                    'Looking for route between stations (', 
                    idx_A, ') ', stations[idx_A, name], ' and (', 
                    idx_B, ') ', stations[idx_B, name]
                )
                # get coordinates
                st_A <- stations[idx_A, .(x_lon, y_lat)]
                st_B <- stations[idx_B, .(x_lon, y_lat)]
                # get cycling directions between the two chosen stations
                route = mp_directions(
                  origin = unlist(unname(st_A)),
                  destination = unlist(unname(st_B)),
                  mode = 'bicycling'
                )
                # extract the route
                route <- mp_get_routes(route)
                # convert coordinates to data.frame, also adding ids of involved stations 
                route <- cbind(idx_A, idx_B, as.data.frame(coordinates(as(route$geomerty, 'Spatial'))))
                # set correct names
                names(route) <- c('start_station_id', 'end_station_id', 'x_lon', 'y_lat')
                # save to database
                dbWriteTable(dbc, 'routes', route, row.names = FALSE, append = TRUE)
                # wait a bit to avoid being stopped by G
                Sys.sleep(runif(1, 1.5, 2.25))
            }
        }
    }
}

# close connection
dbDisconnect(dbc)

# Clean and Exit
# rm(list = ls())
# gc()
