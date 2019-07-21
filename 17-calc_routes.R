#########################################################################
# LONDON Cycle Hire - 17. Calculate routes between each pair of stations
######################################################################### 
# load packages
pkg <- c('data.table', 'mapsapi', 'RMySQL', 'sf', 'sp')
invisible(lapply(pkg, require, char = TRUE))

# define functions
get_segment_id <- function(x){
    strSQL <- paste("
        SELECT segment_id
        FROM segments
        WHERE x_lon1 =", x[, 1], "AND y_lat1 =", x[, 2], "AND x_lon2 =", x[, 3], "AND y_lat2 =", x[, 4]
    )
    as.numeric(dbGetQuery(dbc, strSQL))
}
reset_tables <- function(){
    dbc = dbConnect(MySQL(), group = 'dataOps', dbname = 'cycle_hire_ldn')
    dbSendQuery(dbc, "TRUNCATE TABLE routes_segments" )
    dbSendQuery(dbc, "TRUNCATE TABLE segments" )
    dbSendQuery(dbc, paste("UPDATE routes SET has_route = 0") )
    dbDisconnect(dbc)
}

# connect to database
dbc = dbConnect(MySQL(), group = 'dataOps', dbname = 'cycle_hire_ldn')

# check for new stations
strSQL <- "
    SELECT station_id
    FROM stations 
    WHERE station_id NOT IN (SELECT DISTINCT start_station_id FROM routes) and area <> 'void'
"
stations <- data.table(dbGetQuery(dbc, strSQL))
if(nrow(stations) > 0){
    # Fill <routes> with "new" stations (cross join of all new stations)
    new_stn <- ifelse(nrow(stations) == 1, 
                      paste0('(', stations, ')'),
                      substring(paste(stations, collapse = ','), 2) 
    )
    strSQL <- paste("
        INSERT IGNORE INTO routes (start_station_id, end_station_id)
            SELECT sts.station_id AS start_station_id, ste.station_id AS end_station_id
            FROM stations sts CROSS JOIN stations ste
            WHERE sts.station_id IN ", new_stn, " AND sts.station_id != ste.station_id  
            ORDER BY start_station_id, end_station_id
    ")
    dbSendQuery(dbc, strSQL)
}
# load some data about stations
strSQL <- "
    SELECT station_id, CONCAT(place, ', ', area) AS name, x_lon, y_lat 
    FROM stations 
    WHERE area <> 'void'
"
stations <- data.table(dbGetQuery(dbc, strSQL))
# load id couples for stations without route stored
routes <- data.table(dbGetQuery(dbc, "SELECT route_id, start_station_id, end_station_id FROM routes WHERE NOT has_route") )

# Download routes using google maps API
if(nrow(routes)){
    for(idx in 1:nrow(routes)){
        # get route id
        route_id <- routes[idx, route_id]
        # get stations ids
        idx_A <- routes[idx, start_station_id]
        idx_B <- routes[idx, end_station_id]
        # print message 
        message(
            'Looking for route between stations (', 
            idx_A, ') ', stations[station_id == idx_A, name], ' and (', 
            idx_B, ') ', stations[station_id == idx_B, name]
        )
        # get coordinates
        st_A <- stations[station_id == idx_A, .(x_lon, y_lat)]
        st_B <- stations[station_id == idx_B, .(x_lon, y_lat)]
        # get cycling directions between the two chosen stations
        result = mp_directions(
                    origin = unlist(unname(st_A)),
                    destination = unlist(unname(st_B)),
                    mode = 'bicycling'
#                    key = ''
        )
        # extract the route
        route <- mp_get_routes(result)
        # extract segments coordinates
        sgm <- data.table(st_coordinates(route)[, 1:2])
        # compose each segment on one row
        sgm <- cbind(sgm[1:(nrow(sgm) - 1)], sgm[2:nrow(sgm)] ) 
        # set correct names
        setnames(sgm, c('x_lon1', 'y_lat1', 'x_lon2', 'y_lat2'))
        # save segments to table
        dbWriteTable(dbc, 'segments', sgm, row.names = FALSE, append = TRUE)
        # retrieve ids
        rt_sgm <- data.table( 
                        route_id = route_id, 
                        segment_id = vapply(1:nrow(sgm), FUN.VALUE = numeric(1), function(x) get_segment_id(sgm[x])) 
        )
        # save route segments to table
        dbWriteTable(dbc, 'routes_segments', rt_sgm, row.names = FALSE, append = TRUE)
        # update has_route in routes table
        dbSendQuery(dbc, paste("UPDATE routes SET has_route = 1 WHERE route_id =", route_id) )
        # wait a bit to avoid being stopped by G
        Sys.sleep(runif(1, 0.4, 1.2))
    }
}

# close connection
dbDisconnect(dbc)

# Clean and Exit
rm(list = ls())
gc()
