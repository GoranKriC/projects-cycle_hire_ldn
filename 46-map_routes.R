#########################################################################
# LONDON Cycle Hire - 46. Map Routes
#########################################################################

## Load packages
pkg <- c('data.table')
invisible(lapply(pkg, require, char = TRUE))

## Helper Functions (should be stored in a dedicated package for general use in cycle_hire projects)
query_db <- function(tblname = NULL, strSQL = NULL){
    dbc <- DBI::dbConnect(RMySQL::MySQL(), group = 'dataOps', dbname = 'cycle_hire_ldn')
    result <- data.table(
        if(!is.null(tblname)){
            DBI::dbReadTable(dbc, tblname)
        } else {
            DBI::dbGetQuery(dbc, strSQL)
        }
    )
    DBI::dbDisconnect(dbc)
    return(result)
}
query_stns_area <- function(name, type = 'area', demo = TRUE){
    strSQL <- paste0("
                SELECT station_id 
                FROM stations 
                WHERE ", type, " = '", name, "'"
    )
    query_db(strSQL = strSQL)
}
get_route_id <- function(stn_start, stn_end){
    strSQL <- paste("
        SELECT route_id 
        FROM routes
        WHERE start_station_id =", stn_start, "AND end_station_id =", stn_end
    )
    query_db(strSQL = strSQL)
}
get_route <- function(stn_start, stn_end, as.spatial = FALSE){
    strSQL <- paste(
        "FROM routes rt
        	JOIN routes_segments rs ON rs.route_id = rt.route_id
        	JOIN segments sg ON sg.segment_id = rs.segment_id
        WHERE start_station_id =", stn_start, "AND end_station_id =", stn_end,
        "ORDER BY rs.id"
    )
    strSQL <- paste("
        (SELECT x_lon1 AS x_lon, y_lat1 AS y_lat",
        strSQL,
        ") 
            UNION 
        (SELECT x_lon2, y_lat2",
        strSQL,
        " DESC LIMIT 1)"
    )
    y <- query_db(strSQL = strSQL)
    if(as.spatial){
        y <- SpatialLines(list(Lines( Line(y), ID = as.character(get_route_id(stn_start, stn_end)) )))
    } else {
        y <- cbind(get_route_id(stn_start, stn_end), y)
    }
    return(y)
}

## Plot functions: routes

# Between two stations
plot_route <- function(stn_start, stn_end, tlname = 'CartoDB.DarkMatter'){
    require(leaflet)
    require(magrittr)
    route <- get_route(stn_start, stn_end)
    leaflet() %>% 
        addProviderTiles(tlname) %>%
        addPolylines(data = route, lng = ~x_lon, lat = ~y_lat,
                opacity = 0.7, 
                weight = 4, 
                color = 'red' # ~pal(alternative_id)
        )        
}

# Between a start/end station or area and any stations in a specified 'area', or  
plot_routes <- function(name, type = 'area', stn = NA, is_start = TRUE){
    require(leaflet)
    require(magrittr)
    
    
    stns <- query_stns_area(name, type)
    cbn_stns <- as.data.frame(t(combn(unlist(stns), 2)))
    routes <- as.data.frame(do.call(rbind, lapply(cbn_stns, function(x) get_route(x[1], x[2]))))

    leaflet() %>% 
        addProviderTiles(tlname) %>%
        addPolylines(data = routes, lng = ~x_lon, lat = ~y_lat, group = ~route_id,
                opacity = 0.7, 
                weight = 4, 
                color = 'red' # ~pal(alternative_id)
        )        
}


# Plot functions: segments
plot_segments <- function(stn_start, stn_end){
    
    plot_sgm <- function(s, add_sgm = TRUE){
        sg <- 
        sg <- Line(matrix( c(x1, y1, x2, y2), ncol = 2 ))
        sg <- SpatialLines(list(Lines(list( Line(matrix( c(x1, y1, x2, y2), ncol = 2 )) ), '1')))
        plot(sg, add = add_sgm)
    }
    
    dbc <- DBI::dbConnect(RMySQL::MySQL(), group = 'dataOps', dbname = 'cycle_hire_ldn')
    strSQL <- paste("
        SELECT sg.*
        FROM segments sg 
        	JOIN stations_segments ss ON ss.segment_id = sg.segment_id
        WHERE ss.start_station_id =", sst, "AND ss.end_station_id =", est,
        "ORDER BY segment_id
    ")
    sg <- data.table(DBI::dbGetQuery(dbc, strSQL))
    DBI::dbDisconnect(dbc)
    plot_sgm(1, FALSE)
    
    
    
    
    
}
