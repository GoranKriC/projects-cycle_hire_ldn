#########################################################################
# LONDON Cycle Hire - 18. Calculate segments lengths and distances
#########################################################################

# load packages
pkg <- c('data.table', 'RMySQL')
invisible(lapply(pkg, require, char = TRUE))

# define functions
distance.driving.google <- function(orig, dest){
    xml.url <- paste('http://maps.googleapis.com/maps/api/distancematrix/xml?origins=', orig, '&destinations=', dest, '&mode=bicycling&sensor=false', sep = '')
    xmlfile <- xmlParse(getURL(xml.url))
    results <- numeric(2)
    results[1] <- xmlValue(xmlChildren(xpathApply(xmlfile, "//distance")[[1]])$value)
    results[2] <- xmlValue(xmlChildren(xpathApply(xmlfile, "//duration")[[1]])$value)
    return(results)
}

# connect to database
dbc = dbConnect(MySQL(), group = 'dataOps', dbname = 'cycle_hire_ldn')

# load segments without values
sgm <- data.table(dbGetQuery(dbc, 'SELECT * FROM segments WHERE ISNULL(duration)'))

# process all segments
for(idx in 1:nrow(sgm)){
    # print message 
    message('Processing segments ', sgm[idx, segment_id], ' (', idx, ' out of ', nrow(sgm), ')')
    # calculate measures
    msr <- as.numeric(distance.driving.google(
                paste(sgm[idx, .(y_lat1, x_lon1)], collapse = ','),
                paste(sgm[idx, .(y_lat2, x_lon2)], collapse = ',')
    ))
    # save values to database
    strSQL <- paste("
        UPDATE segments 
        SET length = ", msr[1], ", duration = ", msr[2], "
        WHERE segment_id = ", sgm[idx, segment_id]
    )
    dbSendQuery(dbc, strSQL)
}

# close connection
dbDisconnect(dbc)

# Clean and Exit
rm(list = ls())
gc()
