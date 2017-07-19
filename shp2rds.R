##############################################################################################
# London cycle hire - boundaries conversion to rds format for quicker loading in Shiny apps
##############################################################################################

# load packages
pkg <- c('rgdal', 'RMySQL', 'sp')
invisible( lapply(pkg, require, character.only = TRUE) )
loca.map <- c('CCG', 'LAT', 'NHSR', 'CCR', 'CTRY')

# load additional datasets
db_conn <- dbConnect(MySQL(), group = 'shiny', dbname = 'common')
locations <- suppressWarnings(data.table(dbReadTable(db_conn, 'locations') ) )
dbDisconnect(db_conn)

# load boundaries and build unique list
boundaries <- lapply(loca.map, function(x) readOGR(shp.path, x))
names(boundaries) <- loca.map
for(m in loca.map){
    boundaries[[m]] <- merge(boundaries[[m]], areas[, .(ons_id, nhs_id, name)], by.x = 'id', by.y = 'ons_id')
}

# save boundaries as RDS object
saveRDS(boundaries, paste0(shp.path, '/boundaries.rds'))

# clean and exit
rm(list = ls())
gc()

