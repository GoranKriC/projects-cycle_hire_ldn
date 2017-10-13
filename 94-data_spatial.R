#########################################################################
# LONDON Cycle Hire - Various scripts for MAPPING and SPATIAL ANALYSIS
#########################################################################

### SETUP ----------------------------------------------------------------------------------------------------------------------

# load packages
invisible(lapply(c('ggplot2', 'data.table', 'RMySQL', 'stringr'), require, character = TRUE))

# set scheme
scheme <- 1

# Retrieve db name
dbc = dbConnect(MySQL(), group = 'dataOps', dbname = 'common')
db_name <- dbGetQuery(dbc, paste("SELECT db_name FROM cycle_hires WHERE scheme_id =", scheme))[[1]]
dbDisconnect(dbc)

# connect to database
dbc = dbConnect(MySQL(), group = 'dataOps', dbname = db_name)

### MAP CURRENT ----------------------------------------------------------------------------------------------------------------

# set variables
day_from <- 170601
day_to <- 170630
station_id <- 321
strSQL <- paste(
            "SELECT updated_at, free_docks FROM current WHERE",
            "station_id =", station_id, "AND", 
            "updated_at >=", day_from, "AND", 
            "updated_at <=", day_to 
)

# load data
test <- data.table(dbGetQuery(dbc, strSQL))
test[, updated_at := as.numeric(paste0(day, str_pad(hour, width = 2, side = 'left', pad = '0'), str_pad(min, width = 2, side = 'left', pad = '0')))]

### LAST 24 HOURS
dt <- data.table(dbReadTable(dbc, 'last24'))
dt[, updated_at := strptime(dt$updated_at, '%y%m%d%H%M')]


### PLOT SUMMARIES --------------------------------------------------------------------------------------------------------------

smr_start <- dbReadTable(dbc, 'smr_start')


### Clean & Exit -----------------------------------------------------------------------------------------------------------------
dbDisconnect(dbc)
rm(list = ls())
gc()
