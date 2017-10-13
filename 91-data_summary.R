#####################################################################
# LONDON Cycle Hire - Various scripts for DATA SUMMARY
#####################################################################

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

