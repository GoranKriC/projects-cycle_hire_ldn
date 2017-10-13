

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

### PLOT CURRENT ----------------------------------------------------------------------------------------------------------------

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

# plot time series
ggplot(tail(test, 100), aes(updated_at, free_docks)) + geom_line()


### LAST 24 HOURS
dt <- data.table(dbReadTable(dbc, 'last24'))
dt[, updated_at := strptime(dt$updated_at, '%y%m%d%H%M')]
ggplot(dt[station_id == 826], aes(updated_at, free_docks)) + 
    geom_line() +
 #   xlim(min(dt$updated_at), max(dt$updated_at)) + 
    labs(x = 'Last 24 hours', y = 'free docks')


### PLOT SUMMARIES --------------------------------------------------------------------------------------------------------------

smr_start <- dbReadTable(dbc, 'smr_start')

get.plot.todate <- function(y, tms = 3, stn_id = NULL){
    lapply(c('ggplot2'), require, character.only = TRUE)
    if(!is.null(stn_id)) y <- setDT(y)[station_id == stn_id]
    yp <- y[datetype == 10 + tms]
    yt <- y[datetype == tms & datefield != max(y[datetype == tms, datefield])]
    g <- ggplot() +
            geom_line(data = yp, aes(x = factor(datefield), y = hires, group = 1), colour = 'red') +
            geom_line(data = yt, aes(x = factor(datefield), y = hires, group = 1), colour = 'black')
    g
}

get.plot.todate(smr_start, tms = 5, stn_id = 321)

str(smr_start)
y <- smr_start
stn_id = 321
y <- setDT(y)[station_id == stn_id]
yp <- y[datetype == 13]
yt <- y[datetype == 3 & datefield != max(y[datetype == 3, datefield])]


# clean & exit
dbDisconnect(dbc)
rm(list = ls())
gc()
