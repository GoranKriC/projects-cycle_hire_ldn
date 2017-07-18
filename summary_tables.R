########################################################
# London cycle hire - SUMMARY TABLES FOR SHINY APPS
########################################################
lapply(c('data.table', 'RMySQL'), require, character.only = TRUE)
dbc = dbConnect(MySQL(), group = 'dataOps', dbname = 'london_cycle_hire')

ts <- c('y0', 'qn', 'm0', 'w0', 'start_day')
todate <- list(
    dtf = c(1:6, 11:16),
    now = c(2:5, 7, 9, 2:5, 7:8),
    prev = list( 3, 4, c(5, 6), c(6, 7), 8, 11:19, 11:12, 11:13, 11:14, 11:15, 11:17, 11:18)
)
get.x.pctround <- function(s1, s2) round(10000 * (s1 / s2 - 1))
get.var.todate <- function(num, den, dtf, cols2grp = NULL){
    s1 <- dt[to_date <= num, .( bikes = uniqueN(bike_id), hires = .N, duration = round(mean(duration))), cols2grp]
    s2 <- dt[to_date %in% den, .( bikes = uniqueN(bike_id), hires = .N, duration = round(mean(duration))), cols2grp]
    if(is.null(cols2grp)){
        cbind(dtf, 9, get.x.pctround(s1, s2) )
    } else {
        s <- s2[s1, on = cols2grp]
        if(length(cols2grp) == 1){
            s <- s[, .(get(cols2grp), dtf, 9, get.x.pctround(bikes, i.bikes), get.x.pctround(hires, i.hires), get.x.pctround(duration, i.duration))]
        } else {
            s <- s[, .(get(cols2grp[1]), get(cols2grp[2]), dtf, 9, get.x.pctround(bikes, i.bikes), get.x.pctround(hires, i.hires), get.x.pctround(duration, i.duration))]
        }
        setorder(s)
        s
    }
} 
get.smr <- function(cols2grp = NULL){
    y <- rbindlist(list(
            rbindlist( lapply(1:5, 
                 function(x) 
                     dt[, .(datetype = x, bikes = uniqueN(bike_id), hires = .N, duration = round(mean(duration))), c(datefield = ts[x], cols2grp) ]
            )),
            rbindlist( lapply(1:9, function(x) dt[to_date <= x, .(x, 8, uniqueN(bike_id), .N, round(mean(duration))), cols2grp] ) ),
            dt[, .(99, 8, uniqueN(bike_id), .N, round(mean(duration))), cols2grp],
            rbindlist(lapply(1:length(todate$dtf), function(x) get.var.todate(todate$now[x], todate$prev[[x]], todate$dtf[x], cols2grp)))
    ))
    names(y)[1] <- 'datefield'
    y
}

print('************************************************')
print('LOADING AND PREPARING DATASET...')
calendar <- data.table(dbGetQuery(dbc, 'SELECT d0, w0, m0, qn, y0, to_date FROM calendar'), key = 'd0')
dt <- data.table(dbGetQuery(dbc, "SELECT bike_id, start_station_id, start_day, end_station_id, duration FROM hires"), key = 'start_day')
dt <- dt[calendar]

print('************************************************')
print('UPDATE TOTAL SUMMARY')
y <- get.smr()
dbSendQuery(dbc, "TRUNCATE TABLE smr")
dbWriteTable(dbc, 'smr', y[order(datetype, -datefield)], row.names = FALSE, append = TRUE)

print('************************************************')
print('UPDATE SUMMARIES FROM STARTING POINTS')
y <- get.smr('start_station_id')
dbSendQuery(dbc, "TRUNCATE TABLE smr_start")
setnames(y, 'start_station_id', 'station_id')
dbWriteTable(dbc, 'smr_start', y[order(datetype, station_id, -datefield)], row.names = FALSE, append = TRUE)

print('************************************************')
print('UPDATE SUMMARIES TO ENDING POINTS')
y <- get.smr('end_station_id')
setnames(y, 'end_station_id', 'station_id')
dbSendQuery(dbc, "TRUNCATE TABLE smr_end")
dbWriteTable(dbc, 'smr_end', y[order(datetype, station_id, -datefield)], row.names = FALSE, append = TRUE)

print('************************************************')
print('UPDATE SUMMARIES FROM STARTING POINTS TO ENDING POINTS')
y <- get.smr(c('start_station_id', 'end_station_id'))
dbSendQuery(dbc, "TRUNCATE TABLE smr_start_end")
dbWriteTable(dbc, 'smr_start_end', y[order(datetype, start_station_id, end_station_id, -datefield)], row.names = FALSE, append = TRUE)

# CLEAN AND EXIT
print('DONE!')
dbDisconnect(dbc)
rm(list = ls())
gc()


# dt[m0 == max(m0), .(11, 8, uniqueN(bike_id), .N, round(mean(duration)))],
# dt[qn == max(qn), .(12, 8, uniqueN(bike_id), .N, round(mean(duration)))],
# dt[y0 == max(y0), .(13, 8, uniqueN(bike_id), .N, round(mean(duration)))],
