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
    s1 <- dt[to_date <= num,   .(b1 = uniqueN(bike_id), h1 = .N, d1 = round(mean(duration))), cols2grp]
    s2 <- dt[to_date %in% den, .(b2 = uniqueN(bike_id), h2 = .N, d2 = round(mean(duration))), cols2grp]
    if(is.null(cols2grp)){
        cbind(dtf, 9, get.x.pctround(s1, s2) )
    } else {
        s <- s1[s2, on = cols2grp]
        if(length(cols2grp) == 1){
            s <- s[, .(get(cols2grp), dtf, 9, get.x.pctround(b1, b2), get.x.pctround(h1, h2), get.x.pctround(d1, d2))]
        } else {
            s <- s[, .(get(cols2grp[1]), get(cols2grp[2]), dtf, 9, get.x.pctround(b1, b2), get.x.pctround(h1, h2), get.x.pctround(d1, d2))]
        }
        setorder(s)
        s
    }
} 
get.smr <- function(cols2grp = NULL, depth = 5, min.hire = 1, tds = TRUE){
    y <- rbindlist(list(
            rbindlist(lapply(1:depth, 
                function(x) 
                    dt[, .(x, uniqueN(bike_id), .N, round(mean(duration))), c(cols2grp, ts[x]) ][N >= min.hire]
            )),
            rbindlist(lapply(1:9, 
                function(x) 
                    dt[to_date <= x, .(x, 8, uniqueN(bike_id), .N, round(mean(duration))), cols2grp] 
            )),
            dt[, .(99, 8, uniqueN(bike_id), .N, round(mean(duration))), cols2grp],
            rbindlist(lapply(1:length(todate$dtf), 
                function(x) 
                    get.var.todate(todate$now[x], todate$prev[[x]], todate$dtf[x], cols2grp)
            ))
    ))
    if(tds){
        y <- rbindlist(list(y,
                rbindlist(lapply(1:3, 
                    function(x)
                        dt[day_of_month <= currents[x]][, .(10 + x, uniqueN(bike_id), .N, round(mean(duration))), c(cols2grp, ts[x]) ]
                ))
        ))
        # ADD VARIATIONS
    }
    setnames(y, c(cols2grp, 'datefield', 'datetype', 'bikes', 'hires', 'duration'))
    y
}

print('************************************************')
print('LOADING AND PREPARING DATASET...')
strSQL <- "
    SELECT d0, w0, m0, qn, y0, to_date, day_of_month, day_of_quarter, day_of_year 
    FROM calendar
"
calendar <- data.table(dbGetQuery(dbc, strSQL), key = 'd0')
dt <- data.table(dbGetQuery(dbc, "SELECT bike_id, start_station_id, start_day, end_station_id, duration FROM hires"), key = 'start_day')
dt <- dt[calendar][order(-start_day)]
currents <- c(dt[1, day_of_year], dt[1, day_of_quarter], dt[1, day_of_month])

print('************************************************')
print('UPDATE TOTAL SUMMARY')
y <- get.smr()
dbSendQuery(dbc, "TRUNCATE TABLE smr")
dbWriteTable(dbc, 'smr', y[order(datetype, -datefield)], row.names = FALSE, append = TRUE)

print('************************************************')
print('UPDATE SUMMARIES FROM STARTING POINTS')
y <- get.smr('start_station_id')
setnames(y, 'start_station_id', 'station_id')
dbSendQuery(dbc, "TRUNCATE TABLE smr_start")
dbWriteTable(dbc, 'smr_start', y[order(datetype, station_id, -datefield)], row.names = FALSE, append = TRUE)

print('************************************************')
print('UPDATE SUMMARIES TO ENDING POINTS')
y <- get.smr('end_station_id')
setnames(y, 'end_station_id', 'station_id')
dbSendQuery(dbc, "TRUNCATE TABLE smr_end")
dbWriteTable(dbc, 'smr_end', y[order(datetype, station_id, -datefield)], row.names = FALSE, append = TRUE)

print('************************************************')
print('UPDATE SUMMARIES FROM STARTING POINTS TO ENDING POINTS')
y <- get.smr(c('start_station_id', 'end_station_id'), tds = FALSE)
dbSendQuery(dbc, "TRUNCATE TABLE smr_start_end")
dbWriteTable(dbc, 'smr_start_end', y[order(datetype, start_station_id, end_station_id, -datefield)], row.names = FALSE, append = TRUE)

# CLEAN AND EXIT
print('DONE!')
dbDisconnect(dbc)
rm(list = ls())
gc()
