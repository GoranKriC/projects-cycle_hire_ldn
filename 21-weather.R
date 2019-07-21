#########################################################
# LONDON Cycle Hire - 21. Download Weather in London
#########################################################
# Use ASOS stations (airports) via the Iowa Environment Mesonet

# load packages
pkg <- c('data.table', 'riem', 'RMySQL')
invisible(lapply(pkg, require, char = TRUE))

# open connection to database
dbc <- dbConnect(MySQL(), group = 'dataOps', dbname = 'wd_weather_asos')

# get available networks (= iso2 + "__ASOS")
nt <- data.table(riem_networks())
setnames(nt, c('network_id', 'name'))
dbSendQuery(dbc, "TRUNCATE TABLE networks")
dbWriteTable(dbc, 'networks', nt, append = TRUE, row.names = FALSE)

# Get available stations
st <- data.table('network_id' = character(0), 'station_id' = character(0), 'name' = character(0), 'x_lon' = numeric(0), 'y_lat' = numeric(0))
for(idx in 1:nrow(nt)){
    message('Processing network <', nt[idx, name], '>, ', idx, ' out of ', nrow(nt))
    tmp <- riem_stations(nt[idx, network_id])
    # check if network has stations 
    if(dim(tmp)[1] > 1)
        st <- rbindlist( list( st, data.table(nt[idx, network_id], tmp) ) )
}
dbSendQuery(dbc, "TRUNCATE TABLE stations")
dbWriteTable(dbc, 'stations', st, append = TRUE, row.names = FALSE)


# Get measures
ms <- data.table( riem_measures(station = "EGLC", date_start = (Sys.Date() - 1) ) )
ms[, c('lon', 'lat', 'p01i', 'mslp', 'skyc4', 'skyl4', 'wxcodes', 'metar') := NULL]
setnames(ms, c(
    'station_id', 'datefield', 'temperature', 'dew_point', 'humidity',
    'wind_dir', 'wind_speed', 'pressure', 'visibility', 'wind_gust', 
    'skyc1', 'skyc2', 'skyc3', 'skyl1', 'skyl2', 'skyl3' 
))

# Values for skycx (skylx is the height in feet above airfield level of corresponding layer)
# NCD No Cloud Detected
# NSC NIL Significant Cloud
# FEW 1-2octa Few clouds
# SCT 3-4octa Scattered
# BKN 5-6octa Broken
# OVC 7-8octa Overcloud
# VV  Vertical Visibility Of
# /// Vertical Visibility Undetermined

# convert temp and dew in celsius, wind_speed in kmh/h (knots x 1.852, miles x 0.6213), 
ms[, `:=`(
    datefield = substr(gsub('[^0-9]', '', datefield), 1, 12),
    hour = substr(datefield, 9, 10),
    min = substring(datefield, 11),
    temperature = (temperature - 32) * 5 / 9,
    dew_point = (dew_point - 32) * 5 / 9,
    wind_speed = wind_speed * 1.852,
    pressure = pressure * 33.8637526,
    visibility = visibility * 0.6213,
    wind_gust = wind_gust * 1.852
)]
ms[, datefield := substring(datefield, 1, 8)]

# dbSendQuery(dbc, "TRUNCATE TABLE measures")
dbSendQuery(dbc, paste("DELETE FROM measures WHERE datefield >=", gsub('\\W', '', (Sys.Date() - 1)) ) )
dbWriteTable(dbc, 'measures', unique(ms), append = TRUE, row.names = FALSE)

# close connection
dbDisconnect(dbc)

