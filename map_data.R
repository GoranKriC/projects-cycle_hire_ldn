# 

lapply(c('data.table', 'dplyr', 'jsonlite', 'leaflet', 'RMySQL'), require, character.only = TRUE)
db_conn = dbConnect(MySQL(), group = 'homeserver', dbname = 'londonCycleHire')

stations <- dbReadTable(db_conn, 'stations')
stations <- stations[stations$lat > 0,]
stationIcon <- makeIcon(iconUrl = 'D:/data/UK/LondonCycleHire/cycle-hire-pushpin-icon.gif')
stations %>% leaflet() %>% 
#  setView(lng = -71.0589, lat = 42.3601, zoom = 12) %>% 
  addProviderTiles("CartoDB.Positron") %>% 
  addTiles() %>% 
  addMarkers(~long, ~lat, 
      popup = ~paste('Station n.', station_id, ',', place, ',', area, '\n', 'Total Docks:', docks ), # , '\n', 'Available bikes:', (totDocks - freeDocks)),
      icon = stationIcon,
      clusterOptions = markerClusterOptions()
  )


