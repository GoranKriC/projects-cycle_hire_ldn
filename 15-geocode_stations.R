##################################################
# LONDON Cycle Hire - geocode stations
##################################################
lapply(c('RMySQL', 'ggmap', 'stringr'), require, character.only = TRUE)

extract_uk_postcode <- function(uk_address){
    # postcode should always be SEVEN characters long.
    # IF postcode is GU16 7HZ then: GU ==> Area, GU16 ==> District, GU167 ==> Sector, GU167HZ ==> Unit.
    #  - GU16 is the OUTward code, it can assume be several different formats and be anywhere from 2 to 4 alphanumeric characters long
    #         It identifies the town or district to which the letter is to be sent for further sorting
    #  - 7HZ  is the INward code, it is always 1 numeric character followed by 2 alphabetic character. 
    #         It assists in the delivery of post within a postal district.
    result <- NA
    if(str_sub(uk_address, -4) == ', UK') uk_address <- substr(uk_address, 1, nchar(uk_address) - 4)
    pc <- unlist(strsplit(uk_address, ' '))
    pc_in <- pc[length(pc)]
    if(nchar(pc_in) == 3){
        pc_out <- pc[length(pc) - 1]
        if( length(pc_out) > 0 ){
            if( nchar(pc_out) %in% 2:4 ){
                result <- paste( substr(paste(pc_out, '  '), 1, 4), pc_in, sep = '')
            }
        }
    }
    return(result)
}

# Retrieve db name
dbc = dbConnect(MySQL(), group = 'dataOps', dbname = 'common')
db_name <- dbGetQuery(dbc, "SELECT db_name FROM common.cycle_hires WHERE scheme_id = 1")[[1]]
dbDisconnect(dbc)

# connect to database
dbc = dbConnect(MySQL(), group = 'dataOps', dbname = db_name)

stations <- dbGetQuery(dbc, "SELECT station_id, x_lon, y_lat FROM stations WHERE area = 'void' AND y_lat + x_lon <> 0")

n_stn <- nrow(stations)

if(n_stn){
    for(idx in 1:n_stn){
        print(paste('Working on station', idx, 'out of', n_stn ) )
        address <- revgeocode(c(stations[idx, 2], stations[idx, 3]) )
        postcode <- extract_uk_postcode(address)
        strSQL <- paste( "
            UPDATE stations SET address = '", gsub("'", "''", address), "'", 
            ifelse(is.na(postcode), "", paste(", postcode = '", postcode, "'", sep = '')), 
            " WHERE station_id = ", stations[idx, 1],
            sep = '' 
        )
        dbSendQuery(dbc, strSQL)
    }
} else {
    print('Nothing to do...')
}

# Clean & Exit
dbDisconnect(dbc)
rm(list = ls())
gc()
