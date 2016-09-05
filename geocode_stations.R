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
db_conn = dbConnect(MySQL(), group = 'homeserver', dbname = 'londonCycleHire')

stations <- dbGetQuery(db_conn, "SELECT station_id, `long`, lat FROM stations WHERE address = '' AND lat + `long` != 0")
for(idx in 1:nrow(stations)){
    address <- revgeocode(c(stations[idx, 2], stations[idx, 3]) )
    postcode <- extract_uk_postcode(address)
    strSQL <- paste( "
        UPDATE stations SET address = '", gsub("'", "''", address), "'", 
        ifelse(is.na(postcode), " NULL ", paste(", Gpostcode = '", postcode, "'", sep = '')), 
        " WHERE station_id = ", stations[idx, 1],
        sep = '' 
    )
    dbSendQuery(db_conn, strSQL)
}

# THIS PART IS FOR UPDATING ALL G-POSTCODES STARTING FROM ALREADY KNOWN ADDRESS
# dbSendQuery(db_conn, "UPDATE stations SET Gpostcode = NULL")
# stations <- dbGetQuery(db_conn, "SELECT station_id, address FROM stations WHERE address != ''")
# for(idx in 1:nrow(stations)){
#     postcode <- extract_uk_postcode(stations[idx, 2])
#     if(!is.na(postcode)){
#         strSQL <- paste("
#             UPDATE stations 
#             SET Gpostcode = '", postcode, "'", 
#             " WHERE station_id = ", stations[idx, 1],
#             sep = '' 
#         )
#         dbSendQuery(db_conn, strSQL)
#     }
# }

dbDisconnect(db_conn)
rm(list = ls())
gc()
