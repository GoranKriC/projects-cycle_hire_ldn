# 
libs <- c('RMySQL', 'ggmap', 'stringr')
libs <- lapply(libs, require, character.only = TRUE)
rm(libs) 

extract_uk_postcode <- function(uk_address){
    # postcode should always be SEVEN characters long.
    # IF postcode is GU16 7HZ then GU ==> Area, 16 ==> District, 7 ==> Sector, HZ ==> Unit. 
    #  - GU16 is the OUT code, it can assume be several different formats and be anywhere from 2 to 4 alphanumeric characters long
    #  - 7HZ is the IN code, it is always 1 numeric character followed by 2 alphabetic character
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
        ifelse(is.na(postcode), "", paste(", postcode = '", postcode, "'", sep = '')), 
        " WHERE station_id = ", stations[idx, 1],
        sep = '' 
    )
    dbSendQuery(db_conn, strSQL)
}
dbSendQuery(db_conn, "UPDATE stations st JOIN geography.postcodes pc ON pc.postcode = st.postcode SET st.OA_id = pc.OA_id")

dbDisconnect(db_conn)
rm(list = ls())
gc()
