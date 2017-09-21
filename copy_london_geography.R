#########################################################
# LONDON cycle hire - Populate geographic tables
#########################################################
# RGN = 'E12000007'
# CTY IN ('E13000001', 'E13000002') ==> c("Inner London", "Outer London")

dbc = dbConnect(MySQL(), group = 'dataOps', dbname = 'london_cycle_hire')

# POSTCODES ---------------------------------------------------------------------------------------------------------------------
dbSendQuery(dbc, "TRUNCATE TABLE geo_postcodes;")
strSQL <- "
    INSERT INTO geo_postcodes 
    	SELECT postcode, is_active, x_lon, y_lat, OA, PCS, PCD, PCA
    	FROM geography_uk.postcodes pc
    	WHERE RGN = 'E12000007'
"
dbSendQuery(dbc, strSQL)

# LOOKUPS -----------------------------------------------------------------------------------------------------------------------
dbSendQuery(dbc, "TRUNCATE TABLE geo_lookups;")
strSQL = "
    INSERT INTO geo_lookups
        SELECT OA, x_lon, y_lat, perimeter, area, LSOA, MSOA, LAD, CTY, WARD, PCON, PCS, PCD, PCA
        FROM geography_uk.lookups
        WHERE RGN = 'E12000007'
"
dbSendQuery(dbc, strSQL)

# LOCATIONS ---------------------------------------------------------------------------------------------------------------------
dbSendQuery(dbc, "TRUNCATE TABLE geo_locations;")
locations <- c('LSOA', 'MSOA', 'LAD', 'CTY', 'WARD', 'PCS', 'PCD', 'PCA')
for(loca in locations){
    strSQL = paste("
        INSERT INTO geo_locations
            SELECT * 
            FROM geography_uk.locations
            WHERE location_id IN (SELECT DISTINCT", loca, "FROM geography_uk.lookups WHERE RGN = 'E12000007' )
    ")
    dbSendQuery(dbc, strSQL)
}
    
# UPDATE STATIONS with geo info from postcodes and lookups  ---------------------------------------------------------------------
strSQL = "
    UPDATE stations st 
    	JOIN geo_postcodes pc ON st.postcode = pc.postcode
    SET st.OA = pc.OA, st.PCS = pc.PCS, st.PCD = pc.PCD, st.PCA = pc.PCA 
"
dbSendQuery(dbc, strSQL)
strSQL = "
    UPDATE stations st 
    	JOIN geo_lookups lk ON st.OA = lk.OA
    SET st.LSOA = lk.LSOA, st.MSOA = lk.MSOA, st.LAD = lk.LAD, st.WARD = lk.WARD, st.PCON = lk.PCON
"
dbSendQuery(dbc, strSQL)


# Clean & Exit ------------------------------------------------------------------------------------------------------------------
dbDisconnect(dbc)
rm(list = ls())
gc()

