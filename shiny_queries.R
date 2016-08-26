# 
library('RMySQL')
db_conn = dbConnect(MySQL(), group = 'homeserver', dbname = 'londonCycleHire')

# 
dbSendQuery(db_conn, "TRUNCATE TABLE smr_bikes")
dbSendQuery(db_conn, "
    INSERT INTO smr_bikes 
    	select start_day, bike_id, count(*) as counting
    	from hires 
    	group by start_day, bike_id
")

# 
dbSendQuery(db_conn, "TRUNCATE TABLE smr_sStations")
dbSendQuery(db_conn, "
    INSERT INTO smr_sStations 
    	select start_day, start_station_id, count(*) as counting
    	from hires 
    	group by start_day, start_station_id
")

# 
dbSendQuery(db_conn, "TRUNCATE TABLE smr_eStations")
dbSendQuery(db_conn, "
    INSERT INTO smr_eStations 
    	select start_day, end_station_id, count(*) as counting
    	from hires 
    	group by start_day, end_station_id
")

# 
dbSendQuery(db_conn, "TRUNCATE TABLE ")
dbSendQuery(db_conn, "
    INSERT INTO 
        SELECT glc.name AS borough, COUNT(*)
        FROM hires h
            JOIN stations st ON st.station_id = h.start_station_id
            JOIN geography.lookups glk ON glk.OA_id = st.OA_id
            JOIN geography.locations glc ON glc.id = glk.LA_id AND glc.`type` = 'LA'
            GROUP BY borough
")

# 
dbSendQuery(db_conn, "TRUNCATE TABLE ")
dbSendQuery(db_conn, "
    INSERT INTO

")
