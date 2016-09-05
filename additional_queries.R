library('RMySQL')
db_conn = dbConnect(MySQL(), group = 'homeserver', dbname = 'londonCycleHire')


print('************************************************')
print('ADD/UPDATE OUTPUT AREA ID TO <stations>')
dbSendQuery(db_conn, "
    UPDATE stations st 
        JOIN geo_postcodes pc ON pc.postcode = st.postcode 
    SET st.OA_id = pc.OA_id
")
print('DONE!')

print('************************************************')
print('DELETING RIDES FROM/TO "VOID" STATIONS ')
dbSendQuery(db_conn, "
    DELETE h FROM hires h JOIN (
         SELECT station_id
         FROM stations
         WHERE area = 'void'
    ) t ON t.station_id = h.start_station_id
")
dbSendQuery(db_conn, "
    DELETE h FROM hires h JOIN (
         SELECT station_id
         FROM stations
         WHERE area = 'void'
    ) t ON t.station_id = h.end_station_id
")
print('DONE!')

print('************************************************')
print('UPDATE CNT OF HIRES AND AVG DURATION IN <distances>') # AVG(CASE WHEN duration < 86400 THEN duration ELSE 86400 END)) to limit single hire duration to 24h
dbSendQuery(db_conn, "
    UPDATE distances
    SET hires = 0, duration = NULL
")
dbSendQuery(db_conn, "
    UPDATE distances dt JOIN (
        SELECT start_station_id, end_station_id, count(*) AS c, ROUND(AVG(duration)) as d
        FROM hires
        WHERE start_station_id != end_station_id
        GROUP BY start_station_id, end_station_id
    ) t ON t.start_station_id = dt.start_station_id AND t.end_station_id = dt.end_station_id 
    SET dt.hires = t.c, dt.duration = t.d
")
print('DONE!')

print('**************************************************************************')
print('UPDATE CNT OF HIRES AND AVG DURATION FOR "SELF HIRES" IN <stations>') 
dbSendQuery(db_conn, "
    UPDATE stations st JOIN (
        SELECT start_station_id, COUNT(*) AS c, ROUND(AVG(duration)) AS d
        FROM hires
        WHERE start_station_id = end_station_id
        GROUP BY start_station_id
    ) t ON t.start_station_id = st.station_id
    SET st.hires_self = t.c, st.duration_self = t.d
")
print('DONE!')

print('UPDATE CNT OF HIRES AND AVG DURATION FOR ALL STARTING HIRES IN <stations>') 
dbSendQuery(db_conn, "
    UPDATE stations st JOIN (
        SELECT start_station_id, COUNT(*) AS c, ROUND(AVG(duration)) AS d
        FROM hires
        GROUP BY start_station_id
    ) t ON t.start_station_id = st.station_id
    SET st.hires_started = t.c, st.duration_started = t.d
")
print('DONE!')

print('UPDATE CNT OF HIRES AND AVG DURATION FOR ALL ENDING HIRES IN <stations>') 
dbSendQuery(db_conn, "
    UPDATE stations st JOIN (
        SELECT end_station_id, COUNT(*) AS c, ROUND(AVG(duration)) AS d
        FROM hires
        GROUP BY end_station_id
    ) t ON t.end_station_id = st.station_id
    SET st.hires_ended = t.c, st.duration_ended = t.d
")
print('DONE!')

print('UPDATE CNT OF HIRES AND AVG DURATION FOR NOSELF STARTING HIRES IN <stations>') 
dbSendQuery(db_conn, "
    UPDATE stations st JOIN (
        SELECT start_station_id, COUNT(*) AS c, ROUND(AVG(duration)) AS d
        FROM hires
        WHERE start_station_id != end_station_id
        GROUP BY start_station_id
    ) t ON t.start_station_id = st.station_id
    SET st.hires_started_noself = t.c, st.duration_started_noself = t.d
")
print('DONE!')

print('UPDATE CNT OF HIRES AND AVG DURATION FOR NOSELF ENDING HIRES IN <stations>') 
dbSendQuery(db_conn, "
    UPDATE stations st JOIN (
        SELECT end_station_id, COUNT(*) AS c, ROUND(AVG(duration)) AS d
        FROM hires
        WHERE start_station_id != end_station_id
        GROUP BY end_station_id
    ) t ON t.end_station_id = st.station_id
    SET st.hires_ended_noself = t.c, st.duration_ended_noself = t.d
")
print('DONE!')

print('************************************************')
print('')
# UPDATE 'first_hire', 'last_hire', 'is_active'
dbSendQuery(db_conn, "
    UPDATE stations st JOIN ( 
    	SELECT start_station_id, MIN(start_day) AS sd, MAX(start_day) AS ed 
    	FROM hires 
    	GROUP BY start_station_id
    ) t ON t.start_station_id = st.station_id
    SET st.first_hire = t.sd, st.last_hire = t.ed
")
dbSendQuery(db_conn, "UPDATE stations SET is_active = 1")
dbSendQuery(db_conn, "
    UPDATE stations 
    SET is_active = 0
    WHERE docks = 0 OR first_hire = 0 OR last_hire < ( SELECT MAX(start_day) FROM hires )
")
print('DONE!')





print('************************************************')
print('')
dbSendQuery(db_conn, "TRUNCATE TABLE smr_bikes")
dbSendQuery(db_conn, "
    INSERT INTO smr_bikes 
    	select start_day, bike_id, count(*) as counting
    	from hires 
    	group by start_day, bike_id
")
print('DONE!')

print('************************************************')
print('')
dbSendQuery(db_conn, "TRUNCATE TABLE smr_sStations")
dbSendQuery(db_conn, "
    INSERT INTO smr_sStations 
    	select start_day, start_station_id, count(*) as counting
    	from hires 
    	group by start_day, start_station_id
")
print('DONE!')

print('************************************************')
print('')
dbSendQuery(db_conn, "TRUNCATE TABLE smr_eStations")
dbSendQuery(db_conn, "
    INSERT INTO smr_eStations 
    	select start_day, end_station_id, count(*) as counting
    	from hires 
    	group by start_day, end_station_id
")
print('DONE!')

print('************************************************')
print('')
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
print('DONE!')

print('************************************************')
print('')
dbSendQuery(db_conn, "TRUNCATE TABLE ")
dbSendQuery(db_conn, "
    INSERT INTO

")
print('DONE!')

print('BYE!')
dbDisconnect(db_conn)
rm(list = ls())
gc()
