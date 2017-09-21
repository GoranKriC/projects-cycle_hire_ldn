#######################################################
# LONDON cycle hire - Create MySQL tables
#######################################################
library(RMySQL)

# lapply(dbListConnections(MySQL()), dbDisconnect) # one-liner to kill ALL db connections 
dbc = dbConnect(MySQL(), group = 'dataOps', dbname = 'london_cycle_hire')

# BASE TABLES: stations, distances, hires, current, docks, calendar -------------------------------------------------------------
## BASE TABLE: stations --------------------------------------------------------------------------------------------------------
strSQL = "
    CREATE TABLE stations (
    	station_id SMALLINT(3) UNSIGNED NOT NULL COMMENT 'original from TFL',
        terminal_id CHAR(8) NULL DEFAULT NULL COMMENT 'original from TFL' COLLATE 'utf8_unicode_ci',
        x_lon DECIMAL(8,6) NULL DEFAULT NULL COMMENT 'original from TFL',
        y_lat DECIMAL(8,6) UNSIGNED NULL DEFAULT NULL COMMENT 'original from TFL',
        address VARCHAR(250) NULL DEFAULT NULL COMMENT 'calculated from script <geocode_stations.R> using Google Maps API' COLLATE 'utf8_unicode_ci',
        postcode CHAR(7) NULL DEFAULT NULL COMMENT 'calculated from script <geocode_stations.R> as the minimum distance postcode from given coordinates' COLLATE 'utf8_unicode_ci',
        place VARCHAR(35) NOT NULL DEFAULT '\'\'' COMMENT 'original from TFL' COLLATE 'utf8_unicode_ci',
        area VARCHAR(30) NOT NULL DEFAULT '\'void\'' COMMENT 'original from TFL' COLLATE 'utf8_unicode_ci',
        docks TINYINT(2) UNSIGNED NULL DEFAULT NULL COMMENT 'updated once a day at midnight from script <update_data.R>',
        first_hire INT(8) UNSIGNED NULL DEFAULT NULL,
        last_hire INT(8) UNSIGNED NULL DEFAULT NULL,
        is_active TINYINT(1) UNSIGNED NOT NULL DEFAULT 1,
        hires_started MEDIUMINT(6) UNSIGNED NOT NULL DEFAULT '0' COMMENT 'number of hires that started from the station towards ANY station',
        duration_started SMALLINT(5) UNSIGNED NULL DEFAULT NULL COMMENT 'AVG duration (in seconds) for hires that started from the station towards ANY station',
        hires_ended MEDIUMINT(6) UNSIGNED NOT NULL DEFAULT '0' COMMENT 'number of hires that ended in the station coming from ANY station',
        duration_ended SMALLINT(5) UNSIGNED NULL DEFAULT NULL COMMENT 'AVG duration (in seconds) for hires that ended in the station coming from ANY station',
        hires_self MEDIUMINT(6) UNSIGNED NOT NULL DEFAULT '0' COMMENT 'number of hires that started from and ended in the SAME station',
        duration_self SMALLINT(5) UNSIGNED NULL DEFAULT NULL COMMENT 'AVG duration (in seconds) for hires that started from and ended in the SAME station',
        hires_started_noself MEDIUMINT(6) UNSIGNED NOT NULL DEFAULT '0' COMMENT 'number of hires that started from the station towards ANOTHER station',
        duration_started_noself SMALLINT(5) UNSIGNED NULL DEFAULT NULL COMMENT 'AVG duration (in seconds) for hires that started from the station towards ANOTHER station',
        hires_ended_noself MEDIUMINT(6) UNSIGNED NOT NULL DEFAULT '0' COMMENT 'number of hires that ended in the station coming from ANOTHER station',
        duration_ended_noself SMALLINT(5) UNSIGNED NULL DEFAULT NULL COMMENT 'AVG duration (in seconds) for hires that ended in the station coming from ANOTHER station',
        OA CHAR(9) NULL DEFAULT NULL COMMENT 'found using a join with london.postcodes' COLLATE 'utf8_unicode_ci',
        LSOA CHAR(9) NULL DEFAULT NULL COMMENT 'found using a join with london.oa_lookups' COLLATE 'utf8_unicode_ci',
        MSOA CHAR(9) NULL DEFAULT NULL COMMENT 'found using a join with london.oa_lookups' COLLATE 'utf8_unicode_ci',
        LAD CHAR(9) NULL DEFAULT NULL COMMENT 'found using a join with london.oa_lookups' COLLATE 'utf8_unicode_ci',
        WARD CHAR(9) NULL DEFAULT NULL COMMENT 'found using a join with london.oa_lookups' COLLATE 'utf8_unicode_ci',
        PCON CHAR(9) NULL DEFAULT NULL COMMENT 'found using a join with london.oa_lookups' COLLATE 'utf8_unicode_ci',
        PCS CHAR(6) NULL DEFAULT NULL COMMENT 'calculated as a <LEFT> from postcode' COLLATE 'utf8_unicode_ci',
        PCD CHAR(4) NULL DEFAULT NULL COMMENT 'calculated as a <LEFT> from postcode' COLLATE 'utf8_unicode_ci',
        PCA CHAR(2) NULL DEFAULT NULL COMMENT 'calculated as a <LEFT> from postcode' COLLATE 'utf8_unicode_ci',
        PRIMARY KEY (station_id),
        INDEX (terminal_id),
        INDEX (is_active),
        INDEX (OA),
        INDEX (LSOA),
        INDEX (MSOA),
        INDEX (LAD),
        INDEX (WARD),
        INDEX (PCON),
        INDEX (PCS),
        INDEX (PCD),
        INDEX (PCA),
        INDEX (postcode),
        INDEX (first_hire) USING BTREE,
        INDEX (last_hire) USING BTREE
    ) COLLATE='utf8_unicode_ci' ENGINE=MyISAM ROW_FORMAT=FIXED;
"
dbSendQuery(dbc, strSQL)

## BASE TABLE: distances -------------------------------------------------------------------------------------------------------
strSQL = "
    CREATE TABLE distances (
    	start_station_id SMALLINT(3) UNSIGNED NOT NULL,
    	end_station_id SMALLINT(3) UNSIGNED NOT NULL COMMENT 'id A < id B',
    	distance SMALLINT(5) UNSIGNED NOT NULL COMMENT 'meters',
    	time SMALLINT(5) UNSIGNED NOT NULL COMMENT 'seconds',
    	hires SMALLINT(5) UNSIGNED NOT NULL DEFAULT '0',
    	duration INT(8) UNSIGNED NULL DEFAULT NULL COMMENT 'average in seconds',
    	PRIMARY KEY (start_station_id, end_station_id),
    	INDEX (distance) USING BTREE,
    	INDEX (time) USING BTREE,
    	INDEX (hires) USING BTREE,
    	INDEX (duration) USING BTREE
    ) COLLATE='utf8_unicode_ci' ENGINE=MyISAM ROW_FORMAT=FIXED;
"
dbSendQuery(dbc, strSQL)

## BASE TABLE: hires -----------------------------------------------------------------------------------------------------------
strSQL = "
    CREATE TABLE hires (
    	rental_id INT(10) UNSIGNED NOT NULL,
    	bike_id SMALLINT(5) UNSIGNED NOT NULL,
    	start_station_id SMALLINT(3) UNSIGNED NOT NULL,
    	start_day INT(8) UNSIGNED NOT NULL,
    	start_hour TINYINT(2) UNSIGNED NOT NULL,
    	start_min TINYINT(2) UNSIGNED NOT NULL,
    	end_station_id SMALLINT(3) UNSIGNED NOT NULL,
    	end_day INT(8) UNSIGNED NOT NULL,
    	end_hour TINYINT(2) UNSIGNED NOT NULL,
    	end_min TINYINT(2) UNSIGNED NOT NULL,
    	duration MEDIUMINT(6) UNSIGNED NOT NULL COMMENT 'seconds',
    	PRIMARY KEY (rental_id),
    	INDEX (bike_id),
    	INDEX (start_station_id),
    	INDEX (end_station_id),
    	INDEX (start_day),
    	INDEX (end_day),
    	INDEX (start_hour),
    	INDEX (end_hour)
    ) COLLATE='utf8_unicode_ci' ENGINE=MyISAM ROW_FORMAT=FIXED
"
dbSendQuery(dbc, strSQL)

## BASE TABLE: docks -----------------------------------------------------------------------------------------------------------
strSQL = "
    CREATE TABLE docks (
    	station_id SMALLINT(5) UNSIGNED NOT NULL,
    	date_updated INT(8) UNSIGNED NOT NULL,
    	docks TINYINT(3) UNSIGNED NOT NULL,
    	PRIMARY KEY (station_id, date_updated)
    ) COLLATE='utf8_unicode_ci' ENGINE=MyISAM ROW_FORMAT=FIXED;
"
dbSendQuery(dbc, strSQL)

## BASE TABLE: current ---------------------------------------------------------------------------------------------------------
strSQL = "
    CREATE TABLE current (
    	day MEDIUMINT(8) UNSIGNED NOT NULL,
    	hour TINYINT(2) UNSIGNED NOT NULL,
    	min TINYINT(2) UNSIGNED NOT NULL,
    	station_id SMALLINT(3) UNSIGNED NOT NULL,
    	freeDocks TINYINT(3) UNSIGNED NOT NULL,
    	bikes TINYINT(3) UNSIGNED NOT NULL,
    	PRIMARY KEY (day, hour, min, station_id)
    ) COLLATE='utf8_unicode_ci' ENGINE=MyISAM ROW_FORMAT=FIXED;
"
dbSendQuery(dbc, strSQL)

## BASE TABLE: calendar --------------------------------------------------------------------------------------------------------
strSQL = "
    CREATE TABLE calendar (
    	datefield DATE NOT NULL,
        day_id TINYINT(1) UNSIGNED NOT NULL,
        day_txt CHAR(3) NOT NULL COLLATE 'utf8_unicode_ci',
        day_txt_long CHAR(9) NOT NULL COLLATE 'utf8_unicode_ci',
        is_weekday TINYINT(1) UNSIGNED NOT NULL DEFAULT '0',
        is_leap TINYINT(1) UNSIGNED NOT NULL DEFAULT '0',
        d0 INT(8) UNSIGNED NOT NULL COMMENT '20120104',
        d1 CHAR(6) NOT NULL COMMENT '04 Jan' COLLATE 'utf8_unicode_ci',
        d2 CHAR(8) NOT NULL COMMENT '04/01/12' COLLATE 'utf8_unicode_ci',
        d3 CHAR(8) NOT NULL COMMENT '04-01-12' COLLATE 'utf8_unicode_ci',
        d4 CHAR(9) NOT NULL COMMENT '04-Jan-12' COLLATE 'utf8_unicode_ci',
        d5 CHAR(9) NOT NULL COMMENT '04 Jan 12' COLLATE 'utf8_unicode_ci',
        d6 CHAR(11) NULL DEFAULT NULL COMMENT 'Wed, 04 Jan' COLLATE 'utf8_unicode_ci',
        d7 CHAR(15) NULL DEFAULT NULL COMMENT 'Wed, 04 Jan 12' COLLATE 'utf8_unicode_ci',
        day_of_month TINYINT(2) UNSIGNED NOT NULL,
    	day_of_quarter TINYINT(2) UNSIGNED NULL DEFAULT NULL,
        day_of_year SMALLINT(3) UNSIGNED NOT NULL,
	    day_of_quarters MEDIUMINT(7) UNSIGNED NULL DEFAULT NULL,
        day_of_years MEDIUMINT(7) UNSIGNED NOT NULL,
        day_last_month INT(8) UNSIGNED NULL DEFAULT NULL,
        day_last_year INT(8) UNSIGNED NULL DEFAULT NULL,
        days_past SMALLINT(4) UNSIGNED NULL DEFAULT NULL,
        to_date TINYINT(2) UNSIGNED NOT NULL DEFAULT '0',
        w0 MEDIUMINT(6) UNSIGNED NOT NULL COMMENT '201201',
        w0d INT(8) UNSIGNED NULL DEFAULT NULL COMMENT '20120104',
        w1 CHAR(6) NULL DEFAULT NULL COMMENT '04 Jan' COLLATE 'utf8_unicode_ci',
        w2 CHAR(8) NULL DEFAULT NULL COMMENT '04/01/12' COLLATE 'utf8_unicode_ci',
        w3 CHAR(8) NULL DEFAULT NULL COMMENT '04-01-12' COLLATE 'utf8_unicode_ci',
        w4 CHAR(9) NULL DEFAULT NULL COMMENT '04-Jan-12' COLLATE 'utf8_unicode_ci',
        w5 CHAR(9) NULL DEFAULT NULL COMMENT '04 Jan 12' COLLATE 'utf8_unicode_ci',
        week_of_year TINYINT(2) UNSIGNED NULL DEFAULT NULL,
        week_last_year INT(6) UNSIGNED NULL DEFAULT NULL,
        last_week INT(6) UNSIGNED NULL DEFAULT NULL,
        weeks_past SMALLINT(4) UNSIGNED NULL DEFAULT NULL,
        m0 MEDIUMINT(6) UNSIGNED NOT NULL COMMENT '201201',
        m1 CHAR(8) NULL DEFAULT NULL COMMENT 'Jan 12' COLLATE 'utf8_unicode_ci',
        m2 CHAR(8) NULL DEFAULT NULL COMMENT '01/12' COLLATE 'utf8_unicode_ci',
        m3 CHAR(9) NULL DEFAULT NULL COMMENT '01-12' COLLATE 'utf8_unicode_ci',
        month_of_year TINYINT(2) UNSIGNED NULL DEFAULT NULL,
        month_last_year MEDIUMINT(6) UNSIGNED NULL DEFAULT NULL,
        last_month MEDIUMINT(6) UNSIGNED NULL DEFAULT NULL,
        months_past SMALLINT(4) UNSIGNED NULL DEFAULT NULL,
        q0 CHAR(6) NOT NULL COMMENT 'yyyyQx' COLLATE 'utf8_unicode_ci',
        qn SMALLINT(4) UNSIGNED NOT NULL COMMENT 'yyyyx',
        quarter_of_year CHAR(2) NULL DEFAULT NULL COLLATE 'utf8_unicode_ci',
        quartern_of_year TINYINT(1) UNSIGNED NULL DEFAULT NULL,
        quarter_last_year CHAR(6) NULL DEFAULT NULL COLLATE 'utf8_unicode_ci',
        quartern_last_year SMALLINT(4) UNSIGNED NULL DEFAULT NULL,
        last_quarter SMALLINT(4) UNSIGNED NULL DEFAULT NULL,
        y0 SMALLINT(4) UNSIGNED NOT NULL COMMENT 'yyyy',
        last_year SMALLINT(4) UNSIGNED NULL DEFAULT NULL,
        PRIMARY KEY (datefield) USING BTREE,
        INDEX (d0) USING BTREE,
        INDEX (m0) USING BTREE,
        INDEX (q0) USING BTREE,
        INDEX (y0) USING BTREE,
        INDEX (qn),
        INDEX (w0) USING BTREE,
        INDEX (days_past) USING BTREE,
        INDEX (months_past) USING BTREE,
        INDEX (to_date) USING BTREE,
        INDEX (is_weekday),
        INDEX (weeks_past) USING BTREE,
        INDEX (day_id) USING BTREE,
        INDEX (day_of_year) USING BTREE,
        INDEX (day_of_years) USING BTREE,
        INDEX (day_of_quarter) USING BTREE,
        INDEX (day_of_quarters) USING BTREE,
        INDEX (day_of_month) USING BTREE,
        INDEX (day_last_month),
        INDEX (day_last_year),
        INDEX (week_of_year),
        INDEX (week_last_year),
        INDEX (month_of_year),
        INDEX (month_last_year),
        INDEX (quarter_of_year),
        INDEX (quarter_last_year),
        INDEX (quartern_of_year),
        INDEX (quartern_last_year),
        INDEX (is_leap),
        INDEX (last_week),
        INDEX (last_month),
        INDEX (last_quarter),
        INDEX (last_year)
    ) ENGINE=MyISAM DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci ROW_FORMAT=FIXED
"
dbSendQuery(dbc, strSQL)
strSQL = "
    DELIMITER $$

    DROP PROCEDURE IF EXISTS proc_fill_calendar $$

    CREATE PROCEDURE proc_fill_calendar ()  
        BEGIN
      
      		DECLARE crt_date DATE;
      		SET crt_date = ( SELECT STR_TO_DATE(MIN(start_day), '%Y%m%d') FROM hires );
      
      		TRUNCATE TABLE calendar;
      
      		WHILE crt_date <= ( SELECT STR_TO_DATE(MAX(start_day), '%Y%m%d') FROM hires ) DO
      			INSERT INTO calendar (
    			  					datefield, day_id, day_txt, day_txt_long, d0, d1, d2, d3, d4, d5, w0, m0, q0, qn, y0,
    								day_of_month, day_of_year, day_of_years, week_of_year, month_of_year
    							) 
      				SELECT 
    				  	crt_date, 
    					WEEKDAY(crt_date) + 1, 
    					LEFT(DAYNAME(crt_date), 3), 
    					DAYNAME(crt_date), 
      					DATE_FORMAT(crt_date, '%Y%m%d'), 
      					DATE_FORMAT(crt_date, '%d %b'), 
      					DATE_FORMAT(crt_date, '%d/%m/%y'), 
      					DATE_FORMAT(crt_date, '%d-%m-%y'), 
      					DATE_FORMAT(crt_date, '%d-%b-%y'),
      					DATE_FORMAT(crt_date, '%d %b %y'),
      					YEARWEEK(crt_date, 3), 
      					DATE_FORMAT(crt_date,'%Y%m'),
      					CONCAT(YEAR(crt_date), 'Q', QUARTER(crt_date)),
      					CONCAT(YEAR(crt_date), QUARTER(crt_date)),
      					YEAR(crt_date),
      					DAY(crt_date),
      					DAYOFYEAR(crt_date),
      					CONCAT( YEAR(crt_date), RIGHT( CONCAT('00', DAYOFYEAR(crt_date) ), 3) ),
      					RIGHT(YEARWEEK(crt_date, 3), 2),
      					DATE_FORMAT(crt_date, '%m')
      				;
      			SET crt_date = ADDDATE(crt_date, INTERVAL 1 DAY);
      		END WHILE;
      
      		UPDATE calendar c JOIN 
      			(	SELECT DISTINCT
      					YEARWEEK(datefield, 3) AS d,
      					d0,
      					DATE_FORMAT(MIN(DATE(datefield)),'%d/%m/%y') AS ds,
      					DATE_FORMAT(MIN(DATE(datefield)),'%d-%m-%y') AS dd,
      					DATE_FORMAT(MIN(DATE(datefield)),'%d-%b-%y') AS de,
      					DATE_FORMAT(MIN(DATE(datefield)),'%d %b')    AS dm,
      					DATE_FORMAT(MIN(DATE(datefield)),'%d %b %y') AS dm2
      				FROM calendar
      				GROUP BY d, d0
      			) t ON t.d = c.w0
      		SET w0d = t.d0, w1 = dm, w2 = ds, w3 = dd, w4 = de, w5 = dm2;
      
      		UPDATE calendar c JOIN 
      			(	SELECT DISTINCT
      					DATE_FORMAT(datefield,'%Y%m') AS d,
      					DATE_FORMAT(MIN(DATE(datefield)),'%b %y') AS de,
      					DATE_FORMAT(MIN(DATE(datefield)),'%m-%y') AS dd,
      					DATE_FORMAT(MIN(DATE(datefield)),'%m/%y') AS ds
      				FROM calendar
      				GROUP BY d
      			) t ON t.d = c.m0
      		SET m1 = de, m2 = ds, m3 = dd;
      
      		SET @rt=-1;
      		UPDATE calendar c JOIN (
    		  SELECT d0, @rt:=@rt+1 AS c 
    		  FROM calendar 
    		  ORDER BY d0 DESC
    		) t ON t.d0 = c.d0 SET days_past = t.c;
      
      		DROP TABLE IF EXISTS temp;
      		SET @rt=-1;
      		CREATE TABLE temp AS	
    			SELECT t.w0, @rt:=@rt+1 AS c 
    			FROM ( 
    				SELECT DISTINCT w0
    				FROM calendar 
    				ORDER BY w0 DESC
    			) t;
      		UPDATE calendar c 
    		  JOIN temp t ON t.w0 = c.w0 
    		SET weeks_past = t.c;
      
      		DROP TABLE temp;
      		SET @rt=-1;
      		CREATE TABLE temp AS
    			SELECT t.m0, @rt:=@rt+1 AS c  
    			FROM ( 
    				SELECT DISTINCT m0
    				FROM calendar 
    				ORDER BY m0 DESC
    			) t;
      		UPDATE calendar c 
    		  JOIN temp t ON t.m0 = c.m0 
    		SET months_past = t.c;
      
      		UPDATE calendar SET to_date = 
      			CASE
    	        -- This year
      				WHEN days_past < 7 THEN 1 
      				WHEN days_past < 15 THEN 2
      				WHEN days_past < 30 THEN 3
      				WHEN days_past < 60 THEN 4
      				WHEN days_past < 90 THEN 5
      				WHEN days_past < 120 THEN 6
      				WHEN days_past < 180 THEN 7
      				WHEN days_past < 360 THEN 8
      				WHEN days_past < 365 THEN 9
    				-- last year
    				WHEN days_past < 372 THEN 11 
    				WHEN days_past < 380 THEN 12
    				WHEN days_past < 395 THEN 13
    				WHEN days_past < 425 THEN 14
    				WHEN days_past < 455 THEN 15
    				WHEN days_past < 485 THEN 16
    				WHEN days_past < 545 THEN 17
    				WHEN days_past < 725 THEN 18
    				WHEN days_past < 730 THEN 19
    				-- more than two year ago
    				ELSE 99
      			END;
    
      		UPDATE calendar 
    		SET is_weekday = 1 
    		WHERE day_id <= 5;
    		
      		UPDATE calendar 
    		SET d6 = CONCAT(day_txt, ', ', d1), d7 = CONCAT(day_txt, ', ', d5);
    
      		UPDATE calendar 
    		SET 
    --			day_last_month = CASE days_month = 12 THEN ELSE END, 
    			day_last_year = CONCAT(y0 - 1, RIGHT(d0, 4) ), 
    			week_last_year = CONCAT(y0 - 1, RIGHT(w0, 2) ),
    --			last_week = ,
    			month_last_year = CONCAT(y0 - 1, RIGHT(m0, 2) ), 
    			last_month = CASE WHEN month_of_year = 1 THEN CONCAT(y0 - 1, 12) ELSE m0 - 1 END,
    			quarter_of_year = RIGHT(q0, 2),
    			quartern_of_year = RIGHT(q0, 1),
    			quarter_last_year = CONCAT(y0 - 1, 'Q', RIGHT(q0, 1) ),
    			quartern_last_year = CONCAT(y0 - 1, RIGHT(q0, 1) ),
    			last_quarter = CASE WHEN quartern_of_year = 1 THEN CONCAT(y0 - 1, 4) ELSE qn - 1 END,
    			last_year = y0 - 1
    		;
    
      		UPDATE calendar 
    		SET is_leap = 1 
    		WHERE day_of_month = 29 AND month_of_year = 2
    		;
    
    		UPDATE calendar
    		SET day_of_quarter =
    		 	DATEDIFF(datefield, (makedate( YEAR (datefield), 1) + 
    				INTERVAL QUARTER(datefield) QUARTER - INTERVAL 1 QUARTER)) + 1 
    		;
    
    		UPDATE calendar
    		SET day_of_quarters = CONCAT(qn, RIGHT( CONCAT('00', day_of_quarter), 2) )
    		;
    
    		
      		DROP TABLE temp;
      		
      END $$

    DELIMITER ; 
"
dbSendQuery(dbc, strSQL)

# SUMMARY TABLES: smr, smr_start, smr_end, smr_start_end ------------------------------------------------------------------------

## SUMMARY TABLE: smr ----------------------------------------------------------------------------------------------------------
strSQL = "
    CREATE TABLE smr (
    	datetype TINYINT(2) UNSIGNED NOT NULL COMMENT '1- year, 2- quarter, 3-month, 4- week, 5- day, 6- hour, 8- to_date, 9- in_date',
    	datefield INT(8) UNSIGNED NOT NULL,
    	bikes SMALLINT(5) NOT NULL,
    	hires INT(10) NOT NULL,
    	duration SMALLINT(5) NOT NULL,
    	PRIMARY KEY (datetype, datefield)
    ) COLLATE='utf8_unicode_ci' ENGINE=MyISAM ROW_FORMAT=FIXED;
"
dbSendQuery(dbc, strSQL)

## SUMMARY TABLE: smr_start ----------------------------------------------------------------------------------------------------
strSQL = "
    CREATE TABLE smr_start (
    	datetype TINYINT(2) UNSIGNED NOT NULL COMMENT '1- year, 2- quarter, 3-month, 4- week, 5- day, 6- hour, 8- to_date, 9- in_date',
    	station_id SMALLINT(3) UNSIGNED NOT NULL,
    	datefield INT(8) UNSIGNED NOT NULL,
    	bikes MEDIUMINT(6) NOT NULL,
    	hires MEDIUMINT(6) NOT NULL,
    	duration MEDIUMINT(6) NOT NULL,
    	PRIMARY KEY (datetype, station_id, datefield)
    ) COLLATE='utf8_unicode_ci' ENGINE=MyISAM ROW_FORMAT=FIXED;
"
dbSendQuery(dbc, strSQL)

## SUMMARY TABLE: smr_end ------------------------------------------------------------------------------------------------------
strSQL = "
    CREATE TABLE smr_end (
    	datetype TINYINT(2) UNSIGNED NOT NULL COMMENT '1- year, 2- quarter, 3-month, 4- week, 5- day, 6- hour, 8- to_date, 9- in_date',
    	station_id SMALLINT(3) UNSIGNED NOT NULL,
    	datefield INT(8) UNSIGNED NOT NULL,
    	bikes MEDIUMINT(6) NOT NULL,
    	hires MEDIUMINT(6) NOT NULL,
    	duration MEDIUMINT(6) NOT NULL,
    	PRIMARY KEY (datetype, station_id, datefield)
    ) COLLATE='utf8_unicode_ci' ENGINE=MyISAM ROW_FORMAT=FIXED;
"
dbSendQuery(dbc, strSQL)

## SUMMARY TABLE: smr_start_end ------------------------------------------------------------------------------------------------
strSQL = "
    CREATE TABLE smr_start_end (
    	datetype TINYINT(2) UNSIGNED NOT NULL COMMENT '1-year, 2-quarter, 3-month, 4-week, 5-day, 6-hour, 8-to_date, 9-in_date, 11-, 12-, 13-',
    	start_station_id SMALLINT(3) UNSIGNED NOT NULL,
    	end_station_id SMALLINT(3) UNSIGNED NOT NULL,
    	datefield INT(8) UNSIGNED NOT NULL,
    	bikes MEDIUMINT(7) NOT NULL,
    	hires MEDIUMINT(7) NOT NULL,
    	duration MEDIUMINT(7) NOT NULL,
    	PRIMARY KEY (datetype, start_station_id, end_station_id, datefield)
    ) COLLATE='utf8_unicode_ci' ENGINE=MyISAM ROW_FORMAT=FIXED;
"
dbSendQuery(dbc, strSQL)


# GEOGRAPHY TABLES: geo_postcodes, geo_locations, geo_lookups  ------------------------------------------------------------------

## GEOGRAPHY TABLE: postcodes --------------------------------------------------------------------------------------------------
dbSendQuery(dbc, "DROP TABLE IF EXISTS geo_postcodes;")
strSQL = "
    CREATE TABLE geo_postcodes (
        postcode CHAR(7) NOT NULL DEFAULT '' COLLATE 'utf8_unicode_ci',
    	is_active TINYINT(1) UNSIGNED NOT NULL,
        x_lon DECIMAL(8,6) NOT NULL,
        y_lat DECIMAL(8,6) UNSIGNED NOT NULL,
        OA CHAR(9) NOT NULL COLLATE 'utf8_unicode_ci',
        PCS CHAR(5) NOT NULL COLLATE 'utf8_unicode_ci',
        PCD CHAR(4) NOT NULL COLLATE 'utf8_unicode_ci',
        PCA CHAR(2) NOT NULL COLLATE 'utf8_unicode_ci',
        PRIMARY KEY (postcode),
        INDEX (OA),
        INDEX (PCS),
        INDEX (PCD),
        INDEX (PCA)
    ) COLLATE='utf8_unicode_ci' ENGINE=MyISAM ROW_FORMAT=FIXED
"
dbSendQuery(dbc, strSQL)

## GEOGRAPHY TABLE: lookups ----------------------------------------------------------------------------------------------------
dbSendQuery(dbc, "DROP TABLE IF EXISTS geo_lookups;")
strSQL = "
    CREATE TABLE geo_lookups (
    	OA CHAR(9) NOT NULL COLLATE 'utf8_unicode_ci',
    	x_lon DECIMAL(9,8) NOT NULL,
        y_lat DECIMAL(10,8) UNSIGNED NOT NULL,
        perimeter MEDIUMINT(8) UNSIGNED NOT NULL,
        area INT(10) UNSIGNED NOT NULL,
        LSOA CHAR(9) NOT NULL COLLATE 'utf8_unicode_ci',
        MSOA CHAR(9) NOT NULL COLLATE 'utf8_unicode_ci',
        LAD CHAR(9) NOT NULL COLLATE 'utf8_unicode_ci',
        CTY CHAR(9) NOT NULL COLLATE 'utf8_unicode_ci',
        WARD CHAR(9) NOT NULL COLLATE 'utf8_unicode_ci',
        PCON CHAR(9) NOT NULL COLLATE 'utf8_unicode_ci',
        PCS CHAR(5) NOT NULL COLLATE 'utf8_unicode_ci',
        PCD CHAR(4) NOT NULL COLLATE 'utf8_unicode_ci',
        PCA CHAR(2) NOT NULL COLLATE 'utf8_unicode_ci',
        PRIMARY KEY (OA),
        INDEX (LSOA),
        INDEX (MSOA),
        INDEX (LAD),
        INDEX (CTY),
        INDEX (WARD),
        INDEX (PCON),
        INDEX (PCS),
        INDEX (PCD),
        INDEX (PCA)
    ) COLLATE='utf8_unicode_ci' ENGINE=MyISAM ROW_FORMAT=FIXED
"
dbSendQuery(dbc, strSQL)

## GEOGRAPHY TABLE: locations --------------------------------------------------------------------------------------------------
dbSendQuery(dbc, "DROP TABLE IF EXISTS geo_locations;")
strSQL = "
    CREATE TABLE geo_locations (
        location_id CHAR(9) NOT NULL DEFAULT '' COLLATE 'utf8_unicode_ci',
        name CHAR(75) NOT NULL DEFAULT '' COLLATE 'utf8_unicode_ci',
        type CHAR(4) NOT NULL DEFAULT '' COLLATE 'utf8_unicode_ci',
        parent CHAR(9) NOT NULL DEFAULT '' COLLATE 'utf8_unicode_ci',
        x_lon DECIMAL(8,6) NOT NULL,
        y_lat DECIMAL(8,6) UNSIGNED NOT NULL,
    	perimeter MEDIUMINT(8) UNSIGNED NULL DEFAULT NULL,
    	area INT(10) UNSIGNED NULL DEFAULT NULL,
        PRIMARY KEY (location_id),
        INDEX (type),
        INDEX (parent)
) COLLATE='utf8_unicode_ci' ENGINE=MyISAM ROW_FORMAT=FIXED;
"
dbSendQuery(dbc, strSQL)


# Clean & Exit ------------------------------------------------------------------------------------------------------------------
dbDisconnect(dbc)
rm(list = ls())
gc()


