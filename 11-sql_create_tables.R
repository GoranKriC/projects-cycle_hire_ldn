#######################################################
# LONDON cycle hire - Create MySQL tables
#######################################################
library(RMySQL)

# lapply(dbListConnections(MySQL()), dbDisconnect) # one-liner to kill ALL db connections 

# Retrieve db name
dbc = dbConnect(MySQL(), group = 'dataOps', dbname = 'common')
db_name <- dbGetQuery(dbc, "SELECT db_name FROM common.cycle_hires WHERE scheme_id = 1")[[1]]
dbDisconnect(dbc)

# Connect to database
dbc = dbConnect(MySQL(), group = 'dataOps', dbname = db_name)

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
        place VARCHAR(35) NOT NULL DEFAULT '' COMMENT 'original from TFL' COLLATE 'utf8_unicode_ci',
        area VARCHAR(30) NOT NULL DEFAULT 'void' COMMENT 'original from TFL' COLLATE 'utf8_unicode_ci',
        docks TINYINT(2) UNSIGNED NULL DEFAULT NULL COMMENT 'updated once a day at midnight from script <update_data.R>',
        first_hire INT(8) UNSIGNED NULL DEFAULT NULL,
        last_hire INT(8) UNSIGNED NULL DEFAULT NULL,
        is_active TINYINT(1) UNSIGNED NOT NULL DEFAULT 0,
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


