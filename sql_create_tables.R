# BASE TABLES:  stations, distances, hires, current, docks, geo_locations, geo_lookups, geo_postcodes
# SHINY TABLES: smr_sStations, smr_eStations, smr_seStations, smrW_seStations, smrM_seStations, calendar

### TABLE: stations ---------------------------------------------------------
strSQL = "
CREATE TABLE `stations` (
	`station_id` SMALLINT(3) UNSIGNED NOT NULL COMMENT 'original from TFL',
    `terminal_id` CHAR(8) NULL DEFAULT NULL COMMENT 'original from TFL' COLLATE 'utf8_unicode_ci',
    `lat` DECIMAL(8,6) UNSIGNED NULL DEFAULT NULL COMMENT 'original from TFL',
    `long` DECIMAL(8,6) NULL DEFAULT NULL COMMENT 'original from TFL',
    `address` VARCHAR(250) NULL DEFAULT NULL COMMENT 'calculated from script <geocode_stations.R> using Google Maps API' COLLATE 'utf8_unicode_ci',
    `postcode` CHAR(7) NULL DEFAULT NULL COMMENT 'calculated from script <geocode_stations.R> as the minimum distance postcode from given coordinates' COLLATE 'utf8_unicode_ci',
    `place` VARCHAR(35) NOT NULL DEFAULT '\'\'' COMMENT 'original from TFL' COLLATE 'utf8_unicode_ci',
    `area` VARCHAR(30) NOT NULL DEFAULT '\'void\'' COMMENT 'original from TFL' COLLATE 'utf8_unicode_ci',
    `docks` TINYINT(2) UNSIGNED NULL DEFAULT NULL COMMENT 'updated once a day at midnight from script <update_data.R>',
    `first_hire` INT(8) UNSIGNED NULL DEFAULT NULL COMMENT 'calculated',
    `last_hire` INT(8) UNSIGNED NULL DEFAULT NULL COMMENT 'calculated',
    `is_active` TINYINT(1) UNSIGNED NOT NULL DEFAULT '1',
	`hires_started` MEDIUMINT(6) UNSIGNED NOT NULL DEFAULT '0' COMMENT 'number of hires that started from the station towards ANY station',
	`duration_started` SMALLINT(5) UNSIGNED NULL DEFAULT NULL COMMENT 'AVG duration (in seconds) for hires that started from the station towards ANY station',
	`hires_ended` MEDIUMINT(6) UNSIGNED NOT NULL DEFAULT '0' COMMENT 'number of hires that ended in the station coming from ANY station',
	`duration_ended` SMALLINT(5) UNSIGNED NULL DEFAULT NULL COMMENT 'AVG duration (in seconds) for hires that ended in the station coming from ANY station',
	`hires_self` MEDIUMINT(6) UNSIGNED NOT NULL DEFAULT '0' COMMENT 'number of hires that started from and ended in the SAME station',
	`duration_self` SMALLINT(5) UNSIGNED NULL DEFAULT NULL COMMENT 'AVG duration (in seconds) for hires that started from and ended in the SAME station',
	`hires_started_noself` MEDIUMINT(6) UNSIGNED NOT NULL DEFAULT '0' COMMENT 'number of hires that started from the station towards ANOTHER station',
	`duration_started_noself` SMALLINT(5) UNSIGNED NULL DEFAULT NULL COMMENT 'AVG duration (in seconds) for hires that started from the station towards ANOTHER station',
	`hires_ended_noself` MEDIUMINT(6) UNSIGNED NOT NULL DEFAULT '0' COMMENT 'number of hires that ended in the station coming from ANOTHER station',
	`duration_ended_noself` SMALLINT(5) UNSIGNED NULL DEFAULT NULL COMMENT 'AVG duration (in seconds) for hires that ended in the station coming from ANOTHER station',
    `OA_id` CHAR(9) NULL DEFAULT NULL COMMENT 'found using a join with geo_postcodes' COLLATE 'utf8_unicode_ci',
    `LSOA_id` CHAR(9) NULL DEFAULT NULL COMMENT 'found using a join with geo_lookups' COLLATE 'utf8_unicode_ci',
    `MSOA_id` CHAR(9) NULL DEFAULT NULL COMMENT 'found using a join with geo_lookups' COLLATE 'utf8_unicode_ci',
    `LAD_id` CHAR(9) NULL DEFAULT NULL COMMENT 'found using a join with geo_lookups' COLLATE 'utf8_unicode_ci',
    `WARD_id` CHAR(9) NULL DEFAULT NULL COMMENT 'found using a join with geo_lookups' COLLATE 'utf8_unicode_ci',
    `PCON_id` CHAR(9) NULL DEFAULT NULL COMMENT 'found using a join with geo_lookups' COLLATE 'utf8_unicode_ci',
    `CTY_id` CHAR(9) NULL DEFAULT NULL COMMENT 'found using a join with geo_lookups' COLLATE 'utf8_unicode_ci',
    `PCS_id` CHAR(6) NULL DEFAULT NULL COMMENT 'calculated as a <LEFT> from postcode' COLLATE 'utf8_unicode_ci',
    `PCD_id` CHAR(4) NULL DEFAULT NULL COMMENT 'calculated as a <LEFT> from postcode' COLLATE 'utf8_unicode_ci',
    `PCA_id` CHAR(2) NULL DEFAULT NULL COMMENT 'calculated as a <LEFT> from postcode' COLLATE 'utf8_unicode_ci',
    `Gpostcode` CHAR(7) NULL DEFAULT NULL COMMENT 'calculated from script <geocode_stations.R> using Google Maps API' COLLATE 'utf8_unicode_ci',
    `postcode_manual` CHAR(7) NULL DEFAULT NULL COMMENT 'if present. the minimum distance postcode is judged wrong' COLLATE 'utf8_unicode_ci',
    `postcode tested` TINYINT(1) UNSIGNED NOT NULL DEFAULT '0',
    PRIMARY KEY (`station_id`),
    INDEX `terminal_id` (`terminal_id`),
    INDEX `OA_id` (`OA_id`),
    INDEX `is_active` (`is_active`),
    INDEX `postcode_manual` (`postcode_manual`),
    INDEX `LSOA_id` (`LSOA_id`),
    INDEX `MSOA_id` (`MSOA_id`),
    INDEX `LAD_id` (`LAD_id`),
    INDEX `WARD_id` (`WARD_id`),
    INDEX `PCON_id` (`PCON_id`),
    INDEX `CTY_id` (`CTY_id`),
    INDEX `PCS_id` (`PCS_id`),
    INDEX `PCD_id` (`PCD_id`),
    INDEX `PCA_id` (`PCA_id`),
    INDEX `Gpostcode` (`Gpostcode`),
    INDEX `postcode` (`postcode`),
    INDEX `start_date` (`first_hire`) USING BTREE,
    INDEX `end_date` (`last_hire`) USING BTREE,
    INDEX `postcode tested` (`postcode tested`)
) COLLATE='utf8_unicode_ci' ENGINE=MyISAM ROW_FORMAT=FIXED
"

### TABLE: distances ---------------------------------------------------------
strSQL = "
    CREATE TABLE `distances` (
    	`start_station_id` SMALLINT(3) UNSIGNED NOT NULL,
    	`end_station_id` SMALLINT(3) UNSIGNED NOT NULL COMMENT 'id A < id B',
    	`distance` SMALLINT(5) UNSIGNED NOT NULL COMMENT 'meters',
    	`time` SMALLINT(5) UNSIGNED NOT NULL COMMENT 'seconds',
    	`hires` SMALLINT(5) UNSIGNED NOT NULL,
    	`duration` SMALLINT(5) UNSIGNED NOT NULL COMMENT 'average in seconds',
    	PRIMARY KEY (`start_station_id`, `end_station_id`),
    	INDEX `station_id_A` (`start_station_id`),
    	INDEX `station_id_B` (`end_station_id`)
    ) COLLATE='utf8_unicode_ci' ENGINE=MyISAM
"

### TABLE: hires ---------------------------------------------------------
strSQL = "
    CREATE TABLE `hires` (
    	`rental_id` INT(10) UNSIGNED NOT NULL,
        `bike_id` SMALLINT(5) UNSIGNED NOT NULL,
        `start_station_id` SMALLINT(3) UNSIGNED NOT NULL,
        `start_day` INT(8) UNSIGNED NOT NULL,
        `start_hour` TINYINT(2) UNSIGNED NOT NULL,
        `start_min` TINYINT(2) UNSIGNED NOT NULL,
        `end_station_id` SMALLINT(3) UNSIGNED NOT NULL,
        `end_day` INT(8) UNSIGNED NOT NULL,
        `end_hour` TINYINT(2) UNSIGNED NOT NULL,
        `end_min` TINYINT(2) UNSIGNED NOT NULL,
        `duration` MEDIUMINT(6) UNSIGNED NOT NULL COMMENT 'seconds',
        PRIMARY KEY (`rental_id`),
        INDEX `bike_id` (`bike_id`),
        INDEX `start_station_id` (`start_station_id`),
        INDEX `end_station_id` (`end_station_id`),
        INDEX `start_day` (`start_day`),
        INDEX `end_day` (`end_day`)
    ) COLLATE='utf8_unicode_ci' ENGINE=MyISAM ROW_FORMAT=FIXED
"

### TABLE: current ---------------------------------------------------------
strSQL = "
    CREATE TABLE `current` (
    	`day` MEDIUMINT(8) UNSIGNED NOT NULL,
    	`hour` TINYINT(2) UNSIGNED NOT NULL,
    	`min` TINYINT(2) UNSIGNED NOT NULL,
    	`station_id` SMALLINT(3) UNSIGNED NOT NULL,
    	`freeDocks` TINYINT(3) UNSIGNED NOT NULL,
    	`bikes` TINYINT(3) UNSIGNED NOT NULL,
    	PRIMARY KEY (`day`, `hour`, `min`, `station_id`),
    	INDEX `time` (`hour`),
    	INDEX `station_id` (`station_id`),
    	INDEX `date` (`day`),
    	INDEX `min` (`min`)
    ) COLLATE='utf8_unicode_ci' ENGINE=MyISAM ROW_FORMAT=FIXED
"

### TABLE: docks ---------------------------------------------------------
strSQL = "
    CREATE TABLE `docks` (
    	`station_id` SMALLINT(5) UNSIGNED NOT NULL,
    	`date_updated` INT(8) UNSIGNED NOT NULL,
    	`docks` TINYINT(3) UNSIGNED NOT NULL,
    	PRIMARY KEY (`station_id`, `date_updated`),
    	INDEX `station_id` (`station_id`),
    	INDEX `date_updated` (`date_updated`)
    ) COLLATE='utf8_unicode_ci' ENGINE=MyISAM
"

### TABLE: geo_locations ---------------------------------------------------------
strSQL = "
    CREATE TABLE `geo_locations` (
        `id` CHAR(15) NOT NULL DEFAULT '' COLLATE 'utf8_unicode_ci',
        `name` CHAR(75) NOT NULL DEFAULT '' COLLATE 'utf8_unicode_ci',
        `type` CHAR(4) NOT NULL DEFAULT '' COLLATE 'utf8_unicode_ci',
        `parent` CHAR(9) NOT NULL DEFAULT '' COLLATE 'utf8_unicode_ci',
        PRIMARY KEY (`id`),
        INDEX `type` (`type`),
        INDEX `parent` (`parent`)
    ) COLLATE='utf8_unicode_ci' ENGINE=MyISAM ROW_FORMAT=FIXED
"
strSQL = "
    INSERT INTO geo_locations
        SELECT * 
        FROM geography.locations
        WHERE id IN (SELECT DISTINCT LSOA_id FROM geography.lookups WHERE CTY_id IN ('E13000001', 'E13000002') )
           UNION
        SELECT * 
        FROM geography.locations
        WHERE id IN (SELECT DISTINCT MSOA_id FROM geography.lookups WHERE CTY_id IN ('E13000001', 'E13000002') )
            UNION
        SELECT * 
        FROM geography.locations
        WHERE id IN (SELECT DISTINCT LAD_id FROM geography.lookups WHERE CTY_id IN ('E13000001', 'E13000002') ) AND type = 'LAD'
            UNION
        SELECT * 
        FROM geography.locations
        WHERE id IN (SELECT DISTINCT WARD_id FROM geography.lookups WHERE CTY_id IN ('E13000001', 'E13000002') )
            UNION
        SELECT * 
        FROM geography.locations
        WHERE id IN (SELECT DISTINCT PCON_id FROM geography.lookups WHERE CTY_id IN ('E13000001', 'E13000002') )
            UNION
        SELECT * 
        FROM geography.locations
        WHERE id IN ('E13000001', 'E13000002')
"
### TABLE: geo_lookups ---------------------------------------------------------
strSQL = "
    CREATE TABLE `geo_lookups` (
        `OA_id` CHAR(9) NOT NULL COMMENT 'Output Areas' COLLATE 'utf8_unicode_ci',
        `LSOA_id` CHAR(9) NOT NULL COMMENT 'Lower Layer Super Output Areas' COLLATE 'utf8_unicode_ci',
        `MSOA_id` CHAR(9) NOT NULL COMMENT 'Middle Layer Super Output Areas' COLLATE 'utf8_unicode_ci',
        `LAD_id` CHAR(9) NOT NULL COMMENT 'Local Authority Districts' COLLATE 'utf8_unicode_ci',
        `CTY_id` CHAR(9) NOT NULL COLLATE 'utf8_unicode_ci',
        `WARD_id` CHAR(9) NOT NULL COLLATE 'utf8_unicode_ci',
        `PCON_id` CHAR(9) NOT NULL COLLATE 'utf8_unicode_ci',
        `PCS_id` CHAR(6) NOT NULL COLLATE 'utf8_unicode_ci',
        `PCD_id` CHAR(4) NOT NULL COLLATE 'utf8_unicode_ci',
        `PCA_id` CHAR(2) NOT NULL COLLATE 'utf8_unicode_ci',
        PRIMARY KEY (`OA_id`),
        INDEX `LSOA_id` (`LSOA_id`),
        INDEX `MSOA_id` (`MSOA_id`),
        INDEX `LAD_id` (`LAD_id`),
        INDEX `CTY_id` (`CTY_id`),
        INDEX `WARD_id` (`WARD_id`),
        INDEX `PCON_id` (`PCON_id`),
        INDEX `PCS_id` (`PCS_id`),
        INDEX `PCD_id` (`PCD_id`),
        INDEX `PCA_id` (`PCA_id`)
    ) COLLATE='utf8_unicode_ci' ENGINE=MyISAM ROW_FORMAT=FIXED
"
strSQL = "
    INSERT INTO geo_lookups
        SELECT OA_id, LSOA_id, MSOA_id, LAD_id, CTY_id, WARD_id, PCON_id, PCS_id, PCD_id, PCA_id
        FROM geography.lookups
        WHERE CTY_id IN ('E13000001', 'E13000002')
"
### TABLE: geo_postcodes ---------------------------------------------------------
strSQL = "
    CREATE TABLE `geo_postcodes` (
    	`postcode` CHAR(7) NOT NULL DEFAULT '' COLLATE 'utf8_unicode_ci',
    	`X_lon` DECIMAL(8,6) NOT NULL,
    	`Y_lat` DECIMAL(8,6) UNSIGNED NOT NULL,
    	`OA_id` CHAR(9) NOT NULL COLLATE 'utf8_unicode_ci',
    	`PCS_id` CHAR(5) NOT NULL COLLATE 'utf8_unicode_ci',
    	`PCD_id` CHAR(4) NOT NULL COLLATE 'utf8_unicode_ci',
    	`PCA_id` CHAR(2) NOT NULL COLLATE 'utf8_unicode_ci',
    	`hires_started` MEDIUMINT(6) UNSIGNED NOT NULL DEFAULT '0' COMMENT 'number of hires that started from the station towards ANY station',
    	`duration_started` SMALLINT(5) UNSIGNED NULL DEFAULT NULL COMMENT 'AVG duration (in seconds) for hires that started from the station towards ANY station',
    	`hires_ended` MEDIUMINT(6) UNSIGNED NOT NULL DEFAULT '0' COMMENT 'number of hires that ended in the station coming from ANY station',
    	`duration_ended` SMALLINT(5) UNSIGNED NULL DEFAULT NULL COMMENT 'AVG duration (in seconds) for hires that ended in the station coming from ANY station',
    	`hires_self` MEDIUMINT(6) UNSIGNED NOT NULL DEFAULT '0' COMMENT 'number of hires that started from and ended in the SAME station',
    	`duration_self` SMALLINT(5) UNSIGNED NULL DEFAULT NULL COMMENT 'AVG duration (in seconds) for hires that started from and ended in the SAME station',
    	`hires_started_noself` MEDIUMINT(6) UNSIGNED NOT NULL DEFAULT '0' COMMENT 'number of hires that started from the station towards ANOTHER station',
    	`duration_started_noself` SMALLINT(5) UNSIGNED NULL DEFAULT NULL COMMENT 'AVG duration (in seconds) for hires that started from the station towards ANOTHER station',
    	`hires_ended_noself` MEDIUMINT(6) UNSIGNED NOT NULL DEFAULT '0' COMMENT 'number of hires that ended in the station coming from ANOTHER station',
    	`duration_ended_noself` SMALLINT(5) UNSIGNED NULL DEFAULT NULL COMMENT 'AVG duration (in seconds) for hires that ended in the station coming from ANOTHER station',
    	PRIMARY KEY (`postcode`),
    	INDEX `OA_id` (`OA_id`),
    	INDEX `PCS_id` (`PCS_id`),
    	INDEX `PCD_id` (`PCD_id`),
    	INDEX `PCA_id` (`PCA_id`)
    ) COLLATE='utf8_unicode_ci' ENGINE=MyISAM ROW_FORMAT=FIXED
"
strSQL = "
    INSERT INTO geo_postcodes
        SELECT postcode, pc.OA_id, X_lon, Y_lat, 
        FROM geography.postcodes pc 
        JOIN stations st ON st.postcode = pc.postcode
--        JOIN geo_lookups gl ON gl.OA_id = pc.OA_id
"

### TABLE: calendar ---------------------------------------------------------
strSQL = "
    CREATE TABLE `calendar` (
        `datefield` date NOT NULL,
        `dayID` tinyint(1) unsigned NOT NULL,
        `dayTxt` char(3) COLLATE utf8_unicode_ci NOT NULL,
        `isWeekday` tinyint(1) unsigned NOT NULL,
        `DATEd` int(8) unsigned NOT NULL,
        `DATEd1` char(6) COLLATE utf8_unicode_ci NOT NULL,
        `DATEd2` char(8) COLLATE utf8_unicode_ci NOT NULL,
        `DATEd3` char(8) COLLATE utf8_unicode_ci NOT NULL,
        `DATEd4` char(9) COLLATE utf8_unicode_ci NOT NULL,
        `DATEd5` char(9) COLLATE utf8_unicode_ci NOT NULL,
        `DATEd6` char(11) COLLATE utf8_unicode_ci NOT NULL,
        `DATEd7` char(15) COLLATE utf8_unicode_ci NOT NULL,
        `DATEw` mediumint(6) unsigned NOT NULL,
        `DATEwd` int(8) unsigned NOT NULL,
        `DATEw1` char(6) COLLATE utf8_unicode_ci NOT NULL,
        `DATEw2` char(8) COLLATE utf8_unicode_ci NOT NULL,
        `DATEw3` char(8) COLLATE utf8_unicode_ci NOT NULL,
        `DATEw4` char(9) COLLATE utf8_unicode_ci NOT NULL,
        `DATEw5` char(9) COLLATE utf8_unicode_ci NOT NULL,
        `DATEm` mediumint(6) unsigned NOT NULL,
        `DATEm1` char(8) COLLATE utf8_unicode_ci NOT NULL,
        `DATEm2` char(8) COLLATE utf8_unicode_ci NOT NULL,
        `DATEm3` char(9) COLLATE utf8_unicode_ci NOT NULL,
        `daysPast` smallint(4) unsigned NOT NULL,
        `weeksPast` smallint(4) unsigned NOT NULL,
        `monthsPast` smallint(4) unsigned NOT NULL,
        `quarter` char(6) COLLATE utf8_unicode_ci NOT NULL,
        `quartern` smallint(4) unsigned NOT NULL,
        `year` smallint(4) unsigned NOT NULL,
        `toDate` smallint(4) unsigned NOT NULL,
        `dayOfMonth` tinyint(2) unsigned NOT NULL,
        `dayOfYear` smallint(3) unsigned NOT NULL,
        `dayOfYears` mediumint(7) unsigned NOT NULL,
        PRIMARY KEY (`datefield`) USING BTREE,
        KEY `DATEd` (`DATEd`) USING BTREE,
        KEY `DATEm` (`DATEm`) USING BTREE,
        KEY `quarter` (`quarter`) USING BTREE,
        KEY `year` (`year`) USING BTREE,
        KEY `quarter_n` (`quartern`),
        KEY `DATEw` (`DATEw`) USING BTREE,
        KEY `daysPast` (`daysPast`) USING BTREE,
        KEY `monthsPast` (`monthsPast`) USING BTREE,
        KEY `toDate` (`toDate`) USING BTREE,
        KEY `isWeekday` (`isWeekday`),
        KEY `weeksPast` (`weeksPast`) USING BTREE,
        KEY `dayOfYear` (`dayOfYear`) USING BTREE,
        KEY `dayID` (`dayID`) USING BTREE,
        KEY `dayOfYears` (`dayOfYears`) USING BTREE,
        KEY `dayOfMonth` (`dayOfMonth`) USING BTREE
    ) ENGINE=MyISAM DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci ROW_FORMAT=FIXED
"

### TABLE: smr_sStations ---------------------------------------------------------
strSQL = "
    CREATE TABLE `smr_sStations` (
    	`datefield` INT(8) UNSIGNED NOT NULL,
    	`station_id` SMALLINT(3) UNSIGNED NOT NULL,
    	`hires` SMALLINT(4) UNSIGNED NOT NULL,
    	`duration` MEDIUMINT(7) UNSIGNED NOT NULL
    ) COLLATE='utf8_unicode_ci' ENGINE=MyISAM ROW_FORMAT=FIXED;
"

### TABLE: smr_eStations ---------------------------------------------------------
strSQL = "
    CREATE TABLE `smr_eStations` (
    	`datefield` INT(8) UNSIGNED NOT NULL,
    	`station_id` SMALLINT(3) UNSIGNED NOT NULL,
    	`hires` SMALLINT(4) UNSIGNED NOT NULL,
    	`duration` MEDIUMINT(7) UNSIGNED NOT NULL
    ) COLLATE='utf8_unicode_ci' ENGINE=MyISAM ROW_FORMAT=FIXED;
"

### TABLE: smr_seStations ---------------------------------------------------------
strSQL = "
    CREATE TABLE `smr_seStations` (
    	`datefield` INT(8) UNSIGNED NOT NULL,
    	`sStation_id` SMALLINT(3) UNSIGNED NOT NULL,
    	`eStation_id` SMALLINT(4) UNSIGNED NOT NULL,
    	`hires` SMALLINT(4) UNSIGNED NOT NULL,
    	`duration` MEDIUMINT(7) UNSIGNED NOT NULL
    ) COLLATE='utf8_unicode_ci' ENGINE=MyISAM ROW_FORMAT=FIXED;
"

### TABLE: smrW_seStations ---------------------------------------------------------
strSQL = "
    CREATE TABLE `smrW_seStations` (
    	`datefield` INT(8) UNSIGNED NOT NULL,
    	`sStation_id` SMALLINT(3) UNSIGNED NOT NULL,
    	`eStation_id` SMALLINT(4) UNSIGNED NOT NULL,
    	`hires` SMALLINT(4) UNSIGNED NOT NULL,
    	`duration` MEDIUMINT(7) UNSIGNED NOT NULL
    ) COLLATE='utf8_unicode_ci' ENGINE=MyISAM ROW_FORMAT=FIXED;
"

### TABLE: smrM_seStations ---------------------------------------------------------
strSQL = "
    CREATE TABLE `smrM_seStations` (
    	`datefield` MEDIUMINT(6) UNSIGNED NOT NULL,
    	`sStation_id` SMALLINT(3) UNSIGNED NOT NULL,
    	`eStation_id` SMALLINT(4) UNSIGNED NOT NULL,
    	`hires` SMALLINT(4) UNSIGNED NOT NULL,
    	`duration` MEDIUMINT(7) UNSIGNED NOT NULL
    ) COLLATE='utf8_unicode_ci' ENGINE=MyISAM ROW_FORMAT=FIXED;
"

