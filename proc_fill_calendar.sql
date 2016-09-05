DELIMITER $$

DROP PROCEDURE IF EXISTS proc_fill_calendar $$

CREATE PROCEDURE proc_fill_calendar()

  BEGIN
  
  		DECLARE crt_date DATE;
  		SET crt_date = ( SELECT STR_TO_DATE(MIN(start_day), '%Y%m%d') FROM hires );
  
  		TRUNCATE TABLE calendar;
  
  		WHILE crt_date <= ( SELECT STR_TO_DATE(MAX(start_day), '%Y%m%d') FROM hires ) DO
  			INSERT INTO calendar (datefield, dayID, dayTxt, DATEd, DATEd1, DATEd2, DATEd3, DATEd4, DATEd5, DATEw, DATEm, quarter, quartern, year, dayOfMonth, dayOfYear, dayOfYears) 
  				SELECT crt_date, WEEKDAY(crt_date) + 1, DAYNAME(crt_date),
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
  					CONCAT( YEAR(crt_date), RIGHT( CONCAT('00', DAYOFYEAR(crt_date) ), 3) )
  				;
  			SET crt_date = ADDDATE(crt_date, INTERVAL 1 DAY);
  		END WHILE;
  
  		UPDATE calendar c JOIN 
  			(	SELECT DISTINCT
  					YEARWEEK(datefield, 3) AS d,
  					DATEd,
  					DATE_FORMAT(MIN(DATE(datefield)),'%d/%m/%y') AS ds,
  					DATE_FORMAT(MIN(DATE(datefield)),'%d-%m-%y') AS dd,
  					DATE_FORMAT(MIN(DATE(datefield)),'%d-%b-%y') AS de,
  					DATE_FORMAT(MIN(DATE(datefield)),'%d %b') AS dm,
  					DATE_FORMAT(MIN(DATE(datefield)),'%d %b %y') AS dm2
  				FROM calendar
  				GROUP BY d
  			) t ON t.d = c.DATEw
  		SET DATEwd = t.DATEd, DATEw1 = dm, DATEw2 = ds, DATEw3 = dd, DATEw4 = de, DATEw5 = dm2;
  
  		UPDATE calendar c JOIN 
  			(	SELECT DISTINCT
  					DATE_FORMAT(datefield,'%Y%m') AS d,
  					DATE_FORMAT(MIN(DATE(datefield)),'%b %y') AS de,
  					DATE_FORMAT(MIN(DATE(datefield)),'%m-%y') AS dd,
  					DATE_FORMAT(MIN(DATE(datefield)),'%m/%y') AS ds
  				FROM calendar
  				GROUP BY d
  			) t ON t.d = c.DATEm
  		SET DATEm1 = de, DATEm2 = ds, DATEm3 = dd;
  
  		SET @rt=-1;
  		UPDATE calendar c JOIN (SELECT DATEd AS d, @rt:=@rt+1 AS c FROM calendar ORDER BY DATEd DESC) t ON t.d = c.DATEd SET daysPast = t.c;
  
  		DROP TABLE IF EXISTS temp;
  		SET @rt=-1;
  		CREATE TABLE temp AS	SELECT t.d, @rt:=@rt+1 AS c FROM ( SELECT DISTINCT DATEw d FROM calendar ORDER BY DATEw DESC) t;
  		UPDATE calendar c JOIN temp t ON t.d = c.DATEw SET weeksPast = t.c;
  
  		DROP TABLE temp;
  		SET @rt=-1;
  		CREATE TABLE temp AS	SELECT t.d, @rt:=@rt+1 AS c  FROM ( SELECT DISTINCT DATEm d FROM calendar ORDER BY DATEm DESC) t;
  		UPDATE calendar c JOIN temp t ON t.d = c.DATEm SET monthsPast = t.c;
  
  		UPDATE calendar SET toDate = 
  			CASE
  				WHEN daysPast < 7 THEN 1 
  				WHEN daysPast < 15 THEN 2
  				WHEN daysPast < 30 THEN 3
  				WHEN daysPast < 60 THEN 4
  				WHEN daysPast < 90 THEN 5
  				WHEN daysPast < 120 THEN 6
  				WHEN daysPast < 180 THEN 7
  				WHEN daysPast < 365 THEN 8
  				ELSE 9
  			END;
  		UPDATE calendar SET isWeekday = 1 WHERE dayID <= 5;
  		UPDATE calendar SET DATEd6 = CONCAT(dayTxt, ', ', DATEd1), DATEd7 = CONCAT(dayTxt, ', ', DATEd5);
  
  		DROP TABLE temp;
  		
  END $$

DELIMITER ;
