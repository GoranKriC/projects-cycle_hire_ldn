DELIMITER $$

DROP PROCEDURE IF EXISTS proc_fill_calendar $$

CREATE PROCEDURE proc_fill_calendar()

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
  				WHEN days_past < 7 THEN 1 
  				WHEN days_past < 15 THEN 2
  				WHEN days_past < 30 THEN 3
  				WHEN days_past < 60 THEN 4
  				WHEN days_past < 90 THEN 5
  				WHEN days_past < 120 THEN 6
  				WHEN days_past < 180 THEN 7
  				WHEN days_past < 365 THEN 8
  				ELSE 9
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
		WHERE day_of_month = 29 AND month_of_year = 2;
		
  		DROP TABLE temp;
  		
  END $$

DELIMITER ;