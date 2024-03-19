  WITH CTE_MY_DATE AS (
    SELECT CAST(DATEADD(DAY, SEQ4(), '1900-01-01') AS DATE) AS MY_DATE
      FROM TABLE(GENERATOR(ROWCOUNT=>(50000)))
    UNION ALL 
    SELECT CAST('9999-12-31' AS DATE)  -- Number of days after reference date in previous line
  ),

 
  CTE_HOLIDAY AS (
  	SELECT 
  		  DTE.MY_DATE 
        , CASE 
        	WHEN   (DAYOFYEAR(DTE.MY_DATE)= 1) --New Years DAY
        		OR (DAYOFYEAR(DTE.MY_DATE)= 2 AND DAYOFWEEK(DATEADD(DAY,-1, DTE.MY_DATE)) = 0) --New Years DAY observed
        		OR (MONTH(DTE.MY_DATE) = 1 AND M.ROW_NBR = 3) --Martin Luther King Jr(3rd Monday in Jan)
        		OR (MONTH(DTE.MY_DATE) = 2 AND M.ROW_NBR = 3) --Washington's Birthday (3rd Monday in Feb)
        		OR (MONTH(DTE.MY_DATE) = 5 AND M.ROW_NBR_DESC = 1) --Memorial Day (LAST Monday in May)
        		OR (MONTH(DTE.MY_DATE) = 6 AND DAY(DTE.MY_DATE) = 19 AND YEAR(DTE.MY_DATE) >= 2021) --Juneteenth
        		OR (MONTH(DTE.MY_DATE) = 6 AND DAY(DTE.MY_DATE) = 20 AND YEAR(DTE.MY_DATE) >= 2021 AND DAYOFWEEK(DATEADD(DAY,-1, DTE.MY_DATE)) = 0) --Juneteenth sunday observed
        		OR (MONTH(DTE.MY_DATE) = 7 AND DAY(DTE.MY_DATE) = 4) --4th Of July 
        		OR (MONTH(DTE.MY_DATE) = 7 AND DAY(DTE.MY_DATE) = 5 AND DAYOFWEEK(DATEADD(DAY,-1, DTE.MY_DATE)) = 0) --4th Of July observed
        		OR (MONTH(DTE.MY_DATE) = 9 AND M.ROW_NBR = 1) --Labor DAY (FIRST Monday IN Sep)
        		OR (MONTH(DTE.MY_DATE) = 10 AND M.ROW_NBR = 2) --Columbus Day (2nd Monday in Oct)
        		OR (MONTH(DTE.MY_DATE) = 11 AND DAY(DTE.MY_DATE) = 11) --Veterans Day 
        		OR (MONTH(DTE.MY_DATE) = 11 AND DAY(DTE.MY_DATE) = 12 AND DAYOFWEEK(DATEADD(DAY,-1, DTE.MY_DATE)) = 0) --Veterans Day observed
        		OR (MONTH(DTE.MY_DATE) = 11 AND TG.ROW_NBR = 4) --Thanksgiving DAY (4th Thur in Nov)
        		OR (MONTH(DTE.MY_DATE) = 12 AND DAY(DTE.MY_DATE) = 25) --Christmas 
        		OR (MONTH(DTE.MY_DATE) = 12 AND DAY(DTE.MY_DATE) = 26 AND DAYOFWEEK(DATEADD(DAY,-1, DTE.MY_DATE)) = 0) --Christmas observed
        	THEN 1
         	ELSE 0
         END IS_HOLIDAY

        , CASE 
        	WHEN (DAYOFYEAR(DTE.MY_DATE)= 1) THEN 'New Years Day'
        	WHEN (DAYOFYEAR(DTE.MY_DATE)= 2 AND DAYOFWEEK(DATEADD(DAY,-1, DTE.MY_DATE)) = 0) THEN 'New Years Day Observed'
        	WHEN (MONTH(DTE.MY_DATE) = 1 AND M.ROW_NBR = 3) THEN 'Martin Luther King Jr Day'
        	WHEN (MONTH(DTE.MY_DATE) = 2 AND M.ROW_NBR = 3) THEN 'Washingtons Birthday'
        	WHEN (MONTH(DTE.MY_DATE) = 5 AND M.ROW_NBR_DESC = 1) THEN 'Memorial Day'
        	WHEN (MONTH(DTE.MY_DATE) = 6 AND DAY(DTE.MY_DATE) = 19 AND YEAR(DTE.MY_DATE) >= 2021) THEN 'Juneteenth'
        	WHEN (MONTH(DTE.MY_DATE) = 6 AND DAY(DTE.MY_DATE) = 20 AND YEAR(DTE.MY_DATE) >= 2021 AND DAYOFWEEK(DATEADD(DAY,-1, DTE.MY_DATE)) = 0) THEN 'Juneteenth Observed'
        	WHEN (MONTH(DTE.MY_DATE) = 7 AND DAY(DTE.MY_DATE) = 4) THEN 'Independence Day' 
        	WHEN (MONTH(DTE.MY_DATE) = 7 AND DAY(DTE.MY_DATE) = 5 AND DAYOFWEEK(DATEADD(DAY,-1, DTE.MY_DATE)) = 0) THEN 'Independence Day Observed' 
        	WHEN (MONTH(DTE.MY_DATE) = 9 AND M.ROW_NBR = 1) THEN 'Labor Day'
        	WHEN (MONTH(DTE.MY_DATE) = 10 AND M.ROW_NBR = 2) THEN 'Columbus Day'
        	WHEN (MONTH(DTE.MY_DATE) = 11 AND DAY(DTE.MY_DATE) = 11) THEN 'Veterans Day'
        	WHEN (MONTH(DTE.MY_DATE) = 11 AND DAY(DTE.MY_DATE) = 12 AND DAYOFWEEK(DATEADD(DAY,-1, DTE.MY_DATE)) = 0) THEN 'Veterans Day Observed'
        	WHEN (MONTH(DTE.MY_DATE) = 11 AND TG.ROW_NBR = 4) THEN 'Thanksgiving Day'
        	WHEN (MONTH(DTE.MY_DATE) = 12 AND DAY(DTE.MY_DATE) = 25) THEN 'Christmas Day'
        	WHEN (MONTH(DTE.MY_DATE) = 12 AND DAY(DTE.MY_DATE) = 26 AND DAYOFWEEK(DATEADD(DAY,-1, DTE.MY_DATE)) = 0) THEN 'Christmas Day Observed'
         	ELSE ''
	     END HOLIDAY_NAME
	FROM CTE_MY_DATE AS DTE 
	    LEFT JOIN (
    	SELECT MY_DATE, MONTH(MY_DATE) AS INT_MONTH
    		, ROW_NUMBER() OVER (PARTITION BY YEAR(MY_DATE), MONTH(MY_DATE) ORDER BY MY_DATE) AS ROW_NBR
    	FROM CTE_MY_DATE
    	WHERE DAYOFWEEK(MY_DATE) = 4
  	) TG ON DTE.MY_DATE = TG.MY_DATE 
  	LEFT JOIN (
    	SELECT MY_DATE, MONTH(MY_DATE) AS INT_MONTH
    		, ROW_NUMBER() OVER (PARTITION BY YEAR(MY_DATE), MONTH(MY_DATE) ORDER BY MY_DATE) AS ROW_NBR
    		, ROW_NUMBER() OVER (PARTITION BY YEAR(MY_DATE), MONTH(MY_DATE) ORDER BY MY_DATE DESC) AS ROW_NBR_DESC
    	FROM CTE_MY_DATE
    	WHERE DAYOFWEEK(MY_DATE) = 1
  	) M ON DTE.MY_DATE = M.MY_DATE 
  ),


FINAL AS (
 select * from ( SELECT 
  		  CAST(REPLACE(DTE.MY_DATE, '-', '') AS NUMBER(38,0)) AS DATE_KEY
  		, DTE.MY_DATE AS DATE
  		, CAST(YEAR(DTE.MY_DATE)||RIGHT('000'||DAYOFYEAR(DTE.MY_DATE),3) AS NUMBER(38,0)) AS DATE_INT_JULIAN
  		, CAST(RIGHT(YEAR(DTE.MY_DATE),2)||RIGHT('00'||MONTH(DTE.MY_DATE),2)||RIGHT('00'||DAY(DTE.MY_DATE),2) AS NUMBER(38,0)) AS DATE_INT_YYMMDD
        , CAST((MONTH(DTE.MY_DATE)||RIGHT('00'||DAY(DTE.MY_DATE),2)||RIGHT(YEAR(DTE.MY_DATE),2)) AS NUMBER(38,0)) AS DATE_INT_MMDDYY
        , CAST(YEAR(DTE.MY_DATE)||RIGHT('00'||MONTH(DTE.MY_DATE),2)||RIGHT('00'||DAY(DTE.MY_DATE),2) AS STRING) AS DATE_YYYY_MM_DD
  		, YEAR(DTE.MY_DATE) AS YEAR_INT
  		, QUARTER(DTE.MY_DATE) AS QUARTER_INT
        , MONTH(DTE.MY_DATE) AS MONTH_INT
        , DAY(DTE.MY_DATE) DAY_OF_MONTH
        , DAYOFWEEK(DTE.MY_DATE) DAY_OF_WEEK
        , DAYOFYEAR(DTE.MY_DATE) DAY_OF_YEAR
        , WEEKOFYEAR(DTE.MY_DATE) WEEK_OF_YEAR
        , DATE_TRUNC('week', DTE.MY_DATE) AS START_OF_WEEK
        , LAST_DAY(DTE.MY_DATE, 'week') AS END_OF_WEEK
        , DATE_TRUNC('month', DTE.MY_DATE) AS START_OF_MONTH 
        , LAST_DAY(DTE.MY_DATE, 'month') END_OF_MONTH        
        , DATE_TRUNC('quarter', DTE.MY_DATE) AS START_OF_QUARTER
        , LAST_DAY(DTE.MY_DATE, 'quarter') AS END_OF_QUARTER
        , DATE_TRUNC('year', DTE.MY_DATE) AS START_OF_YEAR 
        , LAST_DAY(DTE.MY_DATE, 'year') AS END_OF_YEAR
        , CASE DAYOFWEEK(DATE_TRUNC('year', DTE.MY_DATE)) 
        	WHEN 5 THEN DATEADD(D,3, DATE_TRUNC('year', DTE.MY_DATE))
        	WHEN 6 THEN DATEADD(D,2, DATE_TRUNC('year', DTE.MY_DATE))
        	ELSE DATEADD(D,1, DATE_TRUNC('year', DTE.MY_DATE))
          END AS START_OF_FISCAL_YEAR 
        , CASE DAYOFWEEK(LAST_DAY(DTE.MY_DATE, 'year'))
        	WHEN 6 THEN DATEADD(D,-1, LAST_DAY(DTE.MY_DATE, 'year'))
        	WHEN 7 THEN DATEADD(D,-2, LAST_DAY(DTE.MY_DATE, 'year'))
        	ELSE LAST_DAY(DTE.MY_DATE, 'year')
          END AS END_OF_FISCAL_YEAR
        , DAYNAME(DTE.MY_DATE) AS DAY_SHORT_NAME
		, CASE DAYOFWEEK(DTE.MY_DATE)
					WHEN 0 THEN 'Sunday'
					WHEN 1 THEN 'Monday'
					WHEN 2 THEN 'Tuesday'
					WHEN 3 THEN 'Wednesday'
					WHEN 4 THEN 'Thursday'
					WHEN 5 THEN 'Friday'
					WHEN 6 THEN 'Saturday'
		  END AS DAY_LONG_NAME        
	    , MONTHNAME(DTE.MY_DATE) MONTH_SHORT_NAME
        , CASE MONTH(DTE.MY_DATE)
        			WHEN 1 THEN 'January'
					WHEN 2 THEN 'February'
					WHEN 3 THEN 'March'
					WHEN 4 THEN 'April'
					WHEN 5 THEN 'May'
					WHEN 6 THEN 'June'
					WHEN 7 THEN 'July'    
					WHEN 8 THEN 'August'
					WHEN 9 THEN 'September'
					WHEN 10 THEN 'October'
					WHEN 11 THEN 'November'
					WHEN 12 THEN 'December'
		  END AS MONTH_LONG_NAME
		, 'Q'||QUARTER(DTE.MY_DATE) AS QUARTER_SHORT_NAME
		, 'Quarter '||QUARTER(DTE.MY_DATE) AS QUARTER_LONG_NAME
        , CASE WHEN DAYOFWEEK(DTE.MY_DATE) NOT IN (0,6) THEN 1 ELSE 0 END AS IS_WEEKDAY
        , CASE WHEN DAYOFWEEK(DTE.MY_DATE) NOT IN (0,6) AND H.IS_HOLIDAY = 0 THEN 1 ELSE 0 END AS IS_BUSINESS_DAY
        , CASE WHEN DAYOFWEEK(DTE.MY_DATE) NOT IN (0) AND H.IS_HOLIDAY = 0 THEN 1 ELSE 0 END AS IS_BANKING_DAY
        , CASE WHEN DAYOFWEEK(DTE.MY_DATE) IN (2,3,4,5,6) THEN 1 ELSE 0 END AS IS_JHA_PROCESSING_DAY
        , H.IS_HOLIDAY
        , H.HOLIDAY_NAME
    FROM CTE_MY_DATE DTE 
    INNER JOIN CTE_HOLIDAY H ON DTE.MY_DATE = H.MY_DATE

    UNION ALL     

    SELECT 
  		  0 AS DATE_KEY
  		, CAST('1900-01-01' AS DATE) AS DATE
  		, 0 AS DATE_INT_JULIAN
  		, CAST(RIGHT(YEAR(CAST('1900-01-01' AS DATE)),2)||RIGHT('00'||MONTH(CAST('1900-01-01' AS DATE)),2)||RIGHT('00'||DAY(CAST('1900-01-01' AS DATE)),2) AS NUMBER(38,0)) AS DATE_INT_YYMMDD
        , CAST((MONTH(CAST('1900-01-01' AS DATE))||RIGHT('00'||DAY(CAST('1900-01-01' AS DATE)),2)||RIGHT(YEAR(CAST('1900-01-01' AS DATE)),2)) AS NUMBER(38,0)) AS DATE_INT_MMDDYY
        , CAST(YEAR(CAST('1900-01-01' AS DATE))||RIGHT('00'||MONTH(CAST('1900-01-01' AS DATE)),2)||RIGHT('00'||DAY(CAST('1900-01-01' AS DATE)),2) AS STRING) AS DATE_YYYY_MM_DD
  		, YEAR(CAST('1900-01-01' AS DATE)) AS YEAR_INT
  		, QUARTER(CAST('1900-01-01' AS DATE)) AS QUARTER_INT
        , MONTH(CAST('1900-01-01' AS DATE)) AS MONTH_INT
        , DAY(CAST('1900-01-01' AS DATE)) DAY_OF_MONTH
        , DAYOFWEEK(CAST('1900-01-01' AS DATE)) DAY_OF_WEEK
        , DAYOFYEAR(CAST('1900-01-01' AS DATE)) DAY_OF_YEAR
        , WEEKOFYEAR(CAST('1900-01-01' AS DATE)) WEEK_OF_YEAR
        , DATE_TRUNC('week', CAST('1900-01-01' AS DATE)) AS START_OF_WEEK
        , LAST_DAY(CAST('1900-01-01' AS DATE), 'week') AS END_OF_WEEK
        , DATE_TRUNC('month', CAST('1900-01-01' AS DATE)) AS START_OF_MONTH 
        , LAST_DAY(CAST('1900-01-01' AS DATE), 'month') END_OF_MONTH        
        , DATE_TRUNC('quarter', CAST('1900-01-01' AS DATE)) AS START_OF_QUARTER
        , LAST_DAY(CAST('1900-01-01' AS DATE), 'quarter') AS END_OF_QUARTER
        , DATE_TRUNC('year', CAST('1900-01-01' AS DATE)) AS START_OF_YEAR 
        , LAST_DAY(CAST('1900-01-01' AS DATE), 'year') AS END_OF_YEAR
        , CASE DAYOFWEEK(DATE_TRUNC('year', CAST('1900-01-01' AS DATE))) 
        	WHEN 5 THEN DATEADD(D,3, DATE_TRUNC('year', CAST('1900-01-01' AS DATE)))
        	WHEN 6 THEN DATEADD(D,2, DATE_TRUNC('year', CAST('1900-01-01' AS DATE)))
        	ELSE DATEADD(D,1, DATE_TRUNC('year', CAST('1900-01-01' AS DATE)))
          END AS START_OF_FISCAL_YEAR 
        , CASE DAYOFWEEK(LAST_DAY(CAST('1900-01-01' AS DATE), 'year'))
        	WHEN 6 THEN DATEADD(D,-1, LAST_DAY(CAST('1900-01-01' AS DATE), 'year'))
        	WHEN 7 THEN DATEADD(D,-2, LAST_DAY(CAST('1900-01-01' AS DATE), 'year'))
        	ELSE LAST_DAY(CAST('1900-01-01' AS DATE), 'year')
          END AS END_OF_FISCAL_YEAR
        , DAYNAME(CAST('1900-01-01' AS DATE)) AS DAY_SHORT_NAME
		, CASE DAYOFWEEK(CAST('1900-01-01' AS DATE))
					WHEN 0 THEN 'Sunday'
					WHEN 1 THEN 'Monday'
					WHEN 2 THEN 'Tuesday'
					WHEN 3 THEN 'Wednesday'
					WHEN 4 THEN 'Thursday'
					WHEN 5 THEN 'Friday'
					WHEN 6 THEN 'Saturday'
		  END AS DAY_LONG_NAME        
	    , MONTHNAME(CAST('1900-01-01' AS DATE)) MONTH_SHORT_NAME
        , CASE MONTH(CAST('1900-01-01' AS DATE))
        			WHEN 1 THEN 'January'
					WHEN 2 THEN 'February'
					WHEN 3 THEN 'March'
					WHEN 4 THEN 'April'
					WHEN 5 THEN 'May'
					WHEN 6 THEN 'June'
					WHEN 7 THEN 'July'    
					WHEN 8 THEN 'August'
					WHEN 9 THEN 'September'
					WHEN 10 THEN 'October'
					WHEN 11 THEN 'November'
					WHEN 12 THEN 'December'
		  END AS MONTH_LONG_NAME
		, 'Q'||QUARTER(CAST('1900-01-01' AS DATE)) AS QUARTER_SHORT_NAME
		, 'Quarter '||QUARTER(CAST('1900-01-01' AS DATE)) AS QUARTER_LONG_NAME
        , CASE WHEN DAYOFWEEK(CAST('1900-01-01' AS DATE)) NOT IN (0,6) THEN 1 ELSE 0 END AS IS_WEEKDAY
        , CASE WHEN DAYOFWEEK(CAST('1900-01-01' AS DATE)) NOT IN (0,6) THEN 1 ELSE 0 END AS IS_BUSINESS_DAY
        , CASE WHEN DAYOFWEEK(CAST('1900-01-01' AS DATE)) NOT IN (0) THEN 1 ELSE 0 END AS IS_BANKING_DAY
        , 0 AS IS_JHA_PROCESSING_DAY
        , 0 AS IS_HOLIDAY
        , '' AS HOLIDAY_NAME)

        order by DATE
)

  select * from FINAL