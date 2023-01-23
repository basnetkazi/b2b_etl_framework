create or replace TABLE DWH_TIME_DAY_D AS
SELECT DISTINCT MY_DATE ID
	,TO_CHAR(MY_DATE, 'YYYYMMDD') DAY_KY
	,TO_CHAR(MY_DATE, 'YYYYMM') MONTH_KY
	,TO_CHAR(MY_DATE, 'MMMM') MONTH_DESC
	,TRUNC(MY_DATE, 'MONTH') MONTH_START_DATE
	,LAST_DAY(MY_DATE, 'MONTH') MONTH_END_DATE
	,YEAR(MY_DATE) || '0' || QUARTER(MY_DATE) QUARTER_KY
	,YEAR(MY_DATE) YEAR_KY
	,CASE
		WHEN MONTH(MY_DATE) < 7
			THEN YEAR(MY_DATE) || '01'
		ELSE YEAR(MY_DATE) || '02'
		END HALF_YEAR_KY
	,MY_DATE || ' 07:00:00' DAY_START_TIME
	,MY_DATE || ' 21:00:00' DAY_END_TIME
	,1 OPEN_CLOSE_CD
	,sysdate() ROW_INSRT_TMS
	,sysdate() ROW_UPDT_TMS
FROM (
	SELECT DATEADD(DAY, SEQ4(), TO_DATE('2019', 'YYYY')) AS MY_DATE
	FROM TABLE (GENERATOR(ROWCOUNT => 3650))
	)
ORDER BY 1;