
--!@ This query is using daily for pacing need across over 200 packages.
  --The data file can be used directly to determine over or underpacing


SET market_id = 927;
SET start_date = '2022_03_01'; -- only update this when the new fiscal year start
SET impression_start_date = '2022_03_01'; --only update this  when the new fiscal year start
SET overacing = 1; -- threshole for the overpacing mark, and please adjust if need
SET underpacing = 0.75;   -- threshole for the underpacing mark, and please adjust if need

-- each package has different start and end data, the total spend to update to date data is need to calculate daily_needed

-- step 1: pull spend to date
WITH temp_amobee_media AS
    (
      SELECT to_market_date(ts,'yyyy-MM-dd','US/Pacific') AS date
            ,advertiser_id
            ,advertiser_name
            ,insertion_order_id
            ,insertion_order_name
            ,package_id
            ,package_name
            ,packag_current_budget
            ,to_epoch(CAST(packag_end_date as string),'yyyy_MM_dd') AS packag_end_date
            ,to_epoch(CAST(packag_start_date as string),'yyyy_MM_dd') AS packag_start_date
            ,date_sub(current_date(),1) AS yesterday_date
            ,SUM(CASE WHEN to_market_date(impression_datetime,'yyyy_MM_dd','US/Pacific') = date_format(date_sub(current_date(),1),'yyyy_MM_dd') THEN cost ELSE 0 END) AS yesterday_spend
            ,SUM(impression) AS impressions
            ,SUM(NVL(cost,0)) AS cost
        FROM amobee_media
       WHERE date >= ${start_date}
             AND date <= date_format(current_date(),'yyyy_MM_dd')
             AND market_id = ${market_id}
             AND to_market_date(impression_datetime, 'yyyy_MM_dd', 'US/Pacific')>= ${impression_start_date}
             AND to_market_date(impression_datetime,'yyyy_MM_dd','US/Pacific') < date_format(current_date(),'yyyy_MM_dd')
             AND event_type = 'impression'
       GROUP BY 1,2,3,4,5,6,7,8,9,10,11
    )

--- Step 2: Filter out the most current budget, some package has mutiple inconsistant flight days, such as 3/1-6/1 then 9/8-12/31
,temp_budget AS
    (
      SELECT package_id
            ,packag_current_budget
            ,packag_end_date
            ,packag_start_date
        FROM
             (SELECT date
          					,package_id
          					,packag_current_budget
                    ,packag_start_date
                    ,packag_end_date
          					,ROW_NUMBER() OVER (PARTITION BY package_id, packag_start_date ORDER BY DATE DESC) as num
          		 FROM temp_amobee_media
             )
       WHERE num=1  --if no num=1, it might use much resources.
    )

-- step 3: find out the remainding spend for each of the package and days
,temp_current_spend AS
(
  SELECT advertiser_name
      	,advertiser_id
      	,insertion_order_name
      	,insertion_order_id
      	,package_name
      	,package_id
        ,packag_current_budget
        ,packag_start_date
        ,packag_end_date
        ,yesterday_date
        ,DATEDIFF(packag_end_date, yesterday_date) AS days_left_in_flight
      	,MAX(yesterday_spend) AS yesterday_spend
      	,SUM(flight_spend) AS flight_spend
    FROM
        (
        SELECT date
              ,advertiser_name
          		,advertiser_id
          		,insertion_order_name
          		,insertion_order_id
          		,package_name
          		,b.package_id
              ,b.packag_current_budget
              ,to_market_date(b.packag_start_date,'yyyy-MM-dd','US/Pacific') AS packag_start_date
              ,to_market_date(b.packag_end_date,'yyyy-MM-dd','US/Pacific') AS packag_end_date
              ,yesterday_date
              ,yesterday_spend
          		,SUM(cost) AS flight_spend
          FROM temp_amobee_media a
               INNER JOIN
               temp_budget b
               ON a.package_id = b.package_id
               AND a.packag_start_date = b.packag_start_date
               AND a.packag_end_date = b.packag_end_date
         GROUP BY
          		1,2,3,4,5,6,7,8,9,10,11,12
        )
  WHERE date > packag_start_date  -- in the event of mutiple flight dates for same package, this elimited the previous flight date spend
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11
)

-- Step 4: combined all piece and find out the daily spend target and label under and overpacing
SELECT advertiser_id
      ,advertiser_name
      ,insertion_order_id
      ,insertion_order_name
      ,package_id
      ,package_name
      ,packag_current_budget
      ,packag_start_date
      ,packag_end_date
      ,yesterday_date
      ,MAX(yesterday_spend) AS yesterday_spend
      ,MAX(daily_needed) AS daily_needed
      ,MAX(daily_needed_percent) AS daily_pacing_percent
      ,CASE WHEN MAX(daily_needed_percent) <= ${underpacing} THEN 'Underpacing'
            WHEN MAX(daily_needed_percent) > ${overacing} THEN 'Overpacing'
            ELSE 0 END AS pacing_status  -- On track flag
 FROM
      (SELECT  advertiser_name
          	  ,advertiser_id
          	  ,insertion_order_name
          	  ,insertion_order_id
          	  ,package_name
          	  ,package_id
              ,packag_current_budget
              ,packag_start_date
              ,packag_end_date
              ,yesterday_date
              ,MAX(yesterday_spend) AS yesterday_spend
              ,MAX((packag_current_budget-flight_spend)/(days_left_in_flight-1)) AS daily_needed
              ,MAX(yesterday_spend)/MAX((packag_current_budget-flight_spend)/(days_left_in_flight-1)) AS daily_needed_percent
          FROM temp_current_spend
         GROUP BY 1,2,3,4,5,6,7,8,9,10
      )
 WHERE packag_end_date >= yesterday_date
 GROUP BY 1,2,3,4,5,6,7,8,9,10
