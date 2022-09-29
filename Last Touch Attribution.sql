
SET filter_field = advertiser_id;
SET console_ids = 1608747656;
SET market_ids = 884;
SET impression_start_date = '2022_08_01';
SET impression_end_date = '2022_08_31';
SET impression_end_plus_window = '2022_09_01'; -- this is for the key_values table, conversion usually happend in real time, but sometime was +1 day delay
SET attribution_window = 60;

SET rdr_contract_id = 1749978473;


WITH temp_xd AS (

SELECT DISTINCT
       person_id,
       tuid
  FROM household_graph_liveramp_latest
),

temp_imps_agg AS (

SELECT to_market_date(impression_datetime, 'MM/dd/yyyy', 'US/Pacific') AS imp_date,
	     advertiser_name,
       advertiser_id,
	     insertion_order_name,
       insertion_order_id,
	     package_name,
       package_id,
	     line_item_name,
       line_item_id,
	     COUNT(DISTINCT NVL(d.person_id,c.id)) AS imp_uu,
	     COUNT(DISTINCT CASE WHEN action = 1 THEN NVL(d.person_id,c.id) END) AS action_uu,
	     SUM(impression) AS imps,
	     SUM(NVL(click,0)) AS clicks,
	     SUM(action) AS console_actions,
	   	 SUM(cost) AS cost
  FROM (
	     SELECT DISTINCT
			        CASE WHEN derived_user_id = -1 THEN user_id ELSE derived_user_id END AS id,
			        impression_datetime,
			        advertiser_name,
              advertiser_id,
			        insertion_order_name,
              insertion_order_id,
			        package_name,
              package_id,
			        line_item_name,
              line_item_id,
			        impression,
			        click,
			        cost,
			        action
	       FROM amobee_media
	      WHERE ${filter_field} IN (${console_ids})
		 	        AND market_id IN (${market_ids})
		 	        AND date >= ${impression_start_date}
		 	        AND date <= ${impression_end_plus_window}
			        AND to_market_date(impression_datetime,'yyyy_MM_dd','US/Pacific') >= ${impression_start_date}
			        AND to_market_date(impression_datetime,'yyyy_MM_dd','US/Pacific') <= ${impression_end_date}
		      	  AND event_type IN ('impression', 'click','action','viewability')
	     ) c
  	   LEFT JOIN
  	   temp_xd d
  	   ON c.id = d.tuid
 GROUP BY 1,2,3,4,5,6,7,8,9

),

temp_imps_event AS (

SELECT DISTINCT
		   NVL(f.person_id,e.id) AS imp_id,
		   impression_datetime/1000 AS imp_ts,
		   ad_call_id,
		   to_market_date(impression_datetime, 'MM/dd/yyyy', 'US/Pacific') AS imp_date,
		   advertiser_name,
       advertiser_id,
		   insertion_order_name,
       insertion_order_id,
		   package_name,
       package_id,
		   line_item_name,
       line_item_id
  FROM (
		   SELECT DISTINCT
				      CASE WHEN derived_user_id = -1 THEN user_id ELSE derived_user_id END AS id,
				      impression_datetime,
				      ad_call_id,
				      advertiser_name,
              advertiser_id,
				      insertion_order_name,
              insertion_order_id,
				      package_name,
              package_id,
				      line_item_name,
              line_item_id
		     FROM amobee_media
		    WHERE ${filter_field} IN (${console_ids})
			 	      AND market_id IN (${market_ids})
			 	      AND date >= ${impression_start_date}
			 	      AND date <= ${impression_end_plus_window}  --give addtional 1 day
				      AND to_market_date(impression_datetime,'yyyy_MM_dd','US/Pacific') >= ${impression_start_date}
				      AND to_market_date(impression_datetime,'yyyy_MM_dd','US/Pacific') <= ${impression_end_date}
				      AND event_type IN ('impression')
	     ) e
  	   LEFT JOIN
  	   temp_xd f
  	   ON e.id = f.tuid
),

temp_txn_event AS (

	SELECT DISTINCT
			   NVL(h.person_id,g.id) AS txn_id,
			   purchase_ts AS txn_ts
	  FROM (
	  	   SELECT DISTINCT
	  	   	      user_id AS id,
	  	   	      value AS purchase_ts
	  	   	 FROM (
	  	   	 		  SELECT DISTINCT
	  	   	 		  	     user_id,
	  	   	 		  	     value --- if the value is epoch time, then keep this. Otherwise, need to extract the epoch time out of the value fields
	  	   	 		  	FROM key_values
	  	   	 		   WHERE contract_id = ${rdr_contract_id}
                       AND market_id IN (${market_ids})
	  	   	 		   	     AND key = 'visittimestamp'  -- pull a key_values list first to determine the correct key
	  	   	 		   	     AND date >= ${impression_start_date}
	  	   	 		   	     AND date <= ${impression_end_plus_window}  -- +1 day, as attribution might be delay for 1 day
	  	   	 		  )
			   ) g
	  	   LEFT JOIN
	  	   temp_xd h
	  	   ON g.id = h.tuid
),

temp_attribution_lta AS (

SELECT imp_date,
		   advertiser_name,
       advertiser_id,
		   insertion_order_name,
       insertion_order_id,
		   package_name,
       package_id,
		   line_item_name,
       line_item_id,
		   COUNT(DISTINCT imp_id) AS offline_action_uu,
		   SUM(txn_count) AS offline_actions
  FROM (
	   	 SELECT i.*,
		      	  RANK() OVER (PARTITION BY i.imp_id, j.txn_ts ORDER BY j.txn_ts - i.imp_ts) AS attr_rank,
		      	  1 AS txn_count
	       FROM temp_imps_event i
	  	        INNER JOIN
	  	        temp_txn_event j
	  	        ON i.imp_id = j.txn_id AND imp_ts < txn_ts AND txn_ts - imp_ts <= ${attribution_window} * 86400
	     )
 WHERE attr_rank = 1
 GROUP BY 1,2,3,4,5,6,7,8,9
)

SELECT *
  FROM (
  	   SELECT 'LTA' AS attribution_type,
  	   		    m.*,
  	   		    NVL(offline_action_uu,0) AS purchasing_uu,
		          NVL(offline_actions,0) AS purchases
	       FROM temp_imps_agg m
	  	        LEFT JOIN
	  	        temp_attribution_lta n
	  	        ON m.imp_date = n.imp_date AND m.advertiser_name = n.advertiser_name AND m.advertiser_id = n.advertiser_id AND m.insertion_order_name = n.insertion_order_name AND m.insertion_order_id = n.insertion_order_id AND m.package_name = n.package_name AND m.package_id = n.package_id AND m.line_item_name = n.line_item_name AND m.line_item_id = n.line_item_id
	      )
