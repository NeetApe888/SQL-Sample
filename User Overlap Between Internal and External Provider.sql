-- Understand the overlap between our platform and the 3rd party provider

SET media_start_date = '2022_09_13';
SET media_end_date = '2022_09_28';
SET market_id = 999;
SET advertiser_id = 1604245613;
SET insertion_order_id = 1608800864;
SET contract_id = 1748930670;

WITH temp_lr_graph AS (
SELECT
household_id,
ip
FROM household_graph_tapad_latest
WHERE dates last_30_days
and ip<> ' ' and ip<>''
GROUP BY 1,2)
, temp_amobee_media AS (
  SELECT
  nvl(b.household_id,a.ip) household_id
  ,MAX(CASE WHEN line_item_id = 1608800885 THEN 1 ELSE 0 END) campaign_1
  ,MAX(CASE WHEN line_item_id = 1608800877 THEN 1 ELSE 0 END) campaign_2
  ,MAX(CASE WHEN line_item_id = 1608800880 THEN 1 ELSE 0 END) campaign_3
  ,MAX(CASE WHEN line_item_id = 1608800881 THEN 1 ELSE 0 END) campaign_4
  ,MAX(CASE WHEN line_item_id = 1608800884 THEN 1 ELSE 0 END) campaign_5
  ,MAX(CASE WHEN line_item_id = 1608800879 THEN 1 ELSE 0 END) campaign_6
  ,'0' AS campaign_7
  ,'0' AS campaign_8
  ,'0' AS campaign_9
  ,'0' AS campaign_10
  ,SUM(impression) impressions
  ,SUM(cost) cost
  ,SUM(CASE WHEN line_item_id = 1608800885 THEN impression ELSE 0 END) campaign_1_imps
  ,SUM(CASE WHEN line_item_id = 1608800877 THEN impression ELSE 0 END) campaign_2_imps
  ,SUM(CASE WHEN line_item_id = 1608800880 THEN impression ELSE 0 END) campaign_3_imps
  ,SUM(CASE WHEN line_item_id = 1608800881 THEN impression ELSE 0 END) campaign_4_imps
  ,SUM(CASE WHEN line_item_id = 1608800884 THEN impression ELSE 0 END) campaign_5_imps
  ,SUM(CASE WHEN line_item_id = 1608800879 THEN impression ELSE 0 END) campaign_6_imps
  ,'0' AS campaign_7_imps
  ,'0' AS campaign_8_imps
  ,'0' AS campaign_9_imps
  ,'0' AS campaign_10_imps
  ,SUM(CASE WHEN line_item_id = 1608800885 THEN cost ELSE 0 END) campaign_1_cost
  ,SUM(CASE WHEN line_item_id = 1608800877 THEN cost ELSE 0 END) campaign_2_cost
  ,SUM(CASE WHEN line_item_id = 1608800880 THEN cost ELSE 0 END) campaign_3_cost
  ,SUM(CASE WHEN line_item_id = 1608800881 THEN cost ELSE 0 END) campaign_4_cost
  ,SUM(CASE WHEN line_item_id = 1608800884 THEN cost ELSE 0 END) campaign_5_cost
  ,SUM(CASE WHEN line_item_id = 1608800879 THEN cost ELSE 0 END) campaign_6_cost
  ,'0' AS campaign_7_cost
  ,'0' AS campaign_8_cost
  ,'0' AS campaign_9_cost
  ,'0' AS campaign_10_cost
FROM amobee_media a
left join temp_lr_graph b
on b.ip = a.ip
WHERE date >=${media_start_date} AND date <=${media_end_date}
AND insertion_order_id IN (${insertion_order_id})
AND market_id IN (${market_id})
AND advertiser_id IN (${advertiser_id})
AND event_type = 'impression'
GROUP BY 1)

,temp_external_media AS (
     SELECT
        nvl(b.household_id,a.ip) household_id
          ,MAX(CASE WHEN keyword ='partnername=Tubi' THEN 1 ELSE 0 END) AS campaign_11
          ,MAX(CASE WHEN keyword ='partnername=Vevo' THEN 1 ELSE 0 END) AS campaign_12
          ,'0' AS campaign_13
          ,'0' AS campaign_14
          ,'0' AS campaign_15
          ,'0' AS campaign_16
          ,'0' AS campaign_17
          ,'0' AS campaign_18
          ,'0' AS campaign_19
          ,'0' AS campaign_20
          ,COUNT(*) impressions
          ,SUM(CASE WHEN keyword ='partnername=Tubi' THEN 1 ELSE 0 END) AS campaign_11_imps
          ,SUM(CASE WHEN keyword ='partnername=Vevo' THEN 1 ELSE 0 END) AS campaign_12_imps
          ,'0' AS campaign_13_imps
          ,'0' AS campaign_14_imps
          ,'0' AS campaign_15_imps
          ,'0' AS campaign_16_imps
          ,'0' AS campaign_17_imps
          ,'0' AS campaign_18_imps
          ,'0' AS campaign_19_imps
          ,'0' AS campaign_20_imps
     FROM keywords a
     left join temp_lr_graph b
     on b.ip = a.ip
     WHERE date>=${media_start_date} AND date<=${media_end_date}
     AND contract_id = ${contract_id}
     GROUP BY 1
)
SELECT
     CASE WHEN
          campaign_1+campaign_2+campaign_3+campaign_4+campaign_5+campaign_6+campaign_7+campaign_8+campaign_9+campaign_10
          + campaign_11+campaign_12+campaign_13+campaign_14+campaign_15+campaign_16+campaign_17+campaign_18+campaign_19+campaign_20
          = 1 THEN 1 ELSE 0 END AS unique_flag
     ,campaign_1
     ,campaign_2
     ,campaign_3
     ,campaign_4
     ,campaign_5
     ,campaign_6
     ,campaign_7
     ,campaign_8
     ,campaign_9
     ,campaign_10
     ,campaign_11
     ,campaign_12
     ,campaign_13
     ,campaign_14
     ,campaign_15
     ,campaign_16
     ,campaign_17
     ,campaign_18
     ,campaign_19
     ,campaign_20
     ,SUM(impressions) AS impressions
     ,SUM(cost) AS cost
     ,SUM(campaign_1_imps) AS campaign_1_imps
     ,SUM(campaign_2_imps) AS campaign_2_imps
     ,SUM(campaign_3_imps) AS campaign_3_imps
     ,SUM(campaign_4_imps) AS campaign_4_imps
     ,SUM(campaign_5_imps) AS campaign_5_imps
     ,SUM(campaign_6_imps) AS campaign_6_imps
     ,SUM(campaign_7_imps) AS campaign_7_imps
     ,SUM(campaign_8_imps) AS campaign_8_imps
     ,SUM(campaign_9_imps) AS campaign_9_imps
     ,SUM(campaign_10_imps) AS campaign_10_imps
     ,SUM(campaign_11_imps) AS campaign_11_imps
     ,SUM(campaign_12_imps) AS campaign_12_imps
     ,SUM(campaign_13_imps) AS campaign_13_imps
     ,SUM(campaign_14_imps) AS campaign_14_imps
     ,SUM(campaign_15_imps) AS campaign_15_imps
     ,SUM(campaign_16_imps) AS campaign_16_imps
     ,SUM(campaign_17_imps) AS campaign_17_imps
     ,SUM(campaign_18_imps) AS campaign_18_imps
     ,SUM(campaign_19_imps) AS campaign_19_imps
     ,SUM(campaign_20_imps) AS campaign_20_imps
     ,SUM(campaign_1_cost) AS campaign_1_cost
     ,SUM(campaign_2_cost) AS campaign_2_cost
     ,SUM(campaign_3_cost) AS campaign_3_cost
     ,SUM(campaign_4_cost) AS campaign_4_cost
     ,SUM(campaign_5_cost) AS campaign_5_cost
     ,SUM(campaign_6_cost) AS campaign_6_cost
     ,SUM(campaign_7_cost) AS campaign_7_cost
     ,SUM(campaign_8_cost) AS campaign_8_cost
     ,SUM(campaign_9_cost) AS campaign_9_cost
     ,SUM(campaign_10_cost) AS campaign_10_cost
     ,COUNT(*) devices
FROM (
     SELECT
          NVL(a.household_id,b.household_id) household_id
          ,MAX(NVL(a.campaign_1,0)) AS campaign_1
          ,MAX(NVL(a.campaign_2,0)) AS campaign_2
          ,MAX(NVL(a.campaign_3,0)) AS campaign_3
          ,MAX(NVL(a.campaign_4,0)) AS campaign_4
          ,MAX(NVL(a.campaign_5,0)) AS campaign_5
          ,MAX(NVL(a.campaign_6,0)) AS campaign_6
          ,MAX(NVL(a.campaign_7,0)) AS campaign_7
          ,MAX(NVL(a.campaign_8,0)) AS campaign_8
          ,MAX(NVL(a.campaign_9,0)) AS campaign_9
          ,MAX(NVL(a.campaign_10,0)) AS campaign_10
          ,MAX(NVL(b.campaign_11,0)) AS campaign_11
          ,MAX(NVL(b.campaign_12,0)) AS campaign_12
          ,MAX(NVL(b.campaign_13,0)) AS campaign_13
          ,MAX(NVL(b.campaign_14,0)) AS campaign_14
          ,MAX(NVL(b.campaign_15,0)) AS campaign_15
          ,MAX(NVL(b.campaign_16,0)) AS campaign_16
          ,MAX(NVL(b.campaign_17,0)) AS campaign_17
          ,MAX(NVL(b.campaign_18,0)) AS campaign_18
          ,MAX(NVL(b.campaign_19,0)) AS campaign_19
          ,MAX(NVL(b.campaign_20,0)) AS campaign_20
          ,SUM(a.impressions+b.impressions) AS impressions
          ,SUM(NVL(a.cost,0)) AS cost
          ,SUM(a.campaign_1_imps) AS campaign_1_imps
          ,SUM(a.campaign_2_imps) AS campaign_2_imps
          ,SUM(a.campaign_3_imps) AS campaign_3_imps
          ,SUM(a.campaign_4_imps) AS campaign_4_imps
          ,SUM(a.campaign_5_imps) AS campaign_5_imps
          ,SUM(a.campaign_6_imps) AS campaign_6_imps
          ,SUM(a.campaign_7_imps) AS campaign_7_imps
          ,SUM(a.campaign_8_imps) AS campaign_8_imps
          ,SUM(a.campaign_9_imps) AS campaign_9_imps
          ,SUM(a.campaign_10_imps) AS campaign_10_imps
          ,SUM(b.campaign_11_imps) AS campaign_11_imps
          ,SUM(b.campaign_12_imps) AS campaign_12_imps
          ,SUM(b.campaign_13_imps) AS campaign_13_imps
          ,SUM(b.campaign_14_imps) AS campaign_14_imps
          ,SUM(b.campaign_15_imps) AS campaign_15_imps
          ,SUM(b.campaign_16_imps) AS campaign_16_imps
          ,SUM(b.campaign_17_imps) AS campaign_17_imps
          ,SUM(b.campaign_18_imps) AS campaign_18_imps
          ,SUM(b.campaign_19_imps) AS campaign_19_imps
          ,SUM(b.campaign_20_imps) AS campaign_20_imps
          ,SUM(a.campaign_1_cost) AS campaign_1_cost
          ,SUM(a.campaign_2_cost) AS campaign_2_cost
          ,SUM(a.campaign_3_cost) AS campaign_3_cost
          ,SUM(a.campaign_4_cost) AS campaign_4_cost
          ,SUM(a.campaign_5_cost) AS campaign_5_cost
          ,SUM(a.campaign_6_cost) AS campaign_6_cost
          ,SUM(a.campaign_7_cost) AS campaign_7_cost
          ,SUM(a.campaign_8_cost) AS campaign_8_cost
          ,SUM(a.campaign_9_cost) AS campaign_9_cost
          ,SUM(a.campaign_10_cost) AS campaign_10_cost
          ,COUNT(*) devices
     FROM temp_amobee_media a
     FULL OUTER JOIN temp_external_media b
     ON a.household_id = b.household_id
     GROUP BY 1
     ) A
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21
