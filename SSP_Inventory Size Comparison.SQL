-- Compare Audience size across mutiple SSPs

SET market_id = 861;
SET contract_id = 1749648680,1749645693,1749645692,1749645770,1749645694,1749647184,1749647183,1749251681,1748961175,1748961174,1749635807,1748969765,1749975271,1749793270,1748958877,1749855576,1748958879,1748964981,1748955582; -- desire audience for this client

-- LR

WITH temp_lr AS
  (
    SELECT DISTINCT
           person_id
          ,tuid
      FROM household_graph_liveramp_latest
  )

--audience table
,temp_audience AS
  (
    SELECT CASE WHEN l.person_id IS NOT NULL THEN l.person_id
           ELSE c.user_id END AS audience_id
          ,c.contract_id
          ,c.contract_name
      FROM categories c
           LEFT JOIN
           temp_lr l
           ON c.user_id = l.tuid
     WHERE dates last_30_days
           AND market_id = ${market_id}
           AND contract_id in (${contract_id})
     GROUP BY 1,2,3
  )
---bids table
,temp_bids AS
  (
    SELECT CASE WHEN l.person_id is NOT NULL THEN l.person_id
                WHEN b.derived_user_id<>-1 THEN b.derived_user_id
                ELSE b.user_id END AS audience_id
          ,b.market_inventory_source_id
          ,b.channel_id
      FROM(
            SELECT user_id
                  ,derived_user_id
                  ,market_inventory_source_id
                  ,channel_id
              FROM bids
             WHERE dates last_30_days
             GROUP BY 1,2,3,4
          )b
           LEFT JOIN
           temp_lr l
           ON b.user_id = l.tuid
     GROUP BY 1,2,3
  )

  ,temp_total AS
  (
    SELECT contract_id
          ,COUNT(audience_id) AS total_audience
      FROM temp_audience
     GROUP BY 1
  )
--final table

SELECT temp_bids.market_inventory_source_id
      ,temp_audience.contract_id
      ,temp_audience.contract_name
      ,temp_bids.channel_id
      ,COUNT(DISTINCT temp_audience.audience_id) audience_size
      ,MAX(temp_total.total_audience) AS total_uu
  FROM temp_audience
       LEFT JOIN
       temp_bids
       ON temp_audience.audience_id = temp_bids.audience_id
       LEFT JOIN
       temp_total
       ON temp_audience.contract_id = temp_total.contract_id
 GROUp BY 1,2,3,4
