WITH warehouses_unique AS (
  SELECT
    global_entity_id
    , warehouse_id
    , warehouse_name
  FROM
    `fulfillment-dwh-production.cl_dmart.warehouses_unique`
  WHERE is_dmart
    AND warehouse_id IS NOT NULL
    AND (global_entity_id LIKE 'PY_%' OR global_entity_id = 'AP_PA')

), table_weekday AS (
  SELECT weekday
  FROM UNNEST(GENERATE_ARRAY(1, 7)) AS weekday
), table_hours AS (
  SELECT hour
  FROM UNNEST(GENERATE_ARRAY(0, 23)) AS hour
), all_combo AS (
  SELECT *
  FROM (
    SELECT global_entity_id
      , warehouse_id
      , warehouse_name
    FROM warehouses_unique
    GROUP BY 1, 2, 3
    )
  FULL JOIN table_weekday ON 1 = 1
  FULL JOIN table_hours ON 1 = 1
)

, weekday_country_wh_combo AS (  
    SELECT global_entity_id
      , warehouse_id
      , warehouse_name
      , weekday_str
    FROM all_combo
    , UNNEST(['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday']) AS weekday_str
    GROUP BY 1, 2, 3, 4
)
, store_transfer_main AS (
    SELECT DISTINCT st.country_code
    , st.global_entity_id
    , st.transfer_reference
    , st.dest_warehouse_id
    , st.dest_warehouse_name
    , st.is_multi_receiving
    FROM `fulfillment-dwh-production.cl_dmart.store_transfers` st
    WHERE st.country_code = "ni"
)
, sm_store_transfer AS (
    SELECT pst.reference
    , pst.id
    , pst.external_id
    , stm.dest_warehouse_id
    , stm.dest_warehouse_name
    , pst.created_at AS st_created_at
    , pst.country_code
    , stm.is_multi_receiving
    , stm.global_entity_id
    FROM `fulfillment-dwh-production.dl_dmart.store_transfer_store_transfer_transfer` pst
    JOIN store_transfer_main stm
        ON pst.reference = stm.transfer_reference
        AND pst.country_code = stm.country_code
    WHERE pst.country_code = "ni"
        AND DATE(pst.created_at) < CURRENT_DATE() - 0
        AND DATE(pst.created_at) >= CURRENT_DATE() - 0 - 35
)
, sm_store_transfer_status AS (
    SELECT A.external_id,
    A.transfer_id AS store_transfer_id,
    A.country_code,
    -- B.name AS status,
    MAX(CASE WHEN B.name = 'CREATED' THEN A.created_at END) AS CREATED_ts,
    MAX(CASE WHEN B.name = 'CANCELED' THEN A.created_at END) AS CANCELED_ts,
    -- MAX(CASE WHEN B.name = 'CANCELLATION' THEN A.created_at END) AS CANCELLATION_ts,
    MAX(CASE WHEN B.name = 'REJECTED' THEN A.created_at END) AS REJECTED_ts,
    -- MAX(CASE WHEN B.name = 'CONFIRMING' THEN A.created_at END) AS CONFIRMING_ts,
    MAX(CASE WHEN B.name = 'PENDING' THEN A.created_at END) AS PENDING_ts,
    MAX(CASE WHEN B.name = 'OUTBOUNDING' THEN A.created_at END) AS OUTBOUNDING_ts,
    MAX(CASE WHEN B.name = 'PARTIALLY_INBOUNDED' THEN A.created_at END) AS PARTIALLY_INBOUNDED_ts,
    MAX(CASE WHEN B.name = 'INBOUNDING' THEN A.created_at END) AS INBOUNDING_ts,
    MAX(CASE WHEN B.name = 'COMPLETED' THEN A.created_at END) AS COMPLETED_ts
    FROM `fulfillment-dwh-production.dl_dmart.store_transfer_store_transfer_transfer_status_history` AS A
    JOIN `fulfillment-dwh-production.dl_dmart.store_transfer_store_transfer_transfer_status` AS B
        ON A.country_code = B.country_code
        AND A.status_id = B.id
    WHERE A.country_code = "ni"
        AND DATE(A.created_at) < CURRENT_DATE() - 0
        AND DATE(A.created_at) >= CURRENT_DATE() - 0 - 35
    GROUP BY 1,2,3
)
, sm_store_transfer_receivingevents_cartupdated AS (
    SELECT stcu.transfer_id AS store_transfer_id
    , stcu.country_code
    , stcu.product_id
    , SUM(stcu.received_qty) AS received_qty
    , "ST_CartUpdated" AS Event
    , min(stcu.created_at) AS event_ts
    FROM `fulfillment-dwh-production.dl_dmart.store_transfer_store_transfer_cart_update` AS stcu
    --Take only those store_transfer where receiving started, updated and completed
    JOIN sm_store_transfer_status sss
        ON stcu.transfer_id = sss.store_transfer_id
        AND stcu.country_code = sss.country_code
        AND sss.INBOUNDING_ts IS NOT NULL
        AND stcu.created_at >= sss.INBOUNDING_ts
    JOIN sm_store_transfer_status sss2
        ON stcu.transfer_id = sss2.store_transfer_id
        AND stcu.country_code = sss2.country_code
        AND sss2.COMPLETED_ts IS NOT NULL
        AND sss2.COMPLETED_ts >= stcu.created_at
    WHERE stcu.country_code = "ni"
        AND DATE(stcu.created_at) < CURRENT_DATE() - 0
        AND DATE(stcu.created_at) >= CURRENT_DATE() - 0 - 35
    GROUP BY 1,2,3
)
, base_output_st AS (
    SELECT ib.store_transfer_id
    , ib.country_code
    , NULL AS product_id
    , "ST_Started" AS Event
    , 0 AS received_qty
    , min(ib.INBOUNDING_ts) AS event_ts
    FROM sm_store_transfer_status ib
    JOIN sm_store_transfer_receivingevents_cartupdated cu
        ON ib.store_transfer_id = cu.store_transfer_id
        AND ib.country_code = cu.country_code
        AND cu.event_ts >= ib.INBOUNDING_ts
    WHERE INBOUNDING_ts IS NOT NULL
    GROUP BY 1,2,3,4,5

    UNION ALL

    SELECT ct.store_transfer_id
    , ct.country_code
    , null product_id
    , "ST_Completed" AS Event
    , 0 AS received_qty
    , min(ct.COMPLETED_ts) AS event_ts
    FROM sm_store_transfer_status ct
    JOIN sm_store_transfer_receivingevents_cartupdated cu
        ON ct.store_transfer_id = cu.store_transfer_id
        AND ct.country_code = cu.country_code
        AND ct.COMPLETED_ts >= cu.event_ts
    WHERE ct.COMPLETED_ts IS NOT NULL
    GROUP BY 1,2,3,4,5

    UNION ALL

    SELECT store_transfer_id
    , country_code
    , product_id
    , Event
    , received_qty
    , event_ts
    FROM sm_store_transfer_receivingevents_cartupdated
    WHERE received_qty > 0 ---only items with at least 1 quantity
)

, st_receivings_complete AS (
    SELECT country_code
    , store_transfer_id
    , product_id
    , received_qty
    , Event
    , event_ts
    , LEAD(event) OVER (PARTITION BY store_transfer_id, country_code ORDER BY event_ts) AS next_event
    , lead(event_ts) OVER (PARTITION BY store_transfer_id, country_code ORDER BY event_ts) AS next_event_ts
    FROM base_output_st
)

, st_bulk_indi_receivings AS (
    SELECT country_code
    , store_transfer_id
    , product_id
    , received_qty
    , Event
    , event_ts
    , next_event
    , next_event_ts
    , CASE WHEN next_event = 'ST_Completed' THEN NULL ELSE TIMESTAMP_DIFF(next_event_ts, event_ts, millisecond)/1000 END AS st_receiving_time_s
    , IF(TIMESTAMP_DIFF(next_event_ts, event_ts, millisecond) = 0 AND next_event_ts IS NOT NULL, 1, 0) AS st_timediff_flag
    FROM st_receivings_complete
)
, test AS (
SELECT DISTINCT A.reference
    , A.external_id AS store_transfer_uuid
    , B.store_transfer_id
    , A.country_code
    , A.st_created_at
    , A.dest_warehouse_id
    , A.dest_warehouse_name
    , A.is_multi_receiving
    , A.global_entity_id
    , count(distinct product_id) OVER(partition by external_id) AS distinct_sku_new
    -- , mst.total_received_qty
    , B.product_id
    , B.received_qty
    , B.Event
    , B.event_ts
    , CAST(st_receiving_time_s AS int) AS receiving_sec
    FROM sm_store_transfer AS A
    INNER JOIN st_bulk_indi_receivings AS B
        ON A.id = B.store_transfer_id
        AND A.country_code = B.country_code
    WHERE event NOT LIKE 'ST_Completed'
)
, rec_time_per_sku_each_country AS (
    SELECT global_entity_id
    , APPROX_QUANTILES(receiving_sec, 100 IGNORE NULLS)[OFFSET(80)] AS receiving_time_per_sku_seconds_for_dc_store_transfer
    FROM test
    GROUP BY 1
)

SELECT wcwc.global_entity_id
, wcwc.warehouse_name
, wcwc.warehouse_id
, wcwc.weekday_str AS weekday
, receiving_time_per_sku_seconds_for_dc_store_transfer
FROM weekday_country_wh_combo AS wcwc
INNER JOIN rec_time_per_sku_each_country USING (global_entity_id)
