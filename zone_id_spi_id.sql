DECLARE naming_regex STRING DEFAULT r'^[Dd]{1}mart_([A-Za-z0-9\-]+)_';

WITH dmarts AS (
  SELECT
    country_code
    , warehouse_id
    , warehouse_name
    , platform_vendor_id as vendor_codes
  FROM
    `fulfillment-dwh-production.cl_dmart.warehouses_unique`,
    unnest(platform_vendor_id) as platform_vendor_id
  WHERE is_dmart
    AND warehouse_id IS NOT NULL
) 
, rooster_zones AS (
  SELECT region,
    RIGHT(c.country_code, 2) as country_code
    ,z.name AS rooster_name,
    -- Extract vendor code from zone_name.
    REGEXP_EXTRACT(z.name, naming_regex) AS vendor_code,
    sp.id AS sp_id,
    sp.zone_id
  FROM `fulfillment-dwh-production.curated_data_shared.countries` c
  LEFT JOIN UNNEST(cities) ci
  LEFT JOIN UNNEST(zones) z
  LEFT JOIN UNNEST(starting_points) sp
  WHERE
    STARTS_WITH(c.country_code, 'dp-')
    AND z.is_active
    AND REGEXP_CONTAINS(z.name, naming_regex)
    #AND REGION = 'Americas'
)
SELECT
  d.country_code
 # , d.warehouse_id
  , d.warehouse_name
  , rooster_name
  , sp_id
  , zone_id
FROM rooster_zones z
RIGHT JOIN dmarts d
  ON z.country_code = d.country_code
  AND z.vendor_code IN (d.vendor_codes)
where z.country_code = 'tr'
ORDER BY 1,2,3
