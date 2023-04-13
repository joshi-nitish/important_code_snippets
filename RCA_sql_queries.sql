select * from `dh-darkstores-stg.dev_dmart.mhs_demand_stage` 
where run_id >= "2023-04-12_18-08"
and run_id <= "2023-04-12_18-08"
--------------------------------------------------------------
select warehouse_name ,avg(perc_overstaffing)
from dh-darkstores-stg.dev_dmart.mhs_sanity_check
where env_name = 'stg'
and run_id >= "2023-04-12_17-10"
and run_id <= "2023-04-12_17-15"
group by warehouse_name
order by avg(perc_overstaffing) desc
--------------------------------------------------------------
-- Compare tasks from two different runs
WITH after AS (
  select 
  shift_datetime,
  expiry_check_mhs as after_po
  from `dh-darkstores-stg.dev_dmart.mhs_demand_stage`
  where run_id >= "2023-04-12_15-20"
  and run_id <=  "2023-04-12_15-35"
  and warehouse_id = '1bb2d629-90fc-4083-a59e-33e844945a6e'
),
before AS (
  select 
  shift_datetime,
  expiry_check_mhs as before_po
  from `dh-darkstores-stg.dev_dmart.mhs_demand_stage`
  where run_id >= "2023-04-12_14-05"
  and run_id <=  "2023-04-12_14-15"
  and warehouse_id = '1bb2d629-90fc-4083-a59e-33e844945a6e'
)

SELECT DATE(b.shift_datetime),
sum(before_po) as before_po,
sum(after_po)as after_po
from after a
join before b using(shift_datetime)
group by DATE(b.shift_datetime)
--------------------------------------------------------------
-- Unique warehouses
SELECT
    global_entity_id
    , warehouse_id
    , warehouse_name
  FROM
    `fulfillment-dwh-production.cl_dmart.warehouses_unique`
  WHERE is_dmart
  and warehouse_id in UNNEST(['61d8d94f-90cc-4956-a7cf-251cf4328ac7'])
  --------------------------------------------------------------
