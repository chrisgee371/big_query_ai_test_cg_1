{{
  config({
    "materialized": "ephemeral"
  })
}}

WITH customer_orders AS (
  SELECT *
  FROM {{ ref('big_query_ai_test_cg_1__customer_orders') }}
),

regions_seed AS (
  SELECT *
  FROM {{ ref('regions') }}
),

orders_with_region_joined AS (
  SELECT *
  FROM customer_orders AS in0
  LEFT JOIN regions_seed AS in1
    ON in0.region_id = in1.region_id
)

SELECT * FROM orders_with_region_joined
