{{
  config({
    "materialized": "ephemeral"
  })
}}

WITH customers_filtered AS (
  SELECT *
  FROM {{ ref('big_query_ai_test_cg_1__customers_filtered') }}
),

orders_source AS (
  SELECT *
  FROM {{ source('chris_demos.demos', 'orders') }}
),

orders_filtered AS (
  SELECT *
  FROM orders_source
  WHERE order_amount >= {{ var('min_order_amount') }}
),

customer_orders_joined AS (
  SELECT *
  FROM customers_filtered AS in0
  INNER JOIN orders_filtered AS in1
    ON in0.customer_id = in1.customer_id
)

SELECT * FROM customer_orders_joined
