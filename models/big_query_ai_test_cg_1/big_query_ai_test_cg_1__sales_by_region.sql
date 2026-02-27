{{
  config({
    "materialized": "ephemeral"
  })
}}

WITH orders_with_region AS (
  SELECT *
  FROM {{ ref('big_query_ai_test_cg_1__orders_with_region') }}
),

sales_aggregated AS (
  SELECT
    region_name,
    COUNT(DISTINCT customer_id) AS customer_count,
    COUNT(order_id) AS order_count,
    SUM(order_amount) AS total_sales,
    AVG(order_amount) AS avg_order_value
  FROM orders_with_region
  GROUP BY region_name
)

SELECT * FROM sales_aggregated
