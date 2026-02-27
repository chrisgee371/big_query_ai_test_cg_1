{{
  config({
    "materialized": "ephemeral"
  })
}}

WITH sales_by_region AS (
  SELECT *
  FROM {{ ref('big_query_ai_test_cg_1__sales_by_region') }}
),

sorted_regions AS (
  SELECT *
  FROM sales_by_region
  ORDER BY total_sales DESC
),

top_regions AS (
  SELECT *
  FROM sorted_regions
  LIMIT 10
)

SELECT * FROM top_regions
