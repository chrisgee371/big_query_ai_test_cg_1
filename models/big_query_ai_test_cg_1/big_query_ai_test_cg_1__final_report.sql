{{
  config({
    "materialized": "table",
    "alias": "sales_report",
    "database": "prophecy-field",
    "schema": "chris_demos"
  })
}}

WITH top_regions AS (
  SELECT *
  FROM {{ ref('big_query_ai_test_cg_1__top_regions') }}
),

final_report AS (
  SELECT
    region_name,
    customer_count,
    order_count,
    total_sales,
    avg_order_value,
    CURRENT_TIMESTAMP() AS report_generated_at
  FROM top_regions
)

SELECT * FROM final_report
